# encoding=utf8
from pyspark import SparkContext, SparkConf
import time
import json
import pymysql
import datetime
from config import config
config = config('../example.ini')

def data_filter(rdd):
    """
    过滤非法字段，测试数据
    :param rdd: list
    :return: True or False
    """
    if len(rdd) >= 4:
        if rdd[1] != "REQ":
            return False
        try:
            json_col = json.loads(rdd[3])
        except Exception:
            return False
        if json_col.get("bot_name"):
            if json_col.get("user_id"):
                if "trio_test" in str(json_col.get("user_id")).lower():
                    return False
                if 'monitor' in str(json_col.get("user_id")).lower():
                    return False
            else:
                return False
        else:
            return False
    else:
        return False
    return True


def sequence_user_id_col(rdd):
    """
    只取两个字段
    :param rdd: a list
    :return: botname_col, user_id_col
    """
    json_col = json.loads(rdd[3])
    botname_col = json_col["bot_name"]
    user_id_col = json_col["user_id"]
    return botname_col, user_id_col


def fetch_name_from_mysql(type_name):
    """
    获取数据库中成人或儿童botname列表
    :param type_name:   adult or  children or triobrain
    :return:
    """
    db = pymysql.connect(host=config['mysql']['ip'],
                         port=config['mysql']['port'],
                         user=config['mysql']['user'],
                         passwd=config['mysql']['password'],
                         database=config['mysql']['database'],
                         charset="utf8")
    cur = db.cursor()
    sql = "select botname from tb_log_botname where type=%s"
    params = type_name
    cur.execute(sql, params)
    botnames = cur.fetchall()
    cur.close()
    db.close()
    botname_list = []
    for unit_tuple in botnames:
        botname_list.append(unit_tuple[0])
    return botname_list


def fetch_new_user_list_per_botname(day):
    """
    获取某一天新用户的botname及其user_id,生成字典返回
    :param day:  new user date
    :return:
    """
    try:
        pool = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'],
                                    port=config['redis-for-uv-pv']['port'],
                                    password=config['redis-for-uv-pv']['password'],
                                    decode_responses=config['redis-for-uv-pv']['decode_responses'])
        redis_connect = redis.Redis(connection_pool=pool)
    except Exception as e:
        raise e
    botname_list = redis_connect.keys("*:" + day)
    adult_list = fetch_name_from_mysql("adult")
    children_list = fetch_name_from_mysql("children")
    triobrain_list = fetch_name_from_mysql("triobrain")
    adult_dict = dict()
    children_dict = dict()
    triobrain_dict = dict()
    if botname_list:
        for bot_name in botname_list:
            dict_key = str(bot_name.split(":")[0])
            if dict_key in adult_list:
                adult_dict[dict_key] = redis_connect.smembers(bot_name)
            elif dict_key in children_list:
                children_dict[dict_key] = redis_connect.smembers(bot_name)
            elif dict_key in triobrain_list:
                triobrain_dict[dict_key] = redis_connect.smembers(bot_name)
    return adult_dict, children_dict, triobrain_dict


def filter_user_id(rdd, botname_dict):
    """
    过滤rdd中不属于某天botname累计新用户中user_id。
    :param rdd:    rdd
    :param botname_dict:  new user dict
    :return:
    """
    name, user_id = rdd
    if str(name) not in botname_dict:
        return False
    if str(user_id) not in botname_dict[name]:
        return False
    return True


def count_retain(rdd, category, day_retain, log_type):
    """
    将算出的结果发送至mysql存储起来
    :param rdd:
    :param category:  基于累计新用户的日期存储
    :param day_retain:  首次日期
    :param log_type:    儿童还是成人
    :return:
    """
    yestorday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    today = time.strftime("%Y-%m-%d", time.localtime())
    db = pymysql.connect(host=config['mysql']['ip'],
                         port=config['mysql']['port'],
                         user=config['mysql']['user'],
                         passwd=config['mysql']['password'],
                         database=config['mysql']['database'],
                         charset="utf8")
    cur = db.cursor()
    for name, num in rdd:
        if log_type == "adult":
            sql = "INSERT INTO tb_log_table_retain_adult(dates,data,category,retainDate,botname,tag,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
            params = [day_retain, num, category, yestorday, str(name), "retain", today]
            cur.execute(sql, params)
        elif log_type == "children":
            sql = "INSERT INTO tb_log_table_retain_children(dates,data,category,retainDate,botname,tag,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
            params = [day_retain, num, category, yestorday, str(name), "retain", today]
            cur.execute(sql, params)
        elif log_type == "triobrain":
            sql = "INSERT INTO tb_log_table_retain_triobrain(dates,data,category,retainDate,botname,tag,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
            params = [day_retain, num, category, yestorday, str(name), "retain", today]
            cur.execute(sql, params)
    db.commit()
    cur.close()
    db.close()


def day_0_insert():
    """
    生成前一天数据并存入mysql
    :return: null
    """
    yestorday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    today = time.strftime("%Y-%m-%d", time.localtime())
    adult_list = fetch_name_from_mysql("adult")
    children_list = fetch_name_from_mysql("children")
    triobrain_list = fetch_name_from_mysql("triobrain")
    try:
        pool = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'],
                                    port=config['redis-for-uv-pv']['port'],
                                    password=config['redis-for-uv-pv']['password'],
                                    decode_responses=config['redis-for-uv-pv']['decode_responses'])
        redis_connect = redis.Redis(connection_pool=pool)
    except Exception as e:
        raise e
    botname_list = redis_connect.keys("*:" + yestorday)
    adult_dict = dict()
    children_dict = dict()
    triobrain_dict = dict()
    if botname_list:
        for bot_name in botname_list:
            dict_key = str(bot_name.split(":")[0])
            if dict_key in adult_list:
                adult_dict[dict_key] = redis_connect.scard(bot_name)
            elif dict_key in children_list:
                children_dict[dict_key] = redis_connect.scard(bot_name)
            elif dict_key in triobrain_list:
                triobrain_dict[dict_key] = redis_connect.scard(bot_name)
    db = pymysql.connect(host=config['mysql']['ip'],
                         port=config['mysql']['port'],
                         user=config['mysql']['user'],
                         passwd=config['mysql']['password'],
                         database=config['mysql']['database'],
                         charset="utf8")
    cur = db.cursor()
    if adult_dict:
        for key_name in adult_dict:
            sql = "INSERT INTO tb_log_table_retain_adult(dates,data,category,retainDate,botname,tag,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
            params = [yestorday, adult_dict[key_name], "new", yestorday, key_name, "retain", today]
            cur.execute(sql, params)
        db.commit()
    if children_dict:
        for key_name in children_dict:
            sql = "INSERT INTO tb_log_table_retain_children(dates,data,category,retainDate,botname,tag,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
            params = [yestorday, children_dict[key_name], "new", yestorday, key_name, "retain", today]
            cur.execute(sql, params)
        db.commit()
    if triobrain_dict:
        for key_name in triobrain_dict:
            sql = "INSERT INTO tb_log_table_retain_triobrain(dates,data,category,retainDate,botname,tag,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
            params = [yestorday, triobrain_dict[key_name], "new", yestorday, key_name, "retain", today]
            cur.execute(sql, params)
        db.commit()
    cur.close()
    db.close()


def start_spark(file_list):
    """
    原始数据去重，并缓存
    :param file_list: file path
    :return:  scc
    """
    data = sc.textFile(file_list)
    spark_rdd = data.map(lambda x: x.split("\t")).filter(data_filter).map(sequence_user_id_col).distinct()
    spark_rdd.cache()
    return spark_rdd


def task_spark(handled_data, botname_dict, category, date_of_new, log_type):
    """
    处理数据
    :param handled_data:  spark sc
    :param botname_dict:     new user dict
    :param category:       留存类型: new，day，week
    :param date_of_new:    new user date
    :param log_type:        adult or children
    :return: null
    """
    if botname_dict:
        handled_data.filter(lambda x: filter_user_id(x, botname_dict)).map(lambda x: (x[0], 1)). \
            reduceByKey(lambda a, b: a + b).foreachPartition(lambda x: count_retain(x, category, date_of_new, log_type))


def del_redis_out_of_expect_data():
    """
    删除9周后的数据
    :return:
    """
    day = (datetime.datetime.now() - datetime.timedelta(days=1 + 7 * 9)).strftime("%Y-%m-%d")
    try:
        pool = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'],
                                    port=config['redis-for-uv-pv']['port'],
                                    password=config['redis-for-uv-pv']['password'])
        redis_connect = redis.Redis(connection_pool=pool)
    except Exception as e:
        raise e
    botname_list = redis_connect.keys("*:" + day)
    if botname_list:
        redis_connect.delete(*botname_list)


def main():
    # 插入0日数据
    day_0_insert()
    del_redis_out_of_expect_data()
    yestorday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    # 儿童
    children_path = "hdfs://triohdfs/data/log/chatlog/home/wanglinxiang/chatlog_whole/childrendata_%s*" % yestorday
    handled_childrendata = start_spark(children_path)
    # 成人
    adult_path = "hdfs://triohdfs/data/log/chatlog/home/wanglinxiang/chatlog_whole/adultdata_%s*" % yestorday
    handled_adultdata = start_spark(adult_path)
    # 垂类
    triobrain_path = "hdfs://triohdfs/data/log/chatlog/triobrainlog/%s/triobrainlog_%s*" % (yestorday[:-2], yestorday)
    handled_triobrandata = start_spark(triobrain_path)
    # 次日到7天的存留量
    for i in range(1, 8):
        day_retain = (datetime.datetime.now() - datetime.timedelta(days=1 + i)).strftime("%Y-%m-%d")
        adult_day_dict, children_day_dict, triobrain_day_dict = fetch_new_user_list_per_botname(day_retain)
        task_spark(handled_data=handled_adultdata, botname_dict=adult_day_dict, category="day",
                   date_of_new=day_retain, log_type="adult")
        task_spark(handled_data=handled_childrendata, botname_dict=children_day_dict, category="day",
                   date_of_new=day_retain, log_type="children")
        task_spark(handled_data=handled_triobrandata, botname_dict=triobrain_day_dict, category="day",
                   date_of_new=day_retain, log_type="triobrain")
    # 14日
    day_14_retain = (datetime.datetime.now() - datetime.timedelta(days=1 + 7 * 2)).strftime("%Y-%m-%d")
    adult_14_day_dict, children_14_day_dict, triobrain_14_day_dict = fetch_new_user_list_per_botname(day_14_retain)
    task_spark(handled_data=handled_adultdata, botname_dict=adult_14_day_dict, category="day",
               date_of_new=day_14_retain, log_type="adult")
    task_spark(handled_data=handled_childrendata, botname_dict=children_14_day_dict, category="day",
               date_of_new=day_14_retain, log_type="children")
    task_spark(handled_data=handled_triobrandata, botname_dict=triobrain_14_day_dict, category="day",
               date_of_new=day_14_retain, log_type="triobrain")
    # 30日
    day_30_retain = (datetime.datetime.now() - datetime.timedelta(days=1 + 30)).strftime("%Y-%m-%d")
    adult_30_day_dict, children_30_day_dict, triobrain_30_day_dict = fetch_new_user_list_per_botname(day_30_retain)
    task_spark(handled_data=handled_adultdata, botname_dict=adult_30_day_dict, category="day",
               date_of_new=day_30_retain, log_type="adult")
    task_spark(handled_data=handled_childrendata, botname_dict=children_30_day_dict, category="day",
               date_of_new=day_30_retain, log_type="children")
    task_spark(handled_data=handled_triobrandata, botname_dict=triobrain_30_day_dict, category="day",
               date_of_new=day_30_retain, log_type="triobrain")
    # 成人周留存
    adult_week_list = []
    for i in range(1, 8):
        day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y%m%d")
        filt_path = "hdfs://triohdfs/data/log/chatlog/home/wanglinxiang/chatlog_whole/adultdata_%s*" % day
        adult_week_list.append(filt_path)
    adult_week_path = ",".join(adult_week_list)
    handled_week_adultdata = start_spark(adult_week_path)
    # 儿童周留存
    children_week_list = []
    for i in range(1, 8):
        day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y%m%d")
        filt_path = "hdfs://triohdfs/data/log/chatlog/home/wanglinxiang/chatlog_whole/childrendata_%s*" % day
        children_week_list.append(filt_path)
    children_week_path = ",".join(children_week_list)
    handled_week_childrendata = start_spark(children_week_path)
    # 垂类周留存
    triobrain_week_list = []
    for i in range(1, 8):
        day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y%m%d")
        filt_path = "hdfs://triohdfs/data/log/chatlog/triobrainlog/%s/triobrainlog_%s*" % (day[:-2], day)
        triobrain_week_list.append(filt_path)
    triobrain_week_path = ",".join(triobrain_week_list)
    handled_week_triobraindata = start_spark(triobrain_week_path)
    # 1周到7周留存量
    for i in range(1, 8):
        week_retain = (datetime.datetime.now() - datetime.timedelta(days=1 + 7 * i)).strftime("%Y-%m-%d")
        adult_week_dict, children_week_dict, triobrain_week_dict = fetch_new_user_list_per_botname(week_retain)
        task_spark(handled_data=handled_week_adultdata, botname_dict=adult_week_dict, category="week",
                   date_of_new=week_retain, log_type="adult")
        task_spark(handled_data=handled_week_childrendata, botname_dict=children_week_dict, category="week",
                   date_of_new=week_retain, log_type="children")
        task_spark(handled_data=handled_week_triobraindata, botname_dict=triobrain_week_dict, category="week",
                   date_of_new=week_retain, log_type="triobrain")


if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sc.addPyFile("hdfs://triohdfs/user/yefei/redis.zip")
    import redis

    main()
    sc.stop()
