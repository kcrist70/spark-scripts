import time
import json
import pymysql
import datetime
from config import config
config = config('../example.ini')

import redis

day_num=4
def fetch_name_from_mysql(type_name):
    """
    获取数据库中成人或垂类botname列表
    :param type_name:   triobrain
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


def day_0_insert():
    """
    生成前一天数据并存入mysql
    :return: null
    """
    day = (datetime.datetime.now() - datetime.timedelta(days=day_num)).strftime("%Y-%m-%d")
    today = time.strftime("%Y-%m-%d", time.localtime())
    triobrain_list = fetch_name_from_mysql("triobrain")
    try:
        pool = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'],
                                    port=config['redis-for-uv-pv']['port'],
                                    password=config['redis-for-uv-pv']['password'],
                                    decode_responses=config['redis-for-uv-pv']['decode_responses'])
        redis_connect = redis.Redis(connection_pool=pool)
    except Exception as e:
        raise e
    botname_list = redis_connect.keys("*:" + day)
    triobrain_dict = dict()
    if botname_list:
        for bot_name in botname_list:
            dict_key = str(bot_name.split(":")[0])
            if dict_key in triobrain_list:
                triobrain_dict[dict_key] = redis_connect.scard(bot_name)
    db = pymysql.connect(host=config['mysql']['ip'],
                         port=config['mysql']['port'],
                         user=config['mysql']['user'],
                         passwd=config['mysql']['password'],
                         database=config['mysql']['database'],
                         charset="utf8")
    cur = db.cursor()
    if triobrain_dict:
        for key_name in triobrain_dict:
            sql = "INSERT INTO tb_log_table_retain_triobrain(dates,data,category,retainDate,botname,tag,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
            params = [day, triobrain_dict[key_name], "new", day, key_name, "retain", today]
            cur.execute(sql, params)
        db.commit()
    cur.close()
    db.close()


day_0_insert()