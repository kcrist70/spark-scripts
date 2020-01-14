import datetime
import time
import redis
import pymysql
from config import config
config = config('../example.ini')

try:
    pool79 = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'],
                                    port=config['redis-for-uv-pv']['port'],
                                    password=config['redis-for-uv-pv']['password'],
                                    decode_responses=config['redis-for-uv-pv']['decode_responses'])
    redis_connect79 = redis.Redis(connection_pool=pool79)
except Exception as e:
    raise e


def fetch_name_from_mysql(type_name):
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

def insert_trio_log(day_retain,num,nickname,type_name = None):
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    db = pymysql.connect(host=config['mysql']['ip'],
                         port=config['mysql']['port'],
                         user=config['mysql']['user'],
                         passwd=config['mysql']['password'],
                         database=config['mysql']['database'],
                         charset="utf8")
    cur = db.cursor()
    if type_name == "adult":
        sql = "INSERT INTO tb_log_table_retain_adult(dates,data,category,retainDate,botname,tag,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
        params = [day_retain, num, "new", day_retain, nickname, "retain", now]
        cur.execute(sql, params)
    elif type_name == "children":
        sql = "INSERT INTO tb_log_table_retain_children(dates,data,category,retainDate,botname,tag,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
        params = [day_retain, num, "new", day_retain, nickname, "retain", now]
        cur.execute(sql, params)
    elif type_name == "triobrain":
        sql = "INSERT INTO tb_log_table_retain_triobrain(dates,data,category,retainDate,botname,tag,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
        params = [day_retain, num, "new", day_retain, nickname, "retain", now]
        cur.execute(sql, params)
    db.commit()
    cur.close()
    db.close()

#50天数据
# for i in range(49):
#     day_retain = (datetime.datetime.now() - datetime.timedelta(days=1 + i)).strftime("%Y-%m-%d")
#     triobrain_botname_list = fetch_name_from_mysql("triobrain")
#     name_list = redis_connect79.keys("*:" + day_retain)
#     for name in name_list:
#         nickname = str(name.split(":")[0])
#         if nickname in triobrain_botname_list:
#             num = redis_connect79.scard(name)
#             insert_trio_log(day_retain, num, nickname, type_name="triobrain")

# 3天数据
for i in range(3):
    day_retain = (datetime.datetime.now() - datetime.timedelta(days=1 + i)).strftime("%Y-%m-%d")
    adult_botname_list = fetch_name_from_mysql("adult")
    children_botname_list = fetch_name_from_mysql("children")
    triobrain_botname_list = fetch_name_from_mysql("triobrain")
    name_list = redis_connect79.keys("*:" + day_retain)
    for name in name_list:
        nickname = str(name.split(":")[0])
        if nickname in adult_botname_list:
            num = redis_connect79.scard(name)
            insert_trio_log(day_retain, num, nickname, type_name="adult")
        elif nickname in children_botname_list:
            num = redis_connect79.scard(name)
            insert_trio_log(day_retain, num, nickname, type_name="children")
        elif nickname in triobrain_botname_list:
            num = redis_connect79.scard(name)
            insert_trio_log(day_retain, num, nickname, type_name="triobrain")
# #成人
# adult_botname_list = fetch_name_from_mysql("adult")
# day_retain = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
# adult_num = all_user_count(adult_botname_list,day_retain)
# if adult_num:
#     insert_trio_log(adult_num,day_retain,"adult")
# #儿童
# children_botname_list = fetch_name_from_mysql("children")
# day_retain = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
# chilren_num = all_user_count(children_botname_list,day_retain)
# if chilren_num:
#     insert_trio_log(chilren_num,day_retain,"children")
# #垂类
# triobrain_botname_list = fetch_name_from_mysql("triobrain")
#
# day_retain = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
# triobrain_num = all_user_count(triobrain_botname_list,day_retain)
# if triobrain_num:
#     insert_trio_log(triobrain_num,day_retain,"triobrain")
# #
#
# name_list = redis_connect79.keys("*:"+ day_retain)
# for name in name_list:
#     num = redis_connect79.scard(name)
#     name = name.decode("utf-8").split(":")[0]
#     if name in adult_botname_list:
#         insert_trio_log(num, day_retain, name, type_name="adult")
#     elif name in children_botname_list:
#         insert_trio_log(num, day_retain, name, type_name="children")
#
#     # if name in triobrain_botname_list:
#     #     # pass
#     #     insert_trio_log(num,day_retain,name,type_name="triobrain")
