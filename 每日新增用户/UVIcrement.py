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


def all_user_count(botname_list,day_retain):
    botname_list = botname_list
    bot_count = 0
    for unit_tuple in botname_list:
        num = redis_connect79.scard(str(unit_tuple) + ":" + day_retain)
        if num:
            bot_count += int(num)
    return bot_count


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

def insert_trio_log(num,day_retain,botname,type_name = None):
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    db = pymysql.connect(host=config['mysql']['ip'],
                         port=config['mysql']['port'],
                         user=config['mysql']['user'],
                         passwd=config['mysql']['password'],
                         database=config['mysql']['database'],
                         charset="utf8")
    cur = db.cursor()
    if botname == "adult" or botname == "children" or botname == "triobrain":
        sql = "INSERT INTO tb_log_chart_day_chat(title,seriesName, data, dates,tag,botname,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
        params = ["每日新增用户", "INC_UV", num, day_retain, "dayUserIncrement", botname, now]
        cur.execute(sql, params)
    elif type_name == "adult":
        sql = "INSERT INTO tb_log_chart_day_adult(title,seriesName,data,dates,tag,botname,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
        params = ["每日新增用户", "INC_UV", num, day_retain, "dayUserIncrement", botname, now]
        cur.execute(sql, params)
    elif type_name == "children":
        sql = "INSERT INTO tb_log_chart_day_children(title,seriesName,data,dates,tag,botname,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
        params = ["每日新增用户", "INC_UV", num, day_retain, "dayUserIncrement", botname, now]
        cur.execute(sql, params)
    elif type_name == "triobrain":
        sql = "INSERT INTO tb_log_chart_day_triobrain(title,seriesName,data,dates,tag,botname,insertTime)VALUES(%s,%s,%s,%s,%s,%s,%s)"
        params = ["每日新增用户", "INC_UV", num, day_retain, "dayUserIncrement", botname, now]
        cur.execute(sql, params)
    db.commit()
    cur.close()
    db.close()


def botname_user_count(botname_list,day_retain):
    botname_list = botname_list
    bot_count = 0
    for unit_tuple in botname_list:
        num = redis_connect79.scard(str(unit_tuple) + ":" + day_retain)
        bot_count += num
    return bot_count

day_retain = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

#成人
adult_botname_list = fetch_name_from_mysql("adult")
adult_num = all_user_count(adult_botname_list,day_retain)
if adult_num:
    insert_trio_log(adult_num,day_retain,"adult")


#儿童
children_botname_list = fetch_name_from_mysql("children")
chilren_num = all_user_count(children_botname_list,day_retain)
if chilren_num:
    insert_trio_log(chilren_num,day_retain,"children")


#垂类
triobrain_botname_list = fetch_name_from_mysql("triobrain")
triobrain_num = all_user_count(triobrain_botname_list,day_retain)
if triobrain_num:
    insert_trio_log(triobrain_num,day_retain,"triobrain")


name_list = redis_connect79.keys("*:"+ day_retain)
for name in name_list:
    num = redis_connect79.scard(name)
    nickname = name.decode("utf-8").split(":")[0]
    if nickname in adult_botname_list:
        insert_trio_log(num, day_retain, nickname, type_name="adult")
    elif nickname in children_botname_list:
        insert_trio_log(num, day_retain, nickname, type_name="children")
    elif nickname in triobrain_botname_list:
        insert_trio_log(num, day_retain, nickname, type_name="triobrain")

