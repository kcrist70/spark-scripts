import redis
import pymysql
from config import config
config = config('../example.ini')
"""
因插错redis表，而进行清除操作

"""


try:
    pool79 = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'],
                                    port=config['redis-for-uv-pv']['port'],
                                    password=config['redis-for-uv-pv']['password'])
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


def del_user_id(name_list,day_retain):
    key_list = redis_connect79.keys("*:" + day_retain)
    for name in key_list:
        nickname = name.decode("utf-8").split(":")[0]
        if nickname in name_list:
            num = redis_connect79.scard(name)
            user_ids = redis_connect79.spop(name, num)
            redis_connect79.srem(nickname,*user_ids)
# #儿童
children_botname_list = fetch_name_from_mysql("children")
day_retain = "2019-04-28"
del_user_id(children_botname_list,day_retain)

