# encoding=utf8
from pyspark import SparkContext, SparkConf
import sys
import time
import json
from config import config
config = config('../example.ini')
"""
计算总量uv，pv
"""


def data_filter(rdd):
    """
    :param rdd: list
    :return: True or False
    """
    if len(rdd) >= 4:
        if rdd[1] != "REQ":
            return False
        try:
            json_col = json.loads(rdd[3])
            time.strptime(rdd[0].strip(), "%Y-%m-%d %H:%M:%S")
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
    :param rdd: a list
    :return: botname_col, user_id_col
    """
    json_col = json.loads(rdd[3])
    botname_col = json_col["bot_name"].strip()
    user_id_col = json_col["user_id"].strip()
    return botname_col, user_id_col



def sequence_pv_col(rdd):
    """
    :param rdd:  a list
    :return:   botname column
    """
    json_col = json.loads(rdd[3])
    botname_col = json_col["bot_name"].strip()
    return botname_col


def user_id_output_to_redis(iter):
    """
    :param iter:  [(botname,user_id),num]
    :return:   to redis set ,uv store
    """
    # day = time.strftime("%Y-%m-%d", time.localtime())

    try:
        pool = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'],
                                    port=config['redis-for-uv-pv']['port'],
                                    password=config['redis-for-uv-pv']['password'])
        redis_connect = redis.Redis(connection_pool=pool)
    except Exception as e:
        raise e
    for bot_user, num in iter:
        botname, user_id = bot_user
        redis_connect.sadd(str(botname), str(user_id))



def pv_output_to_redis(iter):
    """
    :param iter:  botname, count
    :return:  to  redis hash store pv
    """
    try:
        pool = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'],
                                    port=config['redis-for-uv-pv']['port'],
                                    password=config['redis-for-uv-pv']['password'])
        redis_connect = redis.Redis(connection_pool=pool)
    except Exception as e:
        raise e
    for botname, num in iter:
        redis_connect.hincrby("chatlog_pv", str(botname), num)




def main():
    filter_data = sc.textFile(sys.argv[1]).\
        map(lambda x: x.split("\t")).\
        filter(data_filter)
    filter_data.cache()
    filter_data.map(sequence_user_id_col).\
        map(lambda x: (x, 1)).\
        reduceByKey(lambda a, b: a + b).\
        foreachPartition(user_id_output_to_redis)
    filter_data.map(sequence_pv_col).\
        map(lambda x: (x, 1)).\
        reduceByKey(lambda a, b: a + b).\
        foreachPartition(pv_output_to_redis)



if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: chatlog.py <file_path> datetime", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sc.addPyFile("hdfs://triohdfs/user/yefei/redis.zip")
    import redis
    main()

    sc.stop()

