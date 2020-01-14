# encoding=utf8
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext, dstream
from pyspark.streaming.kafka import KafkaUtils
import time
import json
import redis
from config import config
config = config('../example.ini')

"""
总pv  allpv  redis 6379    hash  chatlog_pv  botname
    总量pv 相关pv之和
当天pv  todaypv  redis 6379 hash chatlog_pv_per_day  botname:datetime
    相关hash botname：datetime 之和
总uv  alluser  redis 6379  set   botname	user_id
    相关 scard 之和
当天新增uv todayuser redis 6379 set  botname:datetime	
    相关set scard总量之和
当天uv todayuv  redis 6380 set  botname：datetime
    当天总量uv  相关uv之和
今日qps qps redis 6381 zset botname:datetime  num  datetime
children全量qps  redis 6381 zset  children_qps_per_day  num  datetime
adult 全量 qps   redis  6381 zset adult_qps_per_day
topqps  topqps redis 6381 zset 获取score 最高的值 
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
    date_col = rdd[0].split(" ")[0].strip()
    json_col = json.loads(rdd[3])
    botname_col = json_col["bot_name"]
    user_id_col = json_col["user_id"]
    return date_col,botname_col, user_id_col


def sequence_pv_col(rdd):
    """
    :param rdd:  a list
    :return:   (date_col,botname) column
    """
    date_col = rdd[0].split(" ")[0].strip()
    json_col = json.loads(rdd[3])
    botname_col = json_col["bot_name"]
    return date_col,botname_col


def sequence_qps_col(rdd):
    """
    :param rdd:  a list
    :return:   datetime_col,botname
    """
    json_col = json.loads(rdd[3])
    botname_col = json_col["bot_name"]
    datetime_col = rdd[0].strip()
    return datetime_col, botname_col


def user_id_output_to_redis(rdd):
    """
    :param rdd:  [(time_col,botname,user_id),num]
    :return:   to redis set ,uv store
    """
    try:
        pool79 = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'], port=config['redis-for-uv-pv']['port'],
                                  password=config['redis-for-uv-pv']['password'])
        redis_connect79 = redis.Redis(connection_pool=pool79)
    except Exception as e:
        raise e
    try:
        pool80 = redis.ConnectionPool(host=config['redis-day-uv']['ip'], port=config['redis-day-uv']['port'],
                                  password=config['redis-day-uv']['password'])
        redis_connect80 = redis.Redis(connection_pool=pool80)
    except Exception as e:
        raise e
    for bot_tuple, num in rdd:
        day, botname, user_id = bot_tuple
        redis_connect80.sadd(str(botname) + ":" + day, str(user_id))
        status = redis_connect79.sadd(str(botname), str(user_id))
        if status:
            redis_connect79.sadd(str(botname) + ":" + day, str(user_id))



def pv_output_to_redis(rdd):
    """
    :param rdd:  [(date_col,botname), count]
    :return:  to  redis hash store pv
    """
    try:
        pool79 = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'], port=config['redis-for-uv-pv']['port'],
                                  password=config['redis-for-uv-pv']['password'])
        redis_connect79 = redis.Redis(connection_pool=pool79)
    except Exception as e:
        raise e
    for bot_tuple, num in rdd:
        day, botname = bot_tuple
        with redis_connect79.pipeline(transaction=False) as p:
            p.hincrby("chatlog_pv", str(botname), num).hincrby("chatlog_pv_per_day", str(botname) + ":" + day, num)
            p.execute()


def qps_output_to_redis(rdd, rdd_type):
    """
    :param rdd:  botname, count
    :return:  to  redis hash store pv
    """
    try:
        pool81 = redis.ConnectionPool(host=config['redis-qps']['ip'], port=config['redis-qps']['port'],
                                  password=config['redis-qps']['password'])
        redis_connect81 = redis.Redis(connection_pool=pool81)
    except Exception as e:
        raise e
    today = time.strftime("%Y-%m-%d", time.localtime())
    for datetime_bot, num in rdd:
        datetime_col, botname_col = datetime_bot
        day = datetime_col.split(" ")[0].strip()
        with redis_connect81.pipeline(transaction=False) as p:
            p.zincrby(str(botname_col) + ":" + day, num, str(datetime_col)).zincrby(rdd_type + ":" + day, num, str(datetime_col))
            p.execute()


def main():
    triobrain_topic = "chatlog_triobrain"
    triobrain_qps_store_name = "triobrain_qps_per_day"
    brokers = ','.join(config['kafka-broker'].values())
    sc = SparkContext(appName="chatlog-triobrain-streaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)
    line_triobrain = KafkaUtils.createDirectStream(ssc, [triobrain_topic, ], kafkaParams={"metadata.broker.list": brokers})
    triobrain_filter_data = line_triobrain.map(lambda x: x[1].split("\t")).filter(data_filter)
    triobrain_filter_data.cache()
    # triobrain todayuv：redis80 totaluser：redis79 todayuser：redis79
    triobrain_filter_data.map(sequence_user_id_col).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).foreachRDD(
        lambda rdd: rdd.foreachPartition(user_id_output_to_redis))
    # triobrain 发送至redis79，botname 哈希，用于计算全部pv,当天pv等
    triobrain_filter_data.map(sequence_pv_col).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).foreachRDD(
        lambda rdd: rdd.foreachPartition(pv_output_to_redis))
    # triobrain 发送至redis 81，zset  botname + today ， sore 排序，计算 qps，topqps
    triobrain_filter_data.map(sequence_qps_col).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).foreachRDD(
        lambda rdd: rdd.foreachPartition(lambda x: qps_output_to_redis(x, triobrain_qps_store_name)))
    ssc.start()
    ssc.awaitTermination()



if __name__ == '__main__':
    main()
