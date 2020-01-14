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
    botname_col = json_col["bot_name"]
    user_id_col = json_col["user_id"]
    return botname_col, user_id_col


def sequence_pv_col(rdd):
    """
    :param rdd:  a list
    :return:   botname column
    """
    json_col = json.loads(rdd[3])
    botname_col = json_col["bot_name"]
    return botname_col


def sequence_qps_col(rdd):
    """
    :param rdd:  a list
    :return:   time_col,botname
    """
    json_col = json.loads(rdd[3])
    botname_col = json_col["bot_name"]
    time_col = rdd[0]
    return time_col, botname_col


def user_id_output_to_redis(rdd):
    """
    :param rdd:  [(botname,user_id),num]
    :return:   to redis set ,uv store
    """
    today = time.strftime("%Y-%m-%d", time.localtime())
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
    for bot_user, num in rdd:
        botname, user_id = bot_user
        redis_connect80.sadd(str(botname) + ":" + today, str(user_id))
        status = redis_connect79.sadd(str(botname), str(user_id))
        if status:
            redis_connect79.sadd(str(botname) + ":" + today, str(user_id))
            redis_connect79.sadd("botname_list", str(botname))


def pv_output_to_redis(rdd):
    """
    :param rdd:  botname, count
    :return:  to  redis hash store pv
    """
    try:
        pool79 = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'], port=config['redis-for-uv-pv']['port'],
                                  password=config['redis-for-uv-pv']['password'])
        redis_connect79 = redis.Redis(connection_pool=pool79)
    except Exception as e:
        raise e
    today = time.strftime("%Y-%m-%d", time.localtime())
    for botname, num in rdd:
        redis_connect79.hincrby("chatlog_pv", str(botname), num)
        redis_connect79.hincrby("chatlog_pv_per_day", str(botname) + ":" + today, num)


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
    for time_bot, num in rdd:
        time_col, botname_col = time_bot
        redis_connect81.zincrby(str(botname_col) + ":" + today, num, str(time_col))
        redis_connect81.zincrby(rdd_type + ":" + today, num, str(time_col))


def create_context(children_topic, brokers, children_qps_store_name):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print(
        "Creating new context-------------------------------------------------------------------------------------------------------------------------------")
    sc = SparkContext(appName="PythonStreamingRecoverableNetworkWordCount")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 1)
    line_children = KafkaUtils.createDirectStream(ssc, [children_topic, ],
                                                  kafkaParams={"metadata.broker.list": brokers})
    line_children.checkpoint(15)
    children_filter_data = line_children.map(lambda x: x[1].split("\t")).filter(data_filter)
    children_filter_data.cache()
    # children todayuv：redis80 totaluser：redis79 todayuser：redis79
    children_filter_data.map(sequence_user_id_col).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).foreachRDD(
        lambda rdd: rdd.foreachPartition(user_id_output_to_redis))
    # children 发送至redis 81，zset  botname + today ， sore 排序，计算 qps，topqps
    children_filter_data.map(sequence_pv_col).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).foreachRDD(
        lambda rdd: rdd.foreachPartition(pv_output_to_redis))
    # children 发送至redis79，botname 哈希，用于计算全部pv，当天pv等
    children_filter_data.map(sequence_qps_col).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).foreachRDD(
        lambda rdd: rdd.foreachPartition(lambda x: qps_output_to_redis(x, children_qps_store_name)))
    return ssc


if __name__ == '__main__':
    children_topic = "chatlog_children"
    children_qps_store_name = "children_qps_per_day"
    brokers = ','.join(config['kafka-broker'].values())
    checkpoint = "hdfs://triohdfs/user/yefei/sparkstreaming/chatlog/children/checkpoint"
    ssc = StreamingContext.getOrCreate(checkpoint, lambda: create_context(children_topic=children_topic, brokers=brokers,
                                                                         children_qps_store_name=children_qps_store_name))
    ssc.start()
    ssc.awaitTermination()
