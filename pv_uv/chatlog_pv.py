from pyspark import SparkContext, SparkConf
import sys
import redis
from config import config
config = config('../example.ini')

def out_put(iter):
    try:
        pool = redis.ConnectionPool(host=config['redis-for-uv-pv']['ip'],
                                    port=config['redis-for-uv-pv']['port'],
                                    password=config['redis-for-uv-pv']['password'])
        redis_connect = redis.Redis(connection_pool=pool)
    except Exception as e:
        raise e
    for bot_name, num in iter:
        redis_connect.hset("chatlog_history_pv", bot_name, num)


def main():
    counts = sc.textFile(sys.argv[1]).map(lambda x: x.split("\t")[1]).map(lambda x: (x, 1)).reduceByKey(
        lambda a, b: a + b)
    counts.foreachPartition(out_put)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <input>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    conf.set("spark.dynamicAllocation.maxExecutors", 20)
    sc = SparkContext(conf=conf)
    # sc.addPyFile("hdfs://triohdfs/user/yefei/redis.zip")

    main()

    sc.stop()
