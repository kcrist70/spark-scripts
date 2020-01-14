import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("useage: 10spark.py <filepath>")
        exit(-1)

    sc = SparkContext()
    ssc = StreamingContext(sc,5)

    lines = ssc.textFileStream(sys.argv[1])
    counts = lines.flatMap(lambda l: l.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    counts.pprint()

    ssc.start()

    ssc.awaitTermination()
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = (SparkConf()
        .setAppName("My app")
        .set("spark.executor.memory", "1g").set("mapred.input.dir.recursive",True).set("hive.mapred.supports.subdirectories",True))
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)
my_dataframe = sqlContext.sql('Select * from chat_tmp.tb_phone_intent where filename="trio_phone_intent.2019-05-29" and domain IN ("Resource","NLP") limit 1')
my_dataframe.show() 
"""

"""
SET mapred.input.dir.recursive=true; SET hive.mapred.supports.subdirectories=true;
export HADOOP_USER_NAME=phone
"""