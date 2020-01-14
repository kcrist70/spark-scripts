from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def out_put():
    pass

sc = SparkContext(appName="sparkstreaming")
ssc = StreamingContext(sc,1)

lines = ssc.socketTextStream("localhost",9999)

words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word,1))
wordCounts = pairs.reduceByKey(lambda x,y: x + y)
wordCounts.foreachRDD(lambda rdd:rdd.foreachPartition(out_put))
wordCounts.pprint()

ssc.start()

ssc.awaitTermination()


 