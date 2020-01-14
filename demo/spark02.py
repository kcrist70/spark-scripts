from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    conf = SparkConf().setAppName("appName").setMaster("local[2]")
    sc = SparkContext(conf=conf)


    def my_map():
        data = [1,2,3,4,5]
        rdd1 = sc.parallelize(data)
        rdd2 = rdd1.map(lambda x:x*2)
        print(rdd2.collect())
        rdd2.foreach(lambda x:print(x))
    my_map()







    sc.stop()