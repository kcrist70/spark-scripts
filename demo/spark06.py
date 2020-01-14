from pyspark import SparkContext, SparkConf
import sys

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <input> <output>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)


    def printResult():
        rdd1 = sc.textFile(sys.argv[1]).map(lambda x:x.strip()).map(lambda x:x.split(" ")).filter(lambda x:len(x) == 2).map(lambda x:x[1])
        rdd1.persist()
        totalage = rdd1.map(lambda x:int(x)).reduce(lambda a,b: a + b)
        counts = rdd1.count()
        avgage = totalage/counts

        print(counts)
        print(totalage)
        print(avgage)


    printResult()

    sc.stop()