from pyspark import SparkContext, SparkConf
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <input> <output>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)


    def printResult():
        sc.textFile(sys.argv[1]).map(lambda x: (x.split("\t")[0], 1)).reduceByKey(lambda a, b: a + b).map(
            lambda x: (x[1], x[0])).sortByKey().map(lambda x: (x[1], x[0])).saveAsTextFile(sys.argv[2])


    printResult()

    sc.stop()