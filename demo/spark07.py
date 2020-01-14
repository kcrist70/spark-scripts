from pyspark.sql import SparkSession

def basic(spark):
    df = spark.read.json("file:///home/yefei/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select(df["name"],df['age'] + 1).show()
    df.filter(df['age'] > 21).show()
    df.groupBy("age").count().show()
    df.createOrReplaceTempView("people")


if __name__ == '__main__':
    spark = SparkSession.builder.appName("spark07").enableHiveSupport().getOrCreate()

    basic(spark)




    spark.stop()