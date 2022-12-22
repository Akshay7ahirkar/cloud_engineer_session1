from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("accumulator").config("spark.driver.binAddress","localhost").config("spark.ui.port","4042").master("local[*]").getOrCreate()

    accvar = spark.sparkContext.accumulator(1)
    rdd = spark.sparkContext.parallelize([1, 2, 3])


    def func(x):
        accvar.add(x)


    rdd.foreach(func)
    print("acc Value:" + str(accvar))

    input("enter any word")
    spark.stop()