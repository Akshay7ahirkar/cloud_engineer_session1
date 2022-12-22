import findspark
findspark.init()
from pyspark.sql import SparkSession
if __name__ == '__main__':
    print("Hello Class! ")
    spark=SparkSession.builder.appName("Building RDD") \
        .config("spark.driver.binAddress","localhost") \
        .config("spark.ui.port","4041") \
        .master("local[*]") \
        .getOrCreate()
    ##Parallelize RDD
    data = [1,2,3,4,5,6,7,8,9]
    parallelizeRdd = spark.sparkContext.parallelize(data)
    print("Parallelize RDD:")
    print(parallelizeRdd.collect())
    print(parallelizeRdd.getNumPartitions())

    ##RDD with External File
    rdd1= spark.sparkContext.textFile(r"C:\Users\Q\Desktop\Mock Interviews Question.txt")
    print("RDD with External Files")
    print(rdd1.collect())

    ##RDD with Other RDD
    rdd2= rdd1.map(lambda x:(x,1))
    print("RDD with Other RDD")
    print(rdd2.collect())

    input("enter any word")
    spark.stop()