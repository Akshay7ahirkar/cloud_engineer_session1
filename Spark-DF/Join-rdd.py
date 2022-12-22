from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("joinRDD").config("spark.driver.binAddress","localhost").config("spark.ui.port","4041").master("local[*]").getOrCreate()
    rdd1= spark.sparkContext.textFile(r"C:\Users\Q\Desktop\inputfile.txt")
    rdd12 = rdd1.map(lambda x:x.split(",")).map(lambda x:(x[5],x))
    # print(rdd12.collect())

    rdd2 = spark.sparkContext.textFile(r"C:\Users\Q\Desktop\deptemp.txt")
    header = rdd2.first()
    rdd2 = rdd2.filter(lambda x : x != header)
    rdd22 = rdd2.map(lambda x: x.split(",")).map(lambda x:(x[0],x[1]))
    # print(rdd22.collect())

    joinrdd = rdd12.join(rdd22).cache()
    # print(joinrdd.collect())


    def replacefun(element):
        src= element[0]
        src[5]=element[1]
        return src
    finalrdd = joinrdd.map(lambda x:x[1]).map(replacefun)
    print(finalrdd.collect())

    # finalrdd.saveAsTextFile(r"C:\Users\Q\PycharmProjects\cloud_engineer_session1\Spark-DF\output\rddassignment1")


    input("enter any word")
    spark.stop()