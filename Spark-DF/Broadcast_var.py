from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("broadcast_variable").config("spark.driver.binAddress","localhost").config("spark.ui.port","4042").master("local[*]").getOrCreate()

    states = {"MH":"MAHARASHTRA", "MP":"MADHYAPRADESH","GJ":"GUJRAT"}

    RDD = spark.sparkContext.textFile(r"C:\Users\Q\PycharmProjects\cloud_engineer_session1\Spark-DF\emp_data.txt")
    broadcaststate = spark.sparkContext.broadcast(states)

    def changestateval(x):
        xsplit = x.split(',')
        testlist = []
        for i in xsplit:
            try:
                i = broadcaststate.value[i]
            except:
                pass
            testlist.append(i)
        return','.join(testlist)
    print(RDD.map(lambda x:changestateval(x)).collect())

    input("enter any word")
    spark.stop()