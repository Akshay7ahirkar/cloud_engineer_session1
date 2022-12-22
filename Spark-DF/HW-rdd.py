from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("HWRDD").config("spark.driver.binAddress","localhost").config("spark.ui.port","4042").master("local[*]").getOrCreate()

    ##To add another table and give unique record and save file
    rdd1= spark.sparkContext.textFile(r"C:\Users\Q\Desktop\inputfile.txt")
    # print(rdd1.collect())
    rdd2= spark.sparkContext.textFile(r"C:\Users\Q\Desktop\inputfile1.txt")
    # print(rdd2.collect())
    Union_rdd= rdd1.union(rdd2)
    # print(Union_rdd.collect())
    distinct_rdd= Union_rdd.distinct()
    # print(distinct_rdd.collect())

    # distinct_rdd.saveAsTextFile(r"C:\Users\Q\Desktop\distinct")


    ##AVG Salary Per Dept

    rdd12= rdd1.map(lambda x:x.split(","))
    # print(rdd12.collect())
    A_rdd= rdd12.map(lambda x:(x[5],int(x[6])))
    # print(A_rdd.collect())
    B_rdd = A_rdd.groupByKey().mapValues(list)
    # print(B_rdd.collect())
    C_rdd = B_rdd.map(lambda x:(x[0],sum(x[1])/len(x[1])))
    # print(C_rdd.collect())


    ##emp details with 2nd highest salary

    emp11= rdd12.sortBy(lambda x:x[6],ascending=False)
    header = emp11.first()
    emp12= emp11.filter(lambda x:x != header )
    # print(emp12.take(1))

    # input("enter any word")
    # spark.stop()