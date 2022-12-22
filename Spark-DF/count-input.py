from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark= SparkSession.builder \
           .master("local[*]") \
           .appName("RDD_Count") \
           .config("spark.driver.binAddress","localhost") \
           .config("spark.ui.port","4042") \
           .getOrCreate()
    inputrdd = spark.sparkContext.textFile(r"C:\Users\Q\Desktop\inputfile.txt")
    print(inputrdd.collect())

    ##Aproach 1-- Count M/F

    # input_flatmap = inputrdd.flatMap(lambda x: x.split(","))
    # print(input_flatmap.collect())
    # input_filter = input_flatmap.filter(lambda x:x in ('M','F'))
    # print(input_filter.collect())
    # gendermaprdd= input_filter.map(lambda x:(x,1))
    # print(gendermaprdd.collect())
    # finalrdd = gendermaprdd.reduceByKey(lambda a,b:(a+b))
    # print( finalrdd.collect())


    ##Aproach 2-- Count M/F

    # finalrdd = inputrdd.map(lambda x:x.split(",")) \
    #           .map(lambda x:(x[4],1)) \
    #           .reduceByKey(lambda x,y:x+y)
    # print(finalrdd.collect())

    ##Aproach 3-- Count M/F

    # print(sum([1 for x in input_flatmap.collect() if x == 'M']),"Male")
    # print(sum([1 for x in input_flatmap.collect() if x == 'F']), "Female")

    ##Aproach 4-- Count M/F

    # finalrdd1= inputrdd.map(lambda x:x.split(",")) \
    #            .filter(lambda x:'M' in x[4])
    # finalrdd2= inputrdd.map(lambda x: x.split(",")) \
    #            .filter(lambda x: 'F' in x[4])
    # print("M:",finalrdd1.count())
    # print("F:",finalrdd2.count())

    ##Sum of Salary Per Dept

    # rdd1= inputrdd.map(lambda x: x.split(","))
    # rdd2= rdd1.map(lambda x:(x[5],int(x[6])))
    # finalrdd = rdd2.reduceByKey(lambda x,y:(x+y))
    # print(finalrdd.collect())

