from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
if __name__ == '__main__':
    spark = SparkSession.builder.config("spark.driver.binAddress","localhost").config("spark.ui.port","4041").appName("dataframe").master("local[*]").getOrCreate()

     ##Creating Dataframe Using RDD
    input_data = [('Ram',100),('Shyam',200),('Savita',300)]
    inputrdd = spark.sparkContext.parallelize(input_data)

    # print(inputrdd.collect())

    inputdf = inputrdd.toDF(["first_name","Marks"])
    # inputdf.show()
    # inputdf.printSchema()


    # df1 = spark.createDataFrame(inputrdd).toDF(*["first_name","Marks"])
    # df1.show()
    # df1.printSchema()

   ## Create DF Using Given Text File with user defined schema

    # data = [(1,"Yash Patil",29,"M"),
    #         (2,"Ram Wagh",30,"M"),
    #         (3,"Sita Patil",29,"F")]

    # dataschema = StructType([StructField(name="id", dataType=IntegerType()),
    #                          StructField("name", StringType()),
    #                          StructField("age", IntegerType()),
    #                          StructField("Gender", StringType())])

    #
    #
    # inputdf1 = spark.createDataFrame(data=data,schema=dataschema)
    # inputdf1.printSchema()
    # inputdf1.show()

    ## Create DF Using Pre Defined Schema CSV File Without Header File

    # csvnoheader = spark.read.csv("C:\\Users\\Q\\PycharmProjects\\cloud_engineer_session1\\Dataframe_Intro\\input_withoutheader.csv")
    # csvnoheader.printSchema()
    # csvnoheader.show()

    ## Create DF Using Pre Defined Schema CSV File With Header

    # csvheader = spark.read.csv(
    #     "C:\\Users\\Q\\PycharmProjects\\cloud_engineer_session1\\Dataframe_Intro\\input_withheader.csv",inferSchema=True,header=True)
    # csvheader.printSchema()
    # csvheader.show()

    ## Create DF Using User Defined Schema CSV File With Header
    dataschema1 = StructType([StructField(name="id", dataType=IntegerType()),
                              StructField("Firstname", StringType()),
                              StructField("Lastname", StringType()),
                              StructField("age", IntegerType()),
                              StructField("Gender", StringType()),
                              StructField("Dept_id", IntegerType()),
                              StructField("Salary", LongType())])

    csvwithschema = spark.read.csv("C:\\Users\\Q\\PycharmProjects\\cloud_engineer_session1\\Dataframe_Intro\\input_withoutheader.csv",schema=dataschema1)
    # csvwithschema.printSchema()
    # csvwithschema.show()


    ## Create DF Using Row Function and User defined Schema
    # c1= StructType([StructField('Name',StringType()),
    #                 StructField('Profession',StringType()),
    #                 StructField('Location',StringType())])
    #
    # e = [Row("Max","Doctor","USA"),Row("Mike","Enterprenuer","UK")]
    # df2 = spark.createDataFrame(data=e,schema=c1)
    # df2.printSchema()
    # df2.show()

    ## Create DF Using Dictionary and User Defined Schema
    # student = [{"name":"akshay","age":25},{"name":"Madhuri","age":20},{"name":"Krutika","age":15}]
    # dict_df = spark.createDataFrame(student)
    # dict_df.printSchema()
    # dict_df.show()

    ##Write data in CSV Format
    # csvwithschema.write.csv(r"C:\Users\Q\Desktop\aksh",header=True)
    # csvwithschema.write.partitionBy("Gender").mode("append").csv(r"C:\Users\Q\Desktop\aksh", header=True)

    ##Write Data in Json Format
    # csvwithschema.write.json(r"C:\Users\Q\Desktop\aksh1", mode="overwrite")
    # csvwithschema.write.partitionBy("Gender").mode("append").json(r"C:\Users\Q\Desktop\aksh1")

    ##Write DF in ORC File Format
    # csvwithschema.write.orc(r"C:\Users\Q\Desktop\aksh2")

    ##Write DF in Parquet File Format
    # csvwithschema.write.parquet(r"C:\Users\Q\Desktop\aksh3")
    # csvwithschema.write.partitionBy("Gender").mode("append").parquet(r"C:\Users\Q\Desktop\aksh3")

    ##Read DF From Json File
    # jsondf = spark.read.json(["C:\\Users\\Q\\Desktop\\aksh1\\*","C:\\Users\\Q\\Desktop\\Gender=M\\*"])
    # jsondf = spark.read.json(r"C:\Users\Q\Desktop\aksh3\*")
    # jsondf.printSchema()
    # jsondf.show()

    ##Read DF From Parquet File
    # parquetdf = spark.read.parquet(r"C:\Users\Q\Desktop\aksh3\*")
    # parquetdf.printSchema()
    # parquetdf.show()

    ##Read DF From ORC File
    orcdf = spark.read.orc(r"C:\Users\Q\Desktop\aksh2\*")
    orcdf.printSchema()
    orcdf.show(n=3,truncate=25,vertical=True)


    ##Create Empty DF
    # rdd = spark.sparkContext.emptyRDD()
    # rdd_df = spark.sparkContext.parallelize([])
    # print(rdd.collect())
    # spark.createDataFrame(rdd_df,dataschema1).show()
    # spark.createDataFrame(rdd,dataschema1).show()
    # spark.createDataFrame([],dataschema1).show()










