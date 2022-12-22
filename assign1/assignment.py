from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':


    spark = SparkSession.builder.appName('oracle_connection') \
        .config('spark.jars', r"C:\Spark\Spark\spark-3.1.3-bin-hadoop3.2\spark-3.1.3-bin-hadoop3.2\jars\ojdbc8.jar") \
        .getOrCreate()
    spark = SparkSession.builder.appName('oracle_spark_connection').getOrCreate()
    driver = 'oracle.jdbc.driver.OracleDriver'
    url = 'jdbc:oracle:thin:@localhost:1521/XE'
    user = 'sys as SYSDBA'
    password = 'sys'
    table1 = 'dataor'

    cjDF1 = spark.read.format('jdbc') \
        .option('driver', driver) \
        .option('url', url) \
        .option('user', user) \
        .option('dbtable', table1) \
        .option('password', password).load()
    # cjDF1.show(5500)
    njstate = cjDF1.filter("STATE ='NJ' AND  TABLE_NUMBER='85.(LC)'")
    # njstate.show()



    njstate.createOrReplaceTempView("njstate")
    # spark.sql("select sum(factor) from njstate").show()

    sldf = njstate.withColumn("KEY1",lpad(col("KEY1"),4,'0'))
    # sldf.show(5000)

    rpsldf= sldf.withColumnRenamed("KEY1","Classcode").withColumnRenamed("KEY2","Coverage").withColumnRenamed("KEY3","Symbol").withColumnRenamed("KEY4","Construction_code")
    #rpsldf.show(5000)

    chr = rpsldf.withColumn("Symbol",explode_outer(split(col("symbol"),"&")))
    # chr.show(5000)

    chr1 = chr.withColumn("Construction_code", explode_outer(split(col("Construction_code"), "or")))

    # chr1.show(5000)

    # chr1.createOrReplaceTempView("chr1")
    # spark.sql("select count(factor) from chr1").show()


    df2 = chr1.withColumn('ID',monotonically_increasing_id()+1001)
    # df2.show(1500)

    Finaldf= df2.select('ID','COUNTRY','STATE','TABLE_NUMBER','COMMONSTATE','EFFECTIVE_DATE','EXPIRATION_DATE','Classcode','Coverage','Symbol','Construction_code','FACTOR')
    # Finaldf.show(1500)

    # df0= spark.read.format("avro").load(r"C:\Users\Q\Downloads\twitter.avro")
    # Finaldf.write.option("header",True).option("multiline","True").mode("overwrite").option("delimiter",",").option("nullValue","null").json(r"C:\Users\Q\Desktop\ak6")