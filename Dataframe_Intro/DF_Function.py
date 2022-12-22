from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.config("spark.driver.binAddress", "localhost").config("spark.ui.port", "4050").appName(
        "dataframe").master("local[*]").getOrCreate()

    dataschema1 = StructType([StructField(name="id", dataType=IntegerType()),
                              StructField("Firstname", StringType()),
                              StructField("Lastname", StringType()),
                              StructField("age", IntegerType()),
                              StructField("Gender", StringType()),
                              StructField("Dept_id", IntegerType()),
                              StructField("Salary", LongType())])

    csvwithschema = spark.read.csv(
        "C:\\Users\\Q\\PycharmProjects\\cloud_engineer_session1\\Dataframe_Intro\\input_withoutheader.csv",
        schema=dataschema1)
    # csvwithschema.printSchema()

    # Select Function


    csvwithschema.printSchema()
    # csvwithschema.show()
    # csvwithschema.cache()
    # csvwithschema.select(csvwithschema.id).show()
    # csvwithschema.select(csvwithschema.id,csvwithschema.age.alias("emp_age")).show()
    # csvwithschema.select(['id','age']).show()
    # csvwithschema.select(col("id"),col("age")).show()
    # csvwithschema.select(csvwithschema.columns[:3]).show()

    ##Select With Nested Data
    # nesteddf = spark.read.format("json").load(r"C:\Users\Q\PycharmProjects\cloud_engineer_session1\Dataframe_Intro\nesteddata.json")
    # nesteddf.printSchema()
    # nesteddf.show()
    # nesteddf.select("Address.*").show()
    # nesteddf.select((["Address.City","Address.State"])).show()

    # print(nesteddf.columns)


    ## WithColumn
    ##Existing Col value change
    # csvwithschema.withColumn("salary",col("salary")*10).show()
    # csvwithschema.withColumn("bonus-salary", col("salary") * 10).show()

    ##Change datatype of Exist Col
    # csvwithschema.withColumn("salary",col("salary").cast("string")).printSchema()

    ##Adding new column
    # csvwithschema.withColumn("state",lit("MH")).withColumn("country",lit("India")).show()

    ##withColumnRenamed

    # csvwithschema.withColumnRenamed("id","identifier").show()

    ##Filter
    # csvwithschema.filter(col("Gender")=="M").show()
    # csvwithschema.where((col("Gender")=="M") & (col("Salary")>="25000")).show()

    ##Distinct
    # csvwithschema.distinct().show()
    # dup= spark.read.csv(r"C:\Users\Q\PycharmProjects\cloud_engineer_session1\Dataframe_Intro\dupl.csv")
    # dup.show()
    # dup.distinct().show()

    ##Drop Duplicate ,Drop
    data = [Row(name="Ajay",Age=20),
            Row(name='Satish',Age=25),
            Row(name="Ajay",Age=20),
            Row(name="Ajay",Age=30)]
    # df1= spark.sparkContext.parallelize(data).toDF()
    # df1.show()
    # df1.printSchema()
    # df1.distinct().show()

    # df1.dropDuplicates(['name']).show()

    # df1.drop('age').show()

    ##Groupby() --Aggregate Function

    # csvwithschema.groupby("dept_id").avg("salary").show()
    # csvwithschema.groupby("dept_id").sum("salary").show()
    # csvwithschema.groupBy("dept_id").count().show()
    # csvwithschema.groupby("dept_id").max("salary").show()
    # csvwithschema.groupby("dept_id").mean("salary").show()
    #  csvwithschema.groupby("dept_id").agg(sum("salary").alias("sum_sal")).show()

    data1 = [Row(dept_no=11, dept_name='HR'),
             Row(dept_no=12, dept_name='ADMIN'),
             Row(dept_no=15, dept_name='IT')]
    deptdf = spark.sparkContext.parallelize(data1).toDF()
    # deptdf.printSchema()
    # deptdf.show()

    #Inner Join
    # csvwithschema.join(deptdf,on=csvwithschema.Dept_id == deptdf.dept_no,how='inner').select("id","Firstname","Lastname","age","Gender","Salary","dept_name").show()

    # cond= [csvwithschema.Dept_id == deptdf.dept_no]
    # csvwithschema.join(deptdf,cond,'inner').show()

    #Full Outer
    # csvwithschema.join(deptdf, cond, 'fullouter').show()

    #Left Join
    # csvwithschema.join(deptdf, cond, 'leftouter').show()

    # Right Join
    # csvwithschema.join(deptdf, cond, 'rightouter').show()

    # Anti-Left Join
    # csvwithschema.join(deptdf, cond, 'left_anti').show()

    # LeftSemi Join
    # csvwithschema.join(deptdf, cond, 'semi').show()

    ##Union

    # data2 = [Row(dept_no=11, dept_name='HR'),
    #          Row(dept_no=14, dept_name='Production'),
    #          Row(dept_no=13, dept_name='Sales')]
    # deptdf1 = spark.sparkContext.parallelize(data2).toDF()
    # # deptdf1.show()
    # # deptdf1.printSchema()
    #
    # deptdf.union(deptdf1).show()
    # deptdf.unionAll(deptdf1).show()
    # deptdf.union(deptdf1).distinct().show()






