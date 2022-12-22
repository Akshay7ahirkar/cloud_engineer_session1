#import Required modules and packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum

#Create Spark session
spark = SparkSession.builder.master('yarn').appName('spark-bigquery-demo').getOrCreate()

#define Temp GCS Bucket
bucket = 'ak2'
spark.conf.set('temporaryGcsBucket', bucket)

#read Data into Spark DF
df=spark.read.option("header",True).csv('gs://ak2/greenhouse-gas-emissions-industry-and-household-year-ended-2020.csv')

#select limited col and cast string type
req_df=df.select(col('year'),col('anzsic_descriptor'),col('variable'),col('source'),col('data_value').cast('double'))


#group by list of column and sum data value
req_df=req_df.groupBy('year','anzsic_descriptor','source').agg(sum('data_value').alias('sum_qty')).sort('year')

#writing data to Bigquery
req_df.write.format('bigquery').option('table','BWT_Session.agg_output').save()
spark.stop()


