from pyspark import SparkSession

if __name__ == '__main__':


spark = SparkSession.builder.appName('oracle_connection')\
        .config('spark.jars',r"../lib/ojdbc8.jar")\
        .getOrCreate()