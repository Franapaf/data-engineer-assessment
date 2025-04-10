from pyspark.sql import SparkSession

def get_spark_session(app_name="DataPipelineApp"):
    
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "./jars/postgresql-42.6.0.jar") \
        .config("spark.hadoop.hadoop.native.io", "false") \
        .getOrCreate()