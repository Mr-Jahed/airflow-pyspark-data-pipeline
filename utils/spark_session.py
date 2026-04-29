from pyspark.sql import SparkSession

def create_spark():
    spark = SparkSession.builder \
        .appName("BatchPipeline") \
        .master("local[*]") \
        .getOrCreate()

    return spark