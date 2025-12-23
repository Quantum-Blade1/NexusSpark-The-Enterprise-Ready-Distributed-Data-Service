from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark():
    # We upgraded the master to be dynamic for the NexusSpark cluster
    builder = SparkSession.builder \
       .appName("NexusSpark-Engine") \
       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
       .config("spark.executor.memory", "4g") \
       .config("spark.executor.cores", "2")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()