from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.functions import when, col

spark = SparkSession.builder.enableHiveSupport()\
.config("spark.sql.parquet.writeLegacyFormat",True)\
.getOrCreate()

df = spark.read.parquet('hdfs://namenode:9000/user/spark/transformed_restaurant_detail')

df = df.withColumn("cooking_bin", 
when(col("estimated_cooking_time").between(10, 40), 1).
when(col("estimated_cooking_time").between(41, 80), 2).
when(col("estimated_cooking_time").between(81, 120), 3).
when(col("estimated_cooking_time") > 120, 4).otherwise(None))

df.write.parquet('hdfs://namenode:9000/user/spark/restaurant_detail_new', partitionBy='dt', mode='overwrite')