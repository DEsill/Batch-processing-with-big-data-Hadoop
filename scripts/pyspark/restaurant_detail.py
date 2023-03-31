from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.functions import col

spark = SparkSession.builder.enableHiveSupport()\
.config("spark.sql.parquet.writeLegacyFormat",True)\
.getOrCreate()

df = spark.read.csv('hdfs://namenode:9000//postgres/restaurant_detail_table/part-m-00000', header = False)

rename = {
    '_c0' : 'id',
    '_c1' : 'restaurant_name',
    '_c2' : 'category',
    '_c3' : 'estimated_cooking_time',
    '_c4' : 'latitude',
    '_c5' : 'longitude',
}

df = df.toDF(*[rename[c] for c in df.columns])
df = df.withColumn('estimated_cooking_time', f.col('estimated_cooking_time').cast(t.FloatType()))
df = df.withColumn('latitude', f.col('latitude').cast(t.FloatType()))
df = df.withColumn('longitude', f.col('longitude').cast(t.FloatType()))
df = df.withColumn('dt', f.lit("latest"))
df.write.parquet('hdfs://namenode:9000/user/spark/transformed_restaurant_detail', partitionBy='dt', mode='overwrite')