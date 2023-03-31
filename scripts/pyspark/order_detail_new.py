from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t

spark = SparkSession.builder.enableHiveSupport()\
.config("spark.sql.parquet.writeLegacyFormat",True)\
.getOrCreate()


df = spark.read.parquet('hdfs://namenode:9000/user/spark/order_detail')

df = df.withColumn('discount_no_null', f.col('discount'))
df = df.na.fill(value=0, subset=['discount_no_null'])


df.write.parquet('hdfs://namenode:9000/user/spark/order_detail_new', partitionBy='dt', mode='overwrite')