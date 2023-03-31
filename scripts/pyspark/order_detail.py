from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t

spark = SparkSession.builder.enableHiveSupport()\
.config("spark.sql.parquet.writeLegacyFormat",True)\
.getOrCreate()

df = spark.read.csv('hdfs://namenode:9000//postgres/order_detail_table/part-m-00000', header = False)

rename = {
    '_c0' : 'order_created_timestamp',
    '_c1' : 'status',
    '_c2' : 'price',
    '_c3' : 'discount',
    '_c4' : 'id',
    '_c5' : 'driver_id',
    '_c6' : 'user_id',
    '_c7' : 'restaurant_id',
}

df = df.toDF(*[rename[c] for c in df.columns])
df = df.withColumn('order_created_timestamp', f.to_timestamp(df['order_created_timestamp']))
df = df.withColumn('price', f.col('price').cast(t.IntegerType()))
df = df.withColumn('discount', f.col('discount').cast(t.FloatType()))
df = df.withColumn('dt', f.date_format('order_created_timestamp', "yyyyMMdd"))
df.write.parquet('hdfs://namenode:9000/user/spark/order_detail', partitionBy='dt', mode='overwrite')