DROP TABLE IF EXISTS order_detail_new;

SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

CREATE EXTERNAL TABLE IF NOT EXISTS order_detail_new (
    order_created_timestamp TIMESTAMP,
    status STRING,	 
    price INT,	 
    discount FLOAT,	 
    id STRING,	 
    driver_id STRING,	 
    user_id	STRING,		 
    restaurant_id STRING,
    discount_no_null FLOAT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/spark/order_detail_new';

MSCK REPAIR TABLE order_detail_new;