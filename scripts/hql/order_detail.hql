DROP TABLE IF EXISTS order_detail;

SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

CREATE EXTERNAL TABLE IF NOT EXISTS order_detail (
    order_created_timestamp TIMESTAMP,
    status STRING,
    price INT,
    discount FLOAT,
    id STRING,
    driver_id STRING,
    user_id STRING,
    restaurant_id STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/spark/order_detail';

MSCK REPAIR TABLE order_detail;