DROP TABLE IF EXISTS restaurant_detail;

SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

CREATE EXTERNAL TABLE IF NOT EXISTS restaurant_detail(
    id VARCHAR(55),	 
    restaurant_name	VARCHAR(55),
    category VARCHAR(55),	 
    estimated_cooking_time float,	 
    latitude float,	 
    longitude float	 
)
PARTITIONED BY (dt CHAR(6))
STORED AS PARQUET
LOCATION '/user/spark/transformed_restaurant_detail';

MSCK REPAIR TABLE restaurant_detail;