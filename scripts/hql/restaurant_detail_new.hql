DROP TABLE IF EXISTS restaurant_detail_new;

SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

CREATE TABLE IF NOT EXISTS restaurant_detail_new (
    id VARCHAR(55),	 
    restaurant_name	VARCHAR(55),
    category VARCHAR(55),	 
    estimated_cooking_time float,	 
    latitude float,	 
    longitude float,
    cooking_bin float 
)
PARTITIONED BY (dt CHAR(6))
STORED AS PARQUET
LOCATION '/user/spark/restaurant_detail_new';

MSCK REPAIR TABLE restaurant_detail_new;