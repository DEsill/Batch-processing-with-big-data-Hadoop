sqoop import \
--connect jdbc:postgresql://your_ipv4:5435/postgres \
--table restaurant_detail \
--username postgres \
--password postgres \
--num-mappers 1 \
--target-dir hdfs://namenode:9000//postgres/restaurant_detail_table \
--fields-terminated-by ',' \

sqoop import \
--connect jdbc:postgresql://your_ipv4:5435/postgres \
--table order_detail \
--username postgres \
--password postgres \
--num-mappers 1 \
--target-dir hdfs://namenode:9000//postgres/order_detail_table \
--fields-terminated-by ',' \