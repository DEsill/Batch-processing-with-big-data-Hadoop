#!/bin/bash

# Define the Hive table name
table_name="order_detail"

# Define the base HDFS path to the Parquet files
base_path="hdfs://namenode:9000/user/spark/order_detail"

# Get a list of all the partitions in the Parquet file
partitions=$(hdfs dfs -ls $base_path | awk -F/ '{print $NF}' | grep "dt=" | sort -u)

# Loop through each partition
for partition in $partitions; do
    # Extract the partition value from the partition string
    partition_value=$(echo $partition | awk -F= '{print $2}')

    # Construct the Hive ALTER TABLE command to add the partition
    alter_table_command="ALTER TABLE $table_name ADD PARTITION (dt='$partition_value') LOCATION '$base_path/$partition';"

    # Execute the Hive ALTER TABLE command
    hive -e "$alter_table_command"
done
