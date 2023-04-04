from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import sys

sys.path.append('/opt/airflow/scripts')
sys.path.append('/opt/airflow/output_result')

with DAG(
    dag_id = 'data_pipeline',
    start_date = days_ago(1),
    schedule_interval=timedelta(days=1)
) as dag:

    task1 = BashOperator (
        task_id = 'sqoop_import',
        bash_command = 'docker exec sqoop bash /opt/sqoop/import_sqoop.sh '
    )

    task2 = BashOperator (
        task_id = 'spark_transform_restaurant',
        bash_command = 'docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/scripts/restaurant_detail.py '
    )

    task3 = BashOperator (
        task_id = 'spark_transform_order',
        bash_command = 'docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/scripts/order_detail.py '
    )

    task4 = BashOperator (
        task_id = 'create_restaurant_table',
        bash_command = 'docker exec hive-server hive -f /opt/hql/restaurant_detail.hql '
    )

    task5 = BashOperator (
        task_id = 'create_order_table',
        bash_command = 'docker exec hive-server hive -f /opt/hql/order_detail.hql '
    )

    task6 = BashOperator (
        task_id = 'spark_restaurant_new',
        bash_command = 'docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/scripts/restaurant_detail_new.py '
    )

    task7 = BashOperator (
        task_id = 'spark_order_new',
        bash_command = 'docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /opt/scripts/order_detail_new.py '
    )

    task8 = BashOperator (
        task_id = 'create_restaurant_table_new',
        bash_command = 'docker exec hive-server hive -f /opt/hql/restaurant_detail_new.hql '
    )

    task9 = BashOperator (
        task_id = 'create_order_table_new',
        bash_command = 'docker exec hive-server hive -f /opt/hql/order_detail_new.hql '
    )

    task10 = BashOperator (
        task_id = 'query_result',
        bash_command = 'docker exec hive-server bash /opt/shell/query_result.sh '
    )

    task11 = BashOperator (
        task_id = 'get_cooking_result',
        bash_command = 'docker cp hive-server:/opt/cooking.csv /opt/airflow/output_result '
    )

    task12 = BashOperator (
        task_id = 'get_discount_result',
        bash_command = 'docker cp hive-server:/opt/discount.csv /opt/airflow/output_result '
    )

task1 >> task2 >> task3 >> [task4, task5] >> task6 >> task7 >> [task8, task9] >> task10 >> [task11, task12]