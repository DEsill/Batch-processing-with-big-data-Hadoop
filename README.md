# Batch-processing-with-big-data-Hadoop
This is the latest and most complicated project I ever do.

![2](https://user-images.githubusercontent.com/105791967/227419142-4243c20f-31bf-493f-9cca-d81bf60dee15.png)

This project show data pipeline design that suit for Big data analysis company. Most of the tools I have used in this project are open-source(So it's free) and it's the tools to solve Big data problems.

Prerequisite

- To run this project you have to provide at least 8 GB of memory to Docker in my case I created .wslconfig in my user directory and set memory=8GB and restart the machine

- run ipconfig to find your Ipv4 of your machine and put it in import_sqoop.sh

- create .env file in your workspace and set AIRFLOW_UID=0 and AIRFLOW_GID=0 in that .env

How to run this project

1. run docker-compose up airflow-init

2. run docker-compose up

3. go to http://localhost:8080 to access airflow webserver then unpause DAG and Trigger DAG to test data pipeline
if you can run pipeline succesfully the output will show in output_result directory
