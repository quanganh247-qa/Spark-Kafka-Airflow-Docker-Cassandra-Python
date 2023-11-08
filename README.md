# Realtime Data Streaming | End-to-End Data Engineering Project

## Introduction

This project acts as a thorough manual for creating a data engineering pipeline from start to finish. It makes use of a strong tech stack that includes Apache Airflow, Python, Apache Kafka ,Apache Zookeeper, Apache Spark, and Cassandra to cover every step of the process, from data ingestion to processing to storage. Docker is used to containerize anything for scalability and deployment convenience.

## System Architecture
![image](https://github.com/quanganh247-qa/Spark-Kafka-Airflow-Docker-Cassandra-Python/assets/125935864/29e1bda2-4600-4aa8-a17a-20d34a59a076)

The project is designed with the following components:
- **Data Source**: We use randomuser.me API to generate random user data for our pipeline.
- **Apache Airflow**: Responsible for orchestrating the pipeline
- **Apache Zookeeper and Apache Kafka**: There are used to stream data from the data source to the processing engine.
- **Control Center and Schema Registry** : aids in our Kafka streams' monitoring and schema maintenance.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.
  
## Knowledge
- Using Apache Airflow to establish a data pipeline
- Streaming data in real time using Apache Kafka
- Coordinating remotely with Apache Zookeeper
- echniques for processing data with Apache Spark
- Cassandra data storage options
- Using Docker to containerize your whole data engineering setup

## Technolochies
- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- Docker

## Apache Airflow

Run the following command to clone the necessary repo on your local

``` git clone https://github.com/quanganh247-qa/Spark-Kafka-Airflow-Docker-Cassandra-Python.git ```

Then change the docker-compose-test.yml file with the one in this repo and add  ``` requirements.txt ``` file in the folder . This will bind the Airflow container with Kafka and Spark container and necessary modules will automatically be installed:

```docker-compose -f docker-compose-test.yml up -d```

Now you have a running Airflow container and you can access the UI at  ```https://localhost:8080```

## Apache Kafka

Run the following command to clone the necessary repo on your local

```docker-compose up -d```

![image](https://github.com/quanganh247-qa/Spark-Kafka-Airflow-Docker-Cassandra-Python/assets/125935864/fd13a7e3-2d4e-49d9-815c-0203e0a61ecc)

Then, we can see the messages coming to Kafka topic:

![image](https://github.com/quanganh247-qa/Spark-Kafka-Airflow-Docker-Cassandra-Python/assets/125935864/1cc5426f-bdcf-441c-bd9c-b2e96599371a)

## Cassandra
A Cassandra server will also be created using ```docker-compose.yml```. You can find each environment variable in ```docker-compose.yml```. They are defined in the scripts as well.

The command to access the Cassandra server is as follows:

```docker exec -it cassandra /bin/bash```

After accessing the bash, we can run the following command to access to cqlsh cli.

```cqlsh -u cassandra -p cassandra```

Then, we can run the following commands to create the keyspace spark_streaming and the table random_names:

```CREATE KEYSPACE spark_streaming WITH replication = {'class':'SimpleStrategy','replication_factor':1};```

```CREATE TABLE spark_streaming.random_names(id UUID PRIMARY KEY,first_name TEXT,last_name TEXT,gender TEXT,email TEXT,phone TEXT,address TEXT,registered_date TEXT,picture TEXT);```

![image](https://github.com/quanganh247-qa/Spark-Kafka-Airflow-Docker-Cassandra-Python/assets/125935864/60ee17fc-91a8-4449-8e4d-adae6ec31860)

## Starting DAGs

The data will be delivered to Kafka topics every ten seconds when we flip the OFF button to ON. From the Control center, we can also verify.

![image](https://github.com/quanganh247-qa/Spark-Kafka-Airflow-Docker-Cassandra-Python/assets/125935864/abef0f16-5a74-4b63-943b-86d37b3e711d)
![image](https://github.com/quanganh247-qa/Spark-Kafka-Airflow-Docker-Cassandra-Python/assets/125935864/f468faf6-622c-4ee0-aa70-7fada7d92ee9)

## Spark

First of all we should copy the local PySpark script into the container:

```docker cp spark.py spark_master:/opt/bitnami/spark/```

Next, we need to install the required JAR files inside the jars directory by gaining access to the Spark container.

```docker exec -it spark_master /bin/bash```

We should run the following commands to install the necessary JAR files under for Spark version 3.3.0:

```cd jars```

```curl -O https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.3.0/spark-cassandra-connector_2.12-3.3.0.jar```

```curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.3.0/spark-sql-kafka-0-10_2.13-3.3.0.jar```

While the API data is sent to the Kafka topic random_names regularly, we can submit the PySpark application and write the topic data to Cassandra table

```cd ..```

```spark-submit --master local[2] --jars /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-3.3.0.jar,/opt/bitnami/spark/jars/spark-cassandra-connector_2.12-3.3.0.jar spark_streaming.py```







