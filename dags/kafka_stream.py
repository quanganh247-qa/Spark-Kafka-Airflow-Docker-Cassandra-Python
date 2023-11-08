import uuid
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import time 
from airflow.utils.dates import days_ago
import json
from kafka import KafkaProducer
import requests
import os
import logging
import traceback
from airflow.decorators import dag, task
import pandas as pd


default_args = {
    'owner': 'QA\'s project',
    'start_date': days_ago(0),
    'retries': 1,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    'retry_delay': timedelta(seconds=5)
}


@dag(dag_id='Automation_get_user',
          default_args=default_args,
          description='Automation_get_user',
          schedule_interval=timedelta(days=1))

def xcom_taskflow_dag():

    def create_kafka_producer():
        return KafkaProducer(bootstrap_servers=['broker:29092'])


    def insert_value(df):

        query = """
        INSERT INTO user_table (id, first_name, last_name, gender, email, phone, address, registered_date, picture)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """    
        row_count = 0
        for _, row in df.iterrows():
            values = (row['id'],row['f_name'],row['l_name'],row['gender'],row['email'],row['phone'],row['address'],row['registered_date'],row['picture'])
            cur.execute(query,values)
            row_count += 1
        
        print(f"{row_count} rows inserted into table ")

    @task
    def get_data_user():
        request = requests.get("https://randomuser.me/api/")
        request = request.json()
        data = request["results"][0]

        data_formated ={}

        location = data['location']
        data_formated['id'] = str(uuid.uuid4())
        data_formated['f_name'] = data['name']['first']
        data_formated['l_name'] = data['name']['last']
        data_formated['gender'] = data['gender']
        data_formated['dob'] = data['dob']['date']
        data_formated['phone_number'] = data['phone'] 
        data_formated['registered_date'] = data['registered']['date']
        data_formated['email'] = data['email']
        data_formated['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"    
        data_formated['picture'] = data['picture']['medium']

        return data_formated

    user_data_task = get_data_user()


    @task
    def start_streaming(kafka_data):
        """
        Writes the API data every 10 seconds to Kafka topic random_names
        """
        producer = create_kafka_producer()

        # kafka_data = get_data_user()    

        end_time = datetime.now() + timedelta(seconds=120)  # Chạy script trong 2 phút

        while True:
            if datetime.now() > end_time:
                break

            producer.send("random_names", json.dumps(kafka_data).encode('utf-8'))
            time.sleep(10)           

    kafka_task = start_streaming(user_data_task)

    user_data_task >> kafka_task

data = xcom_taskflow_dag()





# @task
# def tranform_to_df(data):
#     data_list = []
#     # Add the formatted data to the list
#     data_list.append({
#         'id': str(uuid.uuid4()),
#         'f_name': data['f_name'],
#         'l_name': data['l_name'],
#         'gender': data['gender'],
#         'dob': data['dob'],
#         'phone_number': data['phone_number'],
#         'registered_date': data['registered_date'],
#         'email': data['email'] ,
#         'address': data['address'],
#         'picture': data['picture']
#     })
#     df = pd.DataFrame(data_list)
#     return  df

# tranform_data_task = tranform_to_df(user_data_task)