import json
from datetime import timedelta
import logging
import json
import string
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def hello_world_loop():
    for palabra in ['hello', 'world']:
        print(palabra)

def chau_world_loop():
    for palabra in ['chau', 'world']:
        print(palabra)

def conn_succ_dummy():
    for palabra in ['Successful ', 'Connection']:
        print(palabra)

def conn_fail_dummy():
    for palabra in ['Failed ', 'Connection']:
        print(palabra)

dag = DAG('dag_example', default_args=default_args, tags=['example'], start_date=days_ago(2))

dag.doc_md = __doc__

with DAG('test_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    prueba_python_aux = PythonOperator(task_id='prueba_python',
                                   python_callable=hello_world_loop)

    prueba_python2 = PythonOperator(task_id='prueba_python2',
                                   python_callable=chau_world_loop)


    prueba_bash = BashOperator(task_id='prueba_bash',
                               bash_command='echo prueba_bash')



start >> prueba_python_aux >> prueba_python2 >> prueba_bash

