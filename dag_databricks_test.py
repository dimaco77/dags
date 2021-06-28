import json
from datetime import timedelta
import logging
import json
import string
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator


default_args = {
    'owner': 'accenture',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

new_cluster = {
    'spark_version': '2.4.5-db3-scala2.11',
    'node_type_id': 'r3.xlarge',
    'aws_attributes': {
        'availability': 'ON_DEMAND'
    },
    'num_workers': 8
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

dag = DAG(
    dag_id='example_databricks_operator', default_args=default_args,
    schedule_interval='@daily')

with DAG('test_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    notebook_task = DatabricksSubmitRunOperator(
    task_id='notebook_task',
    dag=dag,
    #json=notebook_task_params
    )
    
    spark_jar_task = DatabricksSubmitRunOperator(
    task_id='spark_jar_task',
    dag=dag,
    new_cluster=new_cluster,
    spark_jar_task={
        'main_class_name': 'com.example.ProcessData'
    },
    libraries=[
        {
            'jar': 'dbfs:/lib/etl-0.1.jar'
        }
    ]
    )





start >> notebook_task >> spark_jar_task

