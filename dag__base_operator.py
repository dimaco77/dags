import json
import requests
import datetime
import hashlib
import hmac
import base64
import os
from airflow.models import DAG
from datetime import timedelta
from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
from random import randint
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from accenture_tools import ExecutePipeline, CheckPipelineStatus
from airflow.models import Variable



default_args = {
    'owner': 'Accenture',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),

}


RESOURCE_GROUP = Variable.get('resource_group_name')
ADF_NAME = Variable.get('factory_name')

with DAG('dag__base_operator',
         default_args=default_args,
         schedule_interval=timedelta(minutes=45),
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    t_HelloOperator = ExecutePipeline(task_id="t_HelloOperator", resource_group=RESOURCE_GROUP,pipeline='prueba_pipeline')

    t_CheckPipelineStatus = CheckPipelineStatus(task_id='t_CheckPipelineStatus', adf=ADF_NAME, resource_group=RESOURCE_GROUP)

    end = BashOperator(task_id='end',bash_command='echo prueba_bash')

    start >> t_HelloOperator >> t_CheckPipelineStatus >> end


