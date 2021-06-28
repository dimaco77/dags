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
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator

default_args = {
    'owner': 'accenture',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

notebook_task = {
    "notebook_path": "/Users/ebaquero@suppliers.tenaris.com/example",
}

conn_id="databricks_test"
CLUSTER_ID="0118-154944-alpha847"

# DAG
with DAG(dag_id='dag_execute_azure_databricks',
default_args=default_args,schedule_interval=timedelta(minutes=5),catchup=False) as dag:
         


	#TASK 1
    start = DummyOperator(task_id = 'start')

    #TASK 2
    opr_submit_run = DatabricksSubmitRunOperator(
        task_id="submit_run",
        databricks_conn_id=conn_id,
        # new_cluster=new_cluster,
        existing_cluster_id=CLUSTER_ID,
        notebook_task=notebook_task,
    )

    #TASK 3
    config_databricks_host = BashOperator(  	task_id='config_host',
                                        	bash_command='export DATABRICKS_HOST=https://adb-7736646044667006.6.azuredatabricks.net/?o=7736646044667006')

    #TASK 4
    config_databricks_token = BashOperator(  	task_id='config_token',
                                        	bash_command='export DATABRICKS_TOKEN=dapi42559bf10e02c10f245d8bf11f36c253')



    #TASK 5
    terminated_cluster = BashOperator(  task_id='terminated_cluster',
                                        bash_command='export DATABRICKS_HOST=https://adb-7736646044667006.6.azuredatabricks.net/?o=7736646044667006 && export DATABRICKS_TOKEN=dapi42559bf10e02c10f245d8bf11f36c253 && databricks clusters delete --cluster-id '+CLUSTER_ID)

    #TASK 6
    end = DummyOperator(task_id = 'end')


start >> opr_submit_run >> config_databricks_host >> config_databricks_token >> terminated_cluster >> end
