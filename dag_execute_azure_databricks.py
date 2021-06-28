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

new_cluster = {
    'spark_version': '2.4.5-db3-scala2.11',
    'node_type_id': 'Standard_DS3_v2',
    'num_workers': 2
}

notebook_task = {
    "notebook_path": "/Users/ebaquero@suppliers.tenaris.com/example",
}

# Define params for Run Now Operator
notebook_params = {"Variable": 5}

conn_id="databricks_test"
#databricks_conn_id = "databricks_test"
#hook = DatabricksHook(conn_id)

#hook.run_pipeline(pipeline_name,resource_group_name='RG-TDP-TDL-DEV',factory_name='dftdptdldev-core01')


# DAG
with DAG(dag_id='dag_execute_azure_databricks',
default_args=default_args,schedule_interval = '@once') as dag:
         
    opr_submit_run = DatabricksSubmitRunOperator(
        task_id="submit_run",
        databricks_conn_id=conn_id,
        new_cluster=new_cluster,
        notebook_task=notebook_task,
    )

    opr_run_now = DatabricksRunNowOperator(
        task_id="run_now",
        databricks_conn_id=conn_id,
        job_id=5,
        notebook_params=notebook_params,
    )

	#TASK 1
    start = DummyOperator(task_id = 'start')

	#TASK 2
    end = DummyOperator(task_id = 'end')

start >>opr_submit_run>>opr_run_now>> end