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

databricks_conn_id = "databricks_test"
hook = DatabricksHook(databricks_conn_id)
#hook.run_pipeline(pipeline_name,resource_group_name='RG-TDP-TDL-DEV',factory_name='dftdptdldev-core01')


# DAG
with DAG(dag_id='dag_execute_azure_databricks',
		 default_args=default_args,
		 schedule_interval = '@once') as dag:


	#TASK 1
	start = DummyOperator(task_id = 'start')

    #TASK 3
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

	#TASK 2
	end = DummyOperator(task_id = 'end')




start >> end