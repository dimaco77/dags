from datetime import datetime

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from pendulum import yesterday

from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook
from airflow.hooks.base_hook import BaseHook

default_args = {
    'owner': 'accenture',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}
yesterday_date='{{ yesterday_ds_nodash }}'

azure_data_factory_conn_id = 'azure_data_factory_conn'

def hello_world_loop():
    for palabra in ['hello', 'world']:
        print(palabra)

def chau_world_loop():
    for palabra in ['chau', 'world']:
        print(palabra)


def run_adf_pipeline(pipeline_name):
    '''Runs an Azure Data Factory pipeline using the AzureDataFactoryHook and passes in a date parameter
    '''

    #Create a dictionary with date parameter
    params = {}

    #Make connection to ADF, and run pipeline with parameter
    hook = AzureDataFactoryHook(azure_data_factory_conn_id)
    print('uno')
    hook.run_pipeline(pipeline_name,resource_group_name='RG-TDP-TDL-DEV',factory_name='dftdptdldev-core01',config={'secret':'ydH0m73_v-e_Z0oFAqf_GLUuYJ.rZZB9o9'})


with DAG('dag_execute_adf_data_quality',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    prueba_python_dataFactory = PythonOperator( task_id="get-factory",
                                                python_callable=run_adf_pipeline,
                                                op_kwargs={'pipeline_name':'Orchestration_ps_ts_generic_datasets_dataQuality','date':yesterday_date})

    prueba_bash = BashOperator(task_id='prueba_bash',
                               bash_command='echo prueba_bash')

start >> prueba_python_dataFactory >> prueba_bash
