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
    'owner': 'Accenture',
    'start_date': days_ago(1)
}

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
    v_resource_group_name='RG-TDP-TDL-DEV'
    v_factory_name='dftdptdldev-core01'
    v_linked_service='ls_adls_tdl'
    v_dataset_name='prueba_parquet'
    hook = AzureDataFactoryHook(azure_data_factory_conn_id)

    try:
        #Make connection to ADF, and run pipeline with parameter
        print('NOMBRE DEL PIPELINE: ', hook.get_factory(pipeline_name=pipeline_name,
                                                        resource_group_name=v_resource_group_name,
                                                        factory_name=v_factory_name))
    except:
        print('Fallo get_factory')
        
        
    try:
        if hook._factory_exists(v_resource_group_name,v_factory_name):
            print('Existe ADF')
        else:
            print('No Existe ADF')
    except:
        print('Fallo _factory_exists')
        
        
    try:
        hook.get_linked_service(v_linked_service,v_resource_group_name,v_factory_name)
    except:
        print('Fallo get_linked_service')
        

    try:
        if hook._linked_service_exists(v_resource_group_name,v_factory_name,v_linked_service):
            print('Existe Linked Service ,v_linked_service)
        else:
            print('No Existe Linked Service, v_linked_service)
    except:
        print('Fallo _linked_service_exists')
                  
                  
    try:
        hook.get_dataset(v_dataset_name,v_resource_group_name,v_factory_name):
    except:
        print('Fallo _linked_service_exists')
                  

                  
                  
            
        



with DAG('dag_execute_adf_data_quality',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    prueba_python_aux = PythonOperator(task_id='prueba_python',
                                   python_callable=hello_world_loop)

    prueba_python2 = PythonOperator(task_id='prueba_python2',
                                   python_callable=chau_world_loop)

    prueba_python_dataFactory = PythonOperator( task_id="get-factory",
                                                python_callable=run_adf_pipeline,
                                                #op_kwargs={'pipeline_name':'Orchestration_ps_ts_generic_datasets_dataQuality'})
                                                op_kwargs={'pipeline_name':'prueba_pipeline_1'})

                                               
    prueba_bash = BashOperator(task_id='prueba_bash',
                               bash_command='echo prueba_bash')

start >> prueba_python_aux >> prueba_python2 >> prueba_python_dataFactory >> prueba_bash
