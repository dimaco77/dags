from datetime import datetime

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from pendulum import yesterday

import json
import requests
import datetime

from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
from random import randint
from airflow.utils.dates import days_ago

from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor

#tof_pipeline_run="tof_pipeline_run"
tof_pipeline_run = Variable.get("tof_pipeline_run")
#tof_az_func = "tof_az_func"
tof_az_func = Variable.get("tof_az_func")
#tof_all = "tof_all" 
tof_all = Variable.get("tof_all")

#default arguments
default_args = {
    'owner': Variable.get("owner"),
    'start_date': days_ago(1)
}


#DataBricks connection
notebook_task = {
    "notebook_path": "/Users/ebaquero@suppliers.tenaris.com/example",
}

conn_id="databricks_test"
CLUSTER_ID="0118-154944-alpha847"

#ADF connection
azure_data_factory_conn_id = 'azure_data_factory_conn'

def hello_world_loop():
    for palabra in ['hello', 'world']:
        print(palabra)

def chau_world_loop():
    for palabra in ['chau', 'world']:
        print(palabra)

def run_adf_pipeline(pipeline_name):
    if tof_pipeline_run.lower() != "true" or tof_all.lower() != "true":
        print("----------run_pipeline var")
        print(tof_pipeline_run)
        print("----------run_all var")
        print(tof_az_func)
        print("----------run_func var") 
        print(tof_all)
        print("----------types")
        print(type(tof_pipeline_run))
        print(type(tof_az_func))
        print(type(tof_all))

        '''Runs an Azure Data Factory pipeline using the AzureDataFactoryHook and passes in a date parameter
        '''

        #Create a dictionary with date parameter
        params = {}
        v_resource_group_name = Variable.get("resource_group_name")
        v_factory_name = Variable.get("factory_name")
        v_linked_service = Variable.get("linked_service")
        v_dataset_name = Variable.get("dataset_name")
        hook = AzureDataFactoryHook(azure_data_factory_conn_id)

        try:
            #Make connection to ADF, and run pipeline with parameter
            print('NOMBRE DEL PIPELINE: ', hook.get_factory(pipeline_name=pipeline_name,
                                                            resource_group_name=v_resource_group_name,
                                                            factory_name=v_factory_name))
            print('----------get_factory funciono Correctamente')
        except:
            print('----------Fallo get_factory')
            
            
        try:
            if hook._factory_exists(v_resource_group_name,v_factory_name):
                print('----------Se ha verificado la existencia de la ADF correctamente')
            else:
                print('----------No Existe ADF')
        except:
            print('----------Fallo _factory_exists')
            
            
        try:
            hook.get_linked_service(v_linked_service,v_resource_group_name,v_factory_name)
            print('----------get_linked_service funciono Correctamente')
        except:
            print('----------Fallo get_linked_service')
            

        try:
            if hook._linked_service_exists(v_resource_group_name,v_factory_name,v_linked_service):
                print('----------Existe Linked Service', v_linked_service)
            else:
                print('----------No Existe Linked Service', v_linked_service)
        except:
            print('----------Fallo _linked_service_exists')
                    
                    
        try:
            hook.get_dataset(v_dataset_name,v_resource_group_name,v_factory_name)
            print('----------get_dataset funciono Correctamente')
        except:
            print('----------Fallo get_dataset')
                    

        try:
            if hook._dataset_exists(v_resource_group_name,v_factory_name,v_dataset_name):
                print('----------Existe Dataset' ,v_dataset_name)
            else:
                print('----------No Existe Dataset', v_dataset_name)
        except:
            print('----------Fallo _dataset_exists')

        try:
            hook.get_pipeline(pipeline_name, v_resource_group_name, v_factory_name)
            print('----------get_pipeline funciono Correctamente')
        except:
            print('----------Fallo get_dataset')



        try:
            if hook._pipeline_exists(v_resource_group_name,v_factory_name,pipeline_name):
                print('----------Existe Pipeline' ,pipeline_name)
            else:
                print('----------No Existe Pipeline', pipeline_name)
        except:
            print('----------Fallo _pipeline_exists')


        try:
            hook.run_pipeline(pipeline_name, v_resource_group_name, v_factory_name)
            print('----------run_pipeline funciono Correctamente')
        except:
            print('----------Fallo run_pipeline')
    else:
        print("-----------run var = false")

#IF task fails before retries
def on_retry_callback(context):
    print("Retry works  !  ")

#IF task fails after retries
def failure(context):
    print("-----------Fail works!  ")
    print(context)

#IF task succeeds
def on_succeed_callback(context):
    print("Succeed works  !  ")

#Azure Functions connection
# Build and send a request to the POST API
def post_data(shared_key, body):
    method = 'POST'
    content_type = 'application/json'
    resource = '/api/logs'
    rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    content_length = len(body)
    # Azure function URL
    uri = 'https://logeventanalytics.azurewebsites.net/api/orchestrators/LogAnalyticsEventHubLogHttpStart'

    headers = {
        'content-type': content_type,
        'x-functions-key': shared_key,
        'x-ms-date': rfc1123date
    }

    response = requests.post(uri, data=body, headers=headers)
    if (response.status_code >= 200 and response.status_code <= 299):
        return "Accepted"
    else:
        return "Response code: {}".format(response.reason)


def errorLog(projectName, componentType, componentName, eventType, errorCod, errorDescription):
    # Get the environment variables in order to connect with the Key Vault
    # KEYVAULT_URL = os.getenv('KeyVaultUrl')
    # TENANT_ID = os.getenv('TenantId')
    # CLIENT_ID = os.getenv('ClientId')
    # CLIENT_TOKEN = os.getenv('ClientToken')
    # _credential = ClientSecretCredential(
    #     tenant_id=TENANT_ID,
    #     client_id=CLIENT_ID,
    #     client_secret=CLIENT_TOKEN,
    # )

    # _sc = SecretClient(vault_url=KEYVAULT_URL,credential=_credential)

    # Default key
    shared_key = 'rv2SQN09uXag6DMlkqZ4dB6UfncAx10nmFK7/vOGB02Os53gC3RHYA=='
    # shared_key = _sc.get_secret('sharedkeyCustomLogFunction').value

    json_data = {
        "projectName": projectName,
        "componentType": componentType,
        "componentName": componentName,
        "eventType": eventType,
        "errorCod": errorCod,
        "errorDescription": errorDescription
    }

    json_data = json.dumps(json_data)

    response = post_data(shared_key, json_data)
    print('Error Description: '+errorDescription)
    print(response)


with DAG('dag__demo_fail_callback',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False) as dag:

    start = DummyOperator(task_id='start')

    run_etl_pipeline = PythonOperator( task_id="run_etl_pipeline",
                                                python_callable=run_adf_pipeline,
                                                #op_kwargs={'pipeline_name':'Orchestration_ps_ts_generic_datasets_dataQuality'})
                                                op_kwargs={'pipeline_name':'prueba_pipeline'})

    Log_error_if_failure = DummyOperator(task_id='Log_error_if_failure')

    run_validation_pipeline = PythonOperator( task_id="run_validation_pipeline",
                                                python_callable=run_adf_pipeline,
                                                #op_kwargs={'pipeline_name':'Orchestration_ps_ts_generic_datasets_dataQuality'})
                                                op_kwargs={'pipeline_name':'prueba_pipeline'}
                                                )
    
    send_mail = DummyOperator(task_id='send_mail')

    send_data_to_az_synapse = DummyOperator(task_id='send_data_to_az_synapse')

    send_data_to_az_an_serv = BashOperator(task_id='send_data_to_az_an_serv',
                     bash_command='exit 1',
                     on_failure_callback="failure")

    end = BashOperator(task_id='end',
                        bash_command='exit 0',
                        on_failure_callback="failure")

start >> run_etl_pipeline >> Log_error_if_failure 
run_etl_pipeline >> run_validation_pipeline >> Log_error_if_failure 
run_validation_pipeline >> send_data_to_az_synapse>>send_data_to_az_an_serv >> send_mail >> end
