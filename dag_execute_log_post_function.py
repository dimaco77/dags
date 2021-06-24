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
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator



default_args = {
    'owner': 'Airflow Post Logging App',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),

}


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
    print(response)


with DAG('dag_execute_adf_data_quality',
         default_args=default_args,
         schedule_interval='@daily',
         start_date=days_ago(1),
         catchup=False) as dag:

    start = DummyOperator(task_id='start')


    post_logging_app = PythonOperator( task_id="post_logging_app",
                                                python_callable=errorLog,
                                                op_kwargs={ 'projectName':'project1',
                                                            'componentType':'componentType',
                                                            'componentName':'componentName',
                                                            'eventType':'Critical',
                                                            'errorCod':'errorCod',
                                                            'errorDescription':'Test_Logging_Airflow_'+str(randint(0, 10000000000000))})

    prueba_bash = BashOperator(task_id='prueba_bash',
                               bash_command='echo prueba_bash')

prueba_bash >> post_logging_app