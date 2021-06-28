from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook

azure_data_factory_conn = 'adf_LogAnalytics'

#Get yesterday's date, in the correct format
yesterday_date = '{{ yesterday_ds_nodash }}'

def run_adf_pipeline(pipeline_name, date):
    '''Runs an Azure Data Factory pipeline using the AzureDataFactoryHook and passes in a date parameter
    '''

    #Create a dictionary with date parameter
    params = {}
    params["date"] = date

    #Make connection to ADF, and run pipeline with parameter
    hook = AzureDataFactoryHook(azure_data_factory_conn)
    print('NOMBRE DEL PIPELINE: ', hook.get_pipeline(pipeline_name='pipeline_name',resource_group_name='RG-TDP-PILOT-LAB',factory_name='LogAnalyticsLoggingComponent'))

    #hook.run_pipeline(pipeline_name, parameters=params)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'azure_data_factory_LA',
    start_date=datetime(2019, 1, 1),
    max_active_runs=1,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:

    opr_run_pipeline = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_adf_pipeline,
        op_kwargs={'LoggingComponent': 'Orchestration_ps_ts_generic_datasets_dataQuality', 'date': yesterday_date}
    )
