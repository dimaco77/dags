from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook

azure_data_factory_conn = 'azure_data_factory_conn'

def run_adf_pipeline(pipeline_name, date):
    '''Runs an Azure Data Factory pipeline using the AzureDataFactoryHook and passes in a date parameter
    '''

    #Make connection to ADF, and run pipeline with parameter
    hook = AzureDataFactoryHook(azure_data_factory_conn)
    hook.run_pipeline(pipeline_name, parameters=params)

with DAG(
    'azure_data_factory',
  ) as dag:

    opr_run_pipeline = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_adf_pipeline,
        op_kwargs={'pipeline_name': 'Orchestration_ps_ts_generic_datasets_dataQuality'}
    )
