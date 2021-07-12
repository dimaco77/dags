import datetime as dt
import logging
from os import path
import tempfile

import pandas as pd
import pymssql

from airflow import DAG

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.operators.python import PythonOperator


RANK_QUERY = """
SELECT *
FROM
    OPENROWSET(
        BULK  'https://statdptdldev01.blob.core.windows.net/tdl/raw/industrial/tamsa/tam-pro/pro-producto/capture_year=2021/capture_month=06/capture_day=13/data_2021-06-13-135636-2abd7482-a2ed-4ac7-8e0e-35cfcd2da7cb.parquet',
        FORMAT='PARQUET'
    ) AS [result]
"""

def _execute_query(odbc_conn_id, wasb_conn_id, dataset_container_output, **context):    
    year = context["execution_date"].year
    month = context["execution_date"].month

    # Determine storage account name, needed for query source URL.
    blob_account_name = WasbHook.get_connection(wasb_conn_id).login

    #le puedo pasar parametros de esta manera
    query = RANK_QUERY.format(
        blob_account_name=blob_account_name,
    )

    logging.info(f"Executing query: {query}")

   # odbc_hook = OdbcHook(odbc_conn_id, driver="ODBC Driver 17 for SQL Server")

   # with odbc_hook.get_conn() as conn:
   #     with conn.cursor() as cursor:
   #         cursor.execute(query)

   #         rows = cursor.fetchall()
   #         colnames = [field[0] for field in cursor.description]

    server = 'airflowazure1-ondemand.sql.azuresynapse.net'
    user = 'sqladminuser'
    password = '12345678U!'
    conn = pymssql.connect(server, user, password, "synapsedb")
    cursor = conn.cursor(as_dict=True)
    
    cursor.execute(query)
    rows = cursor.fetchall()
    colnames = [field[0] for field in cursor.description]
    #conn.close() donde cierra la conexion?


    dataSetResult = pd.DataFrame.from_records(rows, columns=colnames)
    logging.info(f"Retrieved {dataSetResult.shape[0]} rows")

    # Write ranking to temp file.
    logging.info(f"Writing results to {dataset_container_output}/{year}/{month:02d}.csv")
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = path.join(tmp_dir, "dataSetResult.csv")
        dataSetResult.to_csv(tmp_path, index=False)

        # Upload file to Azure Blob.
        wasb_hook = WasbHook(wasb_conn_id)
        wasb_hook.load_file(
            tmp_path,
            container_name=dataset_container_output,
            blob_name=f"{year}/{month:02d}.csv",
        )


with DAG(
    dag_id="PoC-Test-Azure-Synapse-Services-Airflow",
    description="DAG demonstrating how to execute a query in Synapse",
    schedule_interval="@monthly",
) as dag:

    query_synapse = PythonOperator(
        task_id="query_synapse",
        python_callable=_execute_query,
        op_kwargs={
            "odbc_conn_id": "my_odbc_conn",
            "wasb_conn_id": "my_wasb_conn",
            "dataset_container_output": "datasetcontaineroutput",
        },
    )

    query_synapse
