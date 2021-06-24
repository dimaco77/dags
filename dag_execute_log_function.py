import json
from datetime import timedelta
import logging
import json
import string
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
#    'projectName': args['projectName'],
#    'componentType': args['componentType'],
#    'componentName' : args['componentName'],
#    'eventType' : args['eventType'],
#    'errorCod': args['errorCod'],
#    'errorDescription' : args['errorDescription']

}


def CheckNull(args):
    if len(args) == 0:
        return 'llego vacio'

def format_json(args):
    if "[" in args or "]" in args:
        #nuevo si viene [ o {}]
        jsonStr = json.dumps(args)
        text1 = jsonStr.replace("[","")
        text2 = text1.replace("]","")
        args = json.loads(text2)


#Set the orchestration name
#instance_id = await client.start_new("LogAnalyticsEventHubLogOrchestrator", None, name)
#logging.info(f"Started orchestration with ID = '{instance_id}'.")
#return client.create_check_status_response(req, instance_id)

dag = DAG('dag_execute_log_function', default_args=default_args, tags=['example'], start_date=days_ago(2))

dag.doc_md = __doc__

# task_post_op, task_get_op and task_put_op are examples of tasks created by instantiating operators
# [START howto_operator_http_task_post_op]
task_post_op = SimpleHttpOperator(
    task_id='post_op',
    endpoint='post',
    data=json.dumps({"priority": 5}),
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.json()['json']['priority'] == 5,
    dag=dag,
)
# [END howto_operator_http_task_post_op]
# [START howto_operator_http_task_post_op_formenc]
task_post_op_formenc = SimpleHttpOperator(
    task_id='post_op_formenc',
    endpoint='post',
    data="name=Joe",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    dag=dag,
)
# [END howto_operator_http_task_post_op_formenc]
# [START howto_operator_http_task_get_op]
task_get_op = SimpleHttpOperator(
    task_id='get_op',
    method='GET',
    http_conn_id='http_default',  #CHECK CONNECTION
    endpoint='get',
    data={"param1": "value1", 
    "param2": "value2"},
    json_data = {
            "projectName": "",
            "componentType": "",
            "componentName" : "",
            "eventType" : "",
            "errorCod": "",
            "errorDescription" :""
            },
    headers={},
    xcom_push=True,
    dag=dag,
)
# [END howto_operator_http_task_get_op]
# [START howto_operator_http_task_get_op_response_filter]
task_get_op_response_filter = SimpleHttpOperator(
    task_id='get_op_response_filter',
    method='GET',
    endpoint='get',
    response_filter=lambda response: response.json()['nested']['property'],
    dag=dag,
)
# [END howto_operator_http_task_get_op_response_filter]
# [START howto_operator_http_task_put_op]
task_put_op = SimpleHttpOperator(
    task_id='put_op',
    method='PUT',
    endpoint='put',
    data=json.dumps({"priority": 5}),
    headers={"Content-Type": "application/json"},
    dag=dag,
)
# [END howto_operator_http_task_put_op]
# [START howto_operator_http_task_del_op]
task_del_op = SimpleHttpOperator(
    task_id='del_op',
    method='DELETE',
    endpoint='delete',
    data="some=data",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    dag=dag,
)
# [END howto_operator_http_task_del_op]
# [START howto_operator_http_http_sensor_check]
task_http_sensor_check = HttpSensor(
    task_id='http_sensor_check',
    http_conn_id='http_default',
    endpoint='',
    request_params={},
    response_check=lambda response: "httpbin" in response.text,
    poke_interval=5,
    dag=dag,
)
# [END howto_operator_http_http_sensor_check]
task_http_sensor_check >> task_get_op 
#task_http_sensor_check >> task_post_op >> task_get_op >> task_get_op_response_filter
#task_get_op_response_filter >> task_put_op >> task_del_op >> task_post_op_formenc
