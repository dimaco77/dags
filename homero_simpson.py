from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator import BashOperator
from airflow.utils import dates
from datetime import datetime


default_args = {
	'owner':'Homero Simpson',
	'start_date':dates.days_ago(1)
}


def primera_parte():
	print('Primera anotacion..\n')
	print('El uso de Airflow en la universidad de Springfield \n')

def segunda_parte():
	print('Segunda anotacion..\n')
	print('El otro día mi hija me dijo que Airflow no se utilizaba en la universidad de Springfield, y yo le dije: qué no Lisa? qué  no? \n')


def tercera_parte():
	print('Tercera anotacion..\n')
	for i in range(150):
		print('Púdrete estupido y sensual Flanders ('+str(i+1) +')')



# DAG
with DAG(dag_id='dag_homero',
		 default_args=default_args,
		 schedule_interval = '@daily') as dag:


	#TASK 1
	prueba_python_1 = PythonOperator(	task_id='prueba_python_1',
										python_callable=primera_parte)

	#TASK 2
	prueba_python_2 = PythonOperator(	task_id='prueba_python_2',
									python_callable=segunda_parte)
	#TASK 3
	prueba_python_3 = PythonOperator(	task_id='prueba_python_3',
									python_callable=tercera_parte)

	#TASK 4
	dimeHola = BashOperator(task_id='dimeHola',
							bash_command='echo "dime {{macros.custom_hola.dime_hola(task)}}"')

# ORDEN DE EJECUCION
prueba_python_1 >> dimeHola
