#Importing our required libraries
from datetime import datetime, timedelta

# we'll need this to instantiate a DAG OBJECT
from airflow import models

# we need this to handle OPERATORS
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

#Here we are going to create DAG object by using required arguments
with models.DAG(
    'ChildDag',
    # These args will be used in to each operator
    # We can overrride them per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dev'],
) as dag:

    start_task1 = DummyOperator(
    task_id='start_task1',
    dag=dag,
    )
    
    wait_for_parent_task = ExternalTaskSensor(
    task_id='wait_for_parent_task',
    external_dag_id='ParentDag1',  # Specify the parent DAG's ID
    external_task_id='sleep_task',  # Specify the parent task's ID
    mode='poke',  
    poke_interval=60,  
    timeout=600,
    dag=dag,
	)
 
    end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
    ) 
   

    start_task1 >> wait_for_parent_task >> end_task