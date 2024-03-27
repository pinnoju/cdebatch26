#Importing our required libraries
from datetime import datetime, timedelta
import time

# we'll need this to instantiate a DAG OBJECT
from airflow import models

# we need this to handle OPERATORS
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def sleep_function(**kwargs):
    # Sleep for 60 seconds
    time.sleep(60)
    print("Task has slept for 60 seconds.")
    
#Here we are going to create DAG object by using required arguments
with models.DAG(
    'ParentDag1',
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

    start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
    )
    
    sleep_task = PythonOperator(
    task_id='sleep_task',
    python_callable=sleep_function,
    provide_context=True,
    dag=dag,
    )
    
    end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
    ) 
   

    start_task >> sleep_task >> end_task