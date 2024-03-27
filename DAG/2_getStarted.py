#Importing our required libraries
from datetime import datetime, timedelta

# we'll need this to instantiate a DAG OBJECT
from airflow import models

# we need this to handle OPERATORS
from airflow.operators.bash import BashOperator

#Here we are going to create DAG object by using required arguments
with models.DAG(
    'FirstDag',
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
#How to define the tasks
    # t1 t2 t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='echo1',
        bash_command='echo hello1',
    )

    t2 = BashOperator(
        task_id='echo2',
        bash_command='echo hello2',
    )
    
    t3 = BashOperator(
        task_id='date',
        bash_command='date',
    )    

    t1 >> t2 >> t3