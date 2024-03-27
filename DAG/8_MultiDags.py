#Importing our required libraries
from datetime import datetime, timedelta

# we'll need this to instantiate a DAG OBJECT
from airflow import models

# we need this to handle OPERATORS
from airflow.operators.bash import BashOperator

#Here we are going to create DAG object by using required arguments
with models.DAG(
    'MultiDag1',
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
) as dag1:

#How to define the tasks
    # t1 t2 t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='echo1',
        bash_command='echo hello1',
        dag=dag1,
    )

    t2 = BashOperator(
        task_id='echo2',
        bash_command='echo hello2',
        dag=dag1,
    )
    
    t3 = BashOperator(
        task_id='date',
        bash_command='date',
        dag=dag1,
    )    

    t1 >> t2 >> t3
    
with models.DAG(
    'MultiDag2',
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
) as dag2:
#How to define the tasks
    # t1 t2 t3 are examples of tasks created by instantiating operators
    t4 = BashOperator(
        task_id='echo11',
        bash_command='echo hello1',
        dag=dag2,
    )

    t5 = BashOperator(
        task_id='echo21',
        bash_command='echo hello2',
        dag=dag2,
    )
    
    t6 = BashOperator(
        task_id='date1',
        bash_command='date',
        dag=dag2,
    )    

    t4 >> t5 >> t6
