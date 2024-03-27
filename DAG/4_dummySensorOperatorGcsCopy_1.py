import datetime

from airflow import models

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.gcs_sensor  import GoogleCloudStorageObjectSensor
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators import bash_operator
from airflow.utils.dates import days_ago
from datetime import date, timedelta


#To define variables in Airflow UI and use it in DAG code
from airflow.models import Variable

# Make sure to update these values as per your environment
#project_id = "gcp26-415706"
#region = "us-central1"

#To define variables in Airflow UI and use it in DAG code
project_id = Variable.get('project_id')
region = Variable.get('region')

#To verify file availability using Sensor operator building below lines of the code
bucket_name = 'usecase5'
today = datetime.datetime.today()
file_prefix="source/dummy_"
file_suffix=".csv"
file_date=today.strftime('%Y-%m-%d')
object_name=file_prefix+file_date+file_suffix

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "project_id": project_id,
    "region": region,
    "start_date": days_ago(1)
}

with models.DAG(
    "dummySensorGCSOpertorsVariable",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval=datetime.timedelta(days=1), 
) as dag:

# Define tasks using DummyOperator
    start_task = DummyOperator(
        task_id='start_task',
        dag=dag,
    )
    
    # Use the GCS Object Sensor to wait for the object to exist
    gcs_object_sensor_task = GoogleCloudStorageObjectSensor(
        task_id='File_Sesnor_Check',
        bucket=bucket_name,
        object=object_name,
        timeout=120,  # Set a timeout in seconds
        poke_interval=10,  # How often to check for the object (in seconds)
        mode='poke',  # Use 'reschedule' mode if you want retries with backoff
    )
    
    #As per requirement create bucket to copy files from existing bucket to new bucket
    createNewBucket = GCSCreateBucketOperator(
            task_id="createNewBucket",
            bucket_name="u4b2cdebatch420",
    )

    #To copy files from one bucket to other bucket    
    copy_files = GCSToGCSOperator(
        task_id='copy_files',
        source_bucket=bucket_name,
        source_object='source/*.json',
        destination_bucket='u4b2cdebatch420',
    )

    end_task = DummyOperator(
        task_id='end_task',
        dag=dag,
    )
    
    # Define the workflow by setting the task dependencies
    start_task >> gcs_object_sensor_task >> createNewBucket >> copy_files >> end_task

