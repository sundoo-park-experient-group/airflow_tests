import boto3

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
from auth import ACCESS_KEY, SECRETE_KEY
# from airflow.utils.weekday import WeekDay
# from airflow.utils.edgemodifier import Label
# from airflow.operators.weekday import BranchDayOfWeekOperator

default_args= {
    'start_date': datetime(2021, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def pushS3():

    session = boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRETE_KEY)
    resource = session.resource('s3')
    client = session.client('s3')
    buckets = ["sundoo-park-sample-bucket-1.0", "sundoo-park-sample-bucket-2.0"]

    bucket_check = False
    for bucket in buckets:
        bucket_name = bucket
        try:
            response = client.head_bucket(Bucket = bucket)
            if response:
                print(f"Bucket: {bucket} exists")
                resourceBucket = resource.Bucket("sundoo-park-sample-bucket-1.0")
                file_list = [x.key for x in resourceBucket.objects.all()]
                print("\t",file_list,"\n")

        except Exception as e:
            print(f"Bucket: {bucket} doesn't exist")
            pass


with DAG('local_to_S3', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    local_to_S3 = PythonOperator(
        task_id="local_to_S3",
        python_callable=pushS3,
    )

    # label_test = Label(label="label_test")

    custom_bash_command = """
        echo "dag_run: {{ dag_run }}"
        echo "ds: {{ds}}"
        echo "ds_nodash: {{ds_nodash}}"
        echo "ts: {{ts}}"
        echo "dag_run.start_date: {{dag_run.start_date}}"
        echo "dag_run.start_date.strftime('%A'): {{dag_run.start_date.strftime('%A')}}"
        echo "dag_run.start_date.strftime('%Y-%m-%d'): {{dag_run.start_date.strftime('%Y-%m-%d')}}"
        echo {{ dag_run.start_date.strftime('%m') }}
    """

    bash_task = BashOperator(
        task_id = "bash_task",
        bash_command = custom_bash_command
    )

    chain(local_to_S3, bash_task)
