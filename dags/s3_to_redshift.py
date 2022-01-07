from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dummy import DummyOperator
# from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

@task
def add_sample_data_to_s3():
    s3_hook = S3Hook(aws_conn_id="my_conn_s3")
    s3_hook.load_string(
        string_data="0, airflow"
    )

@dag(
    start_date=datetime(2021, 12, 1),
    schedule_interval=timedelta(minutes=5),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    catchup=False,
)
def s3_to_redshift():
    start = DummyOperator(task_id="start")

    operation = add_sample_data_to_s3()

    end = DummyOperator(task_id="end")

    start >> operation >> end


s3_to_redshift = s3_to_redshift()
