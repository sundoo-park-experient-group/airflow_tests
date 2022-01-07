from crndata_s3.custom_operator import S3toLocalFSOperator
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

@dag(
    start_date=datetime(2021, 12, 1),
    schedule_interval=timedelta(minutes=5),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    catchup=False,
)
def custom_operator_test():
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end", trigger_rule="none_failed")

    s3toLocalFileSystem = S3toLocalFSOperator(
        task_id="s3toLocalFileSystem",
        bucket_name="sundoo-park-sample-bucket-1.0",
        key_name="2021-12/sample_a.csv",
        file_path="/Documents/Documents/sample_a.csv",
        s3_conn_id="my_conn_s3",
    )
    start >> s3toLocalFileSystem >> end

dag = custom_operator_test()