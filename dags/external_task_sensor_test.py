from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
from airflow.sensors.external_task import ExternalTaskSensor, ExternalTaskMarker
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.exceptions import AirflowSkipException, AirflowException, AirflowSensorTimeout, AirflowRescheduleException

@dag(
    start_date=datetime(2021, 12, 1),
    schedule_interval=timedelta(minutes=5),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    catchup=False,
)
def parent_dag():
    parent_task = ExternalTaskSensor(
        task_id="parent_task",
        external_dag_id="child_dag",
        external_task_id="child_task1",
        allowed_states="",
        failed_states="",
        timeout="",
        execution_delta="",
        execution_date_fn="",

    )

parent_dag = parent_dag()

@dag(
    start_date=datetime(2021, 12, 1),
    schedule_interval=timedelta(minutes=5),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    catchup=False,
)
def child_dag():

    current_year_month = datetime.now().strftime("%Y-%m")
    current_month_day = datetime.now().strftime("%m%d")

    def _failure_callback_func(context):
        print(context)

    child_task1 = S3KeySensor(
        task_id="waiting_for_file_from_S3",
        poke_interval=60 * 1,
        timeout=60 * 5,
        mode="reschedule",
        on_failure_callback=_failure_callback_func,
        aws_conn_id="my_conn_s3",
        bucket_key=f"s3://sundoo-park-sample-bucket-1.0/{current_year_month}/*{current_month_day}*",
        wildcard_match=True,
    )
    child_task2 = DummyOperator(task_id="child_task2")

    child_task1 >> child_task2

child_dag = child_dag()

