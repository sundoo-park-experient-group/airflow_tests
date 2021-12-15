from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.exceptions import AirflowSkipException, AirflowException, AirflowSensorTimeout, AirflowRescheduleException
from auth import ACCESS_KEY, SECRETE_KEY




@dag(
    start_date=datetime(2021, 12, 1),
    schedule_interval=timedelta(minutes=5),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    catchup=False,
)
def sensor_exmples_with_S3():
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    current_ts = datetime.now()
    current_year_month = datetime.now().strftime("%Y-%m")
    current_month_day = datetime.now().strftime("%m%d")


    def _failure_callback_func(context):
        print(context)
        if isinstance(context['exception'], AirflowSensorTimeout):
            print(context)
            print("sensor timed out")

    # PythongSensor's callable function
    def wait_for_minute_hits_zero(*minute):
        print(minute)
        print(minute[1])
        print(current_ts)
        print(current_year_month)
        print(current_month_day)
        # return not int(minute) % 10
        return True

    # PythonSensor that waits until the callable function  returns True
    waiting_for_python_method = PythonSensor(
        task_id = "waiting_for_python_method",
        poke_interval=60 * 1,
        timeout=60 * 5,
        mode="reschedule",
        python_callable=wait_for_minute_hits_zero,
        on_failure_callback=_failure_callback_func,
        op_args=["{{ dag_run.start_date.strftime('%M') }}", "{{ ts }}"],
    )

    waiting_for_file = FileSensor(
        task_id="waiting_for_file",

        #By default, it takes in arguments in seconds
        poke_interval=60 * 1,

        # This is different from the execution timeout. When th execution times out, the entire DAG fails.
        # When Sensor times out, we can set soft_fail to True and skip it.
        # 60 x 5 = 5minutes
        timeout=60 * 5,

        # the default mode is "poke", best practice is reschedule
        mode="reschedule",

        on_failure_callback=_failure_callback_func,

        # If the path given is a directory, then this sensor will only return true if
        # any files exist inside it (either directly, or within a subdirectory)
        filepath='sample.csv',

        fs_conn_id='fs_local',

        # It will soft fail in the Sensor operation, which will lead to skipping.
        soft_fail=True,

    )

    waiting_for_file_from_S3 = S3KeySensor(
        task_id="waiting_for_file_from_S3",
        poke_interval=60*1,
        timeout=60*5,
        mode="reschedule",
        on_failure_callback=_failure_callback_func,
        aws_conn_id="my_conn_s3",

        # bucket_key is used as an entire path to a file or a directory.

        # Option 1 (To check if a directory exists in S3):
        # bucket_key=f"s3://sundoo-park-sample-bucket-1.0/{current_date}/",

        # Option 2 (To check if any file exists in a specified directory): use a wildcard with full path
        bucket_key=f"s3://sundoo-park-sample-bucket-1.0/{current_year_month}/*{current_month_day}*",

        # bucket_name is needed only when bucket_key doesn't start with s3://
        # bucket_name='',

        # wildcard_match is used to include * in the bucket key. * works like a wildcard.
        wildcard_match=True,
    )



    start >> waiting_for_python_method >> waiting_for_file >> waiting_for_file_from_S3 >> end

dag = sensor_exmples_with_S3()