import json

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup

@dag(
    start_date=datetime(2021, 12, 1),
    schedule_interval="@hourly",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    catchup=False,
)
def advanced_task_flow_api_test():
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    with TaskGroup("etl") as etl:
        @task(multiple_outputs=True)
        def extract():
            sample_data_string = '{"101":301.27, "102":433.21, "103":502.22}'

            data_to_dict = json.loads(sample_data_string)
            return data_to_dict

        @task(multiple_outputs=True)
        def transform(data_dict):
            total_result = 0
            for value in data_dict.values():
                total_result += value

            return {"Total_result": total_result}

        @task
        def load(total_result):
            print(f"Total order value is {total_result:.2f}")

        # This is already a dependency relationship for TaskGroup.
        # TaskGroup can set its own dependency relationship
        extract_task = extract()
        transform_task = transform(extract_task)
        load_task = load(transform_task["Total_result"])

        extract_task >> transform_task >> load_task

    # High-level dependency
    chain(start, etl, end)

# the dag function must be invoked
dag = advanced_task_flow_api_test()