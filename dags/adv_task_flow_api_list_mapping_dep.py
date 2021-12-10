import json

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup


'''
Requirements:
    1. runs by minutes
    2. branching -> even_group_task[0,2,4,6,8], odd_group_task[1,3,5,7,9]
    3. following tasks depending on branch condition
    4. chain() method to take advantage of list-to-list mapping dependency
    5. TriggerRule: run End task only all tasks are successfully run.
'''

@dag(
    start_date=datetime(2021, 12, 1),
    schedule_interval=timedelta(minutes=1),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    catchup=False,
)
def adv_task_flow_api_list_mapping_dep():
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    # each Dag run's start_date's minute
    minute = "{{ dag_run.start_date.strftime('%M') }}"

    # Checking if minute is even or odd
    def branch_func(minute):
        if not int(minute) % 2:
            print(minute)
            return "even_min"
        else:
            print(minute)
            return "odd_min"


    check_minute_of_dag_run = BranchPythonOperator(
        task_id="check_minute_of_dag_run",
        python_callable=branch_func,
        op_args=[minute],
    )

    # first divide two tasks depending on even/odd min
    even_min = DummyOperator(task_id="even_min")
    odd_min = DummyOperator(task_id="odd_min")


    with TaskGroup("even_min_group_task") as even_min_group_task:
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

    with TaskGroup("odd_min_group_task") as odd_min_group_task:
        @task(multiple_outputs=True)
        def extract():
            sample_data_string = '{"101":301.27, "102":433.21, "103":502.22}'

            data_to_dict = json.loads(sample_data_string)
            return data_to_dict

        @task(multiple_outputs=True)
        def transform(data_dict):
            total_result = 10000
            for value in data_dict.values():
                total_result -= value

            return {"Total_result": total_result}

        @task
        def load(total_result):
            print(f"Total order value is {total_result:.2f}")

        # This is already a dependency relationship for TaskGroup.
        # TaskGroup can set its own dependency relationship
        extract_task = extract()
        transform_task = transform(extract_task)
        load_task = load(transform_task["Total_result"])

    # High-level dependency
    chain(start, check_minute_of_dag_run, [even_min, odd_min], [even_min_group_task, odd_min_group_task], end)

# the dag function must be invoked
dag = adv_task_flow_api_list_mapping_dep()