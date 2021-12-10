import json

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException


'''
Requirements:
    1. runs by minutes
    2. branching -> even_group_task[0,2,4,6,8], odd_group_task[1,3,5,7,9]
    3. perform following sub-tasks depending on branch condition
    4. chain() method to take advantage of list-to-list mapping dependency
    5. create final tasks dynamically
    
'''


# Reference Data
MINUTE_ACTIVITY_MAPPING = {
    "0": {"even_odd_check": "even", "activity": "I got zero"},
    "1": {"even_odd_check": "odd", "activity": "I got one"},
    "2": {"even_odd_check": "even", "activity": "I got two"},
    "3": {"even_odd_check": "odd", "activity": "I got three"},
    "4": {"even_odd_check": "even", "activity": "I got four"},
    "5": {"even_odd_check": "odd", "activity": "I got five"},
    "6": {"even_odd_check": "even", "activity": "I got six"},
    "7": {"even_odd_check": "odd", "activity": "I got seven"},
    "8": {"even_odd_check": "even", "activity": "I got eight"},
    "9": {"even_odd_check": "odd", "activity": "I got nine"},
}

@dag(
    start_date=datetime(2021, 12, 1),
    schedule_interval=timedelta(minutes=1),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    catchup=False,
)
def adv_task_flow_api_list_mapping_dynamic():
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

    # divide the workflow into two paths depending on even/odd minute
    even_min = DummyOperator(task_id="even_min")
    odd_min = DummyOperator(task_id="odd_min")

    # This function is used to generate tasks dynamically, at "which_even_minute_activity" Task
    def _get_activity(minute):
        min_last_digit = str(int(minute) % 10)
        activity_id = MINUTE_ACTIVITY_MAPPING[min_last_digit]["activity"].lower().replace(" ", "_")
        print("activity_id:", activity_id)

        if MINUTE_ACTIVITY_MAPPING[min_last_digit]["even_odd_check"] == "even":
            return f"even_min_group_task.{activity_id}"

        return f"odd_min_group_task.{activity_id}"

    # This function is used to perform dynamically created task, at "do_activity" Task - task_id is dynamic though.
    def dynamic_task_gen(min, minute, activity):
        if min == str(int(minute) % 10):
            print(f"The Dag_run's start_time ends in {min}, so the activity should print {activity}.")
        else:
            raise AirflowSkipException


    with TaskGroup("even_min_group_task") as even_min_group_task:
        which_even_minute_activity = BranchPythonOperator(
            task_id="which_even_minute_activity",
            python_callable=_get_activity,
            op_args=[minute],
        )


        for min, min_activity in MINUTE_ACTIVITY_MAPPING.items():
            if min_activity["even_odd_check"] == "even":
                activity = min_activity["activity"]

                do_activity = PythonOperator(
                    task_id = activity.lower().replace(" ", "_"),
                    python_callable = dynamic_task_gen,
                    op_args = (min, minute, min_activity["activity"])
                )

        chain(which_even_minute_activity, do_activity)

    with TaskGroup("odd_min_group_task") as odd_min_group_task:
        which_odd_minute_activity = BranchPythonOperator(
            task_id="which_odd_minute_activity",
            python_callable=_get_activity,
            op_args=[minute],
        )

        for min, min_activity in MINUTE_ACTIVITY_MAPPING.items():
            if min_activity["even_odd_check"] == "odd":
                activity = min_activity["activity"]

                # Dynamic Task Generation
                do_activity = PythonOperator(
                    task_id = activity.lower().replace(" ", "_"),
                    python_callable = dynamic_task_gen,
                    op_args = (min, minute, min_activity["activity"])
                )

        which_odd_minute_activity >> do_activity

    chain(start, check_minute_of_dag_run, [even_min, odd_min], [even_min_group_task, odd_min_group_task], end)

# the dag function must be invoked
dag = adv_task_flow_api_list_mapping_dynamic()