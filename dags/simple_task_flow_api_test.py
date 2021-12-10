import json

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.baseoperator import chain

@dag(
    start_date=datetime(2021, 12, 1),
    schedule_interval="@hourly",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    catchup=False,
)
def simple_task_flow_api_test():
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

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

    extract_task = extract()
    transform_task = transform(extract_task)
    load_task = load(transform_task["Total_result"])

    # I can't use chain dependency between XCOM-loaded TASK instances. Only bitshift operator is supported
    # TypeError: Chain not supported between instances of <class 'airflow.models.xcom_arg.XComArg'> and <class 'airflow.models.xcom_arg.XComArg'>
    # chain(start, extract_task, transform_task, load_task, end)

    start >> extract_task >> transform_task >> load_task >> end

# the dag function must be invoked
dag = simple_task_flow_api_test()