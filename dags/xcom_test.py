from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain

from random import uniform
from datetime import datetime, timedelta


def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')

    # This is one way to push the accuracy value to xcom
    # return accuracy

    # Another way to push the accuracy value to xcom
    ti.xcom_push(key="model_accuracy", value=accuracy)


def _choose_best_model(ti):
    print('choose best model')
    accuracies = ti.xcom_pull(key="model_accuracy", task_ids=[
        "processing_tasks.training_model_a",
        "processing_tasks.training_model_b",
        "processing_tasks.training_model_c",
    ])
    print(max(accuracies))


@dag(
    start_date=datetime(2021, 12, 1),
    schedule_interval=timedelta(minutes=5),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    catchup=False,
)
def xcom_test():
    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3'
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        training_model_a = PythonOperator(
            task_id='training_model_a',
            python_callable=_training_model
        )

        training_model_b = PythonOperator(
            task_id='training_model_b',
            python_callable=_training_model
        )

        training_model_c = PythonOperator(
            task_id='training_model_c',
            python_callable=_training_model
        )

    choose_model = PythonOperator(
        task_id='task_4',
        python_callable=_choose_best_model,
        trigger_rule='none_failed'
    )

    downloading_data >> processing_tasks >> choose_model

dag = xcom_test()

