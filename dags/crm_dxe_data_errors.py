import logging

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.helpers import parse_template_string
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from crndata_s3.s3_operators2 import S3IngestOperator2
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'crn-data',
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

env = 'devs'
# env = 'dev'
s3_bucket = f'dxe-data-errors-{env}'
s3_prefix = "rds-to-s3/{{ds_nodash}}/"
s3_keys = ["crn.menu_item_xref", "treat.cust_loyalty_tier", "treat.points_type",
           "treat.points_used", "treat.treat_offer"]
target_bucket = f"crn-data-staging-{env}"
target_prefix = 'incoming/rds-to-s3/{{ds_nodash}}/'
redshift_conn_id = f'crm_redshift_{env}'
iam_role_arn = "IAM_ROLE 'arn:aws:iam::473400450607:role/bdap-redshift-discovery'"
aws_connection = 'aws-default'


@dag(dag_id='crm_dxe_data_errors',
     default_args=default_args,
     schedule_interval='0 10 * * *',
     start_date=days_ago(1),
     catchup=True,
     tags=[f'crn-data-{env}'])
def crm_dxe_data_errors():
    """
    Copies RDS extracted files to CRN Redshift
    Dependency: Create CRM connection on Airflow
    Executed command: COPY crn.giftcard_transaction FROM 's3://customer-data-crn-prod/giftcard.transaction/%(date)s/'
        IAM_ROLE 'arn:aws:iam::184924468174:role/bdap-redshift-discovery' CSV MAXERROR AS 5;
    """

    def _failure_callback_func(context):
        logging.info(f"Found no files to ingest in {s3_bucket}/{s3_prefix}")
        print(context)

    # Sensor to check if respectively-dated directory exists in S3
    checking_file_in_S3 = S3KeySensor(
        task_id="checking_file_in_S3",
        poke_interval=60 * 1,
        timeout=60 * 1,
        mode="reschedule",
        on_failure_callback=_failure_callback_func,
        aws_conn_id=aws_connection,

        # bucket_key is used as an entire path to a file or a directory.
        # bucket_name is not needed when bucket_key start with s3://
        bucket_key=f"s3://{s3_bucket}/{s3_prefix}",
        soft_fail=True,
    )
    with TaskGroup("ingest_process_load") as ingest_process_load:
        # Proceed if we really moved any files
        def files_to_process(bucket, prefix, **_):
            s3_hook = S3Hook(aws_conn_id=aws_connection, verify=True)
            objects: list = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
            logging.info(f"Found {len(objects)} files to process in {bucket}/{prefix}")
            return len(objects) > 0

        for key in s3_keys:
            s3_key_op = S3IngestOperator2(task_id=f"Ingest_{key}_Files",
                                         aws_connection=aws_connection,
                                         input_bucket=s3_bucket,
                                         input_prefix=f"{s3_prefix}{key}/",
                                         ingest_bucket=target_bucket,
                                         ingest_prefix=f"{target_prefix}{key}/",
                                         delimiter="received/",
                                         ignore_file_pattern=["datalake_", "done_"],
                                         received_file_prefix='rds-to-s3/{{ds_nodash}}/received/',
                                         file_extension='*',
                                         delete_after=True,
                                         )

            proceed_op = ShortCircuitOperator(task_id=f'Process_{key}_Files',
                                              python_callable=files_to_process,
                                              op_kwargs={'bucket': target_bucket, 'prefix': target_prefix+key},
                                              )

            schema, table = key.split(".")
            s3_to_redshift_op = S3ToRedshiftOperator(task_id=f"{key}_to_Redshift",
                                                     schema=schema,
                                                     table=table,
                                                     s3_bucket=target_bucket,
                                                     s3_key=f"{target_prefix}{key}/",
                                                     redshift_conn_id=redshift_conn_id,
                                                     aws_conn_id=aws_connection,
                                                     autocommit=True, truncate_table=False,
                                                     copy_options=["FORMAT CSV DELIMITER ',' "
                                                                   "FILLRECORD "
                                                                    "TIMEFORMAT 'auto' "]
                                                     )
            # Task-group dependency
            chain(s3_key_op, proceed_op, s3_to_redshift_op)

    # email_notification_op = EmailOperator(
    #     task_id="send_email_if_fail_one",
    #     to="spark@experientgroup.com",
    #     subject="ALERT: Airflow {{ dag_run.dag_id }} Job Failed",
    #     html_content="<h2>Airflow {{ dag_run.dag_id }} job failed on {{ ds }}<\h2>",
    #     trigger_rule="all_done",
    # )

    # High-level dependency
    checking_file_in_S3 >> ingest_process_load #>> email_notification_op

crm_dxe_data_errors = crm_dxe_data_errors()