import logging

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.dms_create_task import DmsCreateTaskOperator
from airflow.providers.amazon.aws.operators.dms_start_task import DmsStartTaskOperator
from airflow.providers.amazon.aws.operators.dms_stop_task import DmsStopTaskOperator
from airflow.providers.amazon.aws.operators.dms_delete_task import DmsDeleteTaskOperator
from airflow.providers.amazon.aws.sensors.dms_task import DmsTaskCompletedSensor

default_args = {
    'owner': 'crn-data',
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


env = 'dev'
REPLICATION_TASK_ID = 'rds-to-crm-redshift-test'
SOURCE_ENDPOINT_ARN = 'arn:aws:dms:us-east-1:341484775232:endpoint:STD2AIN4MHPTLCYRLKNGYPHDUSQM7SQLGDKZDHY'
TARGET_ENDPOINT_ARN = 'arn:aws:dms:us-east-1:341484775232:endpoint:4L3AIBD3U4PW37TNROXLBCLDRTDPVI5MO2RG2CA'
REPLICATION_INSTANCE_ARN = 'arn:aws:dms:us-east-1:341484775232:rep:JZ6JLH3PSJN4HZK7AXWYZ22YKLGEKWEO7QUE52Q'
TABLE_MAPPINGS = {
    "rules": [
        {
            "rule-type": "transformation",
            "rule-id": "1",
            "rule-name": "1",
            "rule-target": "table",
            "object-locator": {
                "schema-name": "treat",
                "table-name": "points_type"
            },
            "rule-action": "replace-prefix",
            "value": "crn_points_",
            "old-value": "points_"
        },
        {
            "rule-type": "selection",
            "rule-id": "8",
            "rule-name": "8",
            "object-locator": {
                "schema-name": "treat",
                "table-name": "points_type"
            },
            "rule-action": "include",
            "filters": []
        }
    ]
}
# TABLE_MAPPINGS = {
#     "rules": [
#         {
#             "rule-type": "transformation",
#             "rule-id": "1",
#             "rule-name": "1",
#             "rule-target": "table",
#             "object-locator": {
#                 "schema-name": "treat",
#                 "table-name": "treat_offer"
#             },
#             "rule-action": "replace-prefix",
#             "value": "crn_treat_",
#             "old-value": "treat_"
#         },
#         {
#             "rule-type": "transformation",
#             "rule-id": "2",
#             "rule-name": "2",
#             "rule-target": "table",
#             "object-locator": {
#                 "schema-name": "treat",
#                 "table-name": "points_used"
#             },
#             "rule-action": "replace-prefix",
#             "value": "crn_points_",
#             "old-value": "points_"
#         },
#         {
#             "rule-type": "transformation",
#             "rule-id": "3",
#             "rule-name": "3",
#             "rule-target": "table",
#             "object-locator": {
#                 "schema-name": "treat",
#                 "table-name": "points_type"
#             },
#             "rule-action": "replace-prefix",
#             "value": "crn_points_",
#             "old-value": "points_"
#         },
#         {
#             "rule-type": "transformation",
#             "rule-id": "4",
#             "rule-name": "4",
#             "rule-target": "table",
#             "object-locator": {
#                 "schema-name": "treat",
#                 "table-name": "cust_loyalty_tier"
#             },
#             "rule-action": "replace-prefix",
#             "value": "crn_cust_",
#             "old-value": "cust_"
#         },
#         {
#             "rule-type": "transformation",
#             "rule-id": "5",
#             "rule-name": "5",
#             "rule-target": "table",
#             "object-locator": {
#                 "schema-name": "crn",
#                 "table-name": "menu_item_xref"
#             },
#             "rule-action": "replace-prefix",
#             "value": "crn_menu_",
#             "old-value": "menu_"
#         },
#         {
#             "rule-type": "transformation",
#             "rule-id": "6",
#             "rule-name": "6",
#             "rule-target": "schema",
#             "object-locator": {
#                 "schema-name": "treat"
#             },
#             "rule-action": "replace-prefix",
#             "value": "crm",
#             "old-value": "treat"
#         },
#         {
#             "rule-type": "transformation",
#             "rule-id": "7",
#             "rule-name": "7",
#             "rule-target": "schema",
#             "object-locator": {
#                 "schema-name": "crn"
#             },
#             "rule-action": "replace-prefix",
#             "value": "crm",
#             "old-value": "crn"
#         },
#         {
#             "rule-type": "selection",
#             "rule-id": "8",
#             "rule-name": "8",
#             "object-locator": {
#                 "schema-name": "treat",
#                 "table-name": "treat_offer"
#             },
#             "rule-action": "include",
#             "filters": []
#         },
#         {
#             "rule-type": "selection",
#             "rule-id": "9",
#             "rule-name": "9",
#             "object-locator": {
#                 "schema-name": "treat",
#                 "table-name": "points_used"
#             },
#             "rule-action": "include",
#             "filters": []
#         },
#         {
#             "rule-type": "selection",
#             "rule-id": "10",
#             "rule-name": "10",
#             "object-locator": {
#                 "schema-name": "treat",
#                 "table-name": "points_type"
#             },
#             "rule-action": "include",
#             "filters": []
#         },
#         {
#             "rule-type": "selection",
#             "rule-id": "11",
#             "rule-name": "11",
#             "object-locator": {
#                 "schema-name": "treat",
#                 "table-name": "cust_loyalty_tier"
#             },
#             "rule-action": "include",
#             "filters": []
#         },
#         {
#             "rule-type": "selection",
#             "rule-id": "12",
#             "rule-name": "12",
#             "object-locator": {
#                 "schema-name": "crn",
#                 "table-name": "customer_activity"
#             },
#             "rule-action": "include",
#             "filters": []
#         },
#         {
#             "rule-type": "selection",
#             "rule-id": "13",
#             "rule-name": "13",
#             "object-locator": {
#                 "schema-name": "crn",
#                 "table-name": "menu_item_xref"
#             },
#             "rule-action": "include",
#             "filters": []
#         },
#         {
#             "rule-type": "transformation",
#             "rule-id": "14",
#             "rule-name": "14",
#             "rule-target": "table",
#             "object-locator": {
#                 "schema-name": "crn",
#                 "table-name": "customer_activity"
#             },
#             "rule-action": "replace-prefix",
#             "value": "crn_customer_",
#             "old-value": "customer_"
#         }
#     ]
# }

redshift_conn_id = f'crm_redshift_{env}'
aws_connection = 'aws-default'


@dag(dag_id='rds_to_redshift',
     default_args=default_args,
     schedule_interval='0 10 * * *',
     start_date=days_ago(1),
     catchup=True,
     tags=[f'crn-data-{env}'])
def rds_to_redshift():
    """
    Copies RTS RDS data to CRN Redshift
    """
    # [START howto_dms_operators]
    create_task = DmsCreateTaskOperator(
        task_id='create_task',
        replication_task_id=REPLICATION_TASK_ID,
        source_endpoint_arn=SOURCE_ENDPOINT_ARN,
        target_endpoint_arn=TARGET_ENDPOINT_ARN,
        replication_instance_arn=REPLICATION_INSTANCE_ARN,
        table_mappings=TABLE_MAPPINGS,
    )

    start_task = DmsStartTaskOperator(
        task_id='start_task',
        replication_task_arn=create_task.output,
    )

    wait_for_completion = DmsTaskCompletedSensor(
        task_id='wait_for_completion',
        replication_task_arn=create_task.output,
    )

    stop_task = DmsStopTaskOperator(
        task_id='delete_task',
        replication_task_arn=create_task.output,
    )

    start_task >> wait_for_completion >> stop_task

rds_to_redshift = rds_to_redshift()