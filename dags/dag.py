import json
import requests
from datetime import datetime

from airflow import models
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
#from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import python_operator


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 7, 13),
}

#endpoint 
service_url = 'http://127.0.0.1:5000/run_process'

SLACK_CONN_ID = "slack"

dag_name = "composer_pipeline_data-content_ad-params"
dag_tags = [
    "production",
    "ETL",
    "schedule",
    "dockerhost",
    "legacy",
    "git: legacy/data-content",
    "input: dwh",
    "input: blocket",
    "output: dwh",
]
schedule_interval = "10 4 * * *"
riskiness = "High"
utility = (
    "Ads Params Etl processes params from verticals as it would be cars, inmo and big sellers, "
    "then store them in DWH as appended method so a historical data would be always "
    "available."
)

def task_fail_slack_alert(context):
    slack_msg = slack_msg_body(
        context,
        riskiness=riskiness,
        utility=utility,
    )
    failed_alert = SlackWebhookOperator(
        task_id="failed_job_alert",
        http_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username="airflow",
        dag=dag,
    )
    return failed_alert.execute(context=context)


with models.DAG(
    dag_name,
    tags=dag_tags,
    schedule_interval=None,
    default_args=default_args,
    max_active_runs=1,
    on_failure_callback=task_fail_slack_alert,
) as dag:
    
    def execute_ad_params():
        response = requests.post(service_url)
        data = response.json()
        return data

    run_content_ad_params = python_operator.PythonOperator(
        task_id="task_run_content_ad_params",
        python_callable=execute_ad_params,
        provide_context=True,
        dag=dag,
    )

    run_content_ad_params