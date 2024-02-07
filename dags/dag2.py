from __future__ import print_function

import json
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests

from airflow import models
from airflow.operators import bash_operator, python_operator
from airflow.sensors.python import PythonSensor

default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 3, 15),
}

SERVICE_SENSOR_URL = "https://py-scp-pipelines-healthchek-nasdocrtnq-ue.a.run.app"
SERVICE_URL = "https://py-scp-kpi-operacionales-nasdocrtnq-ue.a.run.app"

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.


with models.DAG(
    "trigger_kpi_operacionales",
    tags=[
        "production",
        "ETL",
        "trigger",
        "core",
        "git: traffic",
        "input: dwh",
        "output: dwh",
    ],
    schedule_interval=None,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    
    def check_partition(**kwargs):
        test = requests.get(f"{SERVICE_SENSOR_URL}/{kwargs['table']}")
        #content = json.loads(test.content)
        #status_table = content["body"]["status_table"]

        try:
            content_json = test.json()
            status_table = content_json.get("body", {}).get("status_table")
            return status_table == "OK"
        except json.decoder.JSONDecodeError:
            return False

    # Process data block
    def call_job(**kwargs):
        logging.info("Calling rundeck job")
        data = requests.get(SERVICE_URL + "/rundeck/call")
        logging.info("Done")
        return data.json()

    start_job = bash_operator.BashOperator(
        task_id="Start", bash_command="echo Success."
    )

    end_job = bash_operator.BashOperator(task_id="End", bash_command="echo Success.")

    call_rundeck_job = python_operator.PythonOperator(
        task_id="call_rundeck_Test_Content_Job_KPIS_operacionales_Crontab",
        provide_context=True,
        python_callable=call_job,
    )

    for table in ["stg_fact_day_category_main_xiti", "ods_leads_daily"]:
        check_table_partition_exists = PythonSensor(
            task_id="sensor_partition_exists_{}".format(table),
            poke_interval=300
            mode="reschedule",
            timeout=600
            python_callable=check_partition,
            op_kwargs={"table": table},
        )

        start_job >> check_table_partition_exists >> call_rundeck_job

    call_rundeck_job >> end_job
