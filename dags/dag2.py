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
    "start_date": datetime(2023, 10, 1),  # Fecha de inicio predeterminada
    "end_date": datetime(2023, 10, 1),  # Fecha de fin predeterminada
    # Otros argumentos por defecto...
}

SERVICE_SENSOR_URL = "https://py-scp-pipelines-healthchek-nasdocrtnq-ue.a.run.app"
SERVICE_URL = "https://py-scp-kpi-operacionales-nasdocrtnq-ue.a.run.app"

# Define el DAG...
def get_date():
    start_date = "{{ dag_run.conf['start_date'] if dag_run.conf and 'start_date' in dag_run.conf else execution_date }}"
    end_date = "{{ dag_run.conf['end_date'] if dag_run.conf and 'end_date' in dag_run.conf else execution_date }}"
    return {"start_date": start_date, "end_date": end_date}

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

    # Resto del DAG...

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
            poke_interval=300,
            mode="reschedule",
            timeout=600,
            python_callable=check_partition,
            op_kwargs={"table": table},
        )

        start_job >> check_table_partition_exists >> call_rundeck_job

    call_rundeck_job >> end_job
