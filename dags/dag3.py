from __future__ import print_function

import json
from datetime import datetime, timedelta
import requests

from airflow import models
from airflow.sensors.python import PythonSensor
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook


SERVICE_SENSOR_URL = "https://py-scp-pipelines-healthchek-nasdocrtnq-ue.a.run.app"
sshHook = SSHHook(ssh_conn_id="ssh_public_pentaho")

# Calculate the date
def get_date():
    start_date = "{{ dag_run.conf['start_date'] if dag_run.conf and 'start_date' in dag_run.conf else False}}"
    end_date = "{{ dag_run.conf['end_date'] if dag_run.conf and 'end_date' in dag_run.conf else False}}"
    if start_date and end_date:
        date = {"start_date": start_date, "end_date": end_date}
    else:
        execution_date = "{{ dag_run.logical_date }}"
        execution_date = execution_date - timedelta(days=1)
        execution_date = execution_date.date().strftime('%Y-%m-%d')
        date = {"start_date": execution_date, "end_date": execution_date}
    return date


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    "start_date": datetime(2022, 5, 23),
}

with models.DAG(
    "trigger_bi-insight-dw_blocketdb-kpis_operacionales",
    tags=[
        "production",
        "ETL",
        "trigger",
        "core",
        "git: legacy/bi-insight",
        "input: dwh",
        "output: dwh",
    ],
    schedule_interval=None,
    default_args=default_args,
    max_active_runs=1
) as dag:

    def check_partition(**kwargs):
        test = requests.post(
            f"{SERVICE_SENSOR_URL}/{kwargs['table']}",
            json=kwargs['dates']
        )
        return (
            True if json.loads(test.content)["body"]["status_table"] == "OK" else False
        )

    run_kpi_operacionales = SSHOperator(
        task_id="run_kpi_operacionales",
        ssh_hook=sshHook,
        command="sh /opt/dw_schibsted/yapo_bi/dw_blocketdb/kpis_operacionales/run_jb_kpis_operacionales.sh "
    )

    for table in ["stg_fact_day_category_main_xiti", "ods_leads_daily"]:
        check_table_partition_exists = PythonSensor(
            task_id="sensor_partition_exists_{}".format(table),
            poke_interval=300,
            mode="reschedule",
            timeout=600,
            python_callable=check_partition,
            op_kwargs={"table": table, "dates": get_date()},
        )

        check_table_partition_exists >> run_kpi_operacionales