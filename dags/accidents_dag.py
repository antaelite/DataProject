
import pendulum
import pandas as pd
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import libs.wrangling as wranglingLib
#  Définition du DAG Airflow

logger = logging.getLogger(__name__)  # Airflow captures this per task
api_url = "https://www.dnd5eapi.co/api" # public D&D 5e API
output_folder = "/opt/airflow/data" # ensure this folder exists and is writable

# --- Failure callback for rich console logs ---
def failure_alert(context):
    exc = context.get("exception")
    ti = context.get("ti")
    logger.error(
        "Task FAILED: dag=%s task=%s run_id=%s try=%s",
        getattr(ti, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        context.get("run_id"),
        getattr(ti, "try_number", "?"),
    )
    # Full traceback in the task log:
    logger.exception(exc)

# --- DAG config ---
START_DATE = pendulum.datetime(2025, 11, 5, tz="UTC")

with DAG(
    dag_id="accidents_dag",
    start_date=START_DATE,
    schedule="0 0 * * *",          # daily at 00:00 UTC
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": failure_alert,
    },
    template_searchpath=["/opt/airflow/data/"], 
    tags=["velo", "lyon", "data-cleaning"],
) as dag:


    cleaning_task = PythonOperator(
        task_id="clean_accidents",
        python_callable=wranglingLib.accidents_cleaning,
    )

    cleaning_task
