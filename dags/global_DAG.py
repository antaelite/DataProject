import pendulum
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

logger = logging.getLogger(__name__)  # Airflow captures this per task
api_url = "https://data.grandlyon.com/geoserver/ogc/features/v1/collections/metropole-de-lyon:jcd_jcdecaux.jcdvelov/items" # public D&D 5e API
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
START_DATE = pendulum.datetime(2025, 11, 1, tz="UTC")

def create_dag():
    import libs.ingestion as ingestionLib
    with DAG(
    dag_id="velov_dag",
    #start_date=START_DATE,
    # schedule="0,20,40 * * * *",         # every 20 minutes
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": failure_alert,
    },
    template_searchpath=[output_folder],
    tags=["ingestion"],
    ) as dag:
    
        # -------- Tasks --------
        start = DummyOperator(
            task_id="start",)

        first_node = PythonOperator(
            task_id="ingest_velov_data",
            python_callable=ingestionLib.get_velov_data,
            op_kwargs={"api_url": api_url, "limit": 100, "desired_count": 2000},
        )
        end = DummyOperator(
            task_id="end",)

        # -------- Task dependencies --------
        start >> first_node >> end

    return dag

velov_dag = create_dag()