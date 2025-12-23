import airflow
import datetime
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import pendulum
import logging
import os
import sys

# Ajout du dossier libs au path pour que Python trouve nos modules
sys.path.append(os.path.dirname(__file__))

# IMPORT DE NOS MODULES PERSO
import libs.ingestion as ingestionLib
import libs.wrangling as wranglingLib

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
START_DATE = pendulum.datetime(2025, 11, 1, tz="Europe/Paris")
API_URL = "https://data.grandlyon.com/geoserver/ogc/features/v1/collections/metropole-de-lyon:jcd_jcdecaux.jcdvelov/items"

# Chemins de fichiers 
BASE_DIR = "/opt/airflow/data"
RAW_FILE_PATH = os.path.join(BASE_DIR, "landing", "velov_raw.csv")
CLEAN_FILE_PATH = os.path.join(BASE_DIR, "staging", "velov_clean.csv")

# --- Failure callback ---
def failure_alert(context):
    exc = context.get("exception")
    ti = context.get("ti")
    logger.error(
        "Task FAILED: dag=%s task=%s run_id=%s",
        getattr(ti, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        context.get("run_id"),
    )
    logger.exception(exc)

# --- DAG DEFINITION ---
with DAG(
    dag_id="velov_dag",
    start_date=START_DATE,
    schedule="*/20 * * * *",  # Toutes les 20 mins
    catchup=False,
    max_active_tasks=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": failure_alert,
    },
    tags=["lyon", "velov", "etl"],
) as dag:

    # 1. Start
    start = DummyOperator(task_id="start")
    
    amadoutest = PythonOperator(
        task_id="amadou_test_task",
        python_callable=ingestionLib.amadou_test_function,
    )

    # 2. Ingestion (Landing)
    ingest_task = PythonOperator(
        task_id="ingest_velov_data",
        python_callable=ingestionLib.get_velov_data,
        op_kwargs={
            "api_url": API_URL, 
            "output_path": RAW_FILE_PATH, # <-- On force le chemin ici
            "limit": 100,
            "desired_count": 300,  # Limite par défaut pour éviter de longues exécutions
            "max_loops": 50        # Safeguard pour arrêter pagination excessive
        },
    )

    # 3. Wrangling (Staging)
    wrangle_task = PythonOperator(
        task_id="clean_velov_data",
        python_callable=wranglingLib.clean_velov_data,
        op_kwargs={
            "input_path": RAW_FILE_PATH,   
            "output_path": CLEAN_FILE_PATH 
        },
    )

    # 4. End
    end = DummyOperator(task_id="end")

    # --- ORCHESTRATION (PIPELINE) ---
    start >> ingest_task >> wrangle_task >> end