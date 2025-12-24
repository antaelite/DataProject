import pendulum
import logging
import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import libs.ingestion as ingestionLib
import libs.wrangling as wranglingLib
import libs.production as productionLib

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
START_DATE = pendulum.datetime(2025, 11, 1, tz="Europe/Paris")
VELOV_API_URL = "https://data.grandlyon.com/geoserver/ogc/features/v1/collections/metropole-de-lyon:jcd_jcdecaux.jcdvelov/items"
ACCIDENTS_API_URL = "https://www.dnd5eapi.co/api"

# Chemins de fichiers 
BASE_DIR = "/opt/airflow/data"
RAW_FILE_PATH = os.path.join(BASE_DIR, "landing", "velov_raw.csv")
CLEAN_FILE_PATH = os.path.join(BASE_DIR, "staging", "velov_clean.csv")

# --- Failure callback ---
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
    logger.exception(exc)

default_args_dict = {
    'owner' : "airflow",
    'start_date' : START_DATE,
    'concurrency' : 1,
    'schedule' : None,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1),
    'on_failure_callback': failure_alert,
}
# --- DAG DEFINITION ---
velov_dag = DAG(
    dag_id="velov_dag",
    catchup=False,
    default_args= default_args_dict,
    template_searchpath=[BASE_DIR],
    tags=["lyon", "velov", "etl"],
)

start = DummyOperator(
    task_id="start",
    dag =velov_dag
)

ingest_velov_station_data_task = PythonOperator(
    task_id="ingest_velov_data",
    dag = velov_dag,
    python_callable=ingestionLib.get_velov_data,
    op_kwargs={
        "api_url": VELOV_API_URL, 
        "output_path": RAW_FILE_PATH, # <-- On force le chemin ici
        "limit": 100,
        "desired_count": 300,  # Limite par défaut pour éviter de longues exécutions
        "max_loops": 50        # Safeguard pour arrêter pagination excessive
    },
)

cleaning_velov_station_task = PythonOperator(
    task_id="clean_velov_data",
    dag = velov_dag,
    python_callable=wranglingLib.clean_velov_data,
    op_kwargs={
        "input_path": RAW_FILE_PATH,   
        "output_path": CLEAN_FILE_PATH 
    },
)

cleaning_accidents_data = PythonOperator(
    task_id="clean_accidents",
    dag= velov_dag,
    python_callable=wranglingLib.accidents_cleaning,
    op_kwargs={
        "input_path": os.path.join(BASE_DIR, "landing", "accidentsVelo.csv"),
        "output_path": os.path.join(BASE_DIR, "staging", "accidents_clean_lyon.csv"),
    },
)

# --- ORCHESTRATION (PIPELINE) ---
# Validation tasks (ingestion & wrangling)
check_ingestion_task = PythonOperator(
    task_id="check_ingestion",
    dag=velov_dag,
    python_callable=wranglingLib.check_task_status,
    op_kwargs={"path": RAW_FILE_PATH, "min_rows": 1},
)

check_wrangling_task = PythonOperator(
    task_id="check_wrangling",
    dag=velov_dag,
    python_callable=wranglingLib.check_task_status,
    op_kwargs={"path": CLEAN_FILE_PATH, "min_rows": 1, "required_cols": ["station_id", "lat", "long"]},
)

check_accidents_task = PythonOperator(
    task_id="check_accidents",
    dag=velov_dag,
    python_callable=wranglingLib.check_task_status,
    op_kwargs={"path": os.path.join(BASE_DIR, "staging", "accidents_clean_lyon.csv"), "min_rows": 1, "required_cols": ["Num_Acc", "lat", "long"]},
)

# Assign accidents to stations (requires both clean station & accidents files)
assign_accidents_task = PythonOperator(
    task_id="assign_accidents_to_stations",
    dag=velov_dag,
    python_callable=wranglingLib.assign_accidents_to_stations,
    op_kwargs={
        "stations_path": CLEAN_FILE_PATH,
        "accidents_path": os.path.join(BASE_DIR, "staging", "accidents_clean_lyon.csv"),
        "output_stations_path": os.path.join(BASE_DIR, "staging", "velov_stations_with_accident_counts.csv"),
        "output_accidents_path": os.path.join(BASE_DIR, "staging", "accidents_assigned.csv"),
        "radius_m": 500,
    },
)

save_station_data_to_mongo_task = PythonOperator(
    task_id="save_stations_to_mongo",
    dag=velov_dag,
    python_callable=wranglingLib.save_stations_data_to_mongodb,
    op_kwargs={
        "csv_path": os.path.join(BASE_DIR, "staging", "velov_stations_with_accident_counts.csv"),
    }
)

mongo_to_neo4j_graph_task = PythonOperator(
    task_id="mongo_to_neo4j_graph",
    dag=velov_dag,
    python_callable=productionLib.mongo_to_neo4j_graph,
    op_kwargs={"k": 5}
)

check_neo4j_graph_task = PythonOperator(
    task_id="check_neo4j_graph",
    dag=velov_dag,
    python_callable=productionLib.check_neo4j_graph,
    op_kwargs={
        "min_nodes": 1,
        "min_edges": 1,
    },
)

check_accidents_input_task = PythonOperator(
    task_id="check_accidents_input",
    dag=velov_dag,
    python_callable=wranglingLib.check_task_status,
    op_kwargs={
        "path": os.path.join(BASE_DIR, "landing", "accidentsVelo.csv"),
        "min_rows": 1,
        "required_cols": ["Num_Acc", "lat", "long"],
    },
)

# Verification tasks for assignment outputs
check_assigned_stations_task = PythonOperator(
    task_id="check_assigned_stations",
    dag=velov_dag,
    python_callable=wranglingLib.check_task_status,
    op_kwargs={
        "path": os.path.join(BASE_DIR, "staging", "velov_stations_with_accident_counts.csv"),
        "min_rows": 1,
        "required_cols": ["station_id", "accident_count_nearby"],
    },
)

check_assigned_accidents_task = PythonOperator(
    task_id="check_assigned_accidents",
    dag=velov_dag,
    python_callable=wranglingLib.check_task_status,
    op_kwargs={
        "path": os.path.join(BASE_DIR, "staging", "accidents_assigned.csv"),
        "min_rows": 1,
        "required_cols": ["Num_Acc", "assigned_station_id", "distance_m"],
    },
)

# Verify Mongo insert before building Neo4j graph
check_mongo_stations_task = PythonOperator(
    task_id="check_mongo_stations",
    dag=velov_dag,
    python_callable=wranglingLib.check_mongo_stations,
)

# 4. End
end = DummyOperator(
    task_id="end",
    dag=velov_dag
)


start >> ingest_velov_station_data_task >> check_ingestion_task >> cleaning_velov_station_task >> check_wrangling_task
start >> check_accidents_input_task >> cleaning_accidents_data >> check_accidents_task

# Run assignment after both checks are done
check_wrangling_task >> assign_accidents_task
check_accidents_task >> assign_accidents_task

# Ensure assignment produced expected CSVs before saving to Mongo
assign_accidents_task >> check_assigned_stations_task
assign_accidents_task >> check_assigned_accidents_task
check_assigned_stations_task >> save_station_data_to_mongo_task
check_assigned_accidents_task >> save_station_data_to_mongo_task

save_station_data_to_mongo_task >> check_mongo_stations_task >> mongo_to_neo4j_graph_task >> check_neo4j_graph_task >> end