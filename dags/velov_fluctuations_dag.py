"""DAG: compute and store Velov station fluctuation top-N

Calculates for each station the sum of successive absolute differences of
`available_bikes` (|a-b|+|b-c|+...) and stores top-N results in MongoDB.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import libs.wrangling as wranglingLib

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='velov_fluctuations_dag',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule='0 * * * *',  # hourly
    catchup=False,
    tags=['velov', 'analytics'],
) as dag:

    compute_task = PythonOperator(
        task_id='save_top10_velov_realtime',
        python_callable=wranglingLib.save_top10_to_realtime,
        op_kwargs={
            'db_name': 'VelovRealtimeDB',
            'output_collection': 'velov_top10_fluctuations',
        }
    )

    verify_task = PythonOperator(
        task_id='verify_top10_collection',
        python_callable=wranglingLib.check_top10_collection_exists,
        op_kwargs={
            'db_name': 'VelovRealtimeDB',
            'collection_name': 'velov_top10_fluctuations',
            'min_docs': 1,
        }
    )

    compute_task >> verify_task
