# dags/dag_master_etl.py

from airflow import DAG
from airflow.operators.python import PythonOperator
import great_expectations as gx
from datetime import datetime
import os
import sys
import pendulum

# Agregar ruta si usas un paquete local, opcional
sys.path.append(os.path.abspath("/opt/airflow/dags"))

# Rutas de proyecto
GX_ROOT = os.getenv("GX_PROJECT_ROOT_DIR", "/opt/airflow/great_expectations")

# Importar funciones locales si fueran necesarias
from etls.dag_db_etl import setup_tables, extract_db, transform_db, checkpoint_gx
from etls.dag_api_etl import extract_osm_data_func, transform_osm_data_func, validate_osm_data_with_gx_func
from etls.dag_merge_load import merge_data, load_to_db, stream_to_kafka

# DAG master que orquesta los otros

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),
    'retries': 1
}

with DAG(
    dag_id='dag_master_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Orquestador maestro: DB ETL + API ETL + Merge/Load con Kafka'
) as dag:

    # --- DB ETL ---
    setup_tables_crash = PythonOperator(
        task_id='setup_tables',
        python_callable=setup_tables
    )

    extract_crash_data = PythonOperator(
        task_id='extract_crash_data',
        python_callable=extract_db
    )

    transform_crash_data = PythonOperator(
        task_id='transform_crash_data',
        python_callable=transform_db
    )

    validate_crash_data = PythonOperator(
        task_id='validate_crash_data_gx',
        python_callable=checkpoint_gx
    )

    # --- API OSM ETL ---
    extract_osm_data = PythonOperator(
        task_id='extract_osm_data',
        python_callable=extract_osm_data_func
    )

    transform_osm_data = PythonOperator(
        task_id='transform_osm_data',
        python_callable=transform_osm_data_func
    )

    validate_osm_data = PythonOperator(
        task_id='validate_osm_data_gx',
        python_callable=validate_osm_data_with_gx_func
    )

    # --- Merge y Load ---
    merge_crash_osm_data = PythonOperator(
        task_id='merge_crash_osm_data',
        python_callable=merge_data
    )

    load_crash_db = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_to_db
    )

    stream_to_kafka_task = PythonOperator(
        task_id='stream_to_kafka',
        python_callable=stream_to_kafka
    )

    # Dependencias
    setup_tables_crash >> extract_crash_data >> transform_crash_data >> validate_crash_data
    extract_osm_data >> transform_osm_data >> validate_osm_data
    [validate_crash_data, validate_osm_data] >> merge_crash_osm_data >> [load_crash_db, stream_to_kafka_task]
