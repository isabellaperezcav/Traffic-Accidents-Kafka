# dags/dag_merge_load.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import os
import logging
from kafka import KafkaProducer
import json

# Configuración
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
load_dotenv()

# Variables de conexión a la base de datos
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME_DIM = "CrashTraffic_Dimensional"
DIM_DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME_DIM}"

# Paths de archivos CSV transformados
CRASH_CSV_PATH = os.getenv("CRASH_TRANSFORMED_PATH", "/opt/airflow/data/crash_transformed.csv")
OSM_CSV_PATH   = os.getenv("OSM_TRANSFORMED_PATH", "/opt/airflow/data/osm_transformed.csv")

# Configuración de Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "accidentes_con_osm")


def extend_dimensional_model():
    engine = create_engine(DIM_DB_URL)
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_elemento_vial_osm (
            Elemento_Vial_ID SERIAL PRIMARY KEY,
            OSM_ID VARCHAR(50),
            Tipo_Elemento VARCHAR(50),
            Tags TEXT,
            Ciudad VARCHAR(100)
        );

        ALTER TABLE IF EXISTS hechos_accidentes
        ADD COLUMN IF NOT EXISTS Elemento_Vial_ID INTEGER,
        ADD FOREIGN KEY (Elemento_Vial_ID) REFERENCES dim_elemento_vial_osm(Elemento_Vial_ID);
        """))
    log.info("Modelo dimensional extendido para incluir datos OSM.")


def merge_data():
    df_crash = pd.read_csv(CRASH_CSV_PATH)
    df_osm = pd.read_csv(OSM_CSV_PATH)

    if df_crash.empty or df_osm.empty:
        raise ValueError("Uno de los datasets está vacío. Abortando merge.")

    df_crash["Latitud"] = df_crash["Latitud"].round(6)
    df_crash["Longitud"] = df_crash["Longitud"].round(6)
    df_osm["latitude"] = df_osm["latitude"].round(6)
    df_osm["longitude"] = df_osm["longitude"].round(6)

    merged = pd.merge(
        df_crash,
        df_osm,
        left_on=["Latitud", "Longitud"],
        right_on=["latitude", "longitude"],
        how="inner"
    )

    if merged.empty:
        log.warning("No hubo coincidencias entre lat/lon de accidentes y OSM.")
        return

    merged.to_csv("/opt/airflow/data/merged_final.csv", index=False)
    log.info(f"Merge completado. {len(merged)} filas unidas.")


def load_to_db():
    path = "/opt/airflow/data/merged_final.csv"
    if not os.path.exists(path):
        raise FileNotFoundError("No se encontró el archivo merged_final.csv para cargar en la DB")

    df = pd.read_csv(path)
    if df.empty:
        log.warning("El archivo merged_final.csv está vacío. Abortando carga.")
        return

    engine = create_engine(DIM_DB_URL)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            result = conn.execute(text("""
                INSERT INTO dim_elemento_vial_osm (OSM_ID, Tipo_Elemento, Tags, Ciudad)
                VALUES (:osm_id, :tipo, :tags, :ciudad)
                ON CONFLICT (OSM_ID) DO NOTHING
                RETURNING Elemento_Vial_ID
            """), {
                "osm_id": row["osm_id"],
                "tipo": row["element_type"],
                "tags": row["tags"],
                "ciudad": row["city"]
            })
            osm_id = result.fetchone()

            conn.execute(text("""
                INSERT INTO hechos_accidentes (
                    ID_Hecho, Fecha_ID, Ubicación_ID, Clima_ID, Iluminación_ID,
                    Condición_Camino_ID, Tipo_Accidente_ID, Contribuyente_Principal_ID,
                    Elemento_Vial_ID, Unidades_Involucradas, Total_Lesiones, Fatalidades,
                    Incapacitantes, No_Incapacitantes, Reportadas_No_Evidentes, Sin_Indicación
                )
                VALUES (
                    :id, :fecha_id, :ubicacion_id, :clima_id, :iluminacion_id,
                    :condicion_camino_id, :tipo_accidente_id, :contribuyente_id,
                    :elemento_vial_id, :unidades, :total_lesiones, :fatalidades,
                    :incapacitantes, :no_incapacitantes, :reportadas, :sin_indicacion
                )
                ON CONFLICT (ID_Hecho) DO NOTHING
            """), {
                "id": row["id"],
                "fecha_id": row["fecha_id"],
                "ubicacion_id": row["ubicacion_id"],
                "clima_id": row["clima_id"],
                "iluminacion_id": row["iluminacion_id"],
                "condicion_camino_id": row["condicion_camino_id"],
                "tipo_accidente_id": row["tipo_accidente_id"],
                "contribuyente_id": row["contribuyente_id"],
                "elemento_vial_id": osm_id[0] if osm_id else None,
                "unidades": row["unidades"],
                "total_lesiones": row["total_lesiones"],
                "fatalidades": row["fatalidades"],
                "incapacitantes": row["incapacitantes"],
                "no_incapacitantes": row["no_incapacitantes"],
                "reportadas": row["reportadas_no_evidentes"],
                "sin_indicacion": row["sin_indicacion"]
            })

    log.info(f"Carga a la base de datos completada con {len(df)} filas.")


def stream_to_kafka():
    path = "/opt/airflow/data/merged_final.csv"
    if not os.path.exists(path):
        raise FileNotFoundError("No se encontró el archivo de datos finales para Kafka")

    df = pd.read_csv(path)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for _, row in df.iterrows():
        data = row.to_dict()
        producer.send(KAFKA_TOPIC, value=data)

    producer.flush()
    log.info(f"Enviadas {len(df)} filas a Kafka topic: {KAFKA_TOPIC}")


# DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}


with DAG(
    dag_id='dag_merge_load',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Une datos de accidentes y OSM, carga en DB y transmite a Kafka'
) as dag:

    t1 = PythonOperator(
        task_id='extend_dimensional_model',
        python_callable=extend_dimensional_model
    )

    t2 = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data
    )

    t3 = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db
    )

    t4 = PythonOperator(
        task_id='stream_to_kafka',
        python_callable=stream_to_kafka
    )

    t1 >> t2 >> t3 >> t4

