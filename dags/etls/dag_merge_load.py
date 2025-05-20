# dags/dag_merge_load.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import time
import os, logging, json
from kafka import KafkaProducer

# --------------------------------------------------------------------------- #
# Configuración
# --------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
load_dotenv()

DB_HOST, DB_USER, DB_PASSWORD = map(os.getenv, ["DB_HOST", "DB_USER", "DB_PASSWORD"])
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME_DIM = "CrashTraffic_Dimensional"
DIM_DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME_DIM}"

CRASH_CSV_PATH = os.getenv("CRASH_TRANSFORMED_PATH",
                           "/opt/airflow/data/crash_transformed.csv")
OSM_CSV_PATH   = os.getenv("OSM_TRANSFORMED_PATH",
                           "/opt/airflow/data/raw/osm_transformed.csv")

# ---- al inicio del archivo
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-test:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "accidentes_stream")

# --------------------------------------------------------------------------- #


def extend_dimensional_model() -> None:
    """Crea la tabla OSM y extiende la tabla de hechos (sin UNIQUE)."""
    engine = create_engine(DIM_DB_URL)
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_elemento_vial_osm (
            elemento_vial_id  SERIAL PRIMARY KEY,
            osm_id            VARCHAR(50),               -- <-- sin UNIQUE
            ciudad            VARCHAR(100),
            cerca_escuela     BOOLEAN,
            cerca_hospital    BOOLEAN,
            cruce_senalizado  BOOLEAN,
            cruce_cebra       BOOLEAN,
            cruce_sin_control BOOLEAN,
            semaforo_peatonal BOOLEAN,
            semaforo_simple   BOOLEAN
        );

        ALTER TABLE IF EXISTS hechos_accidentes
        ADD COLUMN IF NOT EXISTS elemento_vial_id INTEGER,
        ADD FOREIGN KEY (elemento_vial_id)
            REFERENCES dim_elemento_vial_osm(elemento_vial_id);
        """))
    log.info("Modelo dimensional extendido para incluir datos OSM.")


def merge_data() -> None:
    df_crash = pd.read_csv(CRASH_CSV_PATH)
    df_osm   = pd.read_csv(OSM_CSV_PATH)

    if df_crash.empty or df_osm.empty:
        raise ValueError("Uno de los datasets está vacío. Abortando merge.")

    df_crash["Start_Lat"] = df_crash["Start_Lat"].round(6)
    df_crash["Start_Lng"] = df_crash["Start_Lng"].round(6)
    df_osm["latitude"]    = df_osm["latitude"].round(6)
    df_osm["longitude"]   = df_osm["longitude"].round(6)

    merged = pd.merge(
        df_crash,
        df_osm,
        left_on=["Start_Lat", "Start_Lng"],
        right_on=["latitude", "longitude"],
        how="inner"
    )

    if merged.empty:
        log.warning("No hubo coincidencias entre lat/lon de accidentes y OSM.")
        return

    merged.to_csv("/opt/airflow/data/merged_final.csv", index=False)
    log.info("Merge completado. %s filas unidas.", len(merged))


BOOL_COLS = [
    "category_school", "category_hospital",
    "crossing_marked", "crossing_zebra", "crossing_uncontrolled",
    "traffic_signals_pedestrian_crossing", "traffic_signals_signal"
]


def load_to_db() -> None:
    csv_path = "/opt/airflow/data/merged_final.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError("No se encontró merged_final.csv para cargar en la DB")

    df = pd.read_csv(csv_path)
    if df.empty:
        log.warning("El archivo merged_final.csv está vacío. Abortando carga.")
        return

    df[BOOL_COLS] = df[BOOL_COLS].astype(bool)  # asegurar tipos booleanos

    engine = create_engine(DIM_DB_URL)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO dim_elemento_vial_osm (
                    osm_id, ciudad,
                    cerca_escuela,  cerca_hospital,
                    cruce_senalizado, cruce_cebra, cruce_sin_control,
                    semaforo_peatonal, semaforo_simple
                ) VALUES (
                    :osm_id, :ciudad,
                    :cerca_escuela, :cerca_hospital,
                    :cruce_senalizado, :cruce_cebra, :cruce_sin_control,
                    :semaforo_peatonal, :semaforo_simple
                )
            """), {
                "osm_id":            row["osm_id"],
                "ciudad":            row["city"],
                "cerca_escuela":     row["category_school"],
                "cerca_hospital":    row["category_hospital"],
                "cruce_senalizado":  row["crossing_marked"],
                "cruce_cebra":       row["crossing_zebra"],
                "cruce_sin_control": row["crossing_uncontrolled"],
                "semaforo_peatonal": row["traffic_signals_pedestrian_crossing"],
                "semaforo_simple":   row["traffic_signals_signal"]
            })

    log.info("Carga a la base de datos completada con %s filas.", len(df))


def stream_to_kafka():
    csv_path = "/opt/airflow/data/merged_final.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError("No se encontró el archivo de datos finales para Kafka")

    df = pd.read_csv(csv_path)

    servers = [s.strip() for s in os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")]

    producer = KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    topic = "nombre_del_topic"

    for _, row in df.iterrows():
        message = row.to_dict()
        producer.send(topic, value=message)
        print(f"Enviado: {message}")
        time.sleep(1)  

    producer.flush()
    producer.close()

# ------------------------------ DAG ----------------------------------------- #
default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1), "retries": 1}

with DAG(
    dag_id="dag_merge_load",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Une datos de accidentes y OSM, carga en DB y transmite a Kafka",
) as dag:

    t1 = PythonOperator(task_id="extend_dimensional_model", python_callable=extend_dimensional_model)
    t2 = PythonOperator(task_id="merge_data",              python_callable=merge_data)
    t3 = PythonOperator(task_id="load_to_db",              python_callable=load_to_db)
    t4 = PythonOperator(task_id="stream_to_kafka",         python_callable=stream_to_kafka)

    t1 >> t2 >> [t3, t4]
