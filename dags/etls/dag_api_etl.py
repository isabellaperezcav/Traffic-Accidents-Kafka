# dags/etls/dag_api_etl.py

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum  # Para manejo de fechas
import overpy
import pandas as pd
import time
import os
import logging

# Importaciones para Great Expectations (si las usas)
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

# Configura el logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Directorio para guardar datos intermedios
DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "C:/Users/ASUS/Desktop/proyecto03/data")
os.makedirs(DATA_DIR, exist_ok=True)

RAW_OSM_PATH         = os.path.join(DATA_DIR, "osm_extracted_raw.csv")
TRANSFORMED_OSM_PATH = os.path.join(DATA_DIR, "osm_transformed.csv")

# Configura las ciudades y bounding boxes
CITIES_BOUNDING_BOXES = {
    "Chicago": (41.64, -87.94, 42.02, -87.52),
}

OSM_ELEMENTS_QUERY_PART = """
    node["highway"="traffic_signals"](area.city);
    node["highway"="crossing"](area.city);
    way["highway"="traffic_signals"](area.city);
    way["highway"="crossing"](area.city);
    relation["highway"="traffic_signals"](area.city);
    relation["highway"="crossing"](area.city);
"""

def _fetch_and_parse_osm_data(api_instance, city_name, bbox, query_part):
    """Obtiene y parsea datos OSM para una ciudad."""
    bbox_part = f"(bbox:{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});"
    full_query = f"""
      [out:json][timeout:120];
      {bbox_part}
      ( {query_part} );
      out body; >; out skel qt;
    """
    logger.info(f"Ejecutando consulta Overpass para: {city_name}")
    result = api_instance.query(full_query)
    logger.info(f"Datos recibidos para {city_name}, procesando elementos...")

    records = []
    # Procesar nodos
    for node in result.nodes:
        tags = node.tags
        records.append({
            "osm_id": str(node.id),
            "latitude": float(node.lat),
            "longitude": float(node.lon),
            "element_type": tags.get("highway", "unknown"),
            "tags": str(tags),
            "city": city_name
        })
    # Procesar ways
    for way in result.ways:
        if way.nodes:
            first_id = way.nodes[0].id
            node_data = next((n for n in result.nodes if n.id == first_id), None)
            if node_data:
                tags = way.tags
                records.append({
                    "osm_id": str(way.id),
                    "latitude": float(node_data.lat),
                    "longitude": float(node_data.lon),
                    "element_type": tags.get("highway", "unknown"),
                    "tags": str(tags),
                    "city": city_name
                })
    return records

def extract_osm_data_func():
    """Extrae datos de OSM para todas las ciudades y guarda un CSV crudo."""
    api = overpy.Overpass()
    all_data = []
    logger.info("Iniciando extracción de datos de OpenStreetMap...")
    for city, bbox in CITIES_BOUNDING_BOXES.items():
        try:
            data = _fetch_and_parse_osm_data(api, city, bbox, OSM_ELEMENTS_QUERY_PART)
            all_data.extend(data)
            logger.info(f"Procesados {len(data)} elementos para {city}.")
            time.sleep(5)
        except overpy.exception.OverpassTooManyRequests:
            logger.error(f"Demasiadas solicitudes para {city}. Esperando 60s y reintentando...")
            time.sleep(60)
            try:
                data = _fetch_and_parse_osm_data(api, city, bbox, OSM_ELEMENTS_QUERY_PART)
                all_data.extend(data)
                logger.info(f"Procesados {len(data)} elementos para {city} en reintento.")
            except Exception as e:
                logger.error(f"Error en reintento para {city}: {e}")
        except Exception as e:
            logger.error(f"Error al consultar Overpass para {city}: {e}")

    if not all_data:
        logger.warning("No se extrajeron datos de OSM. Creando CSV vacío.")
        df = pd.DataFrame(columns=['osm_id','latitude','longitude','element_type','tags','city'])
    else:
        df = pd.DataFrame(all_data)

    df.to_csv(RAW_OSM_PATH, index=False)
    logger.info(f"Extracción de OSM completada. Guardado en {RAW_OSM_PATH} ({len(df)} filas)")

def _apply_osm_transformations(df):
    """Aplica transformaciones a los datos OSM crudos."""
    cols = ['osm_id','latitude','longitude','element_type','tags','city']
    if df.empty:
        logger.warning("DataFrame crudo OSM vacío. No hay nada que transformar.")
        return pd.DataFrame(columns=cols)

    df2 = df.copy()
    df2['latitude']  = pd.to_numeric(df2['latitude'],  errors='coerce')
    df2['longitude'] = pd.to_numeric(df2['longitude'], errors='coerce')
    df2.dropna(subset=['latitude','longitude'], inplace=True)

    if df2.empty:
        logger.warning("Tras limpiar lat/lon inválidos, el DataFrame OSM quedó vacío.")
        return pd.DataFrame(columns=cols)

    valid = ['traffic_signals','crossing']
    df2 = df2[df2['element_type'].isin(valid)]
    df2.drop_duplicates(subset=['osm_id'], inplace=True)

    # Asegurar columnas finales
    for c in cols:
        if c not in df2.columns:
            df2[c] = pd.NA

    return df2[cols]

def transform_osm_data_func():
    """Lee el CSV crudo de OSM, transforma y guarda el resultado."""
    if not os.path.exists(RAW_OSM_PATH):
        logger.error(f"Archivo crudo OSM no encontrado: {RAW_OSM_PATH}")
        pd.DataFrame(columns=['osm_id','latitude','longitude','element_type','tags','city'])\
           .to_csv(TRANSFORMED_OSM_PATH, index=False)
        raise FileNotFoundError(RAW_OSM_PATH)

    logger.info(f"Iniciando transformación de datos OSM desde {RAW_OSM_PATH}...")
    df_raw = pd.read_csv(RAW_OSM_PATH)
    df_trans = _apply_osm_transformations(df_raw)
    df_trans.to_csv(TRANSFORMED_OSM_PATH, index=False)
    logger.info(f"Transformación de OSM completada. Guardado en {TRANSFORMED_OSM_PATH} ({len(df_trans)} filas)")



def validate_osm_data_with_gx_func() -> None:
    gx_root = os.getenv("GX_PROJECT_ROOT_DIR", "/opt/airflow/great_expectations")
    if not os.path.exists(TRANSFORMED_OSM_PATH):
        logger.error(f"Archivo para validación GX no encontrado: {TRANSFORMED_OSM_PATH}")
        raise FileNotFoundError(TRANSFORMED_OSM_PATH)

    logger.info(f"Iniciando validación GX para {TRANSFORMED_OSM_PATH}…")

    context = gx.get_context(context_root_dir=gx_root)

    batch_request = RuntimeBatchRequest(
        datasource_name="runtime_pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="osm_transformed_asset",
        runtime_parameters={"path": TRANSFORMED_OSM_PATH},
        batch_identifiers={"run_id": pendulum.now().to_iso8601_string()},
    )

    # Si el datasource no existe aún, se define explícitamente por única vez
    try:
        context.get_datasource("runtime_pandas_datasource")
    except Exception:
        logger.info("Datasource no encontrado, creando nuevo.")
        context.add_or_update_datasource(
            name="runtime_pandas_datasource",
            class_name="Datasource",
            execution_engine={"class_name": "PandasExecutionEngine"},
            data_connectors={
                "runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["run_id"],
                }
            },
        )

    suite_name = "osm_transformed_suite"
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    result = validator.validate()
    context.build_data_docs()

    if result.success:
        logger.info("✅ Validación GX EXITOSA para OSM.")
    else:
        logger.error("❌ La validación GX FALLÓ para OSM.")
        try:
            context.open_data_docs()
        except Exception:
            pass
        raise ValueError("Los datos OSM no pasaron la validación GX.")


# --- Definición del DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=2),
}

with DAG(
    dag_id='osm_api_etl_dag_with_gx',
    default_args=default_args,
    description='DAG para extraer, transformar y validar datos OSM desde Overpass API',
    schedule='@daily',
    catchup=False,
    tags=['etl','osm','api'],
) as dag:

    t_extract_api = PythonOperator(
        task_id='extract_osm_data_task',
        python_callable=extract_osm_data_func,
    )

    t_transform_api = PythonOperator(
        task_id='transform_osm_data_task',
        python_callable=transform_osm_data_func,
    )

    t_validate_api = PythonOperator(
        task_id='validate_transformed_osm_data_gx_task',
        python_callable=validate_osm_data_with_gx_func,
    )

    t_extract_api >> t_transform_api >> t_validate_api
