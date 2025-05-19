# dags/etls/dag_api_etl.py

from airflow import DAG
from airflow.operators.python import PythonOperator
import overpy
import pandas as pd
import time
import os
import logging
import os
import pendulum

# Importaciones para Great Expectations (si las usas)
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.exceptions.exceptions import DataContextError

# Configura el logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# Ruta fija para datos, definida globalmente

BASE_DATA_DIR = "/opt/airflow/data/"
RAW_FOLDER = os.path.join(BASE_DATA_DIR, "raw")
os.makedirs(RAW_FOLDER, exist_ok=True)

RAW_OSM_PATH = os.path.join(RAW_FOLDER, "osm_extracted_raw.csv")
TRANSFORMED_OSM_PATH = os.path.join(RAW_FOLDER, "osm_transformed.csv")


CITIES_BOUNDING_BOXES = {
    "New York City": (40.4774, -74.2591, 40.9176, -73.7004),
    "Los Angeles": (33.7037, -118.6682, 34.3373, -118.1553),
    "Chicago": (41.6445, -87.9401, 42.0230, -87.5237),
    "Houston": (29.5370, -95.9093, 30.1105, -95.0146),
    "Phoenix": (33.2902, -112.3240, 33.9206, -111.9250),
    "Philadelphia": (39.8670, -75.2803, 40.1379, -74.9558),
    "San Antonio": (29.1872, -98.8000, 29.6872, -98.3000),
    "San Diego": (32.5343, -117.2825, 33.1143, -116.9050),
    "Dallas": (32.6200, -97.0400, 33.0200, -96.5800),
    "Jacksonville": (30.1105, -82.0000, 30.6105, -81.3000),
    "Fort Worth": (32.5200, -97.5000, 33.0200, -96.9000),
    "San Jose": (37.1245, -122.0450, 37.4845, -121.6450),
    "Austin": (30.1105, -98.0000, 30.6105, -97.3000),
    "Columbus": (39.7700, -83.2000, 40.2700, -82.5000),
    "Charlotte": (35.0000, -81.2000, 35.5000, -80.5000)
}

def _fetch_and_parse_osm_data(api_instance, city_name, bbox, max_retries=3, delay_seconds=30):
    """Obtiene y parsea datos OSM para una ciudad con reintentos y manejo de errores de red."""
    import time
    from http.client import IncompleteRead
    import overpy

    bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
    full_query = f"""
    [out:json][timeout:180];
    (
        node["highway"="traffic_signals"]({bbox_str});
        node["highway"="crossing"]({bbox_str});
        way["highway"="traffic_signals"]({bbox_str});
        way["highway"="crossing"]({bbox_str});
        relation["highway"="traffic_signals"]({bbox_str});
        relation["highway"="crossing"]({bbox_str});
    );
    out body;
    >;
    out skel qt;
    """

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Ejecutando consulta Overpass para: {city_name} (Intento {attempt})")
            result = api_instance.query(full_query)
            logger.info(f"Datos recibidos para {city_name}, procesando elementos...")
            break  # Éxito, salir del ciclo
        except overpy.exception.OverpassTooManyRequests:
            logger.warning(f"Demasiadas solicitudes para {city_name}, esperando 60s...")
            time.sleep(60)
        except IncompleteRead as e:
            logger.warning(f"IncompleteRead para {city_name}, esperando {delay_seconds}s... ({e})")
            time.sleep(delay_seconds)
        except overpy.exception.OverpassGatewayTimeout as e:
            logger.warning(f"Timeout de gateway para {city_name}, esperando {delay_seconds}s... ({e})")
            time.sleep(delay_seconds)
        except Exception as e:
            logger.error(f"Error inesperado en intento {attempt} para {city_name}: {e}")
            time.sleep(delay_seconds)
        else:
            break
    else:
        logger.error(f"Error persistente para {city_name} tras {max_retries} intentos.")
        return []  # No se obtuvieron datos

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
    logger = logging.getLogger(__name__)

    if os.path.exists(RAW_OSM_PATH):
        logger.info(f"El archivo {RAW_OSM_PATH} ya existe. Omitiendo extract_osm_data_func().")
        return  # Salta la función si el archivo ya está generado

    api = overpy.Overpass()
    all_data = []
    logger.info("Iniciando extracción de datos de OpenStreetMap...")

    for city, bbox in CITIES_BOUNDING_BOXES.items():
        try:
            data = _fetch_and_parse_osm_data(api, city, bbox, None)
            all_data.extend(data)
            logger.info(f"Procesados {len(data)} elementos para {city}.")
            time.sleep(5)
        except overpy.exception.OverpassTooManyRequests:
            logger.error(f"Demasiadas solicitudes para {city}. Esperando 60s y reintentando...")
            time.sleep(60)
            try:
                data = _fetch_and_parse_osm_data(api, city, bbox, None)
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


import ast
import logging
import pandas as pd
import os

def _apply_osm_transformations(df):
    """Transforma datos OSM crudos en formato booleano para modelo dimensional optimizado."""
    logger = logging.getLogger(__name__)

    # Columnas booleanas finales deseadas
    bool_cols = [
        "category_hospital", "category_school",
        "crossing_marked", "crossing_zebra", "crossing_uncontrolled",
        "traffic_signals_pedestrian_crossing", "traffic_signals_signal"
    ]
    base_cols = ['osm_id', 'latitude', 'longitude', 'city']
    all_cols = base_cols + bool_cols

    if df.empty:
        logger.warning("DataFrame crudo OSM vacío. No hay nada que transformar.")
        return pd.DataFrame(columns=all_cols)

    df2 = df.copy()
    df2['latitude'] = pd.to_numeric(df2['latitude'], errors='coerce')
    df2['longitude'] = pd.to_numeric(df2['longitude'], errors='coerce')
    df2.dropna(subset=['latitude', 'longitude'], inplace=True)

    # Parsear tags a dict
    df2['tags'] = df2['tags'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else {})
    tags_df = df2['tags'].apply(pd.Series)

    # Inicializar columnas booleanas
    for col in bool_cols:
        df2[col] = 0

    # Hospital y escuela
    if "amenity" in tags_df.columns:
        df2.loc[tags_df["amenity"] == "hospital", "category_hospital"] = 1
        df2.loc[tags_df["amenity"] == "school",   "category_school"]   = 1

    # Cruces peatonales
    def map_crossing(val):
        crossing_values = ["uncontrolled", "marked", "zebra"]
        v = str(val).strip().lower()
        return v if v in crossing_values else "other"

    if "crossing" in tags_df.columns:
        crossing_mapped = tags_df["crossing"].apply(map_crossing)
        for val in ["marked", "zebra", "uncontrolled"]:
            df2[f"crossing_{val}"] = (crossing_mapped == val).astype(int)

    # Semáforos
    def map_traffic_signal(val):
        ts_values = ["pedestrian_crossing", "signal"]
        v = str(val).strip().lower()
        return v if v in ts_values else "other"

    if "traffic_signals" in tags_df.columns:
        ts_mapped = tags_df["traffic_signals"].apply(map_traffic_signal)
        for val in ["pedestrian_crossing", "signal"]:
            df2[f"traffic_signals_{val}"] = (ts_mapped == val).astype(int)

    df_result = df2[all_cols].copy()
    return df_result.reset_index(drop=True)

def transform_osm_data_func():
    """Lee el CSV crudo de OSM, transforma etiquetas en columnas booleanas y guarda el resultado."""
    logger = logging.getLogger(__name__)

    if os.path.exists(TRANSFORMED_OSM_PATH):
        logger.info(f"El archivo {TRANSFORMED_OSM_PATH} ya existe. Omitiendo transform_osm_data_func.")
        return  # Salta la función si el archivo ya está generado

    if not os.path.exists(RAW_OSM_PATH):
        logger.error(f"Archivo crudo OSM no encontrado: {RAW_OSM_PATH}")
        

        # Columnas esperadas en el esquema simplificado
        bool_cols = [
            "category_hospital", "category_school",
            "crossing_marked", "crossing_zebra", "crossing_uncontrolled",
            "traffic_signals_pedestrian_crossing", "traffic_signals_signal"
        ]
        base_cols = ['osm_id', 'latitude', 'longitude', 'city']
        all_cols = base_cols + bool_cols

        pd.DataFrame(columns=all_cols).to_csv(TRANSFORMED_OSM_PATH, index=False)
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

    # Asegurar que la suite exista
    try:
        context.get_expectation_suite("osm_transformed_suite")
        logger.info("Suite 'osm_transformed_suite' encontrada correctamente.")
    except DataContextError:
        logger.warning("Suite 'osm_transformed_suite' no encontrada. Creando nueva suite vacía.")
        context.add_expectation_suite("osm_transformed_suite")

    # Definir el batch request para validación
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

    # Obtener el validador y ejecutar validación
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="osm_transformed_suite"
    )

    validation_result = validator.validate()
    logger.info(f"Resultado de la validación GX: {validation_result['success']}")



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