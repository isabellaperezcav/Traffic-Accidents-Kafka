# tests/etls/test_api_etl_transforms.py
import pytest
import pandas as pd
import numpy as np # Para pd.NA o np.nan si es necesario

# Asegúrate de que Python pueda encontrar tus módulos de DAGs.
# Esto podría requerir ajustar PYTHONPATH, usar imports relativos,
# o instalar tu proyecto 'dags' como un paquete editable.
# Para este ejemplo, asumimos que la estructura permite la importación directa.
from dags.etls.dag_api_etl import _apply_osm_transformations_to_df # Ajusta la importación según tu estructura

# --- Fixtures de Pytest (datos de prueba reutilizables) ---
@pytest.fixture
def raw_osm_data_nominal():
    """DataFrame de OSM crudo, caso nominal y válido."""
    data = {
        'osm_id': [101, 102, 103],
        'latitude': ["40.7128", "40.7130", "40.7132"],
        'longitude': ["-74.0060", "-74.0062", "-74.0064"],
        'element_type': ["traffic_signals", "crossing", "traffic_signals"],
        'tags': ["{'name':'signal_A'}", "{'type':'zebra'}", "{}"],
        'city': ["TestCity1", "TestCity1", "TestCity1"]
    }
    return pd.DataFrame(data)

@pytest.fixture
def raw_osm_data_with_issues():
    """DataFrame de OSM crudo con varios problemas a limpiar/manejar."""
    data = {
        'osm_id': [201, 201, 202, 203, 204, 205, 206, 207], # Duplicado
        'latitude': ["41.0", "41.0", "texto_invalido", None, "41.3", "41.4", "91.0", "41.5"], # Tipo incorrecto, Nulo, Fuera de rango (se tratará en GX, aquí solo conversión)
        'longitude': ["-73.0", "-73.0", "-73.1", "-73.2", "-73.3", "texto_invalido", "-73.5", "-190.0"], # Tipo incorrecto, Fuera de rango
        'element_type': ["traffic_signals", "traffic_signals", "crossing", "residential", "traffic_signals", "crossing", "other", "traffic_signals"], # Tipo no deseado, otro tipo
        'tags': ["{}", "{}", "{}", "{}", "{}", "{}", "{}", "{'name':'signal_X'}"],
        'city': ["TestCity2"] * 8,
        'extra_col': list("abcdefgh") # Columna que no debería estar en la salida
    }
    return pd.DataFrame(data)

@pytest.fixture
def raw_osm_data_empty():
    """DataFrame de OSM crudo vacío."""
    return pd.DataFrame(columns=['osm_id', 'latitude', 'longitude', 'element_type', 'tags', 'city', 'random_col'])

# --- Pruebas para _apply_osm_transformations_to_df ---

def test_osm_transform_nominal_case(raw_osm_data_nominal):
    """Prueba la transformación con datos crudos bien formados."""
    transformed_df = _apply_osm_transformations_to_df(raw_osm_data_nominal)
    
    assert len(transformed_df) == 3
    expected_cols = ['osm_id', 'latitude', 'longitude', 'element_type', 'tags', 'city']
    assert list(transformed_df.columns) == expected_cols
    
    assert transformed_df['osm_id'].dtype == 'object' # Convertido a string
    assert pd.api.types.is_float_dtype(transformed_df['latitude'])
    assert pd.api.types.is_float_dtype(transformed_df['longitude'])
    assert transformed_df.loc[0, 'latitude'] == 40.7128
    assert transformed_df.loc[0, 'longitude'] == -74.0060

def test_osm_transform_handles_data_issues(raw_osm_data_with_issues):
    """Prueba cómo la transformación maneja duplicados, tipos incorrectos, nulos y filtrado."""
    transformed_df = _apply_osm_transformations_to_df(raw_osm_data_with_issues)

    # Explicación de filas esperadas:
    # 201: duplicado, se mantiene uno. lat/lon ok. type ok.
    # 202: lat invalido -> NaN -> dropeado.
    # 203: type 'residential' -> filtrado.
    # 204: lat/lon ok. type ok.
    # 205: lon invalido -> NaN -> dropeado.
    # 206: type 'other' -> filtrado (asumiendo que _apply_osm_transformations_to_df solo mantiene 'traffic_signals', 'crossing')
    # 207: lat/lon fuera de rango PERO la función de transformación solo convierte a numérico, GX validaría el rango. type ok.
    # Esperamos filas para osm_id '201', '204', '207' si 'other' NO se filtra.
    # En tu código `_apply_osm_transformations` SÍ filtra por `valid_elements = ['traffic_signals', 'crossing']`
    # Entonces 206 ('other') también se filtra.
    # Esperamos 3 filas: '201', '204', '207'
    
    assert len(transformed_df) == 3, "El número de filas después de la transformación no es el esperado."
    
    expected_ids = ['201', '204', '207']
    assert sorted(transformed_df['osm_id'].tolist()) == sorted(expected_ids)
    
    assert 'residential' not in transformed_df['element_type'].unique()
    assert 'other' not in transformed_df['element_type'].unique()
    assert 'extra_col' not in transformed_df.columns
    
    # Verificar que no haya NaNs en latitud/longitud después de la transformación
    assert transformed_df['latitude'].notna().all()
    assert transformed_df['longitude'].notna().all()
    
    # Verificar conversión numérica de lat/lon fuera de rango (la función solo convierte, GX valida)
    assert transformed_df.loc[transformed_df['osm_id'] == '207', 'latitude'].iloc[0] == 91.0
    assert transformed_df.loc[transformed_df['osm_id'] == '207', 'longitude'].iloc[0] == -190.0


def test_osm_transform_empty_input(raw_osm_data_empty):
    """Prueba con un DataFrame de entrada vacío."""
    transformed_df = _apply_osm_transformations_to_df(raw_osm_data_empty)
    assert transformed_df.empty
    expected_cols = ['osm_id', 'latitude', 'longitude', 'element_type', 'tags', 'city']
    assert list(transformed_df.columns) == expected_cols

def test_osm_transform_all_rows_become_invalid():
    """Prueba un caso donde todas las filas se eliminan durante la transformación."""
    data = { # Todas con lat/lon que se volverán NaN o element_type inválido
        'osm_id': [301, 302, 303],
        'latitude': ["error", "40.0", "40.1"],
        'longitude': ["-70.0", "error", "-70.1"],
        'element_type': ["invalid1", "invalid2", "traffic_signals"], # La última fila se dropeará por 'longitude' error
        'tags': ["{}", "{}", "{}"],
        'city': ["TestCity3"] * 3
    }
    raw_df = pd.DataFrame(data)
    transformed_df = _apply_osm_transformations_to_df(raw_df)
    assert transformed_df.empty
    expected_cols = ['osm_id', 'latitude', 'longitude', 'element_type', 'tags', 'city']
    assert list(transformed_df.columns) == expected_cols