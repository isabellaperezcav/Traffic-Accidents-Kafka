# tests/etls/test_db_etl_transforms.py
import pytest
import pandas as pd
import numpy as np
from datetime import time

from dags.etls.dag_db_etl import _apply_crash_transformations # Ajusta la importación

@pytest.fixture
def raw_crash_data_nominal():
    data = {
        'crash_id': [1, 2],
        'crash_date': ["2023-01-15 10:30:00", "2023-02-20 23:15:45"],
        'latitude': [34.0522, "34.0520"], # Un string que debe convertirse
        'longitude': [-118.2437, -118.2430],
        'weather_condition': ["Clear", " Rainy "],
        'intersection': ["Y", "N"],
        'unidades_involucradas': [2, 1],
        'fatalidades': [0, 1],
        'total_lesiones': [1, 1],
        'incapacitantes': [0,1],
        'no_incapacitantes': [1,0],
        'reportadas_no_evidentes': [0,0],
        'sin_indicacion': [0,0]
        # Faltan otras columnas dimensionales, se añadirán como NA o defaults
    }
    return pd.DataFrame(data)

@pytest.fixture
def raw_crash_data_with_issues():
    data = {
        'crash_id': [10, 11, 12, 13, 14],
        'crash_date': ["2023-03-10", "invalid_date", None, "2023-04-01 05:00", "2023/05/05 08:00:00"], # Formato diferente, invalido, nulo
        'latitude': ["35.123", None, "35.456", "error", "35.789"], # Nulo, error
        'longitude': ["-119.456", "-119.789", "error", "-120.123", "-120.456"], # Error
        'weather_condition': [" cloudy", "Clear", None, "FOG", "  "], # Espacios, nulo, mayus, string vacío
        'unidades_involucradas': ["2", 1, "N/A", 0, 3], # String numérico, N/A
        'fatalidades': [0, "1", 0, "XYZ", 0], # String numérico, XYZ
        'total_lesiones': [1,1,0,0,2],
        'col_extra': ['x', 'y', 'z', 'a', 'b']
    }
    # Faltan otras columnas, se probará si se añaden con defaults/NA
    return pd.DataFrame(data)

@pytest.fixture
def raw_crash_data_empty():
    return pd.DataFrame(columns=['crash_id', 'crash_date', 'latitude', 'longitude', 'weather_condition', 'unidades_involucradas'])


# --- Pruebas para _apply_crash_transformations ---

def test_crash_transform_nominal_case(raw_crash_data_nominal):
    transformed_df = _apply_crash_transformations(raw_crash_data_nominal)
    assert len(transformed_df) == 2
    
    # Comprobar una fila
    row1 = transformed_df[transformed_df['crash_id'] == 1].iloc[0]
    assert row1['dia'] == 15
    assert row1['mes'] == 1
    assert row1['anio'] == 2023
    assert row1['dia_semana'] == "Sunday"
    assert row1['hora'] == time(10, 30, 0)
    assert row1['latitude'] == 34.0522
    assert row1['condicion_climatica'] == "Clear"
    assert row1['unidades_involucradas'] == 2
    assert row1['fatalidades'] == 0
    assert row1['interseccion'] == "Y" # Asumiendo que 'intersection' se mapea a 'interseccion'

    # Comprobar que las columnas no existentes en la entrada pero sí en expected_final_cols se añaden
    # En _apply_crash_transformations, expected_final_cols define la estructura.
    # Si una columna en expected_final_cols no tiene fuente en raw_df Y no tiene un default específico
    # en la lógica de transformación, se añadirá como pd.NA
    # Ejemplo: 'causa_principal' no está en raw_crash_data_nominal
    # assert pd.isna(row1['causa_principal']) # Asumiendo que no se le dio un default
    # Las columnas de lesiones no definidas en la entrada deberían ser 0
    assert row1['incapacitantes'] == 0 # Viene de la entrada
    assert row1['no_incapacitantes'] == 1 # Viene de la entrada
    assert row1['reportadas_no_evidentes'] == 0 # Viene de la entrada
    assert row1['sin_indicacion'] == 0 # Viene de la entrada


def test_crash_transform_handles_data_issues(raw_crash_data_with_issues):
    transformed_df = _apply_crash_transformations(raw_crash_data_with_issues)
    assert len(transformed_df) == 5 # Mantiene el número de filas

    # crash_id 10 (fecha sin hora)
    row10 = transformed_df[transformed_df['crash_id'] == 10].iloc[0]
    assert row10['dia'] == 10 and row10['mes'] == 3 and row10['anio'] == 2023 and row10['hora'] == time(0,0,0)
    assert row10['latitude'] == 35.123
    assert row10['condicion_climatica'] == "Cloudy" # Limpiado y capitalizado
    assert row10['unidades_involucradas'] == 2

    # crash_id 11 (fecha inválida, lat nulo, fatalidad string '1')
    row11 = transformed_df[transformed_df['crash_id'] == 11].iloc[0]
    assert pd.isna(row11['dia']) and pd.isna(row11['hora']) # Fecha inválida -> nulos
    assert pd.isna(row11['latitude']) # Latitud nula se mantiene como nula (GX lo validará)
    assert row11['condicion_climatica'] == "Clear"
    assert row11['fatalidades'] == 1

    # crash_id 12 (lon error, weather nulo, unidades_involucradas N/A)
    row12 = transformed_df[transformed_df['crash_id'] == 12].iloc[0]
    assert pd.isna(row12['longitude']) # Longitud error -> nulo
    assert row12['condicion_climatica'] == "Unknown" # Weather nulo -> default 'Unknown'
    assert row12['unidades_involucradas'] == 0 # 'N/A' -> to_numeric coerce -> NaN -> fillna(0)

    # crash_id 13 (lat error, lon error, weather FOG, fatalidades XYZ)
    row13 = transformed_df[transformed_df['crash_id'] == 13].iloc[0]
    assert pd.isna(row13['latitude'])
    assert pd.isna(row13['longitude'])
    assert row13['condicion_climatica'] == "Fog" # Capitalizado
    assert row13['fatalidades'] == 0 # 'XYZ' -> to_numeric coerce -> NaN -> fillna(0)

    # crash_id 14 (fecha formato alternativo, weather string vacío)
    row14 = transformed_df[transformed_df['crash_id'] == 14].iloc[0]
    assert row14['dia'] == 5 and row14['mes'] == 5 and row14['anio'] == 2023 and row14['hora'] == time(8,0,0)
    assert row14['condicion_climatica'] == "Unknown" # String vacío -> fillna('Unknown')

    assert 'col_extra' not in transformed_df.columns


def test_crash_transform_empty_input(raw_crash_data_empty, expected_crash_columns):
    transformed_df = _apply_crash_transformations(raw_crash_data_empty)
    assert transformed_df.empty
    assert list(transformed_df.columns) == expected_crash_columns

def test_crash_transform_crash_id_generation_if_missing():
    """Prueba si crash_id se genera si falta (basado en la lógica actual)."""
    data_no_id = { # Sin columna 'crash_id'
        'crash_date': ["2023-01-15 10:30:00"],
        'latitude': [34.0522], 'longitude': [-118.2437]
    }
    df_no_id = pd.DataFrame(data_no_id)
    transformed_df = _apply_crash_transformations(df_no_id)
    assert 'crash_id' in transformed_df.columns
    assert transformed_df.loc[0, 'crash_id'] == "crash_0" # Basado en la lógica de tu función