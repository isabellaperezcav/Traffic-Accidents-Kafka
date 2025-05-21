import pytest
import pandas as pd
from dags.etls.dag_api_etl import _apply_osm_transformations

@pytest.fixture
def sample_df():
    """Devuelve un DataFrame de ejemplo con datos OSM crudos."""
    return pd.DataFrame({
        'osm_id': ['123', '456', '789', '101'],
        'latitude': ['40.7128', '34.0522', '41.8781', '29.7604'],
        'longitude': ['-74.0060', '-118.2437', '-87.6298', '-95.3698'],
        'element_type': ['traffic_signals', 'crossing', 'amenity', 'amenity'],
        'tags': [
            "{'highway': 'traffic_signals', 'traffic_signals': 'signal'}",
            "{'highway': 'crossing', 'crossing': 'zebra'}",
            "{'amenity': 'hospital'}",
            "{'amenity': 'school'}"
        ],
        'city': ['New York City', 'Los Angeles', 'Chicago', 'Houston']
    })

@pytest.fixture
def empty_df():
    """Devuelve un DataFrame vacío con las columnas esperadas."""
    return pd.DataFrame(columns=['osm_id', 'latitude', 'longitude', 'element_type', 'tags', 'city'])

def test_apply_osm_transformations_traffic_signals(sample_df):
    """Verifica la transformación de etiquetas de semáforos en columnas booleanas."""
    df_transformed = _apply_osm_transformations(sample_df)

    expected_cols = [
        'osm_id', 'latitude', 'longitude', 'city',
        'category_hospital', 'category_school',
        'crossing_marked', 'crossing_zebra', 'crossing_uncontrolled',
        'traffic_signals_pedestrian_crossing', 'traffic_signals_signal'
    ]
    assert list(df_transformed.columns) == expected_cols
    assert df_transformed.loc[0, 'traffic_signals_signal'] == 1
    assert df_transformed.loc[0, 'traffic_signals_pedestrian_crossing'] == 0
    assert df_transformed.loc[1, 'traffic_signals_signal'] == 0

def test_apply_osm_transformations_crossing(sample_df):
    """Verifica la transformación de etiquetas de cruces peatonales en columnas booleanas."""
    df_transformed = _apply_osm_transformations(sample_df)

    assert df_transformed.loc[1, 'crossing_zebra'] == 1
    assert df_transformed.loc[1, 'crossing_marked'] == 0
    assert df_transformed.loc[1, 'crossing_uncontrolled'] == 0
    assert df_transformed.loc[0, 'crossing_zebra'] == 0

def test_apply_osm_transformations_amenities(sample_df):
    """Verifica la transformación de etiquetas de amenities (hospital, escuela)."""
    df_transformed = _apply_osm_transformations(sample_df)

    assert df_transformed.loc[2, 'category_hospital'] == 1
    assert df_transformed.loc[2, 'category_school'] == 0
    assert df_transformed.loc[3, 'category_school'] == 1
    assert df_transformed.loc[3, 'category_hospital'] == 0
    assert df_transformed.loc[0, 'category_hospital'] == 0
    assert df_transformed.loc[0, 'category_school'] == 0

def test_apply_osm_transformations_empty_df(empty_df):
    """Verifica el manejo de un DataFrame vacío."""
    df_transformed = _apply_osm_transformations(empty_df)

    expected_cols = [
        'osm_id', 'latitude', 'longitude', 'city',
        'category_hospital', 'category_school',
        'crossing_marked', 'crossing_zebra', 'crossing_uncontrolled',
        'traffic_signals_pedestrian_crossing', 'traffic_signals_signal'
    ]
    assert list(df_transformed.columns) == expected_cols
    assert len(df_transformed) == 0

def test_apply_osm_transformations_invalid_tags():
    """Verifica el manejo de etiquetas inválidas pero válidas para ast.literal_eval."""
    df_invalid = pd.DataFrame({
        'osm_id': ['999'],
        'latitude': ['40.7128'],
        'longitude': ['-74.0060'],
        'element_type': ['traffic_signals'],
        'tags': ["{'unknown_tag': 'value'}"],  # Etiqueta válida para ast.literal_eval pero no relevante
        'city': ['New York City']
    })
    df_transformed = _apply_osm_transformations(df_invalid)

    expected_cols = [
        'osm_id', 'latitude', 'longitude', 'city',
        'category_hospital', 'category_school',
        'crossing_marked', 'crossing_zebra', 'crossing_uncontrolled',
        'traffic_signals_pedestrian_crossing', 'traffic_signals_signal'
    ]
    assert list(df_transformed.columns) == expected_cols
    assert df_transformed.loc[0, 'traffic_signals_signal'] == 0
    assert df_transformed.loc[0, 'crossing_zebra'] == 0
    assert df_transformed.loc[0, 'category_hospital'] == 0

def test_apply_osm_transformations_missing_coordinates(sample_df):
    """Verifica el manejo de coordenadas faltantes."""
    df_missing = sample_df.copy()
    df_missing.loc[0, 'latitude'] = None
    df_missing.loc[1, 'longitude'] = None
    df_transformed = _apply_osm_transformations(df_missing)

    assert len(df_transformed) == 2  # Filas con coordenadas válidas
    assert df_transformed['osm_id'].isin(['789', '101']).all()