#test_dag_api_etl
import pandas as pd
from dags.etls.dag_api_etl import _apply_osm_transformations

def test_apply_osm_transformations():
    raw_data = pd.DataFrame([{
        "osm_id": "1",
        "latitude": 40.0,
        "longitude": -74.0,
        "element_type": "traffic_signals",
        "tags": "{'amenity': 'hospital', 'crossing': 'zebra', 'traffic_signals': 'signal'}",
        "city": "New York City"
    }])

    result = _apply_osm_transformations(raw_data)
    assert not result.empty
    assert result.loc[0, "category_hospital"] == 1
    assert result.loc[0, "crossing_zebra"] == 1
    assert result.loc[0, "traffic_signals_signal"] == 1
