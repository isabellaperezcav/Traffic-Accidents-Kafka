#test_dag_db_etl
import pytest
from unittest.mock import MagicMock, patch
from dags.etls.dag_db_etl import transform_db

def test_transform_db_with_mock():
    with patch("dags.dag_db_etl.create_engine"), \
         patch("dags.dag_db_etl.text"), \
         patch("dags.dag_db_etl.pd.DataFrame.to_csv") as mock_to_csv:
        
        # Llama a la función
        batches = transform_db()

        # Verificaciones mínimas
        mock_to_csv.assert_called()
        assert isinstance(batches, list)
