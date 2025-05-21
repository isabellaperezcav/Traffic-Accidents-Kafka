import unittest
from airflow.models import DagBag
import datetime


class TestDagPago(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()
        self.dag = self.dagbag.get_dag('dag_db_etl')  # Usamos el ID correcto del DAG

    def test_dag_loaded(self):
        """Verifica que el DAG se cargue correctamente"""
        self.assertIsNotNone(self.dag)
        self.assertGreater(len(self.dag.tasks), 0)

    def test_task_types(self):
        """Verifica que las tareas no sean None"""
        for task in self.dag.tasks:
            self.assertIsNotNone(task.task_id)
            self.assertIsNotNone(task.owner)

    def test_dag_params_not_none(self):
        """Verifica que los parámetros del DAG no sean None, excepto start_date que puede ser None"""
        self.assertIsNotNone(self.dag.schedule_interval)
        self.assertIsNotNone(self.dag.default_args)
        # No hacemos assert para start_date porque puede ser None

    def test_dag_params_types(self):
        """Verifica los tipos de datos de los parámetros del DAG"""
        self.assertIsInstance(self.dag.schedule_interval, (str, type(None)))
        if self.dag.start_date is not None:
            self.assertIsInstance(self.dag.start_date, datetime.datetime)

        default_args = self.dag.default_args

        if 'retries' in default_args:
            self.assertIsInstance(default_args['retries'], int)
        # Validación eliminada como pediste:
        # if 'retry_delay' in default_args:
        #     self.assertIsInstance(default_args['retry_delay'], datetime.timedelta)
        if 'email_on_failure' in default_args:
            self.assertIsInstance(default_args['email_on_failure'], bool)


if __name__ == '__main__':
    unittest.main()
