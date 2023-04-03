from __future__ import annotations

from unittest import mock
import logging

# Import Operator
from mlflow_provider.hooks.pyfunc import MLflowPyfuncHook

log = logging.getLogger(__name__)


# Mock the Airflow connections
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_MLFLOW_DEFAULT='http://servvice.com:80')
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_MLFLOW_DBX='https://token:password@something.cloud.databricks.com')
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_MLFLOW_BEARER='https://token:password@servvice.com:80')
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_MLFLOW_BASICAUTH='https://username:password@servvice.com:80')
class TestBaseHook:
    """
    Test Pyfunc Hook.
    """

    @mock.patch('mlflow_provider.hooks.pyfunc.MLflowPyfuncHook._set_env_variables')
    def test_get_conn(self, mock_set_env):
        hook = MLflowPyfuncHook(
            mlflow_conn_id = 'mlflow_bearer'
        )

        hook.get_conn()
        mock_set_env.assert_called_once()
