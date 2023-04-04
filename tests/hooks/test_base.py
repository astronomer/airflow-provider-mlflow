from __future__ import annotations

import os
from unittest import mock

# Import Operator
from mlflow_provider.hooks.base import MLflowBaseHook

# Mock the Airflow connections
env_vars = {
        'AIRFLOW_CONN_MLFLOW_DEFAULT': 'http://servvice.com:80',
        'AIRFLOW_CONN_MLFLOW_DBX': 'https://token:password@something.cloud.databricks.com',
        'AIRFLOW_CONN_MLFLOW_BEARER': 'https://token:password@servvice.com:80',
        'AIRFLOW_CONN_MLFLOW_BASICAUTH': 'https://username:password@servvice.com:80'
    }

@mock.patch.dict('os.environ', env_vars)
class TestBaseHook:
    """
    Test Base Hook.
    """

    def test_set_env_variables_bearer_token(self):
        hook = MLflowBaseHook(
            mlflow_conn_id = 'mlflow_bearer'
        )

        hook._set_env_variables()
        assert os.environ['MLFLOW_TRACKING_URI'] == 'servvice.com'
        assert os.environ['MLFLOW_TRACKING_TOKEN'] == 'password'

        assert 'MLFLOW_TRACKING_USERNAME' not in os.environ
        assert 'MLFLOW_TRACKING_PASSWORD' not in os.environ
        assert 'DATABRICKS_HOST' not in os.environ
        assert 'DATABRICKS_TOKEN' not in os.environ

    def test_set_env_variables_basicauth(self):
        hook = MLflowBaseHook(
            mlflow_conn_id = 'mlflow_basicauth'
        )

        hook._set_env_variables()
        assert os.environ['MLFLOW_TRACKING_URI'] == 'servvice.com'
        assert os.environ['MLFLOW_TRACKING_USERNAME'] == 'username'
        assert os.environ['MLFLOW_TRACKING_PASSWORD'] == 'password'
        assert os.environ['LOGNAME'] == 'username'

        assert 'MLFLOW_TRACKING_TOKEN' not in os.environ
        assert 'DATABRICKS_HOST' not in os.environ
        assert 'DATABRICKS_TOKEN' not in os.environ


    def test_set_env_variables_dbx(self):
        hook = MLflowBaseHook(
            mlflow_conn_id = 'mlflow_dbx'
        )

        hook._set_env_variables()
        assert os.environ['MLFLOW_TRACKING_URI'] == 'databricks'
        assert os.environ['DATABRICKS_HOST'] == 'something.cloud.databricks.com'
        assert os.environ['DATABRICKS_TOKEN'] == 'password'

        # assert 'MLFLOW_TRACKING_TOKEN' not in os.environ
        assert 'MLFLOW_TRACKING_USERNAME' not in os.environ
        assert 'MLFLOW_TRACKING_PASSWORD' not in os.environ


    def test_set_env_variable_default(self):
        hook = MLflowBaseHook(
            mlflow_conn_id = 'mlflow_default'
        )

        hook._set_env_variables()
        assert os.environ['MLFLOW_TRACKING_URI'] == 'servvice.com'

        assert 'DATABRICKS_HOST' not in os.environ
        assert 'DATABRICKS_TOKEN' not in os.environ
        assert 'MLFLOW_TRACKING_TOKEN' not in os.environ
        assert 'MLFLOW_TRACKING_USERNAME' not in os.environ
        assert 'MLFLOW_TRACKING_PASSWORD' not in os.environ
