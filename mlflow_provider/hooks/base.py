import os
from abc import ABC
from typing import Any, Callable, Dict, Optional, Union

from airflow.models import Connection
from requests.auth import HTTPBasicAuth

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
import logging


class MLflowBaseHook(BaseHook):
    """
    Base for MLflow hooks that interacts with a Python API

    :param mlflow_conn_id: mlflow http connection
    :type mlflow_conn_id: str
    """

    mlflow_conn_name_attr = 'mlflow_conn_id'
    mlflow_default_conn_name = 'mlflow_default'
    conn_type = 'http'

    mlflow_env_variables = [
        'MLFLOW_TRACKING_URI',
        'MLFLOW_TRACKING_TOKEN',
        'MLFLOW_TRACKING_USERNAME',
        'MLFLOW_TRACKING_PASSWORD'
    ]

    def __init__(
            self,
            mlflow_conn_id: str = mlflow_default_conn_name,
    ) -> None:
        super().__init__()
        self.mlflow_conn_id = mlflow_conn_id

    def _set_env_variables(self, other_env: Optional[Dict[str, str]] = None):
        """
        MLflow Python API requires that auth credentials are stored in ENV Variables

        :param other_env: Optionally provide additional creds for other systems like Sagemaker
        :type other_env: dict
        """

        conn = self.get_connection(self.mlflow_conn_id)
        if 'cloud.databricks.com' in conn.host:
            os.environ['MLFLOW_TRACKING_URI'] = 'databricks'
        else:
            os.environ['MLFLOW_TRACKING_URI'] = conn.host

        if conn.login == 'token':
            os.environ['MLFLOW_TRACKING_TOKEN'] = conn.host
        else:
            os.environ['MLFLOW_TRACKING_USERNAME'] = conn.login
            os.environ['MLFLOW_TRACKING_PASSWORD'] = conn.password

        if other_env is not None:
            for k, v in other_env.items():
                os.environ[k] = v

    def unset_env_variables(self, other_env: Optional[Dict[str, str]] = None):
        """
        Precautionary function to be used after the hook or operator
        has finished executing to clear MLflow credentials from ENV variables.

        :param other_env: Optionally provide additional creds for other systems like Sagemaker
        :type other_env: dict
        """

        for variable_name in self.mlflow_env_variables:
            if variable_name in os.environ:
                try:
                    del os.environ[variable_name]
                except KeyError as e:
                    logging.warning(f'{variable_name} could not be removed because it does not exist. {e}')

        if other_env:
            for k in other_env.keys():
                try:
                    del os.environ[k]
                except KeyError as e:
                    logging.warning(f'{k} could not be removed because it does not exist. {e}')


