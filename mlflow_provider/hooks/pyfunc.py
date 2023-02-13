from typing import Any, Callable, Dict, Optional, Union

import requests
import tenacity
from airflow.models import Connection

from airflow.exceptions import AirflowException
from mlflow_provider.hooks.base import MLflowBaseHook
from mlflow import pyfunc



class MLflowPyfuncHook(MLflowBaseHook):
    """
    Sample Hook that interacts with an HTTP endpoint the Python requests library.

    :param aws_conn_id: AWS connection to use with hook
    :type aws_conn_id: str
    :param target_uri: target system URI to deploy model to. (ie 'sagemaker')
    :type target_uri: str
    """

    # TODO update hook names to match Airflow naming convention
    hook_name = 'MLflow Deployment'

    def __init__(
            self,
            mlflow_conn_id: str
    ) -> None:
        super().__init__()
        self.mlflow_conn_id = mlflow_conn_id


    def get_conn(self) -> pyfunc:
        """
        Returns MLflow deployment Client.
        """

        self._set_env_variables()
        return pyfunc


    # def create_deployment(
    #         self,
    #         name: str,
    #         model_uri: str,
    #         flavor: Optional[str] = None,
    #         config: Optional[dict] = None,
    #         endpoint: Optional[str] = None
    # ):
    #     client = self.get_conn()
    #
    #     result = client.create_deployment(
    #         name=name,
    #         model_uri=model_uri,
    #         flavor=flavor,
    #         config=config,
    #         endpoint=endpoint
    #     )
    #
    #     self._env_cleanup()
    #
    #     return result
    #
    # # TODO should we add ability to store to specified locations - DB, Object storage, etc.?
    # def predict(
    #         self,
    #         deployment_name: str,
    #         inputs: Any,
    #         endpoint: str = None
    # ):
    #     client = self.get_conn()
    #
    #     result = client.predict(
    #         deployment_name = deployment_name,
    #         inputs=inputs,
    #         endpoint=endpoint
    #     )
    #
    #     self._env_cleanup()
    #
    #     return result.to_json()


