from typing import Any, Callable, Dict, Optional, Union

import requests
import tenacity
from airflow.models import Connection

from airflow.exceptions import AirflowException
from mlflow_provider.hooks.base import MLflowBaseHook
from mlflow.deployments import BaseDeploymentClient, get_deploy_client



class MLflowDeploymentHook(MLflowBaseHook):
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
            mlflow_conn_id: str,
            target_uri: str,
            target_conn_id: str = None
    ) -> None:
        super().__init__()
        self.mlflow_conn_id = mlflow_conn_id
        self.target_uri = target_uri
        self.target_conn_id = target_conn_id

    def aws_conn_dict(self) -> Dict:
        aws_conn = self.get_connection(self.target_conn_id)

        aws_env = {
            'AWS_ACCESS_KEY_ID': aws_conn.login,
            'AWS_SECRET_ACCESS_KEY': aws_conn.password
        }

        if 'aws_session_token' in aws_conn.extra_dejson:
            aws_env['AWS_SESSION_TOKEN'] = aws_conn.extra_dejson['aws_session_token']

        return aws_env

    def get_conn(self) -> BaseDeploymentClient:
        """
        Returns MLflow deployment Client.
        """

        target_conn_type = self.get_connection(self.target_conn_id).conn_type

        # TODO see if other connection types for AWS need to be handled
        if target_conn_type == 'aws':
            aws_auth_env = self.aws_conn_dict()
            self._set_env_variables(aws_auth_env)
        else:
            self._set_env_variables()

        return get_deploy_client(self.target_uri)

    def create_deployment(
            self,
            name: str,
            model_uri: str,
            flavor: Optional[str] = None,
            config: Optional[dict] = None,
            endpoint: Optional[str] = None
    ):
        client = self.get_conn()
        
        result = client.create_deployment(
            name=name,
            model_uri=model_uri,
            flavor=flavor,
            config=config,
            endpoint=endpoint
        )

        target_conn_type = self.get_connection(self.target_conn_id).conn_type
        if target_conn_type == 'aws':
            aws_auth_env = self.aws_conn_dict()
            self.unset_env_variables(other_env=aws_auth_env)
        else:
            self.unset_env_variables()

        return result
        



