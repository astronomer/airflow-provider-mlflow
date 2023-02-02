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
    :param target_uri: Sagemaker URI
    :type target_uri: str
    """

    # TODO update hook names to match Airflow naming convention
    hook_name = 'MLflow Deployment'

    def __init__(
            self,
            target_uri: str,
            target_conn_id: str = None
    ) -> None:
        super().__init__()
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
        Returns MLflow Sagemaker Client.
        """

        conn_type = self.get_connection(self.target_conn_id).conn_type

        # TODO see if other connection types for AWS need to be handled
        if conn_type == 'aws':
            aws_auth_env = self.aws_conn_dict()
            self._set_env_variables(aws_auth_env)

        return get_deploy_client(self.target_uri)

