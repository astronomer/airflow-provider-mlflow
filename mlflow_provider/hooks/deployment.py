from __future__ import annotations

from typing import Any, Dict

from mlflow.deployments import BaseDeploymentClient, get_deploy_client

from mlflow_provider.hooks.base import MLflowBaseHook


class MLflowDeploymentHook(MLflowBaseHook):
    """
    Hook that interacts with the mlflow.deployments module in the MLflow library.
    https://www.mlflow.org/docs/latest/python_api/mlflow.deployments.html

    :param aws_conn_id: AWS connection to use with hook
    :type aws_conn_id: str
    :param target_uri: target system URI to deploy model to. (ie 'sagemaker')
    :type target_uri: str
    """

    hook_name = 'MLflow Deployment'

    def __init__(
            self,
            mlflow_conn_id: str,
            target_uri: str,
            target_conn_id: str | None = None
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

    def _env_cleanup(self):
        target_conn_type = self.get_connection(self.target_conn_id).conn_type
        if target_conn_type == 'aws':
            aws_auth_env = self.aws_conn_dict()
            self.unset_env_variables(other_env=aws_auth_env)
        else:
            self.unset_env_variables()

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
            flavor: str | None = None,
            config: dict | None = None,
            endpoint: str | None = None
    ):
        """
        Creates a deployment in the target system.
        https://www.mlflow.org/docs/latest/python_api/mlflow.deployments.html#mlflow.deployments.BaseDeploymentClient.create_deployment

        :param name: Unique name to use for deployment.
        :type name: str
        :param model_uri: URI of model to deploy
        :type model_uri: str
        :param flavor: (optional) Model flavor to deploy. If unspecified, a default flavor will be chosen.
        :type flavor: str
        :param config: (optional) Dict containing updated target-specific configuration for the deployment
        :type config: dict
        :param endpoint: (optional) Endpoint to create the deployment under. May not be supported by all targets
        :type endpoint: str
        :return: Dict corresponding to created deployment, which must contain the ‘name’ key.
        :rtype: dict
        """
        client = self.get_conn()

        result = client.create_deployment(
            name=name,
            model_uri=model_uri,
            flavor=flavor,
            config=config,
            endpoint=endpoint
        )

        self._env_cleanup()

        return result

    # TODO should we add ability to store to specified locations - DB, Object storage, etc.?
    def predict(
            self,
            deployment_name: str,
            inputs: Any,
            endpoint: str | None = None
    ):
        """
        Makes a prediction request to the specified deployment. https://www.mlflow.org/docs/latest/python_api/mlflow.deployments.html#mlflow.deployments.BaseDeploymentClient.predict

        :param deployment_name: Name of deployment to predict against
        :type deployment_name: str
        :param inputs: Input data (or arguments) to pass to the deployment or model endpoint for inference
        :type inputs: Any
        :param endpoint: Endpoint to predict against. May not be supported by all targets
        :type endpoint:
        :return: A mlflow.deployments.PredictionsResponse instance representing the predictions and associated Model Server response metadata as a JSON.
        :rtype: dict
        """
        client = self.get_conn()

        result = client.predict(
            deployment_name = deployment_name,
            inputs=inputs,
            endpoint=endpoint
        )

        self._env_cleanup()

        return result.to_json()
