from __future__ import annotations

from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from mlflow_provider.hooks.deployment import MLflowDeploymentHook


class CreateDeploymentOperator(BaseOperator):
    """
    Deploy MLflow models

    :param name: Unique name to use for deployment
    :type name: str
    :param model_uri: URI of MLflow model
    :type model_uri: str
    :param target_uri: URI of location to deploy the model (ie 'sagemaker')
    :type target_uri: str
    :param target_conn_id: Airflow connection id for target system
    :type target_conn_id: str
    :param flavor: Model flavor to deploy. If unspecified, a default flavor will be chosen.
    :type flavor: str
    :param config: Target-specific configuration for the deployment
    :type config: dict
    :param endpoint: Endpoint to create the deployment under. May not be supported by all targets
    :type endpoint: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'name',
        'model_uri',
        'endpoint',
        'target_uri',
        'flavor',
        'config',
        'endpoint',
    ]
    template_fields_renderers = {'config': 'json'}
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
            self,
            *,
            mlflow_conn_id: str = 'mlflow_default',
            name: str,
            model_uri: str,
            target_uri: str,
            target_conn_id: str | None = None,
            flavor: str | None = None,
            config: dict | None = None,
            endpoint: str | None = None,
            **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.name = name
        self.model_uri = model_uri
        self.target_uri = target_uri
        self.target_conn_id = target_conn_id
        self.flavor = flavor
        self.config = config
        self.endpoint = endpoint
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        client = MLflowDeploymentHook(
            mlflow_conn_id=self.mlflow_conn_id,
            target_uri=self.target_uri,
            target_conn_id=self.target_conn_id
        )

        result = client.create_deployment(
            name=self.name,
            model_uri = self.model_uri,
            flavor = self.flavor,
            config = self.config,
            endpoint = self.endpoint
        )

        return result


class PredictOperator(BaseOperator):
    """
    Get predictions from an MLflow deployment

    :param deployment_name: Name of deployment to predict against
    :type deployment_name: str
    :param inputs: Input data (or arguments) to pass to the deployment or model endpoint for inference
    :type inputs: Any
    :param endpoint: Endpoint to predict against. May not be supported by all targets
    :type endpoint: str
    :param target_uri: URI of location to deploy the model (ie 'sagemaker')
    :type target_uri: str
    :param target_conn_id:  Connection id for target system
    :type target_conn_id: str
    """

    template_fields = [
        'deployment_name',
        'endpoint',
        'target_uri',
        'target_conn_id'
    ]
    template_fields_renderers:Dict[str, str] = {}
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
            self,
            *,
            mlflow_conn_id: str = 'mlflow_default',
            deployment_name: str,
            inputs: Any = None,
            endpoint: str | None = None,
            target_uri: str,
            target_conn_id: str | None = None,
            **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.deployment_name = deployment_name
        self.inputs = inputs
        self.endpoint = endpoint
        self.target_uri = target_uri
        self.target_conn_id = target_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        client = MLflowDeploymentHook(
            mlflow_conn_id=self.mlflow_conn_id,
            target_uri=self.target_uri,
            target_conn_id=self.target_conn_id
        )

        result = client.predict(
            deployment_name=self.deployment_name,
            inputs = self.inputs,
            endpoint = self.endpoint
        )

        return result
