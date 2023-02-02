from typing import Any, Callable, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from mlflow_provider.hooks.deployment import MLflowDeploymentHook


class CreateDeploymentOperator(BaseOperator):

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'name',
        'model_uri',
        'endpoint'
    ]
    # template_fields_renderers = {'tags': 'json'}
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
            target_conn_id: str = None,
            flavor: Optional[str] = None,
            config: Optional[dict] = None,
            endpoint: Optional[str] = None,
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

        client = MLflowDeploymentHook(target_uri=self.target_uri, target_conn_id=self.target_conn_id)

        result = client.create_deployment(
            name=self.name,
            model_uri = self.model_uri,
            flavor = self.flavor,
            config = self.config,
            endpoint = self.endpoint
        )

        client.unset_env_variables()

        return result




