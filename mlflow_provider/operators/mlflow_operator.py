from typing import Any, Callable, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from mlflow_provider.hooks.mlflow_hook import MLflowClientHook


class CreateRegisteredModelOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action.

    :param mlflow_conn_id: connection to run the operator with
    :type mlflow_conn_id: str
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: str
    :param method: The HTTP method to use, default = "POST"
    :type method: str
    :param data: The data to pass
    :type data: a dictionary of key/value string pairs
    :param headers: The HTTP headers to be added to the request
    :type headers: a dictionary of string key/value pairs
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'name',
        'tags',
        'description'
    ]
    template_fields_renderers = {'tags': 'json'}
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
        self,
        *,
        mlflow_conn_id: str = 'mlflow_default',
        name: str,
        tags: Optional[list[Dict[str, str]]],
        description: Optional[str],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.method = 'POST'
        self.endpoint = 'api/2.0/mlflow/registered-models/create'
        self.name = name
        self.tags = tags or None
        self.description = description or None
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        request_params = {
            'name': self.name,
        }

        if self.description:
            request_params['description'] = self.description

        if self.tags:
            request_params['tags'] = self.tags

        hook = MLflowClientHook(self.method, mlflow_conn_id=self.mlflow_conn_id)

        self.log.info("Call HTTP method")

        print(request_params)

        response = hook.run(
            endpoint=self.endpoint,
            request_params=request_params)

        # TODO handle response errors codes

        return response.json()
