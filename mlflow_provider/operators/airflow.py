from typing import Any, Callable, Dict, Optional, Union, List

import numpy
import pandas
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from scipy.sparse import csc_matrix, csr_matrix

from mlflow_provider.hooks.pyfunc import MLflowPyfuncHook


class AirflowPredict(BaseOperator):
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
        'model_uri',
        'dst_path'
    ]
    template_fields_renderers = {}
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
            self,
            *,
            mlflow_conn_id: str = 'mlflow_default',
            model_uri: str,
            supress_warnings: bool = False,
            dst_path: Optional[str] = None,
            data: Union[pandas.core.frame.DataFrame, pandas.core.series.Series, numpy.ndarray, csc_matrix, csr_matrix, List[Any], Dict[str, Any]],
            **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.model_uri = model_uri
        self.supress_warnings = supress_warnings
        self.dst_path = dst_path
        self.data = data
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        pyfunc = MLflowPyfuncHook(
            mlflow_conn_id=self.mlflow_conn_id
        ).get_conn()

        loaded_model = pyfunc.load_model(
            model_uri = self.model_uri,
            supress_warnings = self.supress_warnings,
            dst_path = self.dst_path
        )

        result = loaded_model.predict(data=self.data)

        return result.to_json()
