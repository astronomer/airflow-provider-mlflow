from __future__ import annotations
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Union, List, Sequence

from numpy import ndarray
from pandas.core.frame import DataFrame
from pandas.core.series import Series
from airflow.exceptions import AirflowException
from airflow.operators.python import _BasePythonVirtualenvOperator
from airflow.utils import python_virtualenv
from scipy.sparse import csc_matrix, csr_matrix

from mlflow_provider.hooks.pyfunc import MLflowPyfuncHook


def _model_load_and_predict(
        host,
        login,
        password,
        model_uri,
        suppress_warnings,
        dst_path,
        data,
):
    """
    Python Callable passed to _BasePythonVirtualenvOperator

    :param host: MLflow host
    :type host: str
    :param login: MLflow login (for API keys use 'token')
    :type login: str
    :param password: MLflow password (if using an API Key use that here)
    :type password: str
    :param model_uri: MLflow model URI
    :type model_uri: str
    :param suppress_warnings:
    :type suppress_warnings: bool
    :param dst_path: The local filesystem path to which to download the model artifact. This directory must already exist. If unspecified, a local output path will be created.
    :type dst_path: Optional[str]
    :param data: Model input
    :type data: Union[pandas.DataFrame, numpy.ndarray, scipy.sparse.(csc.csc_matrix | csr.csr_matrix), List[Any], or Dict[str, numpy.ndarray].
    :return: Predictions/Inference results
    """

    import os
    from mlflow import pyfunc
    from numpy import ndarray
    from numpy import array as nparray

    # Setup env variables for authentication
    if 'cloud.databricks.com' in host:
        os.environ['MLFLOW_TRACKING_URI'] = 'databricks'
        os.environ['DATABRICKS_HOST'] = host
        os.environ['DATABRICKS_TOKEN'] = password
    else:
        os.environ['MLFLOW_TRACKING_URI'] = host

    if login == 'token':
        os.environ['MLFLOW_TRACKING_TOKEN'] = password
    else:
        os.environ['LOGNAME'] = login
        os.environ['MLFLOW_TRACKING_USERNAME'] = login
        os.environ['MLFLOW_TRACKING_PASSWORD'] = password


    # Load Model
    loaded_model = pyfunc.load_model(
        model_uri=model_uri,
        suppress_warnings=suppress_warnings,
        dst_path=dst_path
    )

    loaded_model.metadata.signature = None

    # Run Inference and convert results to list of json depending on result type
    if isinstance(data, list):
        result = loaded_model.predict(data=nparray(data))
    else:
        result = loaded_model.predict(data=data)

    if type(result) is ndarray:
        return result.tolist()
    else:
        return result.to_json()


class ModelLoadAndPredictOperator(_BasePythonVirtualenvOperator):
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
    template_fields: Sequence[str] = (
        'model_uri',
        'dst_path',
        'data',
        "op_args", "op_kwargs"
    )
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}
    template_ext = ()
    ui_color = '#f4a460'

    def __init__(
            self,
            *,
            mlflow_conn_id: str = 'mlflow_default',
            model_uri: str,
            suppress_warnings: bool = False,
            dst_path: str | None = None,
            data: Union[DataFrame, Series, ndarray, csc_matrix, csr_matrix, List[Any], Dict[str, Any]],
            **kwargs: Any
    ) -> None:
        self.requirements = None
        self.system_site_packages = False
        self.mlflow_conn_id = mlflow_conn_id
        self.model_uri = model_uri
        self.suppress_warnings = suppress_warnings
        self.dst_path = dst_path
        self.data = data
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")
        super().__init__(
            python_callable=_model_load_and_predict,
            use_dill=False,
            op_args=None,
            op_kwargs={
                "host": f"{{{{ conn.{self.mlflow_conn_id}.host }}}}",
                "login": f"{{{{ conn.{self.mlflow_conn_id}.login }}}}",
                "password": f"{{{{ conn.{self.mlflow_conn_id}.password }}}}",
                'model_uri':self.model_uri,
                'suppress_warnings':self.suppress_warnings,
                'dst_path':self.dst_path,
                'data':self.data,
            },
            string_args=None,
            templates_dict=None,
            templates_exts=None,
            expect_airflow=False,
            **kwargs)

    def execute(self, context: Dict[str, Any]) -> Any:

        pyfunc = MLflowPyfuncHook(
            mlflow_conn_id=self.mlflow_conn_id
        ).get_conn()

        # Requirements file from MLflow model artifacts
        requirements_file_name = pyfunc.get_model_dependencies(self.model_uri)
        print(requirements_file_name)

        for line in open(requirements_file_name, 'r'):
            print(line)


        # Create virtualenv and run python callable
        with TemporaryDirectory(prefix="venv") as tmp_dir:
            tmp_path = Path(tmp_dir)

            python_virtualenv.prepare_virtualenv(
                venv_directory=tmp_dir,
                python_bin=None,
                system_site_packages=self.system_site_packages,
                requirements_file_path=requirements_file_name
            )
            python_path = tmp_path / "bin" / "python"

            return self._execute_python_callable_in_subprocess(python_path, tmp_path)

    def _iter_serializable_context_keys(self):
        yield from self.BASE_SERIALIZABLE_CONTEXT_KEYS
        if self.system_site_packages or "apache-airflow" in self.requirements:
            yield from self.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS
        elif "pendulum" in self.requirements:
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS
