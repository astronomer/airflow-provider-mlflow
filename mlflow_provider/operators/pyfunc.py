import json
import logging
from abc import ABC
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict, Optional, Union, List

import numpy
import pandas
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils import python_virtualenv
from airflow.operators.python import _BasePythonVirtualenvOperator, PythonVirtualenvOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from scipy.sparse import csc_matrix, csr_matrix

import yaml
from mlflow_provider.hooks.pyfunc import MLflowPyfuncHook


def _model_load_and_predict(
        host,
        login,
        password,
        model_uri,
        suppress_warnings,
        dst_path,
        data
) -> json:

    # pyfunc_hook = MLflowPyfuncHook(mlflow_conn_id=self.mlflow_conn_id).get_conn()
    import os
    from mlflow import pyfunc

    if 'cloud.databricks.com' in host:
        os.environ['MLFLOW_TRACKING_URI'] = 'databricks'
        os.environ['DATABRICKS_HOST'] = host
        os.environ['DATABRICKS_TOKEN'] = password
    else:
        os.environ['MLFLOW_TRACKING_URI'] = host

    if login == 'token':
        os.environ['MLFLOW_TRACKING_TOKEN'] = host
    else:
        os.environ['MLFLOW_TRACKING_USERNAME'] = login
        os.environ['MLFLOW_TRACKING_PASSWORD'] = password


    loaded_model = pyfunc.load_model(
        model_uri=model_uri,
        suppress_warnings=suppress_warnings,
        dst_path=dst_path
    )

    result = loaded_model.predict(data=data)
    return result.to_json()


class AirflowPredict(_BasePythonVirtualenvOperator):
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
            suppress_warnings: bool = False,
            dst_path: Optional[str] = None,
            data: Union[pandas.core.frame. DataFrame, pandas.core.series.Series, numpy.ndarray, csc_matrix, csr_matrix, List[Any], Dict[str, Any]],
            # python_callable: Optional[Callable] = _model_load_and_predict,
            **kwargs: Any
    ) -> None:
        self.requirements = None
        self.system_site_packages = False
        self.mlflow_conn_id = mlflow_conn_id
        self.model_uri = model_uri
        self.suppress_warnings = suppress_warnings
        self.dst_path = dst_path
        self.data = data
        self.conn = BaseHook.get_connection(self.mlflow_conn_id)
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")
        super().__init__(
            python_callable=_model_load_and_predict,
            use_dill=False,
            op_args=None,
            op_kwargs={
                'host':self.conn.host,
                'login':self.conn.login,
                'password':self.conn.password,
                'model_uri':self.model_uri,
                'suppress_warnings':self.suppress_warnings,
                'dst_path':self.dst_path,
                'data':self.data
            },
            string_args=None,
            templates_dict=None,
            templates_exts=None,
            expect_airflow=False,
            **kwargs)


    def execute(self, context: Dict[str, Any]) -> Any:

        from mlflow import artifacts

        pyfunc = MLflowPyfuncHook(
            mlflow_conn_id=self.mlflow_conn_id
        ).get_conn()

        requirements_file_name = pyfunc.get_model_dependencies(self.model_uri)
        print(requirements_file_name)

        for line in open(requirements_file_name, 'r'):
            print(line)


        # conda_yaml_path = pyfunc.get_model_dependencies(self.model_uri, 'conda')
        # with open(conda_yaml_path, "r") as yml:
        #     try:
        #         conda_yaml = yaml.safe_load(yml)
        #         logging.info(conda_yaml)
        #     except yaml.YAMLError as exc:
        #         raise AirflowException(exc)
        #
        # for dependency in conda_yaml['dependencies']:
        #     if type(dependency) is str and 'python=' in dependency:
        #         python_version = dependency.split('=')[-1]
        #
        #         print(python_version)

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
