from __future__ import annotations
from typing import Any, Dict, Optional, List

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from mlflow_provider.hooks.client import MLflowClientHook


class CreateRegisteredModelOperator(BaseOperator):
    """
    Creates a new registered model in MLflow.

    :param mlflow_conn_id: connection to run the operator with
    :type mlflow_conn_id: str
    :param name: name of the registered model to be created
    :type name: str
    :param tags: tags to add to the registered model
    :type tags: list[Dict[str, str]]
    :param description: description of model
    :type description: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'name',
        'tags',
        'description'
    ]
    template_fields_renderers: Dict[str, str] = {'tags': 'json'}
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
            self,
            *,
            mlflow_conn_id: str = 'mlflow_default',
            name: str,
            tags: List[Dict[str, str]] | None = None,
            description: str | None = None,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.method = 'POST'
        self.endpoint = 'api/2.0/mlflow/registered-models/create'
        self.name = name
        self.tags = tags
        self.description = description
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        request_params: Dict[str, Any] = {
            'name': self.name,
        }

        if self.description:
            request_params['description'] = self.description

        if self.tags:
            request_params['tags'] = self.tags

        hook = MLflowClientHook(self.method, mlflow_conn_id=self.mlflow_conn_id)

        self.log.info("Call HTTP method")

        response = hook.run(
            endpoint=self.endpoint,
            request_params=request_params)

        if response.status_code != 200:
            raise AirflowException(f"Error {response.status_code}: {response.text}")

        return response.json()


class GetRegisteredModelOperator(BaseOperator):
    """
    Gets a registered model from MLflow based on name.

    :param mlflow_conn_id: connection to run the operator with
    :type mlflow_conn_id: str
    :param name: name of the registered model to get
    :type name: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'name',
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
            self,
            *,
            mlflow_conn_id: str = 'mlflow_default',
            name: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.method = 'GET'
        self.endpoint = 'api/2.0/mlflow/registered-models/get'
        self.name = name
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        request_params = {
            'name': self.name,
        }

        hook = MLflowClientHook(self.method, mlflow_conn_id=self.mlflow_conn_id)

        self.log.info("Call HTTP method")

        response = hook.run(
            endpoint=self.endpoint,
            request_params=request_params)

        if response.status_code != 200:
            raise AirflowException(f"Error {response.status_code}: {response.text}")

        return response.json()


# TODO Update and rename endpoints


class DeleteRegisteredModelOperator(BaseOperator):
    """
    Deletes a registered model from MLflow based on name.

    :param mlflow_conn_id: connection to run the operator with
    :type mlflow_conn_id: str
    :param name: name of the registered model to delete
    :type name: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'name',
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
            self,
            *,
            mlflow_conn_id: str = 'mlflow_default',
            name: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.method = 'DELETE'
        self.endpoint = 'api/2.0/mlflow/registered-models/delete'
        self.name = name
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        request_params = {
            'name': self.name,
        }

        hook = MLflowClientHook(self.method, mlflow_conn_id=self.mlflow_conn_id)

        self.log.info("Call HTTP method")

        response = hook.run(
            endpoint=self.endpoint,
            request_params=request_params)

        if response.status_code != 200:
            raise AirflowException(f"Error {response.status_code}: {response.text}")


class GetLatestModelVersionsOperator(BaseOperator):
    """
    Gets the latest model versions from MLflow Registry based on name.

    :param mlflow_conn_id: connection to run the operator with
    :type mlflow_conn_id: str
    :param name: name of the registered model to get versions for
    :type name: str
    :param stages: List of stages to get
    :type stages: list
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'name',
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
            self,
            *,
            mlflow_conn_id: str = 'mlflow_default',
            name: str,
            stages: List[str] | None = None,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.method = 'POST'
        self.endpoint = 'api/2.0/mlflow/registered-models/get-latest-versions'
        self.name = name
        self.stages = stages
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        request_params: Dict[str, Any] = {
            'name': self.name,
        }

        if self.stages:
            request_params['stages'] = self.stages

        hook = MLflowClientHook(self.method, mlflow_conn_id=self.mlflow_conn_id)

        self.log.info("Call HTTP method")

        response = hook.run(
            endpoint=self.endpoint,
            request_params=request_params)

        if response.status_code != 200:
            raise AirflowException(f"Error {response.status_code}: {response.text}")

        return response.json()


class CreateModelVersionOperator(BaseOperator):
    """
    Create  a model version in MLflow Registry.

    :param mlflow_conn_id: connection to run the operator with
    :type mlflow_conn_id: str
    :param name: name of the registered model
    :type name: str
    :param source: URI indicating the location of the model artifacts
    :type source: str
    :param run_id: MLflow run ID for correlation, if source was generated by an experiment run in MLflow tracking server
    :type run_id: str
    :param tags: Additional metadata for model version
    :type tags: list
    :param run_link: MLflow run link - this is the exact link of the run that generated this model version, potentially hosted at another instance of MLflow.
    :type run_link: str
    :param description: description for model version
    :type description: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'name',
        'source',
        'run_id',
        'tags',
        'run_link',
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
            source: str,
            run_id: str | None = None,
            tags: List[Dict[str, str]] | None = None,
            run_link: str | None = None,
            description: str | None = None,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.method = 'POST'
        self.endpoint = 'api/2.0/mlflow/model-versions/create'
        self.name = name
        self.source = source
        self.run_id = run_id
        self.tags = tags
        self.run_link = run_link
        self.description = description
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        request_params: Dict[str, Any] = {
            'name': self.name,
            'source': self.source
        }

        if self.run_id:
            request_params['run_id'] = self.run_id

        if self.tags:
            request_params['tags'] = self.tags

        if self.run_link:
            request_params['run_link'] = self.run_link

        if self.description:
            request_params['description'] = self.description

        hook = MLflowClientHook(self.method, mlflow_conn_id=self.mlflow_conn_id)

        self.log.info("Call HTTP method")

        response = hook.run(
            endpoint=self.endpoint,
            request_params=request_params)

        if response.status_code != 200:
            raise AirflowException(f"Error {response.status_code}: {response.text}")

        return response.json()


class GetModelVersionOperator(BaseOperator):
    """
    Get specific model version from MLflow Registry.

    :param mlflow_conn_id: connection to run the operator with
    :type mlflow_conn_id: str
    :param name: name of the registered model
    :type name: str
    :param version: Model version number
    :type version: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'name',
        'version'
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
            self,
            *,
            mlflow_conn_id: str = 'mlflow_default',
            name: str,
            version: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.method = 'GET'
        self.endpoint = 'api/2.0/mlflow/model-versions/get'
        self.name = name
        self.version = version
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        request_params = {
            'name': self.name,
            'version': self.version
        }

        hook = MLflowClientHook(self.method, mlflow_conn_id=self.mlflow_conn_id)

        self.log.info("Call HTTP method")

        response = hook.run(
            endpoint=self.endpoint,
            request_params=request_params)

        if response.status_code != 200:
            raise AirflowException(f"Error {response.status_code}: {response.text}")

        return response.json()


# TODO UpdateModelVersion


class DeleteModelVersionOperator(BaseOperator):
    """
    Delete specific model version from MLflow Registry.

    :param mlflow_conn_id: connection to run the operator with
    :type mlflow_conn_id: str
    :param name: name of the registered model
    :type name: str
    :param version: Model version number
    :type version: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'name',
        'version'
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
            self,
            *,
            mlflow_conn_id: str = 'mlflow_default',
            name: str,
            version: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.method = 'DELETE'
        self.endpoint = 'api/2.0/mlflow/model-versions/delete'
        self.name = name
        self.version = version
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        request_params = {
            'name': self.name,
            'version': self.version
        }

        hook = MLflowClientHook(self.method, mlflow_conn_id=self.mlflow_conn_id)

        self.log.info("Call HTTP method")

        response = hook.run(
            endpoint=self.endpoint,
            request_params=request_params)

        if response.status_code != 200:
            raise AirflowException(f"Error {response.status_code}: {response.text}")


# TODO determine if we want to implement Search ModelVersions

# TODO determine if we want to implement Get Download URI For ModelVersion Artifacts


class TransitionModelVersionStageOperator(BaseOperator):
    """
    Transition model version to new stage.

    :param mlflow_conn_id: connection to run the operator with
    :type mlflow_conn_id: str
    :param name: name of the registered model
    :type name: str
    :param version: Model version number
    :type version: str
    :param stage: Transition model_version to new stage
    :type stage: str
    :param archive_existing_versions: When transitioning a model version to a particular stage, this flag dictates whether all existing model versions in that stage should be atomically moved to the “archived” stage.
    :type archive_existing_versions: bool
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'name',
        'version'
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
            self,
            *,
            mlflow_conn_id: str = 'mlflow_default',
            name: str,
            version: str,
            stage: str,
            archive_existing_versions: Optional[bool] = False,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.mlflow_conn_id = mlflow_conn_id
        self.method = 'POST'
        self.endpoint = 'api/2.0/mlflow/model-versions/transition-stage'
        self.name = name
        self.version = version
        self.stage = stage
        self.archive_existing_versions = archive_existing_versions
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:

        request_params = {
            'name': self.name,
            'version': self.version,
            'stage': self.stage,
            'archive_existing_versions': self.archive_existing_versions
        }

        hook = MLflowClientHook(self.method, mlflow_conn_id=self.mlflow_conn_id)

        self.log.info("Call HTTP method")

        response = hook.run(
            endpoint=self.endpoint,
            request_params=request_params)

        if response.status_code != 200:
            raise AirflowException(f"Error {response.status_code}: {response.text}")

        return response.json()

# TODO determine if we want to implement Search RegisteredModels

# TODO determine how to implement registered model and model version tags
