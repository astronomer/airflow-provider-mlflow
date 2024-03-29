import requests_mock
from unittest import mock

from mlflow_provider.operators.registry import *


# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_MLFLOW_CONN='http://https%3A%2F%2Fwww.httpbin.org%2F')
class TestRegistryOperators:

    @requests_mock.Mocker(kw='m')
    def test_operator_create_registered_model(self, **kwargs):

        # Mock endpoint
        m = kwargs['m']
        m.post('https://www.httpbin.org/api/2.0/mlflow/registered-models/create', json={'data': 'mocked response'})

        operator = CreateRegisteredModelOperator(
            task_id='run_operator',
            mlflow_conn_id='mlflow_conn',
            name='model_name',
            description='test description',
            tags=[{'key': 'key1', 'value': 'value1'}]
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        response_payload = operator.execute(context={})

        # Assert the API call returns expected mocked payload
        assert response_payload['data'] == 'mocked response'

    @requests_mock.Mocker(kw='m')
    def test_operator_get_registered_model(self, **kwargs):

        # Mock endpoint
        m = kwargs['m']
        m.get('https://www.httpbin.org/api/2.0/mlflow/registered-models/get', json={'data': 'mocked response'})

        operator = GetRegisteredModelOperator(
            task_id='run_operator',
            mlflow_conn_id='mlflow_conn',
            name='model_name',
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        response_payload = operator.execute(context={})

        # Assert the API call returns expected mocked payload
        assert response_payload['data'] == 'mocked response'

    @requests_mock.Mocker(kw='m')
    def test_operator_delete_registered_model(self, **kwargs):

        # Mock endpoint
        m = kwargs['m']
        m.delete('https://www.httpbin.org/api/2.0/mlflow/registered-models/delete', json={})

        operator = DeleteRegisteredModelOperator(
            task_id='run_operator',
            mlflow_conn_id='mlflow_conn',
            name='model_name',
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        response_payload = operator.execute(context={})

        # Assert the API call returns expected mocked payload
        assert response_payload is None

    @requests_mock.Mocker(kw='m')
    def test_operator_get_model_versions(self, **kwargs):

        # Mock endpoint
        m = kwargs['m']
        m.post(
            'https://www.httpbin.org/api/2.0/mlflow/registered-models/get-latest-versions',
            json={'data': 'mocked response'}
        )

        operator = GetLatestModelVersionsOperator(
            task_id='run_operator',
            mlflow_conn_id='mlflow_conn',
            name='model_name',
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        response_payload = operator.execute(context={})

        # Assert the API call returns expected mocked payload
        assert response_payload['data'] == 'mocked response'

    @requests_mock.Mocker(kw='m')
    def test_operator_create_model_version(self, **kwargs):

        # Mock endpoint
        m = kwargs['m']
        m.post(
            'https://www.httpbin.org/api/2.0/mlflow/model-versions/create',
            json={'data': 'mocked response'}
        )

        operator = CreateModelVersionOperator(
            task_id='run_operator',
            mlflow_conn_id='mlflow_conn',
            name='model_name',
            source='s3://test_source'
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        response_payload = operator.execute(context={})

        # Assert the API call returns expected mocked payload
        assert response_payload['data'] == 'mocked response'

    @requests_mock.Mocker(kw='m')
    def test_operator_get_model_version(self, **kwargs):

        # Mock endpoint
        m = kwargs['m']
        m.get(
            'https://www.httpbin.org/api/2.0/mlflow/model-versions/get',
            json={'data': 'mocked response'}
        )

        operator = GetModelVersionOperator(
            task_id='run_operator',
            mlflow_conn_id='mlflow_conn',
            name='model_name',
            version='1'
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        response_payload = operator.execute(context={})

        # Assert the API call returns expected mocked payload
        assert response_payload['data'] == 'mocked response'


    @requests_mock.Mocker(kw='m')
    def test_operator_delete_model_version(self, **kwargs):

        # Mock endpoint
        m = kwargs['m']
        m.delete(
            'https://www.httpbin.org/api/2.0/mlflow/model-versions/delete',
            json={}
        )

        operator = DeleteModelVersionOperator(
            task_id='run_operator',
            mlflow_conn_id='mlflow_conn',
            name='model_name',
            version='1'
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        response_payload = operator.execute(context={})

        # Assert the API call returns expected mocked payload
        assert response_payload is None

    @requests_mock.Mocker(kw='m')
    def test_operator_transition_model_version_stage(self, **kwargs):

        # Mock endpoint
        m = kwargs['m']
        m.post(
            'https://www.httpbin.org/api/2.0/mlflow/model-versions/transition-stage',
            json={'data': 'mocked response'}

        )

        operator = TransitionModelVersionStageOperator(
            task_id='run_operator',
            mlflow_conn_id='mlflow_conn',
            name='model_name',
            version='1',
            stage='Staging'
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        response_payload = operator.execute(context={})

        # Assert the API call returns expected mocked payload
        assert response_payload['data'] == 'mocked response'
