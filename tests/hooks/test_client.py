import requests_mock
from unittest import mock

from mlflow_provider.hooks.client import MLflowClientHook

# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_MLFLOW_CONNECTION='http://https%3A%2F%2Fwww.httpbin.org%2F')
class TestMLflowClientHook:

    @requests_mock.Mocker(kw='m')
    def test_post(self, **kwargs):

        # Mock endpoint
        m = kwargs['m']
        m.post('https://www.httpbin.org/api/endpoint', json={'data': 'mocked response'})

        # Instantiate hook
        hook = MLflowClientHook(
            mlflow_conn_id='mlflow_connection',
            method='post'
        )

        # Hook's run method executes an API call
        response = hook.run(
            endpoint='api/endpoint',
            request_params={'param1': 'value1'}
        )

        # Retrieve response payload
        payload = response.json()

        # Assert success status code
        assert response.status_code == 200

        # Assert the API call returns expected mocked payload
        assert payload['data'] == 'mocked response'

    @requests_mock.Mocker(kw='m')
    def test_get(self, **kwargs):

        # Mock endpoint
        m = kwargs['m']
        m.get('https://www.httpbin.org/api/endpoint', json={'data': 'mocked response'})

        # Instantiate hook
        hook = MLflowClientHook(
            mlflow_conn_id='mlflow_connection',
            method='get'
        )

        # Hook's run method executes an API call
        response = hook.run(
            endpoint='api/endpoint',
            request_params={'param1': 'value1', 'param2': 'value2'}
        )

        # Retrieve response payload
        payload = response.json()

        # Assert success status code
        assert response.status_code == 200

        # Assert the API call returns expected mocked payload
        assert payload['data'] == 'mocked response'

    @requests_mock.Mocker(kw='m')
    def test_delete(self, **kwargs):

        # Mock endpoint
        m = kwargs['m']
        m.delete('https://www.httpbin.org/api/endpoint')

        # Instantiate hook
        hook = MLflowClientHook(
            mlflow_conn_id='mlflow_connection',
            method='DELETE'
        )

        # Hook's run method executes an API call
        response = hook.run(
            endpoint='api/endpoint',
            request_params={'param1': 'value1'}
        )

        # Assert success status code
        assert response.status_code == 200
