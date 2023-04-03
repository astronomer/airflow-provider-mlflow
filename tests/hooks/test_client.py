"""
Unittest module to test Hooks.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.hooks.test_sample_hook.TestMLflowClientHook

"""

import logging
import requests_mock
import unittest
from unittest import mock

# Import Hook
from mlflow_provider.hooks.client import MLflowClientHook


log = logging.getLogger(__name__)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_MLFLOW_CONNECTION='http://https%3A%2F%2Fwww.httpbin.org%2F')
class TestMLflowClientHook(unittest.TestCase):
    """
    Test MLflow Client Hook.
    """

    @requests_mock.mock()
    def test_post(self, m):

        # Mock endpoint
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

    @requests_mock.mock()
    def test_get(self, m):

        # Mock endpoint
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

    @requests_mock.mock()
    def test_delete(self, m):

        # Mock endpoint
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


if __name__ == '__main__':
    unittest.main()
