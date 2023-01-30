"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_mlflow_registry_operators.TestRegistryOperators

"""

import json
import logging
import os
import pytest
import requests_mock
import unittest
from unittest import mock

# Import Operator
from mlflow_provider.operators.registry import *
from mlflow_provider.hooks.mlflow_hook import MLflowClientHook


log = logging.getLogger(__name__)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_MLFLOW_CONN='http://https%3A%2F%2Fwww.httpbin.org%2F')
class TestRegistryOperators(unittest.TestCase):
    """
    Test Registry Operators.
    """

    @requests_mock.mock()
    def test_operator_create_registered_model(self, m):

        # Mock endpoint
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

        log.info(response_payload)

        # Assert the API call returns expected mocked payload
        assert response_payload['data'] == 'mocked response'

    @requests_mock.mock()
    def test_operator_get_registered_model(self, m):

        # Mock endpoint
        m.get('https://www.httpbin.org/api/2.0/mlflow/registered-models/get', json={'data': 'mocked response'})

        operator = GetRegisteredModelOperator(
            task_id='run_operator',
            mlflow_conn_id='mlflow_conn',
            name='model_name',
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        response_payload = operator.execute(context={})

        log.info(response_payload)

        # Assert the API call returns expected mocked payload
        assert response_payload['data'] == 'mocked response'

    @requests_mock.mock()
    def test_operator_delete_registered_model(self, m):

        # Mock endpoint
        m.delete('https://www.httpbin.org/api/2.0/mlflow/registered-models/delete', json={})

        operator = DeleteRegisteredModelOperator(
            task_id='run_operator',
            mlflow_conn_id='mlflow_conn',
            name='model_name',
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        response_payload = operator.execute(context={})

        log.info(response_payload)

        # Assert the API call returns expected mocked payload
        assert response_payload is None



if __name__ == '__main__':
    unittest.main()
