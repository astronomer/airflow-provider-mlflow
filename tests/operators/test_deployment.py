from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock

import logging

import pytest
from pandas import DataFrame

from mlflow_provider.operators.deployment import *

log = logging.getLogger(__name__)

TEST_MLFLOW_CONN_ID = 'mlflow_conn'
TEST_NAME = 'test_model_name'
TEST_MODEL_URI = 's3://test_bucket/test_model'
TEST_TARGET_URI = 'sagemaker'
TEST_TARGET_CONN_ID = 'target_conn'
TEST_DEPLOYMENT_NAME = 'test_deployment_name'

TEST_SAMPLE = {
    "columns": ["test_column"],
    "data": [[1], [2]]
}

TEST_ENDPOINT = 'test_endpoint'

# Mock the Airflow connections
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_MLFLOW_CONN='http://username:password@servvice.com:80/https?headers=header')
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_TARGET_CONN='aws://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY@')
class TestCreateDeploymentOperator:

    @mock.patch("mlflow_provider.hooks.deployment.MLflowDeploymentHook.create_deployment")
    def test_execute(self, mock_hook):
        operator = CreateDeploymentOperator(
            task_id='test_task_id',
            mlflow_conn_id = TEST_MLFLOW_CONN_ID,
            name = TEST_NAME,
            model_uri = TEST_MODEL_URI,
            target_uri = TEST_TARGET_URI,
            target_conn_id=TEST_TARGET_CONN_ID,
        )

        operator.execute(context=MagicMock())
        mock_hook.assert_called_once_with(
            name=TEST_NAME,
            model_uri=TEST_MODEL_URI,
            flavor = None,
            config = None,
            endpoint = None
        )

@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_MLFLOW_CONN='http://username:password@servvice.com:80/https?headers=header')
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_TARGET_CONN='aws://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY@')
class TestPredictOperator:

    @mock.patch("mlflow_provider.hooks.deployment.MLflowDeploymentHook.predict")
    def test_operator_predict(self, mock_hook):
        """
        Test predict operator to make sure it calls the hook with correct parameters.
        """

        with pytest.raises(ValueError):
            operator = PredictOperator(
                task_id='test_prediction',
                target_uri = TEST_TARGET_URI,
                target_conn_id = TEST_TARGET_CONN_ID,
                deployment_name = TEST_DEPLOYMENT_NAME,
                inputs=DataFrame(data=TEST_SAMPLE['data'], columns=TEST_SAMPLE['columns'])
        )

            operator.execute(context=MagicMock())
            mock_hook.assert_called_once_with(
                deployment_name = TEST_DEPLOYMENT_NAME,
                inputs = DataFrame(data=TEST_SAMPLE['data'], columns=TEST_SAMPLE['columns']),
                endpoint = TEST_ENDPOINT
            )
