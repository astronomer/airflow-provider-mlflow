from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock
import logging

import pytest

# Import Operator
from mlflow_provider.operators.pyfunc import *


log = logging.getLogger(__name__)

TEST_MLFLOW_CONN_ID = 'mlflow_conn'
TEST_NAME = 'test_model_name'
TEST_MODEL_URI = 's3://test_bucket/test_model'
TEST_TARGET_URI = 'sagemaker'
TEST_TARGET_CONN_ID = 'target_conn'
TEST_DEPLOYMENT_NAME = 'test_deployment_name'

TEST_SAMPLE = {
    "columns": ["test_column"],
    "data": [
        [1], [2], [3]
    ]
}

TEST_ENDPOINT = 'test_endpoint'

# Mock the Airflow connections
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_MLFLOW_CONN='http://username:password@servvice.com:80/https?headers=header')
class TestAirflowPredictOperator:
    """
    Test Airflow Predict Operator.
    """

    @mock.patch("mlflow.pyfunc.get_model_dependencies")
    @mock.patch("mlflow_provider.operators.pyfunc.AirflowPredictOperator._execute_python_callable_in_subprocess")
    def test_execute(self, mock_get_dependencies, mock_execute_python_callable):

        with pytest.raises(Exception):
            operator = AirflowPredictOperator(
                task_id='test_task_id',
                mlflow_conn_id = TEST_MLFLOW_CONN_ID,
                model_uri = TEST_MODEL_URI,
                data=TEST_SAMPLE
            )

            operator.execute(context=MagicMock())
            mock_get_dependencies.assert_called_once_with(TEST_MODEL_URI)
            mock_execute_python_callable.assert_called_once()
