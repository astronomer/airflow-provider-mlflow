from __future__ import annotations

from unittest import mock
from mlflow.exceptions import MlflowException

import pytest

# Import Operator
from mlflow_provider.hooks.deployment import MLflowDeploymentHook

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
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_NON_AWS_TARGET_CONN='something_else://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY@')
class TestDeploymentHook:
    """
    Test Deployment Hook.
    """

    @mock.patch("mlflow_provider.hooks.deployment.MLflowDeploymentHook.aws_conn_dict")
    def test_aws_conn_dict_called(self, mock_aws_dict):
        hook = MLflowDeploymentHook(
            mlflow_conn_id = TEST_MLFLOW_CONN_ID,
            target_uri = TEST_TARGET_URI,
            target_conn_id = TEST_TARGET_CONN_ID
        )

        hook.get_conn()
        mock_aws_dict.assert_called_once()


    @mock.patch("mlflow_provider.hooks.deployment.MLflowDeploymentHook.aws_conn_dict")
    def test_non_aws_conn_dict(self, mock_aws_dict):

        with pytest.raises(MlflowException):
            hook = MLflowDeploymentHook(
                mlflow_conn_id = TEST_MLFLOW_CONN_ID,
                target_uri = 'not_aws',
                target_conn_id = 'non_aws_target_conn'
            )

            hook.get_conn()
            mock_aws_dict.assert_not_called()

    @mock.patch("mlflow_provider.hooks.deployment.MLflowDeploymentHook._set_env_variables")
    def test_aws_conn_set_env(self, mock_set_env):

        aws_env = {
            'AWS_ACCESS_KEY_ID': 'AKIAIOSFODNN7EXAMPLE',
            'AWS_SECRET_ACCESS_KEY': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
        }

        hook = MLflowDeploymentHook(
            mlflow_conn_id = TEST_MLFLOW_CONN_ID,
            target_uri = TEST_TARGET_URI,
            target_conn_id = TEST_TARGET_CONN_ID
        )

        hook.get_conn()
        mock_set_env.assert_called_once_with(aws_env)

    @mock.patch("mlflow_provider.hooks.deployment.MLflowDeploymentHook._set_env_variables")
    def test_get_conn_set_env(self, mock_set_env):

        hook = MLflowDeploymentHook(
            mlflow_conn_id = TEST_MLFLOW_CONN_ID,
            target_uri = TEST_TARGET_URI,
            target_conn_id = TEST_TARGET_CONN_ID
        )

        hook.get_conn()
        mock_set_env.assert_called_once()

    @mock.patch("mlflow_provider.hooks.deployment.MLflowDeploymentHook._env_cleanup")
    @mock.patch("mlflow_provider.hooks.deployment.MLflowDeploymentHook.get_conn")
    def test_create_deployment(self, mock_get_conn, mock_env_cleanup):

        hook = MLflowDeploymentHook(
            mlflow_conn_id = TEST_MLFLOW_CONN_ID,
            target_uri=TEST_TARGET_URI,
            target_conn_id=TEST_TARGET_CONN_ID
        )

        hook.create_deployment(TEST_NAME, TEST_MODEL_URI)
        mock_get_conn.assert_called_once()
        mock_env_cleanup.assert_called_once()


    @mock.patch("mlflow_provider.hooks.deployment.MLflowDeploymentHook._env_cleanup")
    @mock.patch("mlflow_provider.hooks.deployment.MLflowDeploymentHook.get_conn")
    def test_predict(self, mock_get_conn, mock_env_cleanup):

        hook = MLflowDeploymentHook(
            mlflow_conn_id = TEST_MLFLOW_CONN_ID,
            target_uri=TEST_TARGET_URI,
            target_conn_id=TEST_TARGET_CONN_ID
        )

        hook.predict(deployment_name=TEST_DEPLOYMENT_NAME, inputs=TEST_SAMPLE)
        mock_get_conn.assert_called_once()
        mock_env_cleanup.assert_called_once()
