from mlflow import pyfunc

from mlflow_provider.hooks.base import MLflowBaseHook


class MLflowPyfuncHook(MLflowBaseHook):
    """
    Sample Hook that interacts with an HTTP endpoint the Python requests library.

    :param aws_conn_id: AWS connection to use with hook
    :type aws_conn_id: str
    :param target_uri: target system URI to deploy model to. (ie 'sagemaker')
    :type target_uri: str
    """

    # TODO update hook names to match Airflow naming convention
    hook_name = 'MLflow Pyfunc'

    def __init__(
            self,
            mlflow_conn_id: str
    ) -> None:
        super().__init__()
        self.mlflow_conn_id = mlflow_conn_id


    def get_conn(self) -> pyfunc:
        """
        Returns MLflow deployment Client.
        """

        self._set_env_variables()
        return pyfunc
