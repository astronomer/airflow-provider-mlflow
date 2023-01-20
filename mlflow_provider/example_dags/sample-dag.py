from pendulum import datetime
from datetime import timedelta

from airflow.decorators import dag, task

from mlflow_provider.operators.mlflow_operator import MLflowOperator
from mlflow_provider.hooks.mlflow_hook import MLflowClientHook
from mlflow_provider.sensors.sample_sensor import SampleSensor


@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    # ``default_args`` will get passed on to each task. You can override them on a per-task basis during
    # operator initialization.
    default_args={"retries": 2},
    tags=["example"],
    default_view="graph",
    catchup=False
)
def sample_workflow():
    """
    ### Sample DAG

    Showcases the sample provider package's operator and sensor.

    To run this example, create an HTTP connection with:
    - id: conn_sample
    - type: http
    - host: www.httpbin.org
    """

    # task_get_op = MLflowOperator(task_id="get_op", method="get")

    # task_sensor = SampleSensor(task_id="sensor", sample_conn_id="conn_sample", endpoint="")

    # task_get_op #>> task_sensor

    @task
    def hook_get_example():
        hook = MLflowClientHook(method="GET")
        response = hook.run(
            endpoint='api/2.0/mlflow/experiments/get-by-name',
            request_params={'experiment_name': 'census_prediction'}
        )

        print(response)
        return response

    def hook_post_example():
        hook = MLflowClientHook()
        response = hook.run(
            endpoint='api/2.0/mlflow/registered-models/create',
            data={'name': 'census_prediction'}
        )

        print(response)
        return response

    test_hook_get = hook_get_example()
    test_hook_post = hook_post_example()


sample_workflow()
