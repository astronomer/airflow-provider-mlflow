from pendulum import datetime
from datetime import timedelta

from airflow.decorators import dag, task

from mlflow_provider.operators.mlflow_operator import MLflowOperator
from mlflow_provider.hooks.mlflow_hook import MLflowHook
from mlflow_provider.sensors.sample_sensor import SampleSensor


@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    # ``default_args`` will get passed on to each task. You can override them on a per-task basis during
    # operator initialization.
    default_args={"retries": 2, sample_conn_id: "conn_sample"},
    tags=["example"],
    default_view="graph",
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
    def hook_example():
        hook = MLflowHook()
        hook.run(

        )

sample_workflow_dag = sample_workflow()
