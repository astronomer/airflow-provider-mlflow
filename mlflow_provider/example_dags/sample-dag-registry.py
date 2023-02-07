# TODO Cleanup up sample dag with user friendly exampeles
from airflow.utils.helpers import chain
from pendulum import datetime

from airflow.decorators import dag, task

from mlflow_provider.hooks.client import MLflowClientHook
from mlflow_provider.operators.registry import (
    CreateRegisteredModelOperator,
    GetRegisteredModelOperator,
    DeleteRegisteredModelOperator,
    GetLatestModelVersionsOperator,
    CreateModelVersionsOperator,
    GetModelVersionOperator,
    DeleteModelVersionOperator,
    TransitionModelVersionStageOperator
)


@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    # ``default_args`` will get passed on to each task. You can override them on a per-task basis during
    # operator initialization.
    default_args={
        # "retries": 2
        'mlflow_conn_id': 'mlflow_databricks'
    },
    tags=["example"],
    default_view="graph",
    catchup=False
)
def registry_workflow():
    """
    ### Sample DAG

    Showcases the sample provider package's operator and sensor.

    To run this example, create an HTTP connection with:
    - id: mlflow_default
    - type: http
    - host: MLflow tracking URI (if MLFlow is hosted on Databricks use your Databricks host)
    """

    # task_get_op = MLflowOperator(task_id="get_op", method="get")

    # task_sensor = SampleSensor(task_id="sensor", mlflow_conn_id="conn_sample", endpoint="")

    # task_get_op #>> task_sensor

    # @task
    # def hook_get_example():
    #     hook = MLflowDeploymentHook(method="GET")
    #     response = hook.run(
    #         endpoint='api/2.0/mlflow/experiments/get-by-name',
    #         request_params={'experiment_name': 'census_prediction'}
    #     )
    #
    #     return response.json()
    #
    # @task
    # def hook_post_example():
    #     hook = MLflowDeploymentHook()
    #     response = hook.run(
    #         endpoint='api/2.0/mlflow/registered-models/create',
    #         request_params={'name': 'census_prediction1', 'description': 'test',
    #                         'tags': [{'key': 'name1', 'value': 'value1'}, {'key': 'name2', 'value': 'value2'}]
    #                         }
    #     )
    #
    #     return response.json()

    # hook_get = hook_get_example()
    # hook_post = hook_post_example()


    create_registered_model = CreateRegisteredModelOperator(
        task_id='create_registered_model',
        name='census_pred1',
        tags=[{'key': 'name1', 'value': 'value1'}, {'key': 'name2', 'value': 'value2'}],
        description='test description'
    )

    get_registered_model = GetRegisteredModelOperator(
        task_id='get_registered_model',
        name='census_pred1',
        trigger_rule='all_done'
    )

    get_latest_model_versions = GetLatestModelVersionsOperator(
        task_id='get_latest_model_versions',
        name='census_pred1'
    )

    create_model_version = CreateModelVersionsOperator(
        task_id='create_model_version',
        name='census_pred1',
        source='dbfs:/databricks/mlflow-tracking/2440198899248958/a30d7eba481f4c14ad3b1a4d76c8187a/artifacts/model',
        run_id='a30d7eba481f4c14ad3b1a4d76c8187a'
    )

    get_model_version = GetModelVersionOperator(
        task_id='get_model_version',
        name='census_pred1',
        version='1'
    )

    transition_model = TransitionModelVersionStageOperator(
        task_id='transition_model',
        name='census_pred1',
        version='2',
        stage='Staging',
        archive_existing_versions=True
    )

    # delete_model_version = DeleteModelVersionOperator(
    #     task_id='delete_model_version',
    #     name='census_pred1',
    #     version='1'
    # )

    # delete_registered_model = DeleteRegisteredModelOperator(
    #     task_id='delete_registered_model',
    #     name='census_pred2'
    # )

    chain(
        create_registered_model,
        get_registered_model,
        create_model_version,
        get_latest_model_versions,
        get_model_version,
        transition_model
        # delete_model_version,
        # delete_registered_model
    )


registry_workflow = registry_workflow()
