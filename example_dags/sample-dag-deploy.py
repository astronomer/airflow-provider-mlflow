
import pandas
from airflow.decorators import dag
from airflow.utils.helpers import chain
from pendulum import datetime

from mlflow_provider.operators.deployment import (
    CreateDeploymentOperator,
    PredictOperator,
)
from mlflow_provider.operators.registry import (
    CreateModelVersionsOperator,
    CreateRegisteredModelOperator,
    TransitionModelVersionStageOperator,
)

test_sample = {
    "columns": ["capital_gain", "capital_loss", "hours_per_week", "workclass_Federal-gov", "workclass_Local-gov",
                "workclass_Never-worked", "workclass_Private", "workclass_Self-emp-inc", "workclass_Self-emp-not-inc",
                "workclass_State-gov", "workclass_Unknown", "workclass_Without-pay", "education_10th", "education_11th",
                "education_12th", "education_1st-4th", "education_5th-6th", "education_7th-8th", "education_9th",
                "education_Assoc-acdm", "education_Assoc-voc", "education_Bachelors", "education_Doctorate",
                "education_HS-grad", "education_Masters", "education_Preschool", "education_Prof-school",
                "education_Some-college", "occupation_Adm-clerical", "occupation_Armed-Forces",
                "occupation_Craft-repair", "occupation_Exec-managerial", "occupation_Farming-fishing",
                "occupation_Handlers-cleaners", "occupation_Machine-op-inspct", "occupation_Other-service",
                "occupation_Priv-house-serv", "occupation_Prof-specialty", "occupation_Protective-serv",
                "occupation_Sales", "occupation_Tech-support", "occupation_Transport-moving", "occupation_Unknown",
                "race_Amer-Indian-Eskimo", "race_Asian-Pac-Islander", "race_Black", "race_Other", "race_White",
                "sex_Female", "sex_Male", "income_bracket_gt_50k", "native_country_Cambodia", "native_country_Canada",
                "native_country_China", "native_country_Columbia", "native_country_Cuba",
                "native_country_Dominican-Republic", "native_country_Ecuador", "native_country_El-Salvador",
                "native_country_England", "native_country_France", "native_country_Germany", "native_country_Greece",
                "native_country_Guatemala", "native_country_Haiti", "native_country_Holand-Netherlands",
                "native_country_Honduras", "native_country_Hong", "native_country_Hungary", "native_country_India",
                "native_country_Iran", "native_country_Ireland", "native_country_Italy", "native_country_Jamaica",
                "native_country_Japan", "native_country_Laos", "native_country_Mexico", "native_country_Nicaragua",
                "native_country_Outlying-US", "native_country_Peru", "native_country_Philippines",
                "native_country_Poland", "native_country_Portugal", "native_country_Puerto-Rico",
                "native_country_Scotland", "native_country_South", "native_country_Taiwan", "native_country_Thailand",
                "native_country_Trinadad-Tobago", "native_country_United-States", "native_country_Unknown",
                "native_country_Vietnam", "native_country_Yugoslavia", "age_bins"],
    "data": [
        [3411, 0, 34, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2],
        [1234, 0, 40, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1]
    ]
}

@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'mlflow_conn_id': 'mlflow_databricks'
    },
    tags=["example"],
    default_view="graph",
    catchup=False,
)
def deploy_workflow():
    """
    ### Sample DAG

    Showcases the sample provider package's operator and sensor.

    To run this example, create an HTTP connection with:
    - id: mlflow_default
    - type: http
    - host: MLflow tracking URI (if MLFlow is hosted on Databricks use your Databricks host)
    """

    create_registered_model = CreateRegisteredModelOperator(
        task_id='create_registered_model',
        name='census_pred2',
        tags=[{'key': 'name1', 'value': 'value1'}, {'key': 'name2', 'value': 'value2'}],
        description='test description'
    )

    create_model_version = CreateModelVersionsOperator(
        task_id='create_model_version',
        name="{{ ti.xcom_pull(task_ids='create_registered_model')['registered_model']['name'] }}",
        source='dbfs:/databricks/mlflow-tracking/3853418861027039/8f7878c1b6ac4393938ef0a0ee5f392c/artifacts/model',
        run_id='8f7878c1b6ac4393938ef0a0ee5f392c'
    )

    transition_model = TransitionModelVersionStageOperator(
        task_id='transition_model',
        name="{{ ti.xcom_pull(task_ids='create_registered_model')['registered_model']['name'] }}",
        version="{{ ti.xcom_pull(task_ids='create_model_version')['model_version']['version'] }}",
        stage='Staging',
        archive_existing_versions=False
    )

    create_deployment = CreateDeploymentOperator(
        task_id='create_deployment',
        name="census-deployment-{{ ds_nodash }}",
        model_uri="{{ ti.xcom_pull(task_ids='transition_model')['model_version']['source'] }}",
        target_uri='sagemaker:/us-east-2',
        target_conn_id='aws_default',
        config={
            'image_url': "{{ var.value.mlflow_pyfunc_image_url }}",
            'execution_role_arn': "{{ var.value.sagemaker_execution_arn }}"
        },
        flavor='python_function'
    )

    test_prediction = PredictOperator(
        task_id='test_prediction',
        target_uri='sagemaker:/us-east-2',
        target_conn_id='aws_default',
        deployment_name="{{ ti.xcom_pull(task_ids='create_deployment')['name'] }}",
        inputs=pandas.DataFrame(data=test_sample['data'], columns=test_sample['columns'])
    )

    chain(
        create_registered_model,
        create_model_version,
        transition_model,
        create_deployment,
        test_prediction
    )

deploy_workflow = deploy_workflow()
