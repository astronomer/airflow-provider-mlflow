"""
Example prediction task in Airflow with MLflow.

The model used for prediction was created with this example:
https://github.com/mlflow/mlflow/blob/master/examples/lightgbm/lightgbm_native/train.py
"""

import numpy as np
from airflow.decorators import dag
from pendulum import datetime

from mlflow_provider.operators.pyfunc import AirflowPredictOperator

test_sample = np.array([
    [6.1, 2.8, 4.7, 1.2],
    [5.7, 3.8, 1.7, 0.3],
    [7.7, 2.6, 6.9, 2.3],
    [6. , 2.9, 4.5, 1.5],
    [6.8, 2.8, 4.8, 1.4],
    [5.4, 3.4, 1.5, 0.4],
    [5.6, 2.9, 3.6, 1.3],
    [6.9, 3.1, 5.1, 2.3],
    [6.2, 2.2, 4.5, 1.5],
    [5.8, 2.7, 3.9, 1.2],
    [6.5, 3.2, 5.1, 2. ],
    [4.8, 3. , 1.4, 0.1],
    [5.5, 3.5, 1.3, 0.2],
    [4.9, 3.1, 1.5, 0.1],
    [5.1, 3.8, 1.5, 0.3],
    [6.3, 3.3, 4.7, 1.6],
    [6.5, 3. , 5.8, 2.2],
    [5.6, 2.5, 3.9, 1.1],
    [5.7, 2.8, 4.5, 1.3],
    [6.4, 2.8, 5.6, 2.2],
    [4.7, 3.2, 1.6, 0.2],
    [6.1, 3. , 4.9, 1.8],
    [5. , 3.4, 1.6, 0.4],
    [6.4, 2.8, 5.6, 2.1],
    [7.9, 3.8, 6.4, 2. ],
    [6.7, 3. , 5.2, 2.3],
    [6.7, 2.5, 5.8, 1.8],
    [6.8, 3.2, 5.9, 2.3],
    [4.8, 3. , 1.4, 0.3],
    [4.8, 3.1, 1.6, 0.2]
])

@dag(
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args={
        'mlflow_conn_id': 'mlflow_astronomer_dev'
    },
    tags=["example"],
    default_view="graph",
    catchup=False,
    doc_md=__doc__
)
def airflow_predict():
    """
    ### Sample DAG

    Showcases the sample provider package's operator and sensor.

    To run this example, create an HTTP connection with:
    - id: mlflow_default
    - type: http
    - host: MLflow tracking URI (if MLFlow is hosted on Databricks use your Databricks host)
    """

    AirflowPredictOperator(
        task_id='predict',
        model_uri='mlflow-artifacts:/3/51bc0f22c9504691811f494cc8ad9613/artifacts/model',
        data=test_sample
    )

airflow_predict = airflow_predict()
