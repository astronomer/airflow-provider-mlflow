## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.

__version__ = "1.1.0"

def get_provider_info():
    return {
        "package-name": "airflow-provider-mlflow", # Required
        "name": "MLflow", # Required
        "description": "An MLflow Provider Package for Apache Airflow.", # Required
        "connection-types": [
            {"connection-type": "http", "hook-class-name": "mlflow_provider.hooks.base.MLflowBaseHook"}
        ],
        "versions": [__version__] # Required
    }
