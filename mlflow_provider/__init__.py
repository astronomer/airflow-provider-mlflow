## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features. We recognize it's a bit unclean to define these in multiple places, but at this point it's the only workaround if you'd like your custom conn type to show up in the Airflow UI.

__version__ = "1.0.1"

def get_provider_info():
    return {
        "package-name": "airflow-provider-sample", # Required
        "name": "Sample Airflow Provider", # Required
        "description": "A sample template for airflow providers.", # Required
        "hook-class-names": ["mlflow_provider.hooks.sample_hook.MLflowHook"],
        "extra-links": ["mlflow_provider.operators.sample_operator.ExtraLink"],
        "versions": [__version__] # Required
    }
