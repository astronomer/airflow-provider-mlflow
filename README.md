MLflow Provider Package for Apache Airflow
========

An Airflow provider to interact with MLflow using Operators and Hooks for the following:
- Registry
- Deployments
- Pyfunc

https://mlflow.org/

Hooks & Operators
================
These try to follow the same naming convention as the MLflow API as closely as possible.

Hooks:
  - mlflow_provider.hooks.client.MLflowClientHook: Interact with MLflow Rest API endpoints
  - mlflow_provider.hooks.deployment.MLflowDeploymentHook: Interact with mlflow.deployment
  - mlflow_provider.hooks.pyfunc.MLflowPyfuncHook: Interact with mlflow.pyfunc

Operators:
  - mlflow_provider.operators.registry.CreateRegisteredModelOperator
  - mlflow_provider.operators.registry.GetRegisteredModelOperator
  - mlflow_provider.operators.registry.DeleteRegisteredModelOperator
  - mlflow_provider.operators.registry.GetLatestModelVersionsOperator
  - mlflow_provider.operators.registry.CreateModelVersionOperator
  - mlflow_provider.operators.registry.GetModelVersionOperator
  - mlflow_provider.operators.registry.DeleteModelVersionOperator
  - mlflow_provider.operators.registry.TransitionModelVersionStageOperator
  - mlflow_provider.operators.deployment.CreateDeploymentOperator
  - mlflow_provider.operators.deployment.PredictOperator
  - mlflow_provider.operators.pyfunc.AirflowPredictOperator

Quick Start
==========================

`pip install airflow-provider-mlflow`

Examples can be found in the `example_dags` directory.
