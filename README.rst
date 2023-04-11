MLflow Provider Package for Apache Airflow
==========================================

An `Apache Airflow <https://airflow.apache.org/>`_ provider to interact with MLflow using Operators and Hooks for the following:

- Registry
- Deployments
- Pyfunc

https://mlflow.org/docs/latest/index.html

Quick Start
-----------
**Install and update using pip:**

.. code-block:: bash

    pip install airflow-provider-mlflow

**Setting up Connections:**

Connection Type: HTTP

- Local MLflow
    - Host: http://localhost (if running Airflwo in docker: http://host.docker.internal)
    - Port: 5000

- Hosted with Username/Password
    - Connection Type: HTTP
    - Host: Your MLflow host URL
    - Login: Your MLflow username
    - Password: Your MLflow password

- Databricks
    - Host: Your Databricks host URL (https://<instance-name>.cloud.databricks.com)
    - Login: 'token'
    - Password: Your Databricks token


Examples can be found in the `example_dags <https://github.com/astronomer/airflow-provider-mlflow/tree/main/example_dags>`_ directory of the repo.

Changelog
---------

We follow `Semantic Versioning <https://semver.org/>`_ for releases.
Check `CHANGELOG.rst <https://github.com/astronomer/airflow-provider-mlflow/blob/main/CHANGELOG.rst>`_
for the latest changes.


License
-------

`Apache License 2.0 <https://github.com/astronomer/astronomer-providers/blob/main/LICENSE>`_
