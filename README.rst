⚠️ Discontinuation of project
=============================

    This project is no longer actively maintained by Astronomer, but we’d love to see it live on in the community. While Astronomer has paused development and is not accepting new contributions, bug fixes or releases, the code is still here for you to explore, fork and adapt under the terms of its license.
    Please note that it may not work with the latest dependencies or platforms, and it could contain security vulnerabilities. Astronomer can’t offer guarantees or warranties for its use.
    If you’re interested in adopting or stewarding this project, we’d be happy to chat — reach us at oss@astronomer.io. Thanks for being part of the open-source journey and helping keep great ideas alive!




MLflow Provider Package for Apache Airflow
==========================================

.. image:: https://badge.fury.io/py/airflow-provider-mlflow.svg
    :target: https://badge.fury.io/py/airflow-provider-mlflow
    :alt: PyPI Version
.. image:: https://img.shields.io/pypi/pyversions/airflow-provider-mlflow
    :target: https://img.shields.io/pypi/pyversions/airflow-provider-mlflow
    :alt: PyPI - Python Version
.. image:: https://readthedocs.org/projects/airflow-provider-mlflow/badge/?version=latest
    :target: https://airflow-provider-mlflow.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status
.. image:: https://img.shields.io/pypi/l/astronomer-providers?color=blue
    :target: https://img.shields.io/pypi/l/astronomer-providers?color=blue
    :alt: PyPI - License

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
