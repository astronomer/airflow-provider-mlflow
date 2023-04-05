from __future__ import annotations
from typing import Any, Dict, Optional

import requests
from airflow.hooks.base import BaseHook
from requests.auth import HTTPBasicAuth


class MLflowClientHook(BaseHook):
    """
    Hook that interacts with an HTTP endpoint with Python requests library.
    This hook is used to interact with the MLflow REST API. https://www.mlflow.org/docs/latest/rest-api.html

    :param method: the API method to be called
    :type method: str
    :param mlflow_conn_id: connection that has the base API url i.e https://www.google.com/
        and optional authentication credentials.
    :type mlflow_conn_id: str
    :param auth_type: The auth type for the service
    :type auth_type: AuthBase of python requests lib
    """

    conn_name_attr = 'mlflow_conn_id'
    default_conn_name = 'mlflow_default'
    conn_type = 'http'
    hook_name = 'MLflow Client'

    def __init__(
            self,
            method: str = 'POST',
            mlflow_conn_id: str = default_conn_name,
            auth_type: Any = HTTPBasicAuth,
    ) -> None:
        super().__init__()
        self.mlflow_conn_id = mlflow_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self.auth_type: Any = auth_type

    def get_conn(self, headers: Optional[Dict[Any, Any]] = None) -> requests.Session:
        """
        Returns http session to use with requests.

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        session = requests.Session()

        if self.mlflow_conn_id:
            conn = self.get_connection(self.mlflow_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url = self.base_url + ":" + str(conn.port)

            if conn.login and conn.login != 'token':
                session.auth = self.auth_type(conn.login, conn.password)
            elif conn.login == 'token':
                if headers is not None:
                    headers['Authorization'] = 'Bearer {}'.format(conn.password)
                else:
                    headers = {'Authorization': 'Bearer {}'.format(conn.password)}

            if conn.extra:
                try:
                    session.headers.update(conn.extra_dejson)
                except TypeError:
                    self.log.warning(
                        'Connection to %s has invalid extra field.', conn.host)
        if headers:
            session.headers.update(headers)

        return session

    def run(
            self,
            endpoint: str | None = None,
            headers: Dict[str, Any] | None = None,
            request_params: Dict[str, Any] | None = None,
            **request_kwargs: Any,
    ) -> Any:
        """
        Executes the request

        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :type endpoint: str
        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        :param request_params: request params,
        :type request_params: dict
        """

        session = self.get_conn(headers)

        if self.base_url and not self.base_url.endswith('/') and endpoint and not endpoint.startswith('/'):
            url = self.base_url + '/' + endpoint
        else:
            url = (self.base_url or '') + (endpoint or '')

        if self.method == 'GET':
            # GET uses params
            if request_params is None:
                req = requests.Request(self.method, url, headers=headers)
            else:
                req = requests.Request(self.method, url, headers=headers, params=request_params)
        else:
            # POST uses json
            req = requests.Request(self.method, url, headers=headers, json=request_params)

        prepped_request = session.prepare_request(req)

        self.log.info("Sending '%s' to url: %s", self.method, url)

        try:
            response = session.send(prepped_request)
            return response

        except requests.exceptions.ConnectionError as ex:
            raise ex
