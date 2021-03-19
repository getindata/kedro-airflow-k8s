"""
Airflow representation classes
"""
import datetime
from time import sleep
from typing import Dict, List, NamedTuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class DAGModel(NamedTuple):
    """
    Represents DAG model as seen from Airflow
    """

    dag_id: str
    tags: List[Dict[str, str]]


class MissingDAGException(BaseException):
    def __init__(self, dag_id: str, tag: str):
        super().__init__(
            f"DAG of id {dag_id} with tag {tag} could not be found"
        )


class AirflowClient:
    MAX_RETRIES = 10
    RETRY_INTERVAL = 10
    VERIFY = False
    """
    Client of Airflow. Supports both low level functionalities of Airflow and some high
     level aggregates on top of them.
    """

    def __init__(
        self,
        rest_api_url: str,
        max_retries: int = MAX_RETRIES,
        retry_interval: int = RETRY_INTERVAL,
    ):
        """
        :param rest_api_url: full url to service rest API
        """
        self.rest_api_url = rest_api_url
        self.max_retries = max_retries
        self.retry_interval = retry_interval

    @staticmethod
    def create_http_session(status_forcelist: Optional[List[int]] = None):
        retry_strategy = Retry(
            total=AirflowClient.MAX_RETRIES,
            status_forcelist=[429, 500, 502, 503, 504]
            + (status_forcelist or []),
            method_whitelist=["GET", "POST", "PATCH", "DELETE"],
            backoff_factor=1,
            raise_on_status=True,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def get_dag(self, dag_id: str) -> DAGModel:
        """
        Retrieves DAG model information from Airflow
        :param dag_id:
        :return: DAGModel or None if it does not exist
        """
        res = AirflowClient.create_http_session().get(
            f"{self.rest_api_url}/dags/{dag_id}",
            headers={"Content-Type": "application/json"},
            verify=AirflowClient.VERIFY,
        )
        if res.status_code != 200:
            raise RuntimeError(res.json().get("title"))
        dag_json = res.json()
        dag = DAGModel(dag_id=dag_json["dag_id"], tags=dag_json["tags"])
        return dag

    def wait_for_dag(self, dag_id: str, tag: str) -> DAGModel:
        """
        Like `get_dag`, but tries many times if dag is lazily loaded by Airflow.
        :param dag_id:
        :param tag: tag to be present in DAG
        :return: DAGModel
        """
        session = AirflowClient.create_http_session([404])
        count = 0
        while count <= self.max_retries:
            res = session.get(
                url=f"{self.rest_api_url}/dags/{dag_id}",
                headers={"Content-Type": "application/json"},
                verify=AirflowClient.VERIFY,
            )
            dag_json = res.json()
            if res.status_code != 200:
                raise RuntimeError(dag_json.get("title"))
            dag = DAGModel(dag_id=dag_json["dag_id"], tags=dag_json["tags"])
            if [tag for dag_tag in dag.tags if dag_tag["name"] == tag]:
                return dag
            count += 1
            sleep(self.retry_interval)

        raise MissingDAGException(dag_id, tag)

    def trigger_dag_run(self, dag_id: str) -> str:
        """
        Triggers run for dag.
        :param dag_id:
        :return: Airflow DAG run identifier
        """
        session = AirflowClient.create_http_session()

        res = session.post(
            url=f"{self.rest_api_url}/dags/{dag_id}/dagRuns",
            json={},
            verify=AirflowClient.VERIFY,
        )
        if res.status_code != 200:
            raise RuntimeError(res.json().get("title"))
        return res.json()["dag_run_id"]

    @staticmethod
    def _check_state(response):
        if response.status_code != 200:
            return "unknown"
        state = response.json()["state"]
        return state if state != "running" else None

    def _wait_for_dag_run_completion(
        self, dag_id: str, dag_run_id: str, wait_for_completion
    ) -> str:
        check_start = datetime.datetime.now()
        session = AirflowClient.create_http_session()
        last_state = "unknown"
        while (datetime.datetime.now() - check_start) < datetime.timedelta(
            minutes=wait_for_completion
        ):
            res = session.get(
                f"{self.rest_api_url}/dags/{dag_id}/dagRuns/{dag_run_id}",
                headers={"Content-Type": "application/json"},
                verify=AirflowClient.VERIFY,
            )
            last_state = AirflowClient._check_state(res)
            if last_state:
                break

            sleep(self.retry_interval)
        return last_state

    def wait_for_dag_run_completion(
        self, dag_id: str, dag_run_id: str, wait_for_completion: int = 0
    ) -> str:
        """
        Waits for dag run completion, either success or failure
        :param wait_for_completion:
        :param dag_id:
        :param dag_run_id:
        :return: status "success" "running" (if didn't finish upon completion) "failed"
                "unknown" (if wait_for_completion non-positivie)
        """
        if wait_for_completion:
            return self._wait_for_dag_run_completion(
                dag_id, dag_run_id, wait_for_completion
            )
        else:
            return "unknown"
