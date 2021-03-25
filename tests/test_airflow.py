import pytest
import responses

from kedro_airflow_k8s.airflow import AirflowClient, MissingDAGException


class TestAirflow:
    @responses.activate
    def test_get_dag(self):
        response_data = {
            "dag_id": "test_id",
            "tags": [{"name": "commit_sha:123456"}],
        }
        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags/test_id",
            json=response_data,
        )
        client = AirflowClient("https://test.airflow.com", max_retries=0)

        dag = client.get_dag("test_id")

        assert dag.dag_id == "test_id"
        assert dag.tags == [{"name": "commit_sha:123456"}]

    @responses.activate
    def test_wait_for_dag(self):
        response_data = {
            "dag_id": "test_id",
            "tags": [{"name": "commit_sha:123456"}],
        }
        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags/test_id",
            json=response_data,
        )
        client = AirflowClient("https://test.airflow.com", max_retries=0)

        dag = client.wait_for_dag("test_id", "commit_sha:123456")

        assert dag.dag_id == "test_id"
        assert dag.tags == [{"name": "commit_sha:123456"}]

    @responses.activate
    def test_wait_for_dag_timeout(self):
        response_data = {
            "dag_id": "test_id",
            "tags": [{"name": "commit_sha:654321"}],
        }
        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags/test_id",
            json=response_data,
        )
        client = AirflowClient(
            "https://test.airflow.com", max_retries=0, retry_interval=0
        )

        with pytest.raises(MissingDAGException):
            client.wait_for_dag("test_id", "commit_sha:123456")

    @responses.activate
    def test_trigger_dag_run(self):
        response_data = {
            "dag_run_id": "test_run_id",
        }
        responses.add(
            responses.POST,
            "https://test.airflow.com/api/v1/dags/test_id/dagRuns",
            json=response_data,
        )
        client = AirflowClient("https://test.airflow.com", max_retries=0)

        dag_run_id = client.trigger_dag_run("test_id")
        assert dag_run_id == "test_run_id"

    @responses.activate
    def test_wait_for_dag_run_completion(self):
        response_data_dag_run = {"state": "success"}
        response_data_task_instance = {
            "task_instances": [{"state": "success"}]
        }
        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags/test_id/dagRuns/test_dag_run_id",
            json=response_data_dag_run,
        )
        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags/test_id/dagRuns/test_dag_run_id/"
            "taskInstances",
            json=response_data_task_instance,
        )
        client = AirflowClient("https://test.airflow.com", max_retries=0)

        status = client.wait_for_dag_run_completion(
            "test_id", "test_dag_run_id", 1
        )

        assert status == "success"

    @responses.activate
    def test_wait_for_dag_run_retry(self):
        running_response, success_response = {"state": "running"}, {
            "state": "success"
        }
        response_data_task_instance = {
            "task_instances": [{"state": "success"}]
        }
        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags/test_id/dagRuns/test_dag_run_id",
            json=running_response,
        )
        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags/test_id/dagRuns/test_dag_run_id",
            json=success_response,
        )
        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags/test_id/dagRuns/test_dag_run_id/"
            "taskInstances",
            json=response_data_task_instance,
        )
        client = AirflowClient(
            "https://test.airflow.com", max_retries=0, retry_interval=0
        )

        status = client.wait_for_dag_run_completion(
            "test_id", "test_dag_run_id", 1
        )

        assert status == "success"

    @responses.activate
    def test_wait_for_dag_run_unknown(self):
        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags/test_id/dagRuns/test_dag_run_id",
            status=403,
        )
        client = AirflowClient(
            "https://test.airflow.com", max_retries=0, retry_interval=0
        )

        status = client.wait_for_dag_run_completion(
            "test_id", "test_dag_run_id", 1
        )

        assert status == "unknown"

    @responses.activate
    def test_wait_for_dag_run_task_instance_failed(self):
        response_data_dag_run = {"state": "success"}
        response_data_task_instance = {
            "task_instances": [
                {"task_id": "1", "state": "success"},
                {"task_id": "2", "state": "failed"},
            ]
        }
        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags/test_id/dagRuns/test_dag_run_id",
            json=response_data_dag_run,
        )
        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags/test_id/dagRuns/test_dag_run_id/"
            "taskInstances",
            json=response_data_task_instance,
        )
        client = AirflowClient(
            "https://test.airflow.com", max_retries=0, retry_interval=0
        )
        status = client.wait_for_dag_run_completion(
            "test_id", "test_dag_run_id", 1
        )

        assert status == "failed"

    @responses.activate
    def test_list_dags(self):
        list_of_dags = {
            "dags": [
                {
                    "dag_id": "match",
                    "tags": [
                        {"name": "generated_with_kedro_airflow_k8s:0.1.1"},
                        {"name": "experiment_name:test_experiment"},
                    ],
                },
                {
                    "dag_id": "not_generated_by_plugin",
                    "tags": [{"name": "experiment_name:test_experiment"}],
                },
                {
                    "dag_id": "without_experiment_name",
                    "tags": [
                        {"name": "generated_with_kedro_airflow_k8s:0.1.1"}
                    ],
                },
            ]
        }

        responses.add(
            responses.GET,
            "https://test.airflow.com/api/v1/dags?limit=1000",
            json=list_of_dags,
        )

        client = AirflowClient(
            "https://test.airflow.com", max_retries=0, retry_interval=0
        )

        dags = client.list_dags("generated_with_kedro_airflow_k8s")

        assert len(dags) == 2
        assert dags[0].dag_id == "match"
        assert dags[0].tags == [
            {"name": "generated_with_kedro_airflow_k8s:0.1.1"},
            {"name": "experiment_name:test_experiment"},
        ]
        assert dags[1].dag_id == "without_experiment_name"
        assert dags[1].tags == [
            {"name": "generated_with_kedro_airflow_k8s:0.1.1"}
        ]
