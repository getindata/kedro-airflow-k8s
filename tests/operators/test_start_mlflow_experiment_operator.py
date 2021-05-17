import unittest
from unittest import mock
from unittest.mock import MagicMock, Mock

import pendulum
import pytest
from airflow import DAG
from airflow.models import TaskInstance
from airflow.utils import timezone
from mlflow.entities import LifecycleStage
from mlflow.exceptions import MlflowException
from mlflow.protos import databricks_pb2

from kedro_airflow_k8s.operators import start_mlflow_experiment


class TestStartMLflowExperimentOperator(unittest.TestCase):
    @staticmethod
    def create_context(task):
        dag = DAG(dag_id="dag")
        tzinfo = pendulum.timezone("Europe/Amsterdam")
        execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        task_instance = TaskInstance(task=task, execution_date=execution_date)
        return {
            "dag": dag,
            "ts": execution_date.isoformat(),
            "task": task,
            "ti": task_instance,
        }

    def test_init_with_new_experiment(self):
        with mock.patch.object(
            start_mlflow_experiment.StartMLflowExperimentOperator,
            "create_mlflow_client",
        ) as create_mlflow_client:
            mlflow_client = MagicMock()
            mlflow_client.create_experiment.return_value = "new-id"
            mlflow_client.create_run.return_value.info.run_id = "next-run-id"

            create_mlflow_client.return_value = mlflow_client
            op = start_mlflow_experiment.StartMLflowExperimentOperator(
                task_id="test",
                mlflow_url="http://test.mlflow.com",
                experiment_name="test-experiment",
            )

            context = self.create_context(op)

            assert op.execute(context=context) == "next-run-id"

    def test_init_with_existing_experiment(self):
        with mock.patch.object(
            start_mlflow_experiment.StartMLflowExperimentOperator,
            "create_mlflow_client",
        ) as create_mlflow_client:
            mlflow_client = MagicMock()
            mlflow_client.create_experiment.side_effect = Mock(
                side_effect=MlflowException(
                    message="Experiment exists",
                    error_code=databricks_pb2.RESOURCE_ALREADY_EXISTS,
                )
            )
            mlflow_client.create_run.return_value.info.run_id = (
                "another-run-id"
            )

            create_mlflow_client.return_value = mlflow_client
            op = start_mlflow_experiment.StartMLflowExperimentOperator(
                task_id="test",
                mlflow_url="http://test.mlflow.com",
                experiment_name="test-experiment",
            )

            context = self.create_context(op)

            assert op.execute(context=context) == "another-run-id"

    def test_init_with_existing_deleted_experiment(self):
        with pytest.raises(MlflowException):
            with mock.patch.object(
                start_mlflow_experiment.StartMLflowExperimentOperator,
                "create_mlflow_client",
            ) as create_mlflow_client:
                mlflow_client = MagicMock()
                mlflow_client.create_experiment.side_effect = Mock(
                    side_effect=MlflowException(
                        message="Experiment exists",
                        error_code=databricks_pb2.RESOURCE_ALREADY_EXISTS,
                    )
                )
                mlflow_client.get_experiment_by_name.return_value.lifecycle_stage = (
                    LifecycleStage.DELETED
                )

                create_mlflow_client.return_value = mlflow_client
                op = start_mlflow_experiment.StartMLflowExperimentOperator(
                    task_id="test",
                    mlflow_url="http://test.mlflow.com",
                    experiment_name="test-experiment",
                )

                context = self.create_context(op)
                op.execute(context=context)

    def test_init_with_mlflow_internal_error(self):
        with pytest.raises(MlflowException):
            with mock.patch.object(
                start_mlflow_experiment.StartMLflowExperimentOperator,
                "create_mlflow_client",
            ) as create_mlflow_client:
                mlflow_client = MagicMock()
                mlflow_client.create_experiment.side_effect = Mock(
                    side_effect=MlflowException(
                        message="Experiment exists",
                        error_code=databricks_pb2.INTERNAL_ERROR,
                    )
                )

                create_mlflow_client.return_value = mlflow_client
                op = start_mlflow_experiment.StartMLflowExperimentOperator(
                    task_id="test",
                    mlflow_url="http://test.mlflow.com",
                    experiment_name="test-experiment",
                )

                context = self.create_context(op)
                op.execute(context=context)

    def test_log_docker_image_within_run(self):
        with mock.patch.object(
            start_mlflow_experiment.StartMLflowExperimentOperator,
            "create_mlflow_client",
        ) as create_mlflow_client:
            mlflow_client = MagicMock()
            mlflow_client.create_experiment.side_effect = Mock(
                side_effect=MlflowException(
                    message="Experiment exists",
                    error_code=databricks_pb2.RESOURCE_ALREADY_EXISTS,
                )
            )
            mlflow_client.create_run.return_value.info.run_id = (
                "another-run-id"
            )

            create_mlflow_client.return_value = mlflow_client
            op = start_mlflow_experiment.StartMLflowExperimentOperator(
                task_id="test",
                mlflow_url="http://test.mlflow.com",
                experiment_name="test-experiment",
                image="registry.com/someimage:aabbcc",
            )

            context = self.create_context(op)

            assert op.execute(context=context) == "another-run-id"
            mlflow_client.log_param.assert_called_with(
                "another-run-id", "image", "registry.com/someimage:aabbcc"
            )
