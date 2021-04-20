import unittest
from unittest.mock import patch

import pendulum
from airflow import DAG
from airflow.models import TaskInstance
from airflow.utils import timezone
from kubernetes import client

from kedro_airflow_k8s.operators import create_pipeline_storage


class TestCreatePipelineStorageOperator(unittest.TestCase):
    @staticmethod
    def create_context(task):
        dag = DAG(dag_id="dag")
        tzinfo = pendulum.timezone("Europe/Amsterdam")
        execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        task_instance = TaskInstance(task=task, execution_date=execution_date)
        return {
            "dag": dag,
            "ts_nodash": execution_date.isoformat().replace("-", "_"),
            "task": task,
            "ti": task_instance,
        }

    def test_create_pipeline_storage(self):
        with patch("kubernetes.config.load_incluster_config"), patch(
            "kubernetes.client.ApiClient"
        ), patch.object(
            client.CoreV1Api, "create_namespaced_persistent_volume_claim"
        ) as create:
            op = create_pipeline_storage.CreatePipelineStorageOperator(
                task_id="test",
                pvc_name="some_{{ ts_nodash }}",
                namespace="test_ns",
                access_modes=["ReadWriteOnce"],
                volume_size="10Gi",
                storage_class_name="fast-ssd",
            )

            context = self.create_context(op)

            assert op.execute(context=context) == "some_{{ ts_nodash }}"
            create.assert_called_once()
