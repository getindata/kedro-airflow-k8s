import unittest
from unittest.mock import patch

import pendulum
from airflow import DAG
from airflow.models import TaskInstance
from airflow.utils import timezone
from kubernetes import client

from kedro_airflow_k8s.operators import delete_pipeline_storage


class TestDeletePipelineStorageOperator(unittest.TestCase):
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

    def test_delete_pipeline_storage(self):
        with patch("kubernetes.config.load_incluster_config"), patch(
            "kubernetes.client.ApiClient"
        ), patch.object(
            client.CoreV1Api, "delete_namespaced_persistent_volume_claim"
        ) as delete:
            op = delete_pipeline_storage.DeletePipelineStorageOperator(
                task_id="test",
                pvc_name="some_{{ ts_nodash }}",
                namespace="test_ns",
            )

            context = self.create_context(op)

            op.execute(context=context)
            delete.assert_called_once()
            assert op.trigger_rule == "all_done"
