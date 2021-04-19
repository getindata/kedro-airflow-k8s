import unittest

import pendulum
from airflow import DAG
from airflow.models import TaskInstance
from airflow.utils import timezone

from kedro_airflow_k8s.operators.node_pod import NodePodOperator


class TestNodePodOperator(unittest.TestCase):
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

    def test_task_create(self):
        task = NodePodOperator(
            node_name="test_node_name",
            namespace="airflow",
            volume_disabled=False,
            pvc_name="shared_storage",
            image="registry.gitlab.com/test_image",
            image_pull_policy="Always",
            env="test-pipelines",
            task_id="test-node-name",
            startup_timeout=120,
            volume_owner=100,
            mlflow_enabled=False,
            requests_cpu="500m",
            requests_memory="2Gi",
            limits_cpu="2",
            limits_memory="10Gi",
            node_selector_labels={
                "size/k8s.io": "huge",
            },
        )

        pod = task.create_pod_request_obj()

        assert pod.metadata.name.startswith("test-node-name")
        assert "test-node-name" != pod.metadata.name
        assert pod.metadata.namespace == "airflow"
        assert len(pod.spec.containers) == 1
        container = pod.spec.containers[0]
        assert container.image == "registry.gitlab.com/test_image"
        assert container.image_pull_policy == "Always"
        assert container.args == [
            "kedro",
            "run",
            "-e",
            "test-pipelines",
            "--node",
            "test_node_name",
        ]
        assert pod.spec.security_context == {"fsGroup": 100}
        assert container.resources.limits == {"cpu": "2", "memory": "10Gi"}
        assert container.resources.requests == {"cpu": "500m", "memory": "2Gi"}
        assert pod.spec.node_selector == {"size/k8s.io": "huge"}
