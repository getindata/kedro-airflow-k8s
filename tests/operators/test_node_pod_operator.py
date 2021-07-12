import unittest

from kedro_airflow_k8s.operators.node_pod import NodePodOperator


class TestNodePodOperator(unittest.TestCase):
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
            labels={"running": "airflow"},
            tolerations=[
                {
                    "key": "group",
                    "value": "data-processing",
                    "effect": "NoExecute",
                }
            ],
            annotations={"iam.amazonaws.com/role": "airflow"},
            pipeline="data_science_pipeline",
            parameters="ds:{{ ds }}",
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
            "--pipeline",
            "data_science_pipeline",
            "--node",
            "test_node_name",
            "--params",
            "ds:{{ ds }}",
        ]

        assert len(pod.spec.volumes) == 1
        volume = pod.spec.volumes[0]
        assert volume.name == "storage"
        assert volume.persistent_volume_claim.claim_name == "shared_storage"
        assert len(container.volume_mounts) == 1
        volume_mount = container.volume_mounts[0]
        assert volume_mount.mount_path == "/home/kedro/data"
        assert volume_mount.name == "storage"

        assert pod.spec.security_context.fs_group == 100
        assert container.resources.limits == {"cpu": "2", "memory": "10Gi"}
        assert container.resources.requests == {"cpu": "500m", "memory": "2Gi"}
        assert pod.spec.node_selector == {"size/k8s.io": "huge"}
        assert pod.spec.tolerations[0].value == "data-processing"
        assert pod.metadata.annotations["iam.amazonaws.com/role"] == "airflow"

    def test_task_create_no_limits_and_requests(self):
        task = NodePodOperator(
            node_name="test_node_name",
            namespace="airflow",
            pvc_name="shared_storage",
            image="registry.gitlab.com/test_image",
            image_pull_policy="Always",
            env="test-pipelines",
            task_id="test-node-name",
            volume_owner=100,
            mlflow_enabled=False,
        )

        pod = task.create_pod_request_obj()

        assert len(pod.spec.containers) == 1
        container = pod.spec.containers[0]
        assert container.resources.limits == {}
        assert container.resources.requests == {}
        assert pod.spec.node_selector is None
