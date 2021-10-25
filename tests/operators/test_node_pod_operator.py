import unittest

from airflow.kubernetes.pod_generator import PodGenerator
from kubernetes.client.models.v1_env_var import V1EnvVar

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
            annotations={
                "iam.amazonaws.com/role": "airflow",
                "vault.hashicorp.com/agent-inject-template-foo": '{{- with secret "database/creds/db-app" -}}\npostgres://{{ .Data.username }}:{{ .Data.password }}@postgres:5432/mydb\n{{- end }}\n',  # noqa: E501
            },
            pipeline="data_science_pipeline",
            parameters="ds:{{ ds }}",
            env_vars={"var1": "var1value"},
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
        assert (
            pod.metadata.annotations[
                "vault.hashicorp.com/agent-inject-template-foo"
            ]
            == """{{- with secret "database/creds/db-app" -}}
postgres://{{ .Data.username }}:{{ .Data.password }}@postgres:5432/mydb
{{- end }}
"""
        )

        assert pod.spec.service_account_name == "default"
        assert len(pod.spec.image_pull_secrets) == 0
        assert container.env[0] == V1EnvVar(name="var1", value="var1value")

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
        assert not pod.spec.node_selector

    def test_task_with_service_account(self):
        task = NodePodOperator(
            node_name="test_node_name",
            namespace="airflow",
            pvc_name="shared_storage",
            image="registry.gitlab.com/test_image",
            image_pull_policy="Always",
            env="test-pipelines",
            task_id="test-node-name",
            service_account_name="custom_service_account",
            image_pull_secrets="top,secret",
            mlflow_enabled=False,
        )

        pod = task.create_pod_request_obj()

        assert pod.spec.service_account_name == "custom_service_account"
        assert len(pod.spec.image_pull_secrets) == 2
        assert pod.spec.image_pull_secrets[0].name == "top"
        assert pod.spec.image_pull_secrets[1].name == "secret"

    def test_task_with_custom_k8s_pod_template(self):
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
            kubernetes_pod_template=f"""
type: Pod
metadata:
  name: {PodGenerator.make_unique_pod_id('test-node-name')}'
  labels:
    test: mylabel
spec:
  containers:
    - name: base
""",
        )
        pod = task.create_pod_request_obj()

        assert pod.metadata.name.startswith("test-node-name")
        assert "test-node-name" != pod.metadata.name
        assert pod.metadata.labels["test"] == "mylabel"
