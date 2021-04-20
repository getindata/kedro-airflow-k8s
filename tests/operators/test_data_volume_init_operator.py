import unittest

from kedro_airflow_k8s.operators.data_volume_init import DataVolumeInitOperator


class TestDataVolumeInitOperator(unittest.TestCase):
    def test_task_create(self):
        task = DataVolumeInitOperator(
            namespace="airflow",
            pvc_name="shared_storage",
            image="registry.gitlab.com/test_image",
            image_pull_policy="Always",
            volume_owner=100,
            startup_timeout=120,
            source="/home/airflow/test",
        )

        pod = task.create_pod_request_obj()

        assert pod.metadata.name.startswith("data-volume-init")
        assert "data-volume-init" != pod.metadata.name
        assert pod.metadata.namespace == "airflow"
        assert len(pod.spec.containers) == 1
        container = pod.spec.containers[0]
        assert container.image == "registry.gitlab.com/test_image"
        assert container.image_pull_policy == "Always"
        assert container.args == [
            "cp --verbose -r /home/airflow/test/* /home/airflow/testvolume",
        ]
        assert pod.spec.security_context.fs_group == 100
        assert len(pod.spec.volumes) == 1
        volume = pod.spec.volumes[0]
        assert volume.name == "storage"
        assert volume.persistent_volume_claim.claim_name == "shared_storage"
        assert len(container.volume_mounts) == 1
        volume_mount = container.volume_mounts[0]
        assert volume_mount.mount_path == "/home/airflow/testvolume"
        assert volume_mount.name == "storage"
