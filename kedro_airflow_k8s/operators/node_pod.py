from typing import Dict, Optional

from airflow.kubernetes.pod_generator import PodGenerator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s


class NodePodOperator(KubernetesPodOperator):
    def __init__(
        self,
        node_name: str,
        namespace: str,
        pvc_name: str,
        image: str,
        image_pull_policy: str,
        env: str,
        task_id: str,
        startup_timeout: int = 600,
        volume_disabled: bool = False,
        volume_owner: int = 0,
        mlflow_enabled: bool = True,
        requests_cpu: Optional[str] = None,
        requests_memory: Optional[str] = None,
        limits_cpu: Optional[str] = None,
        limits_memory: Optional[str] = None,
        node_selector_labels: Optional[Dict[str, str]] = None,
        source: str = "/home/kedro/data",
    ):
        self._task_id = task_id
        self._volume_disabled = volume_disabled
        self._pvc_name = pvc_name
        self._mlflow_enabled = mlflow_enabled

        super().__init__(
            task_id=task_id,
            security_context=self.create_security_context(
                volume_disabled, volume_owner
            ),
            namespace=namespace,
            image=image,
            image_pull_policy=image_pull_policy,
            arguments=["kedro", "run", "-e", env, "--node", node_name],
            volume_mounts=[
                k8s.V1VolumeMount(mount_path=source, name="storage")
            ]
            if not volume_disabled
            else [],
            resources=self.create_resources(
                requests_cpu, requests_memory, limits_cpu, limits_memory
            ),
            startup_timeout_seconds=startup_timeout,
            is_delete_operator_pod=True,
            pod_template_file=self.minimal_pod_template,
            node_selector=node_selector_labels,
        )

    def create_resources(
        self, requests_cpu, requests_memory, limits_cpu, limits_memory
    ):
        requests = {}
        if requests_cpu:
            requests["cpu"] = requests_cpu
        if requests_memory:
            requests["memory"] = requests_memory
        limits = {}
        if limits_cpu:
            limits["cpu"] = limits_cpu
        if limits_memory:
            limits["memory"] = limits_memory
        return k8s.V1ResourceRequirements(limits=limits, requests=requests)

    @property
    def minimal_pod_template(self):
        minimal_pod_template = f"""
apiVersion: v1
kind: Pod
metadata:
  name: {PodGenerator.make_unique_pod_id(self._task_id)}
spec:
  containers:
    - name: base
      env:
"""
        if self._mlflow_enabled:
            minimal_pod_template += """
        - name: MLFLOW_RUN_ID
          value: {{ task_instance.xcom_pull(key="mlflow_run_id") }}
"""
        if not self._volume_disabled:
            minimal_pod_template += f"""
  volumes:
    - name: storage
      persistentVolumeClaim:
        claimName: {self._pvc_name}
"""
        return minimal_pod_template

    def create_security_context(
        self, volume_disabled: bool, volume_owner: int
    ) -> k8s.V1PodSecurityContext:
        return (
            k8s.V1PodSecurityContext(fs_group=volume_owner)
            if not volume_disabled
            else k8s.V1PodSecurityContext()
        )
