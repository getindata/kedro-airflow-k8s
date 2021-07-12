"""
Module contains Apache Airflow operator that creates k8s pod for execution of
kedro node.
"""

import logging
from typing import Dict, List, Optional

from airflow.kubernetes.pod_generator import PodGenerator
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s


class NodePodOperator(KubernetesPodOperator):
    """
    Operator starts pod with target image with kedro projects and executes one node from
    the pipeline. This class simplifies creation of pods by providing convenient options.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        node_name: str,
        namespace: str,
        image: str,
        image_pull_policy: str,
        env: str,
        task_id: str,
        pipeline: str = "__default__",
        pvc_name: Optional[str] = None,
        startup_timeout: int = 600,
        volume_disabled: bool = False,
        volume_owner: int = 0,
        mlflow_enabled: bool = True,
        requests_cpu: Optional[str] = None,
        requests_memory: Optional[str] = None,
        limits_cpu: Optional[str] = None,
        limits_memory: Optional[str] = None,
        node_selector_labels: Optional[Dict[str, str]] = None,
        labels: Optional[Dict[str, str]] = None,
        tolerations: Optional[List[Dict[str, str]]] = None,
        annotations: Optional[Dict[str, str]] = None,
        secrets: Optional[List[Secret]] = None,
        source: str = "/home/kedro/data",
        parameters: Optional[str] = "",
    ):
        """

        :param node_name: name from the kedro pipeline
        :param namespace: k8s namespace the pod will execute in
        :param pvc_name: name of the shared storage attached to this pod
        :param image: image to be mounted
        :param image_pull_policy: k8s image pull policy
        :param env: kedro pipeline configuration name, provided with '-e' option
        :param pipeline: kedro pipeline name, provided with '--pipeline' option
        :param task_id: Airflow id to override
        :param startup_timeout: after the amount provided in seconds the pod start is
                                timed out
        :param volume_disabled: if set to true, shared volume is not attached
        :param volume_owner: if volume is not disabled, fs group associated with this pod
        :param mlflow_enabled: if mlflow_run_id value is passed from xcom
        :param requests_cpu: k8s requests cpu value
        :param requests_memory: k8s requests memory value
        :param limits_cpu: k8s limits cpu value
        :param limits_memory: k8s limits memory value
        :param node_selector_labels: dictionary of node selector labels to be put into
                                     pod node selector
        :param labels: dictionary of labels to apply on pod
        :param tolerations: dictionary tolerations for nodes
        :param annotations: dictionary of annotations to apply on pod
        :param source: mount point of shared storage
        :param parameters: additional kedro run parameters
        """
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
            arguments=[
                "kedro",
                "run",
                "-e",
                env,
                "--pipeline",
                pipeline,
                "--node",
                node_name,
                "--params",
                parameters,
            ],
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
            node_selectors=node_selector_labels,
            labels=labels,
            tolerations=self.create_tolerations(tolerations),
            annotations=annotations,
            secrets=secrets,
        )

    def execute(self, context):
        """
        Executes task in pod with provided configuration (super implementation used).
        :param context:
        :return:
        """
        logging.info(self.create_pod_request_obj())
        return super().execute(context)

    @staticmethod
    def create_resources(
        requests_cpu, requests_memory, limits_cpu, limits_memory
    ) -> k8s.V1ResourceRequirements:
        """
        Creates k8s resources based on requests and limits
        :param requests_cpu:
        :param requests_memory:
        :param limits_cpu:
        :param limits_memory:
        :return:
        """
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
        """
        This template is required since 'volumes' arguments are not templated via direct
        API nor passing xcom values in pod definition.
        :return: partial pod definition that should be complemented by other operator
                parameters
        """
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

    @staticmethod
    def create_security_context(
        volume_disabled: bool, volume_owner: int
    ) -> k8s.V1PodSecurityContext:
        """
        Creates security context based on volume information
        :param volume_disabled:
        :param volume_owner:
        :return:
        """
        return (
            k8s.V1PodSecurityContext(fs_group=volume_owner)
            if not volume_disabled
            else k8s.V1PodSecurityContext()
        )

    @staticmethod
    def create_tolerations(
        tolerations: Optional[List[Dict[str, str]]] = None
    ) -> List[k8s.V1Toleration]:
        """
        Creates k8s tolerations
        :param tolerations:
        :return:
        """
        if not tolerations:
            return []
        return [
            k8s.V1Toleration(
                effect=toleration.get("effect"),
                key=toleration.get("key"),
                operator=toleration.get("operator"),
                value=toleration.get("value"),
            )
            for toleration in tolerations
        ]
