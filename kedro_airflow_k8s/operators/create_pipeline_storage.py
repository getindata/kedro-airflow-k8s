"""
Module contains Apache Airflow operator that creates dynamic PV for data shared by
experiment tasks.
"""
from typing import List, Optional

from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults
from kubernetes import client, config
from kubernetes.client import models as k8s


class CreatePipelineStorageOperator(PythonOperator):
    """
    This class manages creation of persistent volume claim based on dynamic storage
    defined by storage class
    """

    @apply_defaults
    def __init__(
        self,
        pvc_name: str,
        namespace: str,
        access_modes: List[str],
        volume_size: str,
        storage_class_name: Optional[str] = None,
        task_id: str = "create_pipeline_storage",
        **kwargs,
    ) -> None:
        """

        :param pvc_name: name of the pvc to be created, can include expressions like
                ts_nodash
        :param namespace: pvc will be created in this namespace
        :param access_modes: access modes requested by pvc
        :param volume_size: size of storage to be provisioned by the claim
        :param storage_class_name: optional class to be used by the claim for
                storage generation
        :param task_id: name of the task represented by operator
        :param kwargs:
        """
        super().__init__(
            task_id=task_id,
            python_callable=self.create_pipeline_storage,
            op_args=[pvc_name],
            **kwargs,
        )
        self._namespace = namespace
        self._access_modes = access_modes
        self._volumes_size = volume_size
        self._storage_class_name = storage_class_name

    def create_pipeline_storage(self, pvc_name, ti, **kwargs):
        """
        Executed usually on pipeline start to create persistent volume claim to
         be used by experiment. Name of the pvc can be dynamic and is passed to
          xcom as `pvc_name`.
        :param pvc_name: pvc_name
        :param ti: airflow ti
        :return: pvc name
        """
        with client.ApiClient(config.load_incluster_config()) as api_client:
            v1 = client.CoreV1Api(api_client)

            pvc = k8s.V1PersistentVolumeClaim(
                metadata=k8s.V1ObjectMeta(
                    name=pvc_name, namespace=self._namespace
                ),
                spec=k8s.V1PersistentVolumeClaimSpec(
                    access_modes=self._access_modes,
                    storage_class_name=self._storage_class_name,
                    resources=k8s.V1ResourceRequirements(
                        requests={"storage": self._volumes_size}
                    ),
                ),
            )

            v1.create_namespaced_persistent_volume_claim(self._namespace, pvc)
            ti.xcom_push("pvc_name", pvc_name)

            return pvc_name
