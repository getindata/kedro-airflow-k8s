"""
Module contains Apache Airflow operator that initialize attached PV with data sourced
 from image.
"""
import logging

from airflow.kubernetes.pod_generator import PodGenerator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)


class DataVolumeInitOperator(KubernetesPodOperator):
    """
    Operator starts pod with target image and copies pointed data to attached PV.
    In some cases, initial data is provided in the image and it should be copied
    to the shared storage before the pipeline processing starts. This class simplifies
    creation of pods by providing convenient options.
    """

    def __init__(
        self,
        namespace: str,
        pvc_name: str,
        image: str,
        volume_owner: int,
        image_pull_policy: str,
        startup_timeout: int,
        source: str = "/home/kedro/data",
        task_id: str = "data_volume_init",
    ):
        """
        :param namespace: pod is started in this k8s namespace
        :param pvc_name: name of the pvc the data is copied to
        :param image: image to be mounted
        :param volume_owner: indicates fs group to use in security context
        :param image_pull_policy: k8s image pull policy
        :param startup_timeout: after the amount provided in seconds the pod start is
                                timed out
        :param source: the location where the data is provided inside the image
        :param task_id: Airflow id to override
        """
        self._namespace = namespace
        self._volume_owner = volume_owner
        self._pvc_name = pvc_name
        self._image = image
        self._source = source
        self._target = f"{self._source}volume"

        super().__init__(
            task_id=task_id,
            is_delete_operator_pod=True,
            startup_timeout_seconds=startup_timeout,
            pod_template_file=self.definition,
            image_pull_policy=image_pull_policy,
        )

    @property
    def definition(self):
        """
        :return: definition of pod which is used here instead of API directly, since
             since it's fully templated and PVC name has dynamic part
        """
        data_volume_init_definition = f"""
apiVersion: v1
kind: Pod
metadata:
  name: {self.create_name()}
  namespace: {self._namespace}
spec:
  securityContext:
    fsGroup: {self._volume_owner}
  volumes:
    - name: storage
      persistentVolumeClaim:
        claimName: {self._pvc_name}
  containers:
    - name: base
      image: {self._image}
      command:
        - "bash"
        - "-c"
      args:
        - cp --verbose -r {self._source}/* {self._target}
      volumeMounts:
        - mountPath: "{self._target}"
          name: storage
            """
        return data_volume_init_definition

    @staticmethod
    def create_name():
        """
        Dynamically created name for the pod. Name should be unique so it does not
        collide with parallel runs.
        :return:
        """
        return PodGenerator.make_unique_pod_id("data-volume-init")

    def execute(self, context):
        """
        Executes task in pod with provided configuration (super implementation used).
        :param context:
        :return:
        """
        logging.info(
            f"Copy image {self._image} data from {self._source} to {self._target}"
        )
        logging.info(str(self.create_pod_request_obj()))
        return super().execute(context)
