from airflow.kubernetes.pod_generator import PodGenerator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)


class DataVolumeInitOperator(KubernetesPodOperator):
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
        self._namespace = namespace
        self._volume_owner = volume_owner
        self._pvc_name = pvc_name
        self._image = image
        self._source = source

        super().__init__(
            task_id=task_id,
            is_delete_operator_pod=True,
            startup_timeout_seconds=startup_timeout,
            pod_template_file=self.definition,
            image_pull_policy=image_pull_policy,
        )

    @property
    def definition(self):
        data_volume_init_definition = f"""
apiVersion: v1
kind: Pod
metadata:
  name: {PodGenerator.make_unique_pod_id('data-volume-init')}
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
        - cp --verbose -r {self._source}/* {self._source}volume
      volumeMounts:
        - mountPath: "{self._source}volume"
          name: storage
            """
        return data_volume_init_definition
