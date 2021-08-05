"""
Module contains Apache Airflow operator that initialize attached PV with data sourced
 from image.
"""
import logging

import jinja2
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from jinja2.loaders import BaseLoader

VOLUME_INIT_TEMPLATE = """
apiVersion: v1
kind: Pod
metadata:
    name: {{ name }}
    namespace: {{ namespace }}
spec:
    securityContext:
        fsGroup: {{ volume_owner }}
    volumes:
        - name: storage
          persistentVolumeClaim:
            claimName: {{ pvc_name }}
    {%- if service_account_name %}
    serviceAccountName: {{ service_account_name }}
    {%- endif %}
    containers:
        - name: base
          image: {{ image }}
          command:
            - "bash"
            - "-c"
          args:
            - cp --verbose -r {{ source }}/* {{ target }}
          volumeMounts:
            - mountPath: "{{ target }}"
              name: storage
    {%- if image_pull_secrets %}
    imagePullSecrets:
    {% for secret in image_pull_secrets.split(",") %}
      - name: {{ secret }}
    {%- endfor  %}
    {%- endif  %}

"""


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
        service_account_name: str = "default",
        image_pull_secrets: str = None,
        source: str = "/home/kedro/data",
        task_id: str = "data_volume_init",
        **kwargs,
    ):
        """
        :param namespace: pod is started in this k8s namespace
        :param pvc_name: name of the pvc the data is copied to
        :param image: image to be mounted
        :param volume_owner: indicates fs group to use in security context
        :param image_pull_policy: k8s image pull policy
        :param startup_timeout: after the amount provided in seconds the pod start is
                                timed out
        :param service_account_name: service account pod will run as
        :param image_pull_secrets: image pull secrets to be passed to containers, as
                ',' separated values
        :param source: the location where the data is provided inside the image
        :param task_id: Airflow id to override
        """
        self._namespace = namespace
        self._volume_owner = volume_owner
        self._pvc_name = pvc_name
        self._image = image
        self._source = source
        self._target = f"{self._source}volume"
        self._service_account_name = service_account_name
        self._image_pull_secrets = image_pull_secrets
        super().__init__(
            task_id=task_id,
            is_delete_operator_pod=True,
            startup_timeout_seconds=startup_timeout,
            pod_template_file=self.definition,
            image_pull_policy=image_pull_policy,
            service_account_name=None,
            # service_account_name to be overridden by template
            **kwargs,
        )

    @property
    def definition(self):
        """
        :return: definition of pod which is used here instead of API directly, since
             it's fully templated and PVC name has dynamic part
        """
        data_volume_init_definition = (
            jinja2.Environment(
                autoescape=True, loader=BaseLoader(), lstrip_blocks=True
            )
            .from_string(VOLUME_INIT_TEMPLATE)
            .render(
                name=self.create_name(),
                namespace=self._namespace,
                volume_owner=self._volume_owner,
                pvc_name=self._pvc_name,
                service_account_name=self._service_account_name,
                image=self._image,
                image_pull_secrets=self._image_pull_secrets,
                source=self._source,
                target=self._target,
            )
        )
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
