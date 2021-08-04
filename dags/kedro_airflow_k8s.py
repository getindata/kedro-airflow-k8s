import logging

from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta
from datetime import datetime

EXPERIMENT_NAME = "kedro-airflow-k8s"

args = {
    'owner': 'airflow',
}



"""
Module contains Apache Airflow operator that creates dynamic PV for data shared by
experiment tasks.
"""
import logging
from typing import Dict, List, Optional

from airflow.operators.python import BaseOperator
from airflow.utils.decorators import apply_defaults
from kubernetes import client, config
from kubernetes.client import models as k8s


class CreatePipelineStorageOperator(BaseOperator):
    """
    This class manages creation of persistent volume claim based on dynamic storage
    defined by storage class
    """

    template_fields = ["pvc_name"]

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
        super().__init__(task_id=task_id, **kwargs)
        self._namespace = namespace
        self._access_modes = access_modes
        self._volumes_size = volume_size
        self._storage_class_name = storage_class_name
        self.pvc_name = pvc_name

    def execute(self, context: Dict):
        """
        Executed usually on pipeline start to create persistent volume claim to
        be used by experiment. Name of the pvc can be dynamic and is passed to
        xcom as `pvc_name`.
        :param context: Airflow context
        :return: pvc name
        """
        with client.ApiClient(config.load_incluster_config()) as api_client:
            logging.info(
                f"Creating PVC [{self._namespace}:{self.pvc_name}] "
                f"of size {self._volumes_size} "
                f"with storage class {self._storage_class_name} "
                f"and access modes: {str(self._access_modes)}"
            )
            pvc = self.create_pvc()
            k8s_client = client.CoreV1Api(api_client)
            k8s_client.create_namespaced_persistent_volume_claim(
                self._namespace, pvc
            )
            logging.info("PVC created")
            context["ti"].xcom_push("pvc_name", self.pvc_name)

            return self.pvc_name

    def create_pvc(self):
        """
        Creates k8s pvc definition object
        :return:
        """
        pvc = k8s.V1PersistentVolumeClaim(
            metadata=k8s.V1ObjectMeta(
                name=self.pvc_name, namespace=self._namespace
            ),
            spec=k8s.V1PersistentVolumeClaimSpec(
                access_modes=self._access_modes,
                storage_class_name=self._storage_class_name,
                resources=k8s.V1ResourceRequirements(
                    requests={"storage": self._volumes_size}
                ),
            ),
        )
        return pvc

"""
Module contains Apache Airflow operator that deletes dynamic PV used  by
experiment tasks.
"""
import logging
from typing import Dict

from airflow.operators.python import BaseOperator
from airflow.utils.decorators import apply_defaults
from kubernetes import client, config


class DeletePipelineStorageOperator(BaseOperator):
    """
    This class manages deletion of persistent volume claim. If used, will be triggered
    regardless of Airflow tasks execution result.
    """

    template_fields = ["pvc_name"]

    @apply_defaults
    def __init__(
        self,
        pvc_name: str,
        namespace: str,
        task_id: str = "delete_pipeline_storage",
        **kwargs,
    ) -> None:
        """

        :param pvc_name: name of the pvc to be created, can include expressions like
                ts_nodash
        :param namespace: namespace of created pvc
        :param task_id: name of the task represented by operator
        :param kwargs:
        """
        super().__init__(
            task_id=task_id,
            trigger_rule="all_done",
            **kwargs,
        )
        self._namespace = namespace
        self.pvc_name = pvc_name

    def execute(self, context: Dict):
        """
        Executed usually on pipeline end to delete persistent volume claim
         used by experiment. Name of the pvc can be dynamic.
        :param context: Airflow context
        """
        with client.ApiClient(config.load_incluster_config()) as api_client:
            logging.info(f"Deleteing PVC [{self._namespace}:{self.pvc_name}]")
            self.delete_namespace(api_client)
            logging.info("PVC deleted")

    def delete_namespace(self, api_client):
        """
        Delete namespace with given k8s client
        :param api_client:
        :return:
        """
        k8s_client = client.CoreV1Api(api_client)
        k8s_client.delete_namespaced_persistent_volume_claim(
            name=self.pvc_name, namespace=self._namespace
        )


"""
Module contains Apache Airflow operator that initialize attached PV with data sourced
 from image.
"""
import logging
from pathlib import Path

import jinja2
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
        service_account_name: str = None,
        image_pull_secrets: str = None,
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
        self._service_account_name = service_account_name
        self._image_pull_secrets = image_pull_secrets
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
             it's fully templated and PVC name has dynamic part
        """
        loader = jinja2.FileSystemLoader(str(Path(__file__).parent))

        data_volume_init_definition = (
            jinja2.Environment(
                autoescape=True, loader=loader, lstrip_blocks=True
            )
            .get_template("data_volume_init_template.j2")
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
        image_pull_secrets: Optional[str] = None,
        service_account_name: Optional[str] = None,
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
        :param image_pull_secrets: Any image pull secrets to be given to the pod.
                                   If more than one secret is required, provide a
                                   comma separated list: secret_a,secret_b
        :param service_account_name: Name of the service account
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
            image_pull_secrets=image_pull_secrets,
            service_account_name=service_account_name,
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


with DAG(
    dag_id='kedro_airflow_k8s',
    description='None',
    default_args=args,
    schedule_interval=None,
    start_date=datetime(2021, int('07'), int('21')),
    tags=['commit_sha:abcdef',
            'generated_with_kedro_airflow_k8s:0.6.0',
            'experiment_name:'+EXPERIMENT_NAME],
    params={},
) as dag:

    pvc_name = 'kedro-airflow-k8s.{{ ts_nodash | lower  }}'




    create_pipeline_storage = CreatePipelineStorageOperator(
        pvc_name=pvc_name,
        namespace='test_ns',
        access_modes=['ReadWriteMany'],
        volume_size='3Gi',
        storage_class_name='with-encryption'
    )

    delete_pipeline_storage=DeletePipelineStorageOperator(
        pvc_name=pvc_name,
        namespace='test_ns'
    )


    data_volume_init=DataVolumeInitOperator(
        namespace='test_ns',
        pvc_name=pvc_name,
        image='test/image:latest',
        image_pull_policy='IfNotPresent',
        startup_timeout=120,
        volume_owner=0,
        image_pull_secrets="pull_secrets",
        service_account_name="service_account",

    )



    tasks = {}
    with TaskGroup("kedro", prefix_group_id=False) as kedro_group:

        tasks["start"] = NodePodOperator(
            node_name="start",
            namespace="test_ns",
            volume_disabled=False,
            pvc_name=pvc_name,
            image="test/image:latest",
            image_pull_policy="IfNotPresent",
            image_pull_secrets="pull_secrets",
            service_account_name="service_account",
            env="&lt;MagicMock name=&#39;mock.env&#39; id=&#39;140458852450448&#39;&gt;",
            pipeline="test_pipeline_name",
            task_id="start",
            startup_timeout=120,
            volume_owner=0,
            mlflow_enabled=False,
            requests_cpu="2",
            requests_memory="1Gi",
            limits_cpu="4",
            limits_memory="4Gi",
            node_selector_labels={
            },
            labels={
            },
            tolerations=[
            ],
            annotations={
            },
            secrets=[
                Secret("env", None, "airflow-secrets", None),
                Secret("env", "DB_PASSWORD", "database-secrets", "password"),
            ],
            parameters="""
                ds:{{ ds }},
                pre_ds:{{ pre_ds }},
                env:{{ var.value.env }},
            """,
        )

        tasks["task-1"] = NodePodOperator(
            node_name="task_1",
            namespace="test_ns",
            volume_disabled=False,
            pvc_name=pvc_name,
            image="test/image:latest",
            image_pull_policy="IfNotPresent",
            image_pull_secrets="pull_secrets",
            service_account_name="service_account",
            env="&lt;MagicMock name=&#39;mock.env&#39; id=&#39;140458852450448&#39;&gt;",
            pipeline="test_pipeline_name",
            task_id="task-1",
            startup_timeout=120,
            volume_owner=0,
            mlflow_enabled=False,
            requests_cpu="2",
            requests_memory="1Gi",
            limits_cpu="4",
            limits_memory="4Gi",
            node_selector_labels={
            },
            labels={
            },
            tolerations=[
            ],
            annotations={
            },
            secrets=[
                Secret("env", None, "airflow-secrets", None),
                Secret("env", "DB_PASSWORD", "database-secrets", "password"),
            ],
            parameters="""
                ds:{{ ds }},
                pre_ds:{{ pre_ds }},
                env:{{ var.value.env }},
            """,
        )

        tasks["task-2"] = NodePodOperator(
            node_name="task_2",
            namespace="test_ns",
            volume_disabled=False,
            pvc_name=pvc_name,
            image="test/image:latest",
            image_pull_policy="IfNotPresent",
            image_pull_secrets="pull_secrets",
            service_account_name="service_account",
            env="&lt;MagicMock name=&#39;mock.env&#39; id=&#39;140458852450448&#39;&gt;",
            pipeline="test_pipeline_name",
            task_id="task-2",
            startup_timeout=120,
            volume_owner=0,
            mlflow_enabled=False,
            requests_cpu="16",
            requests_memory="128Gi",
            limits_cpu="",
            limits_memory="",
            node_selector_labels={
                "target/k8s.io": "mammoth",
                "custom_label": "test",
            },
            labels={
            },
            tolerations=[
            ],
            annotations={
            },
            secrets=[
                Secret("env", None, "airflow-secrets", None),
                Secret("env", "DB_PASSWORD", "database-secrets", "password"),
            ],
            parameters="""
                ds:{{ ds }},
                pre_ds:{{ pre_ds }},
                env:{{ var.value.env }},
            """,
        )

        tasks["finish"] = NodePodOperator(
            node_name="finish",
            namespace="test_ns",
            volume_disabled=False,
            pvc_name=pvc_name,
            image="test/image:latest",
            image_pull_policy="IfNotPresent",
            image_pull_secrets="pull_secrets",
            service_account_name="service_account",
            env="&lt;MagicMock name=&#39;mock.env&#39; id=&#39;140458852450448&#39;&gt;",
            pipeline="test_pipeline_name",
            task_id="finish",
            startup_timeout=120,
            volume_owner=0,
            mlflow_enabled=False,
            requests_cpu="2",
            requests_memory="1Gi",
            limits_cpu="4",
            limits_memory="4Gi",
            node_selector_labels={
            },
            labels={
            },
            tolerations=[
            ],
            annotations={
            },
            secrets=[
                Secret("env", None, "airflow-secrets", None),
                Secret("env", "DB_PASSWORD", "database-secrets", "password"),
            ],
            parameters="""
                ds:{{ ds }},
                pre_ds:{{ pre_ds }},
                env:{{ var.value.env }},
            """,
        )



        tasks["start"] >> tasks["task-1"]

        tasks["start"] >> tasks["task-2"]

        tasks["task-1"] >> tasks["finish"]

        tasks["task-2"] >> tasks["finish"]




    with TaskGroup("external_pipelines", prefix_group_id=False):
        ExternalTaskSensor(external_dag_id='parent_dag',
                            external_task_id='',
                            check_existence=True,
                            execution_delta=timedelta(minutes=0),
                            task_id='wait_for_parent_dag_None',
                            timeout=1440 * 60,
                            ) >> kedro_group





    create_pipeline_storage >> data_volume_init
    data_volume_init >> delete_pipeline_storage





    data_volume_init >> kedro_group



    kedro_group >> delete_pipeline_storage


if __name__ == "__main__":
    dag.cli()