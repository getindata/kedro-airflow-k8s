"""
Module contains Apache Airflow operator that deletes dynamic PV used  by
experiment tasks.
"""

from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults
from kubernetes import client, config


class DeletePipelineStorageOperator(PythonOperator):
    """
    This class manages deletion of persistent volume claim. If used, will be triggered
    regardless of Airflow tasks execution result.
    """

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
            python_callable=self.delete_pipeline_storage,
            op_args=[pvc_name],
            trigger_rule="all_done",
            **kwargs,
        )
        self._namespace = namespace

    def delete_pipeline_storage(self, pvc_name, ti, **kwargs):
        """
        Executed usually on pipeline end to delete persistent volume claim
         used by experiment. Name of the pvc can be dynamic.
        :param pvc_name: pvc_name
        :param ti: airflow ti
        """
        with client.ApiClient(config.load_incluster_config()) as api_client:
            v1 = client.CoreV1Api(api_client)
            v1.delete_namespaced_persistent_volume_claim(
                name=pvc_name, namespace=self._namespace
            )
