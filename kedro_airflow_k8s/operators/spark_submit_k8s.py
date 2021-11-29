"""
Module contains Apache Airflow operator that creates k8s pod for execution of
kedro pyspark node.
"""
import logging
from typing import Dict, List, Optional

import airflow.providers.apache.spark.operators.spark_submit


class SparkSubmitK8SOperator(
    airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator
):
    """
    Operator starts pod with target image with kedro pyspark grouped nodes.
    This class simplifies creation of pods by providing convenient options to
    SparkSubmitOperator
    """

    def __init__(
        self,
        node_name: str,
        kedro_script: str,
        image: str,
        run_name: str,
        nodes: List[str],
        env: str,
        conf: Dict[str, str] = None,
        requests_cpu: Optional[str] = None,
        limits_cpu: Optional[str] = None,
        limits_memory: Optional[str] = None,
        image_pull_policy: str = "IfNotPresent",
        driver_port: Optional[str] = None,
        block_manager_port: Optional[str] = None,
        secrets: Dict[str, str] = None,
        labels: Dict[str, str] = None,
        service_account_name: str = "default",
        local_storage_class_name: Optional[str] = None,
        local_storage_size: Optional[str] = None,
        namespace: str = "default",
        **kwargs,
    ):
        nodes_list = ",".join(nodes)
        conf = conf or {}
        secrets = secrets or {}
        labels = labels or {}

        sa_conf_name = (
            "spark.kubernetes.authenticate.driver.serviceAccountName"
        )
        base_conf = {
            "spark.kubernetes.namespace": namespace,
            "spark.submit.deployMode": "cluster",
            "spark.kubernetes.container.image": image,
            "spark.kubernetes.container.image.pullPolicy": image_pull_policy,
            sa_conf_name: service_account_name,
        }
        base_conf.update(
            {
                f"spark.executorEnv.{k}": v
                for k, v in kwargs.get("env_vars", {}).items()
            }
        )
        base_conf.update(self.setup_secrets(labels, secrets))
        if driver_port:
            base_conf["spark.driver.port"] = driver_port
        if block_manager_port:
            base_conf["spark.blockManager.port"] = block_manager_port
        base_conf.update(
            self.setup_storage(local_storage_class_name, local_storage_size)
        )
        base_conf.update(
            self.setup_resources(limits_cpu, limits_memory, requests_cpu)
        )

        base_conf.update(conf)
        logging.info(f"K8S configuration for {kedro_script} is: {base_conf}")
        super().__init__(
            task_id=f"kedro-{node_name}",
            application=kedro_script,
            conf=base_conf,
            name=f"{run_name}_{node_name}",
            application_args=[
                "run",
                f"--env={env}",
                f"--node={nodes_list}",
                "--runner=ThreadRunner",
            ],
            **kwargs,
        )

    @staticmethod
    def setup_resources(limits_cpu, limits_memory, requests_cpu):
        """
        Sets up driver and executor configuration for k8s limits and requests
        :param limits_cpu:
        :param limits_memory:
        :param requests_cpu:
        :return:
        """
        base_conf = {}
        for worker_type in ["driver", "executor"]:
            if limits_memory:
                base_conf[f"spark.{worker_type}.memory"] = limits_memory
            if limits_cpu:
                base_conf[
                    f"spark.kubernetes.{worker_type}.limit.cores"
                ] = limits_cpu
            if requests_cpu:
                base_conf[f"spark.{worker_type}.cores"] = requests_cpu
        return base_conf

    @staticmethod
    def setup_secrets(labels, secrets):
        """
        Sets up driver and executor references to k8s secrets and injection points
        :param labels:
        :param secrets:
        :return:
        """
        base_conf = {}
        for worker_type in ["driver", "executor"]:
            base_conf.update(
                {
                    f"spark.kubernetes.{worker_type}.secrets.{k}": v
                    for k, v in secrets.items()
                }
            )
            base_conf.update(
                {
                    f"spark.kubernetes.{worker_type}.label.{k}": v
                    for k, v in labels.items()
                }
            )
        return base_conf

    @staticmethod
    def setup_storage(local_storage_class_name, local_storage_size):
        """
        Sets up driver and executor extra storage for data spilling
        :param local_storage_class_name:
        :param local_storage_size:
        :return:
        """
        base_conf = {}
        if local_storage_class_name and local_storage_size:
            for worker_type in ["driver", "executor"]:
                storage_prop = (
                    f"spark.kubernetes.{worker_type}.volumes.persistentVolumeClaim."
                    "spark-local-dir-1"
                )
                base_conf[f"{storage_prop}.options.claimName"] = "OnDemand"
                base_conf[
                    f"{storage_prop}.options.storageClass"
                ] = local_storage_class_name
                base_conf[
                    f"{storage_prop}.options.sizeLimit"
                ] = local_storage_size
                base_conf[f"{storage_prop}.mount.path"] = "/storage"
                base_conf[f"{storage_prop}.mount.readOnly"] = "false"
        return base_conf
