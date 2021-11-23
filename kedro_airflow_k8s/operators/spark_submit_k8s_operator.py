import logging
from typing import Dict, List, Optional

import airflow.providers.apache.spark.operators.spark_submit


class SparkSubmitK8SOperator(
    airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator
):
    def __init__(
        self,
        node_name: str,
        kedro_script: str,
        image: str,
        run_name: str,
        nodes: List[str],
        env: str,
        conf: Dict[str, str] = {},
        requests_cpu: Optional[str] = None,
        limits_cpu: Optional[str] = None,
        limits_memory: Optional[str] = None,
        image_pull_policy: str = "IfNotPresent",
        driver_port: Optional[str] = None,
        block_manager_port: Optional[str] = None,
        secrets: Dict[str, str] = {},
        labels: Dict[str, str] = {},
        service_account_name: str = "default",
        local_storage_class_name: Optional[str] = None,
        local_storage_size: Optional[str] = None,
        namespace: str = "default",
        **kwargs,
    ):
        nodes_list = ",".join(nodes)
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
        base_conf.update(
            {
                f"spark.kubernetes.driver.secrets.{k}": v
                for k, v in secrets.items()
            }
        )
        base_conf.update(
            {
                f"spark.kubernetes.executor.secrets.{k}": v
                for k, v in secrets.items()
            }
        )
        base_conf.update(
            {
                f"spark.kubernetes.driver.label.{k}": v
                for k, v in labels.items()
            }
        )
        base_conf.update(
            {
                f"spark.kubernetes.executor.label.{k}": v
                for k, v in labels.items()
            }
        )
        if driver_port:
            base_conf["spark.driver.port"] = driver_port
        if block_manager_port:
            base_conf["spark.blockManager.port"] = block_manager_port
        if local_storage_class_name and local_storage_size:
            storage_prop = (
                "spark.kubernetes.executor.volumes.persistentVolumeClaim."
                "spark-local-dir-kedro"
            )
            base_conf[f"{storage_prop}.options.claimName"] = "OnDemand"
            base_conf[
                f"{storage_prop}.options.storageClass"
            ] = local_storage_class_name
            base_conf[f"{storage_prop}.options.sizeLimit"] = local_storage_size
            base_conf[f"{storage_prop}.mount.path"] = "/storage"
            base_conf[f"{storage_prop}.mount.readOnly"] = "false"
        if limits_memory:
            base_conf["spark.driver.memory"] = limits_memory
            base_conf["spark.executor.memory"] = limits_memory
        if limits_cpu:
            base_conf["spark.kubernetes.driver.limit.cores"] = limits_cpu
            base_conf["spark.kubernetes.executor.limit.cores"] = limits_cpu
        if requests_cpu:
            base_conf["spark.driver.cores"] = requests_cpu
            base_conf["spark.executor.cores"] = requests_cpu

        base_conf.update(conf)
        logging.info(f"K8S configuration for {kedro_script} is: {base_conf}")
        super().__init__(
            task_id=f"kedro-{node_name}",
            application=kedro_script,
            conf=base_conf,
            name=f"{run_name}_{node_name}",
            application_args=[f"--env={env}", f"--nodes={nodes_list}"],
            **kwargs,
        )
