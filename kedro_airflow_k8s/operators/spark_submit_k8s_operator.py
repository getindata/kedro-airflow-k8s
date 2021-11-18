from typing import List

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
        namespace: str = "default",
        num_executors: int = 1,
        conn_id: str = "spark_default",
        **kwargs,
    ):
        self._kedro_script = kedro_script
        nodes_list = ",".join(nodes)
        super().__init__(
            task_id=f"kedro-{node_name}",
            application="local:///home/kedro/src/pyspark_iris_in_airflow/pyspark_run.py",
            conf={
                "spark.kubernetes.namespace": namespace,
                "spark.submit.deployMode": "cluster",
                "spark.kubernetes.container.image": image,
                "spark.kubernetes.driver.secrets.gsa": "/tmp/gsa",
                "spark.kubernetes.executor.secrets.gsa": "/tmp/gsa",
                "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow",
            },
            jars="local:///home/kedro/jars/gcs-connector-hadoop3-latest.jar",
            name=f"{run_name}_{node_name}",
            conn_id=conn_id,
            num_executors=num_executors,
            application_args=[f"--env={env}", f"--nodes={nodes_list}"],
            env_vars={
                "GOOGLE_APPLICATION_CREDENTIALS": "/tmp/gsa/secrets_sa",
                "GOOGLE_AUDIENCE": "Airflow",
            },
            **kwargs,
        )
