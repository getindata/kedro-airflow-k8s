import unittest

from kedro_airflow_k8s.operators.spark_submit_k8s import SparkSubmitK8SOperator


class TestSparkSubmitK8SOperator(unittest.TestCase):
    def test_task_create(self):
        task = SparkSubmitK8SOperator(
            node_name="test-spark-node",
            kedro_script="local:///test/location.py",
            image="spark-specific-image:v2",
            run_name="yet-another-experiment",
            nodes=["prepare_data", "train"],
            env="pipelines",
            conf={"spark.test": "unit-case"},
            requests_cpu="2",
            limits_cpu="4",
            limits_memory="10g",
            image_pull_policy="Always",
            driver_port="40000",
            block_manager_port="50000",
            secrets={"password_file": "/var/passwords"},
            labels={
                "spark": "true",
                "kedro": "true",
            },
            service_account_name="airflow",
            local_storage_class_name="cmek",
            local_storage_size="100Gi",
            namespace="airflow-spark",
            env_vars={
                "MLFLOW_RUN_ID": "{{ ti.xcom_pull(key='mlflow_run_id') }}",
                "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets",
            },
            jars="local:///jars/gcs_connector.jar,local:///jars/bq_connector.jar",
            packages="com:test_package:2.1",
            repositories="http://test/repo/maven",
            num_executors="1",
            conn_id="spark_k8s",
        )

        conf = task._conf
        expected_conf = {
            "spark.blockManager.port": "50000",
            "spark.driver.cores": "2",
            "spark.driver.memory": "10g",
            "spark.driver.port": "40000",
            "spark.executor.cores": "2",
            "spark.executor.memory": "10g",
            "spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets",
            "spark.executorEnv.MLFLOW_RUN_ID": "{{ ti.xcom_pull(key='mlflow_run_id') }}",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "airflow",
            "spark.kubernetes.container.image": "spark-specific-image:v2",
            "spark.kubernetes.container.image.pullPolicy": "Always",
            "spark.kubernetes.driver.label.kedro": "true",
            "spark.kubernetes.driver.label.spark": "true",
            "spark.kubernetes.driver.limit.cores": "4",
            "spark.kubernetes.driver.secrets.password_file": "/var/passwords",
            "spark.kubernetes.driver.volumes.persistentVolumeClaim."
            "spark-local-dir-1.mount.path": "/storage",
            "spark.kubernetes.driver.volumes.persistentVolumeClaim."
            "spark-local-dir-1.mount.readOnly": "false",
            "spark.kubernetes.driver.volumes.persistentVolumeClaim."
            "spark-local-dir-1.options.claimName": "OnDemand",
            "spark.kubernetes.driver.volumes.persistentVolumeClaim."
            "spark-local-dir-1.options.sizeLimit": "100Gi",
            "spark.kubernetes.driver.volumes.persistentVolumeClaim."
            "spark-local-dir-1.options.storageClass": "cmek",
            "spark.kubernetes.executor.label.kedro": "true",
            "spark.kubernetes.executor.label.spark": "true",
            "spark.kubernetes.executor.limit.cores": "4",
            "spark.kubernetes.executor.secrets.password_file": "/var/passwords",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim."
            "spark-local-dir-1.mount.path": "/storage",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim."
            "spark-local-dir-1.mount.readOnly": "false",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim."
            "spark-local-dir-1.options.claimName": "OnDemand",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim."
            "spark-local-dir-1.options.sizeLimit": "100Gi",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim."
            "spark-local-dir-1.options.storageClass": "cmek",
            "spark.kubernetes.namespace": "airflow-spark",
            "spark.submit.deployMode": "cluster",
            "spark.test": "unit-case",
        }

        assert conf == expected_conf
        assert task.task_id == "kedro-test-spark-node"
        assert task._application == "local:///test/location.py"
        assert task._name == "yet-another-experiment_test-spark-node"
        assert task._application_args == [
            "run",
            "--env=pipelines",
            "--node=prepare_data,train",
            "--runner=ThreadRunner",
        ]
