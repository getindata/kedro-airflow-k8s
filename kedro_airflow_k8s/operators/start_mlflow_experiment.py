import logging

from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults


class StartMLflowExperimentOperator(PythonOperator):
    @apply_defaults
    def __init__(
        self,
        mlflow_url: str,
        experiment_name: str,
        task_id: str = "start_mlflow_run",
        **kwargs,
    ) -> None:
        super().__init__(
            task_id=task_id, python_callable=self.start_mlflow_run, **kwargs
        )
        self.experiment_name = experiment_name
        self.mlflow_url = mlflow_url

    def create_mlflow_client(self):
        from mlflow.tracking import MlflowClient

        return MlflowClient(self.mlflow_url)

    def start_mlflow_run(self, ti, **kwargs):
        from mlflow.protos.databricks_pb2 import (
            RESOURCE_ALREADY_EXISTS,
            ErrorCode,
        )
        from mlflow.exceptions import MlflowException
        from mlflow.entities.lifecycle_stage import LifecycleStage

        mlflow_client = self.create_mlflow_client()
        try:
            experiment_id = mlflow_client.create_experiment(
                self.experiment_name
            )
            logging.info(
                f"Experiment {self.experiment_name} created with id {experiment_id}"
            )
        except MlflowException as create_error:
            if create_error.error_code != ErrorCode.Name(
                RESOURCE_ALREADY_EXISTS
            ):
                raise

            experiment = mlflow_client.get_experiment_by_name(
                self.experiment_name
            )
            if experiment.lifecycle_stage == LifecycleStage.DELETED:
                logging.error(
                    f"Experiment {self.experiment_name} already DELETED"
                )
                raise
            experiment_id = experiment.experiment_id
            logging.info(
                f"Experiment {self.experiment_name} exists with id {experiment_id}"
            )

        run_id = mlflow_client.create_run(experiment_id).info.run_id
        ti.xcom_push("mlflow_run_id", run_id)

        return run_id
