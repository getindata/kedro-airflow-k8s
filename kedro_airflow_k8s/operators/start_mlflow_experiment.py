"""
Module contains Apache Airflow operator that starts experiment in mlflow.
"""
import abc
import logging
import os
from typing import Dict, List, Optional

from airflow.models import Variable
from airflow.operators.python import BaseOperator
from airflow.utils.decorators import apply_defaults


class AuthHandler:
    @abc.abstractmethod
    def obtain_credentials(self) -> Dict[str, str]:
        pass


class NullAuthHandler(AuthHandler):
    @staticmethod
    def instance():
        return NullAuthHandler()

    def obtain_credentials(self) -> Dict[str, str]:
        return {}


class GoogleOAuth2AuthHandler(AuthHandler):
    log = logging.getLogger("GoogleOAuth2AuthHandler")

    def __init__(self, audience: Optional[str] = None):
        if not audience:
            audience = os.environ["GOOGLE_AUDIENCE"]
        self.audience = audience

    def obtain_credentials(self) -> Dict[str, str]:
        from google.auth.transport.requests import Request
        from google.oauth2 import id_token

        gac = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", None)
        token = ""

        if gac:
            try:
                self.log.debug(
                    "Attempt to get JWT token for %s." + self.audience
                )
                token = id_token.fetch_id_token(Request(), self.audience)
                self.log.info("Obtained JWT token for authentication.")
            except Exception as e:
                self.log.error("Failed to obtain IAP access token. " + str(e))
            finally:
                return {"MLFLOW_TRACKING_TOKEN": token}


class VarsAuthHandler(AuthHandler):
    def __init__(self, params: List[str]):
        self.params = params

    def obtain_credentials(self) -> Dict[str, str]:
        return {key: Variable.get(key) for key in self.params}


class StartMLflowExperimentOperator(BaseOperator):
    """
    This class manages start of experiment in mlflow, by injecting experiment run_id to
    xcom. It's also creates new mlflow experiment if required.
    """

    @apply_defaults
    def __init__(
        self,
        mlflow_url: str,
        experiment_name: str,
        task_id: str = "start_mlflow_run",
        image: str = None,
        auth_handler: AuthHandler = NullAuthHandler.instance(),
        **kwargs,
    ) -> None:
        """

        :param mlflow_url: full URL to the server (with protocl,
                            f.e. https://project.mlflow.com)
        :param experiment_name: MLflow compliant experiment name
        :param task_id: name of the task represented by operator
        :param kwargs:
        """
        super().__init__(task_id=task_id, **kwargs)
        self.experiment_name = experiment_name
        self.mlflow_url = mlflow_url
        self.image = image
        self.auth_handler = auth_handler

    def create_mlflow_client(self):
        """
        Creates MlFlowClient based on internal url, on demand as it cannot be stored
        internally in operator due to the JSON serialization issue.
        :return: mlflow client object
        """
        from mlflow.tracking import MlflowClient

        return MlflowClient(self.mlflow_url)

    # pylint: disable=W0613
    # pylint: disable=C0103
    def execute(self, context: Dict):
        """
        On pipeline start it may be required to create experiment if it does not exist
        which happens in this method. Obtains experiment run_id and passes to xcom as
         `mlflow_run_id` so it can be referenced by ML related tasks.
        :param context: Airflow context
        :return: mlflow experiment run_id
        """
        auth_vars = self.auth_handler.obtain_credentials()
        for k, v in auth_vars.items():
            os.environ[k] = v

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
        if self.image is not None:
            mlflow_client.log_param(run_id, "image", self.image)

        mlflow_client.log_param(run_id, "dag_id", context["dag"].dag_id)
        mlflow_client.log_param(
            run_id, "execution_date", str(context.get("execution_date"))
        )

        context["ti"].xcom_push("mlflow_run_id", run_id)

        return run_id
