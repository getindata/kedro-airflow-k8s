import json

import boto3
from airflow.exceptions import AirflowException
from airflow.operators.python import BaseOperator

try:
    from airflow.providers.amazon.aws.operators.ecs import EcsOperator
except ImportError:
    from airflow.providers.amazon.aws.operators.ecs import (
        ECSOperator as EcsOperator,
    )


class TaskDefinitionResolverOperator(BaseOperator):
    def __init__(
        self,
        task_id,
        dag_commit_id,
        family,
        **kwargs,
    ):
        super().__init__(task_id=task_id, **kwargs)
        self.dag_commit_id = dag_commit_id
        self.family = family

    def _iterate_task_definitions(self):
        client = boto3.client("ecs")

        response_iterator = client.get_paginator(
            "list_task_definitions"
        ).paginate(
            familyPrefix=self.family,
            sort="DESC",
            PaginationConfig={
                "MaxItems": 100,
                "PageSize": 100,
            },
        )
        for response in response_iterator:
            for td_id in response["taskDefinitionArns"]:
                yield (
                    td_id,
                    client.describe_task_definition(taskDefinition=td_id),
                )

    def execute(self, context):
        for td_id, td_spec in self._iterate_task_definitions():
            image = td_spec["taskDefinition"]["containerDefinitions"][0][
                "image"
            ]
            if f":{self.dag_commit_id}" in image:
                context["ti"].xcom_push("task_definition_arn", td_id)
                return

        raise AirflowException(
            f"Cannot find Task Definition for commit {self.dag_commit_id}"
        )


class AWSFargateOperator(EcsOperator):
    template_fields = ("task_definition", "overrides")

    def __init__(
        self,
        task_id,
        pipeline,
        node_name,
        execution_params,
        task_definition,
        env,
        mlflow_enabled,
        parameters="",
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            launch_type="FARGATE",
            task_definition=task_definition,
            cluster=execution_params["cluster"],
            overrides={
                "containerOverrides": [
                    {
                        "name": "main",
                        "command": [
                            "-e",
                            env,
                            "--pipeline",
                            pipeline,
                            "--node",
                            node_name,
                            "--params",
                            parameters,
                        ],
                    }
                ]
            },
            network_configuration={
                "awsvpcConfiguration": {
                    "securityGroups": execution_params["security_groups"],
                    "subnets": execution_params["subnets"],
                },
            },
            **kwargs,
        )
        self.mlflow_enabled = mlflow_enabled

    def execute(self, context):
        if self.mlflow_enabled:
            env = json.loads(
                context["task_instance"].xcom_pull(key="mlflow_auth_context")
            )
            env["MLFLOW_RUN_ID"] = context["task_instance"].xcom_pull(
                key="mlflow_run_id"
            )
            self.overrides["containerOverrides"][0]["environment"] = [
                {"name": key, "value": value} for key, value in env.items()
            ]
        super().execute(context)
