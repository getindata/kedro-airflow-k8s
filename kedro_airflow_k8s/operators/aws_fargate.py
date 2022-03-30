import boto3
from airflow.exceptions import AirflowException
from airflow.operators.python import BaseOperator
from airflow.providers.amazon.aws.operators.ecs import EcsOperator


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

    def execute(self, context):
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
                td = client.describe_task_definition(taskDefinition=td_id)
                image = td["taskDefinition"]["containerDefinitions"][0][
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
                            "airflow",
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
        )
