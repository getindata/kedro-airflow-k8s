import json
import unittest
from unittest.mock import MagicMock

import boto3
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.utils import timezone
from moto import mock_ecs

from kedro_airflow_k8s.operators.aws_fargate import (
    AWSFargateOperator,
    TaskDefinitionResolverOperator,
)


class TaskDefinitionResolverOperatorTest(unittest.TestCase):
    @staticmethod
    def create_context(task):
        dag = DAG(dag_id="dag")
        tzinfo = pendulum.timezone("Europe/Amsterdam")
        execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        task_instance = TaskInstance(task=task, execution_date=execution_date)
        return {
            "dag": dag,
            "execution_date": execution_date,
            "ts": execution_date.isoformat(),
            "task": task,
            "ti": task_instance,
        }

    @mock_ecs
    def test_should_resolve_td_if_image_tag_is_same(self):
        # given
        ecs_client = boto3.client("ecs")
        td = ecs_client.register_task_definition(
            family="my-training",
            containerDefinitions=[
                {"name": "main", "image": "my.repo/my-image:abcd123"}
            ],
        )

        # when
        op = TaskDefinitionResolverOperator(
            task_id="test",
            dag_commit_id="abcd123",
            family="my-training",
        )

        context = self.create_context(op)
        op.execute(context=context)

        # then
        assert (
            context["ti"].xcom_pull(key="task_definition_arn")
            == td["taskDefinition"]["taskDefinitionArn"]
        )

    @mock_ecs
    def test_should_resolve_td_if_image_tag_is_full(self):
        # given
        ecs_client = boto3.client("ecs")
        td = ecs_client.register_task_definition(
            family="my-training",
            containerDefinitions=[
                {
                    "name": "main",
                    "image": "my.repo/my-image:ffabcd1234abcs1234abcd1234",
                }
            ],
        )

        # when
        op = TaskDefinitionResolverOperator(
            task_id="test",
            dag_commit_id="ffabcd1",
            family="my-training",
        )

        context = self.create_context(op)
        op.execute(context=context)

        # then
        assert (
            context["ti"].xcom_pull(key="task_definition_arn")
            == td["taskDefinition"]["taskDefinitionArn"]
        )

    @mock_ecs
    def test_should_fail_if_td_is_missing(self):
        # given
        ecs_client = boto3.client("ecs")
        ecs_client.register_task_definition(
            family="my-training",
            containerDefinitions=[
                {"name": "main", "image": "my.repo/my-image:1.0"}
            ],
        )

        # when
        op = TaskDefinitionResolverOperator(
            task_id="test",
            dag_commit_id="1234567",
            family="my-training",
        )

        context = self.create_context(op)

        # then
        with self.assertRaises(AirflowException) as context:
            op.execute(context=context)
        assert (
            str(context.exception)
            == "Cannot find Task Definition for commit 1234567"
        )


class AWSFargateOperatorTest(unittest.TestCase):
    def create_context(self, task):
        dag = DAG(dag_id="dag")
        tzinfo = pendulum.timezone("Europe/Amsterdam")
        execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
        task_instance = TaskInstance(task=task, execution_date=execution_date)
        return {
            "dag": dag,
            "execution_date": execution_date,
            "ts": execution_date.isoformat(),
            "task": task,
            "task_instance": task_instance,
        }

    def create_operator(self, mlflow_support: bool):
        return AWSFargateOperator(
            task_id="run-training",
            pipeline="training",
            node_name="run",
            execution_params={
                "cluster": "my-cluster",
                "security_groups": ["sg1"],
                "subnets": ["net1"],
            },
            task_definition="arn:aws:ecs:eu-central-1:12:task-definition/my-training:1",
            env="kedro-env",
            mlflow_enabled=mlflow_support,
        )

    def test_start_fargate_container_without_mlflow_support(self):
        # given
        op = self.create_operator(False)
        client = MagicMock()
        op.hook = MagicMock()
        op.hook.get_conn.return_value = client
        context = self.create_context(op)

        # when
        op.execute(context=context)

        # then
        client.run_task.assert_called_once_with(
            cluster="my-cluster",
            taskDefinition="arn:aws:ecs:eu-central-1:12:task-definition/my-training:1",
            overrides={
                "containerOverrides": [
                    {
                        "name": "main",
                        "command": [
                            "-e",
                            "kedro-env",
                            "--pipeline",
                            "training",
                            "--node",
                            "run",
                            "--params",
                            "",
                        ],
                    }
                ]
            },
            startedBy="airflow",
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "securityGroups": ["sg1"],
                    "subnets": ["net1"],
                }
            },
        )

    def test_start_fargate_container_with_mlflow_support(self):
        # given
        op = self.create_operator(True)
        client = MagicMock()
        op.hook = MagicMock()
        op.hook.get_conn.return_value = client
        context = self.create_context(op)
        context["task_instance"].xcom_push(
            "mlflow_auth_context", json.dumps({"TOKEN": "abc"})
        )
        context["task_instance"].xcom_push("mlflow_run_id", "run-id")

        # when
        op.execute(context=context)

        # then
        client.run_task.assert_called_once_with(
            cluster="my-cluster",
            taskDefinition="arn:aws:ecs:eu-central-1:12:task-definition/my-training:1",
            overrides={
                "containerOverrides": [
                    {
                        "name": "main",
                        "command": [
                            "-e",
                            "kedro-env",
                            "--pipeline",
                            "training",
                            "--node",
                            "run",
                            "--params",
                            "",
                        ],
                        "environment": [
                            {"name": "TOKEN", "value": "abc"},
                            {"name": "MLFLOW_RUN_ID", "value": "run-id"},
                        ],
                    }
                ]
            },
            startedBy="airflow",
            launchType="FARGATE",
            networkConfiguration={
                "awsvpcConfiguration": {
                    "securityGroups": ["sg1"],
                    "subnets": ["net1"],
                }
            },
        )
