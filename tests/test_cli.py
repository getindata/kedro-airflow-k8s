import unittest
from pathlib import Path
from unittest.mock import MagicMock

from click.testing import CliRunner

from kedro_airflow_k8s.cli import generate
from kedro_airflow_k8s.config import PluginConfig
from kedro_airflow_k8s.context_helper import ContextHelper

test_config = PluginConfig(
    {
        "image": "docker.registry.com/getindata/kedro-project-image",
        "namespace": "airflow",
    }
)


def pipeline_fixture():
    def node(name):
        nd = MagicMock()
        nd.name = name
        return nd

    start = node("start")
    task_1, task_2 = node("task_1"), node("task_2")
    finish = node("finish")
    nodes = [start, task_1, task_2, finish]

    node_dependencies = {
        task_1: [start],
        task_2: [start],
        finish: [task_1, task_2],
    }

    pipeline = MagicMock()
    pipeline.nodes = nodes
    pipeline.node_dependencies = node_dependencies

    return pipeline


class TestPluginCLI(unittest.TestCase):
    def test_generate(self):
        context_helper = MagicMock(ContextHelper)
        context_helper.context.package_name = "kedro_airflow_k8s"
        context_helper.context.pipelines.get = lambda x: pipeline_fixture()
        context_helper.project_name = "kedro_airflow_k8s"
        context_helper.config = {
            "namespace": "test_ns",
            "image": "test/image:latest",
        }
        context_helper.mlflow_config = {
            "mlflow_tracking_uri": "mlflow.url.com"
        }
        context_helper.session.store["git"].commit_sha = "abcdef"

        config = dict(context_helper=context_helper)

        runner = CliRunner()

        result = runner.invoke(generate, [], obj=config)
        assert result.exit_code == 0
        assert Path("dags/kedro_airflow_k8s.py").exists()

        dag_content = Path("dags/kedro_airflow_k8s.py").read_text()
        assert 'EXPERIMENT_NAME = "kedro_airflow_k8s"' in dag_content
        assert "namespace='test_ns'" in dag_content
        assert "image='test/image:latest'" in dag_content
        assert 'MlflowClient("mlflow.url.com")' in dag_content
        assert "commit_sha:abcdef" in dag_content
