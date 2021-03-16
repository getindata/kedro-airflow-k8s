import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

from click.testing import CliRunner

from kedro_airflow_k8s.cli import compile, upload_pipeline
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
    def test_compile(self):
        context_helper = MagicMock(ContextHelper)
        context_helper.context.package_name = "kedro_airflow_k8s"
        context_helper.context.pipelines.get = lambda x: pipeline_fixture()
        context_helper.project_name = "kedro_airflow_k8s"
        context_helper.config = {
            "namespace": "test_ns",
            "image": "test/image:latest",
            "access_mode": "ReadWriteMany",
            "request_storage": "3Gi",
        }
        context_helper.mlflow_config = {
            "mlflow_tracking_uri": "mlflow.url.com"
        }
        context_helper.session.store["git"].commit_sha = "abcdef"

        config = dict(context_helper=context_helper)

        runner = CliRunner()

        result = runner.invoke(compile, [], obj=config)
        assert result.exit_code == 0
        assert Path("dags/kedro_airflow_k8s.py").exists()

        dag_content = Path("dags/kedro_airflow_k8s.py").read_text()
        assert 'EXPERIMENT_NAME = "kedro_airflow_k8s"' in dag_content
        assert "namespace='test_ns'" in dag_content
        assert "image: test/image:latest" in dag_content
        assert "mlflow_url='mlflow.url.com'" in dag_content
        assert "commit_sha:abcdef" in dag_content
        assert "access_modes=['ReadWriteMany']" in dag_content
        assert "'storage':'3Gi'" in dag_content

    def test_upload_pipeline(self):
        context_helper = MagicMock(ContextHelper)
        context_helper.context.package_name = "kedro_airflow_k8s"
        context_helper.context.pipelines.get = lambda x: pipeline_fixture()
        context_helper.project_name = "kedro_airflow_k8s"
        context_helper.config = {
            "namespace": "test_ns",
            "image": "test/image:latest",
            "access_mode": "ReadWriteMany",
            "request_storage": "3Gi",
        }
        context_helper.mlflow_config = {
            "mlflow_tracking_uri": "mlflow.url.com"
        }
        context_helper.session.store["git"].commit_sha = "abcdef"

        config = dict(context_helper=context_helper)

        runner = CliRunner()

        output_directory = TemporaryDirectory(
            prefix="test_upload_pipeline", suffix=".py"
        )
        result = runner.invoke(
            upload_pipeline,
            ["--output", str(output_directory.name)],
            obj=config,
        )
        assert result.exit_code == 0
        assert Path(output_directory.name).exists()

        dag_content = (
            Path(output_directory.name) / "kedro_airflow_k8s.py"
        ).read_text()
        assert len(dag_content) > 0
