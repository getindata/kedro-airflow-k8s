from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner

from kedro_airflow_k8s import airflow
from kedro_airflow_k8s.cli import compile, run_once, schedule, upload_pipeline
from kedro_airflow_k8s.context_helper import ContextHelper


class TestPluginCLI:
    @pytest.fixture(scope="class")
    def pipeline(self):
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

    @pytest.fixture(scope="class")
    def context_helper(self, pipeline):
        context_helper = MagicMock(ContextHelper)
        context_helper.context.package_name = "kedro_airflow_k8s"
        context_helper.context.pipelines.get.return_value = pipeline
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
        context_helper.airflow_config = {
            "airflow_rest_api_uri": "airflow.url.com/api/v1"
        }
        return context_helper

    def test_compile(self, context_helper):
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
        assert "schedule_interval=None" in dag_content

    def test_upload_pipeline(self, context_helper):
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

    def test_schedule(self, context_helper):
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        output_directory = TemporaryDirectory(
            prefix="test_schedule", suffix=".py"
        )
        result = runner.invoke(
            schedule,
            [
                "--output",
                str(output_directory.name),
                "--cron-expression",
                "0 0 0 5 *",
            ],
            obj=config,
        )
        assert result.exit_code == 0
        assert Path(output_directory.name).exists()

        dag_content = (
            Path(output_directory.name) / "kedro_airflow_k8s.py"
        ).read_text()
        assert "schedule_interval='0 0 0 5 *'" in dag_content

    def test_run_once(self, context_helper):
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        output_directory = TemporaryDirectory(
            prefix="test_run_once", suffix=".py"
        )

        with patch.object(
            airflow.AirflowClient, "wait_for_dag"
        ) as wait_for_dag:
            with patch.object(
                airflow.AirflowClient, "trigger_dag_run"
            ) as trigger_dag_run:
                wait_for_dag.return_value = airflow.DAGModel(
                    dag_id="kedro_airflow_k8s",
                    tags=[{"name": "demo"}, {"name": "commit_sha:abcdef"}],
                )

                result = runner.invoke(
                    run_once,
                    [
                        "--output",
                        str(output_directory.name),
                    ],
                    obj=config,
                )
                assert result.exit_code == 0
                trigger_dag_run.assert_called_once_with("kedro_airflow_k8s")

                assert Path(output_directory.name).exists()

                dag_content = (
                    Path(output_directory.name) / "kedro_airflow_k8s.py"
                ).read_text()
                assert len(dag_content) > 0

    def test_run_once_upload_error(self, context_helper):
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        output_directory = TemporaryDirectory(
            prefix="test_run_once", suffix=".py"
        )

        with pytest.raises(airflow.MissingDAGException):
            with patch.object(
                airflow.AirflowClient, "wait_for_dag"
            ) as wait_for_dag:
                wait_for_dag.side_effect = Mock(
                    side_effect=airflow.MissingDAGException(
                        "kedro_airflow_k8s", "commit_sha:abcdef"
                    )
                )

                result = runner.invoke(
                    run_once,
                    [
                        "--output",
                        str(output_directory.name),
                    ],
                    obj=config,
                )
                assert result.exit_code == 1
