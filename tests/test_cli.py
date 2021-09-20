import os
import webbrowser
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, Mock, patch

import click
import pytest
from click.testing import CliRunner

from kedro_airflow_k8s import airflow
from kedro_airflow_k8s.airflow import DAGModel
from kedro_airflow_k8s.cli import (
    compile,
    init,
    list_pipelines,
    run_once,
    schedule,
    ui,
    upload_pipeline,
)
from kedro_airflow_k8s.config import PluginConfig
from kedro_airflow_k8s.context_helper import ContextHelper


class TestPluginCLI:
    @pytest.fixture(scope="class")
    def pipeline(self):
        def node(name, resource=None):
            nd = MagicMock()
            nd.name = name
            if resource:
                nd.tags = set({f"resources:{resource}"})
            return nd

        start = node("start")
        task_1, task_2 = node("task_1"), node("task_2", resource="huge")
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
        context_helper.pipeline = pipeline
        context_helper.pipeline_name = "test_pipeline_name"
        context_helper.project_name = "kedro_airflow_k8s"
        context_helper.config = PluginConfig(
            {
                "host": "airflow.url.com",
                "run_config": {
                    "image": "test/image:latest",
                    "namespace": "test_ns",
                    "experiment_name": "kedro_airflow_k8s",
                    "cron_expression": None,
                    "startup_timeout": 120,
                    "start_date": "20210721",
                    "image_pull_secrets": "pull_secrets",
                    "service_account_name": "service_account",
                    "volume": {
                        "access_modes": ["ReadWriteMany"],
                        "size": "3Gi",
                        "storageclass": "with-encryption",
                    },
                    "secrets": [
                        {"secret": "airflow-secrets"},
                        {
                            "secret": "database-secrets",
                            "deploy_target": "DB_PASSWORD",
                            "key": "password",
                        },
                    ],
                    "macro_params": ["ds", "pre_ds"],
                    "variables_params": ["env"],
                    "resources": {
                        "__default__": {
                            "requests": {"cpu": "2", "memory": "1Gi"},
                            "limits": {"cpu": "4", "memory": "4Gi"},
                            "annotations": {
                                "vault.hashicorp.com/agent-inject-template-foo": '{{- with secret "database/creds/db-app" -}}\npostgres://{{ .Data.username }}:{{ .Data.password }}@postgres:5432/mydb?sslmode=disable\n{{- end }}\n'  # noqa: E501
                            },
                        },
                        "huge": {
                            "node_selectors": {
                                "target/k8s.io": "mammoth",
                                "custom_label": "test",
                            },
                            "requests": {"cpu": "16", "memory": "128Gi"},
                        },
                    },
                    "authentication": {"type": "GoogleOAuth2", "params": []},
                    "env_vars": ["var1", "var2"],
                },
            }
        )
        context_helper.mlflow_config = {
            "mlflow_tracking_uri": "mlflow.url.com"
        }
        context_helper.session.store["git"].commit_sha = "abcdef"
        return context_helper

    def test_compile(self, context_helper):
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        result = runner.invoke(
            compile, ["--image", "image:override"], obj=config
        )

        assert result.exit_code == 0
        assert Path("dags/kedro_airflow_k8s.py").exists()

        dag_content = Path("dags/kedro_airflow_k8s.py").read_text()

        assert 'EXPERIMENT_NAME = "kedro-airflow-k8s"' in dag_content
        assert "namespace='test_ns'" in dag_content
        assert 'image="image:override"' in dag_content
        assert "mlflow_url='mlflow.url.com'" in dag_content
        assert "commit_sha:abcdef" in dag_content
        assert "access_modes=['ReadWriteMany']" in dag_content
        assert "volume_size='3Gi'" in dag_content
        assert "schedule_interval=None" in dag_content
        assert "storage_class_name='with-encryption'" in dag_content
        assert 'requests_memory="1Gi"' in dag_content
        assert 'limits_memory="4Gi"' in dag_content
        assert 'requests_memory="128Gi"' in dag_content
        assert 'requests_cpu="2"' in dag_content
        assert 'limits_cpu="4"' in dag_content
        assert 'requests_cpu="16"' in dag_content
        assert '"target/k8s.io": "mammoth"' in dag_content
        assert "startup_timeout=120" in dag_content
        assert 'pipeline="test_pipeline_name"' in dag_content
        assert "start_date=datetime(2021, int('07'), int('21'))" in dag_content
        assert 'image_pull_secrets="pull_secrets"' in dag_content
        assert 'service_account_name="service_account"' in dag_content
        assert "auth_handler=GoogleOAuth2AuthHandler()" in dag_content
        assert (
            """env_vars={
                "var1": "{{ var.value.var1 }}",
                "var2": "{{ var.value.var2 }}",
            }"""
            in dag_content
        )
        assert (
            """secrets=[
                Secret("env", None, "airflow-secrets", None),
                Secret("env", "DB_PASSWORD", "database-secrets", "password"),
            ]"""
            in dag_content
        )

        assert (
            """parameters=\"\"\"
                ds:{{ ds }},
                pre_ds:{{ pre_ds }},
                env:{{ var.value.env }},
            \"\"\","""
            in dag_content
        )

        assert (
            '''"vault.hashicorp.com/agent-inject-template-foo": """{{- with secret "database/creds/db-app" -}}
postgres://{{ .Data.username }}:{{ .Data.password }}@postgres:5432/mydb?sslmode=disable
{{- end }}
"""'''  # noqa: E501
            in dag_content
        )

    def test_compile_with_dependencies(self, context_helper):
        context_helper.config._raw["run_config"].update(
            {"external_dependencies": [{"dag_id": "parent_dag"}]}
        )
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        result = runner.invoke(compile, [], obj=config)
        assert result.exit_code == 0
        assert Path("dags/kedro_airflow_k8s.py").exists()

        dag_content = Path("dags/kedro_airflow_k8s.py").read_text()
        assert "ExternalTaskSensor" in dag_content
        assert "external_dag_id='parent_dag'," in dag_content
        assert "task_id='wait_for_parent_dag_None'," in dag_content

    def test_compile_with_auth_vars(self, context_helper):
        context_helper.config._raw["run_config"].update(
            {"authentication": {"type": "Vars", "params": ["var1", "var2"]}}
        )
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        result = runner.invoke(compile, [], obj=config)
        assert result.exit_code == 0
        assert Path("dags/kedro_airflow_k8s.py").exists()
        dag_content = Path("dags/kedro_airflow_k8s.py").read_text()

        assert 'auth_handler=VarsAuthHandler(["var1","var2",])' in dag_content

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
        context_helper.config._raw["run_config"].update(
            {"external_dependencies": [{"dag_id": "parent_dag"}]}
        )
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        output_directory = TemporaryDirectory(
            prefix="test_run_once", suffix=".py"
        )

        with patch("kedro_airflow_k8s.cli.AirflowClient") as AirflowMock:
            airflow_client = AirflowMock.return_value
            airflow_client.wait_for_dag.return_value = airflow.DAGModel(
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

            airflow_client.trigger_dag_run.assert_called_once_with(
                "kedro_airflow_k8s"
            )

        assert Path(output_directory.name).exists()

        dag_content = (
            Path(output_directory.name) / "kedro_airflow_k8s.py"
        ).read_text()
        assert len(dag_content) > 0
        assert "xternalTaskSensor(external_dag_id=" not in dag_content

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

    def test_run_once_with_wait_for_completion(self, context_helper):
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        output_directory = TemporaryDirectory(
            prefix="test_run_once", suffix=".py"
        )

        with patch("kedro_airflow_k8s.cli.AirflowClient") as AirflowMock:
            airflow_client = AirflowMock.return_value
            airflow_client.wait_for_dag.return_value = airflow.DAGModel(
                dag_id="kedro_airflow_k8s",
                tags=[{"name": "demo"}, {"name": "commit_sha:abcdef"}],
            )
            airflow_client.trigger_dag_run.return_value = "test-dag-run-id"
            airflow_client.wait_for_dag_run_completion.return_value = "success"

            result = runner.invoke(
                run_once,
                [
                    "--output",
                    str(output_directory.name),
                    "--wait-for-completion",
                    10,
                ],
                obj=config,
            )

        assert result.exit_code == 0
        assert Path(output_directory.name).exists()

        dag_content = (
            Path(output_directory.name) / "kedro_airflow_k8s.py"
        ).read_text()
        assert len(dag_content) > 0

    def test_list_pipelines(self, context_helper):
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        click.echo = Mock()
        with patch.object(airflow.AirflowClient, "list_dags") as list_dags:
            list_dags.return_value = [
                DAGModel(
                    dag_id="match0",
                    tags=[
                        {"name": "generated_with_kedro_airflow_k8s:0.1.2"},
                        {"name": "experiment_name:zxw_experiment"},
                    ],
                ),
                DAGModel(
                    dag_id="match1",
                    tags=[
                        {"name": "generated_with_kedro_airflow_k8s:0.1.1"},
                        {"name": "experiment_name:test_experiment"},
                    ],
                ),
                DAGModel(
                    dag_id="match2",
                    tags=[
                        {"name": "generated_with_kedro_airflow_k8s:0.1.2"},
                        {"name": "experiment_name:test_experiment"},
                    ],
                ),
            ]
            result = runner.invoke(
                list_pipelines,
                [],
                obj=config,
            )

        assert result.exit_code == 0
        click.echo.assert_called_once()
        tabulate_result = click.echo.call_args_list[0][0][0]
        assert "match0" in tabulate_result
        assert "match1" in tabulate_result
        assert "match2" in tabulate_result
        assert "test_experiment" in tabulate_result
        assert "zxw_experiment" in tabulate_result

    def test_ui(self, context_helper):
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        webbrowser.open_new_tab = Mock()

        result = runner.invoke(
            ui,
            [],
            obj=config,
        )

        assert result.exit_code == 0
        webbrowser.open_new_tab.assert_called_once_with("airflow.url.com")

    def test_ui_with_dag_view(self, context_helper):
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        webbrowser.open_new_tab = Mock()

        result = runner.invoke(
            ui,
            ["--dag-name", "test-dag"],
            obj=config,
        )

        assert result.exit_code == 0
        webbrowser.open_new_tab.assert_called_once_with(
            "airflow.url.com/tree?dag_id=test-dag"
        )

    def test_init(self, context_helper):
        context_helper.context.project_path = Path("test-name")
        config = dict(context_helper=context_helper)

        runner = CliRunner()

        cwd = os.getcwd()
        try:
            with TemporaryDirectory() as td:
                os.chdir(td)
                result = runner.invoke(
                    init,
                    [
                        "--with-github-actions",
                        "--output=gs://dag.bucket",
                        "https://test.apache.airflow.com",
                    ],
                    obj=config,
                )

                assert result.exit_code == 0

                assert (Path(td) / "conf/base/airflow-k8s.yaml").exists()
                assert (
                    "{{"
                    not in (
                        Path(td) / "conf/base/airflow-k8s.yaml"
                    ).read_text()
                )
                assert (
                    Path(td) / ".github/workflows/on-merge-to-master.yml"
                ).exists()
                assert (
                    "PROJECT_NAME: test-name"
                    in (
                        Path(td) / ".github/workflows/on-merge-to-master.yml"
                    ).read_text()
                )
                assert (Path(td) / ".github/workflows/on-push.yml").exists()
                assert (
                    "PROJECT_NAME: test-name"
                    in (Path(td) / ".github/workflows/on-push.yml").read_text()
                )
        finally:
            os.chdir(cwd)
