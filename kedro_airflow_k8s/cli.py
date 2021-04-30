import webbrowser
from pathlib import Path
from typing import Dict, List, Optional

import click
import fsspec
from tabulate import tabulate

from kedro_airflow_k8s.airflow import AirflowClient
from kedro_airflow_k8s.config import PluginConfig
from kedro_airflow_k8s.context_helper import ContextHelper
from kedro_airflow_k8s.template import (
    get_cron_expression,
    get_dag_filename_and_template_stream,
)


@click.group("airflow-k8s")
def commands():
    """Kedro plugin adding support for Airflow on K8S"""
    pass


@commands.group(
    name="airflow-k8s",
    context_settings=dict(help_option_names=["-h", "--help"]),
)
@click.option(
    "-e", "--env", "env", type=str, default="local", help="Environment to use."
)
@click.option(
    "-p",
    "--pipeline",
    "pipeline",
    type=str,
    default="__default__",
    required=False,
    help="Pipeline name to pick.",
)
@click.pass_obj
@click.pass_context
def airflow_group(ctx, metadata, env, pipeline):
    ctx.ensure_object(dict)
    ctx.obj["context_helper"] = ContextHelper.init(metadata, env, pipeline)


@airflow_group.command()
@click.option(
    "-i",
    "--image",
    "image",
    type=str,
    required=False,
    help="Image to override.",
)
@click.pass_context
def compile(ctx, image, target_path="dags/"):
    """Create an Airflow DAG for a project"""
    dag_filename, template_stream = get_dag_filename_and_template_stream(
        ctx, image=image, cron_expression=get_cron_expression(ctx)
    )

    target_path = Path(target_path) / dag_filename

    with fsspec.open(str(target_path), "wt") as f:
        template_stream.dump(f)


@airflow_group.command()
@click.option(
    "-o",
    "--output",
    "output",
    type=str,
    required=False,
    help="Location where DAG file should be uploaded, for GCS use gs:// or "
    "gcs:// prefix, other notations indicate locally mounted filesystem",
)
@click.option(
    "-i",
    "--image",
    "image",
    type=str,
    required=False,
    help="Image to override.",
)
@click.pass_context
def upload_pipeline(ctx, output: str, image: str):
    """
    Uploads pipeline to Airflow DAG location
    """
    dag_filename, template_stream = get_dag_filename_and_template_stream(
        ctx, image=image, cron_expression=get_cron_expression(ctx)
    )

    output = output or ctx.obj["context_helper"].config.output
    with fsspec.open(f"{output}/{dag_filename}", "wt") as f:
        template_stream.dump(f)


@airflow_group.command()
@click.option(
    "-o",
    "--output",
    "output",
    type=str,
    required=False,
    help="Location where DAG file should be uploaded, for GCS use gs:// or "
    "gcs:// prefix, other notations indicate locally mounted filesystem",
)
@click.option(
    "-c",
    "--cron-expression",
    type=str,
    help="Cron expression for recurring run",
    required=False,
)
@click.pass_context
def schedule(ctx, output: str, cron_expression: str):
    """
    Uploads pipeline to Airflow with given schedule
    """
    dag_filename, template_stream = get_dag_filename_and_template_stream(
        ctx, cron_expression=get_cron_expression(ctx, cron_expression)
    )

    output = output or ctx.obj["context_helper"].config.output
    with fsspec.open(f"{output}/{dag_filename}", "wt") as f:
        template_stream.dump(f)


@airflow_group.command()
@click.option(
    "-o",
    "--output",
    "output",
    type=str,
    required=False,
    help="Location where DAG file should be uploaded, for GCS use gs:// or "
    "gcs:// prefix, other notations indicate locally mounted filesystem",
)
@click.option(
    "-d",
    "--dag-name",
    "dag_name",
    type=str,
    required=False,
    help="Allows overriding dag id and dag file name for a purpose of multiple variants"
    " of experiments",
)
@click.option(
    "-w",
    "--wait-for-completion",
    "wait_for_completion",
    type=int,
    required=False,
    help="If set, tells plugin to wait for dag run to finish and how long (minutes)",
)
@click.option(
    "-i",
    "--image",
    "image",
    type=str,
    required=False,
    help="Image to override.",
)
@click.pass_context
def run_once(
    ctx,
    output: Optional[str],
    dag_name: Optional[str],
    wait_for_completion: Optional[int],
    image: Optional[str],
):  # pylint: disable=too-many-arguments
    """
    Uploads pipeline to Airflow and runs once
    """
    dag_filename, template_stream = get_dag_filename_and_template_stream(
        ctx,
        dag_name=dag_name,
        image=image,
        cron_expression=None,
        with_external_dependencies=False,
    )
    context_helper = ctx.obj["context_helper"]
    output = output or context_helper.config.output

    with fsspec.open(f"{output}/{dag_filename}", "wt") as f:
        template_stream.dump(f)

    airflow_client = AirflowClient(context_helper.config.host)
    dag = airflow_client.wait_for_dag(
        dag_id=dag_name or context_helper.config.run_config.run_name,
        tag=f'commit_sha:{context_helper.session.store["git"]["commit_sha"]}',
    )
    dag_run_id = airflow_client.trigger_dag_run(dag.dag_id)

    if (wait_for_completion or 0) > 0:
        assert (
            airflow_client.wait_for_dag_run_completion(
                dag.dag_id, dag_run_id, wait_for_completion
            )
            == "success"
        )


@airflow_group.command()
@click.pass_context
def list_pipelines(ctx):
    """
    List pipelines generated by this plugin
    """

    context_helper = ctx.obj["context_helper"]
    airflow_client = AirflowClient(context_helper.config.host)

    dags = airflow_client.list_dags("generated_with_kedro_airflow_k8s")

    def name(tags: List[Dict[str, str]]) -> str:
        experiment_tag = [
            t["name"] for t in tags if t["name"].startswith("experiment_name")
        ][0]
        return experiment_tag[len("experiment_name") + 1 :]  # noqa: E203

    pipelines = [[name(d.tags), d.dag_id] for d in dags]
    pipelines.sort()
    click.echo(tabulate(pipelines, headers=["Name", "ID"]))


@airflow_group.command()
@click.option(
    "-d",
    "--dag-name",
    "dag_name",
    type=str,
    required=False,
    help="View for this specific DAG will be opened",
)
@click.pass_context
def ui(ctx, dag_name: Optional[str] = None):
    """Open Apache Airflow UI in new browser tab"""
    host = ctx.obj["context_helper"].config.host
    if dag_name:
        host = f"{host}/tree?dag_id={dag_name}"
    webbrowser.open_new_tab(host)


@airflow_group.command()
@click.argument("airflow_url", type=str)
@click.option("--with-github-actions", is_flag=True, default=False)
@click.option("--output", type=str, default=False)
@click.pass_context
def init(ctx, airflow_url: str, with_github_actions: bool, output: str):
    """Initializes configuration for the plugin"""
    context_helper = ctx.obj["context_helper"]
    project_name = context_helper.context.project_path.name
    if with_github_actions:
        image = f"gcr.io/${{google_project_id}}/{project_name}:${{commit_id}}"
        run_name = f"{project_name}:${{commit_id}}"
    else:
        image = project_name
        run_name = project_name

    sample_config = PluginConfig.sample_config(
        url=airflow_url,
        image=image,
        project=project_name,
        run_name=run_name,
        output=output,
    )
    config_path = Path.cwd().joinpath("conf/base/airflow-k8s.yaml")
    config_path.parent.mkdir(exist_ok=True, parents=True)
    with open(config_path, "w") as f:
        f.write(sample_config)

    click.echo(f"Configuration generated in {config_path}")

    if with_github_actions:
        PluginConfig.initialize_github_actions(
            project_name,
            where=Path.cwd(),
            templates_dir=Path(__file__).parent / "templates",
        )
