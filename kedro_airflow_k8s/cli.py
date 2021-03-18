from collections import defaultdict
from pathlib import Path
from typing import Optional

import click
import fsspec
import jinja2
from jinja2.environment import TemplateStream
from slugify import slugify

from kedro_airflow_k8s.airflow import AirflowClient
from kedro_airflow_k8s.context_helper import ContextHelper


def _create_template_stream(
    context_helper, dag_name: str = None, schedule_interval: str = None
) -> TemplateStream:
    loader = jinja2.FileSystemLoader(str(Path(__file__).parent))
    jinja_env = jinja2.Environment(
        autoescape=True, loader=loader, lstrip_blocks=True
    )
    jinja_env.filters["slugify"] = slugify
    template = jinja_env.get_template("airflow_dag_template.j2")

    package_name = context_helper.context.package_name

    pipeline = context_helper.context.pipelines.get("__default__")
    dependencies = defaultdict(list)

    nodes_with_no_deps = set(node.name for node in pipeline.nodes)
    for node, parent_nodes in pipeline.node_dependencies.items():
        for parent in parent_nodes:
            dependencies[parent].append(node)
            nodes_with_no_deps = nodes_with_no_deps - set([node.name])

    all_parent_nodes = set()
    for _, parent_nodes in pipeline.node_dependencies.items():
        all_parent_nodes = all_parent_nodes.union(
            set(parent.name for parent in parent_nodes)
        )
    bottom_nodes = set(node.name for node in pipeline.nodes) - all_parent_nodes

    return template.stream(
        dag_name=dag_name or package_name,
        experiment_name=package_name,
        dependencies=dependencies,
        project_name=context_helper.project_name,
        pipeline=pipeline,
        config=context_helper.config,
        git_info=context_helper.session.store["git"],
        base_nodes=nodes_with_no_deps,
        bottom_nodes=bottom_nodes,
        mlflow_url=context_helper.mlflow_config["mlflow_tracking_uri"],
        env=context_helper.env,
        schedule_interval=schedule_interval,
        include_start_mlflow_experiment_operator=(
            Path(__file__).parent / "operators/start_mlflow_experiment.py"
        ).read_text(),
    )


def get_dag_filename_and_template_stream(
    ctx, cron_expression=None, dag_name=None
):
    context_helper = ctx.obj["context_helper"]
    package_name = context_helper.context.package_name
    dag_filename = f"{dag_name or package_name}.py"
    template_stream = _create_template_stream(
        context_helper, dag_name=dag_name, schedule_interval=cron_expression
    )
    return dag_filename, template_stream


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
@click.pass_obj
@click.pass_context
def airflow_group(ctx, metadata, env):
    ctx.ensure_object(dict)
    ctx.obj["context_helper"] = ContextHelper.init(metadata, env)


@airflow_group.command()
@click.pass_context
def compile(ctx, target_path="dags/"):
    """Create an Airflow DAG for a project"""
    dag_filename, template_stream = get_dag_filename_and_template_stream(ctx)

    target_path = Path(target_path) / dag_filename

    with fsspec.open(str(target_path), "wt") as f:
        template_stream.dump(f)


@airflow_group.command()
@click.option(
    "-o",
    "--output",
    "output",
    type=str,
    required=True,
    help="Location where DAG file should be uploaded, for GCS use gs:// or "
    "gcs:// prefix, other notations indicate locally mounted filesystem",
)
@click.pass_context
def upload_pipeline(ctx, output: str):
    """
    Uploads pipeline to Airflow DAG location
    """
    dag_filename, template_stream = get_dag_filename_and_template_stream(ctx)

    with fsspec.open(f"{output}/{dag_filename}", "wt") as f:
        template_stream.dump(f)


@airflow_group.command()
@click.option(
    "-o",
    "--output",
    "output",
    type=str,
    help="Location where DAG file should be uploaded, for GCS use gs:// or "
    "gcs:// prefix, other notations indicate locally mounted filesystem",
)
@click.option(
    "-c",
    "--cron-expression",
    type=str,
    help="Cron expression for recurring run",
    required=True,
)
@click.pass_context
def schedule(ctx, output: str, cron_expression: str):
    """
    Uploads pipeline to Airflow with given schedule
    """
    dag_filename, template_stream = get_dag_filename_and_template_stream(
        ctx, cron_expression
    )

    with fsspec.open(f"{output}/{dag_filename}", "wt") as f:
        template_stream.dump(f)


@airflow_group.command()
@click.option(
    "-o",
    "--output",
    "output",
    type=str,
    required=True,
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
@click.pass_context
def run_once(
    ctx,
    output: str,
    dag_name: Optional[str],
    wait_for_completion: Optional[int],
):
    """
    Uploads pipeline to Airflow and runs once
    """
    dag_filename, template_stream = get_dag_filename_and_template_stream(
        ctx, dag_name=dag_name
    )
    context_helper = ctx.obj["context_helper"]

    with fsspec.open(f"{output}/{dag_filename}", "wt") as f:
        template_stream.dump(f)

    airflow_client = AirflowClient(
        context_helper.airflow_config["airflow_rest_api_uri"]
    )
    dag = airflow_client.wait_for_dag(
        dag_id=dag_name or context_helper.context.package_name,
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
