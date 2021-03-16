from collections import defaultdict
from pathlib import Path
from typing import Callable

import click
import jinja2
from jinja2.environment import TemplateStream
from slugify import slugify

from kedro_airflow_k8s.context_helper import ContextHelper


def _create_template_stream(context_helper) -> TemplateStream:
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
        dag_name=package_name,
        dependencies=dependencies,
        project_name=context_helper.project_name,
        pipeline=pipeline,
        config=context_helper.config,
        git_info=context_helper.session.store["git"],
        base_nodes=nodes_with_no_deps,
        bottom_nodes=bottom_nodes,
        mlflow_url=context_helper.mlflow_config["mlflow_tracking_uri"],
        env=context_helper.env,
        include_start_mlflow_experiment_operator=(
            Path(__file__).parent / "operators/start_mlflow_experiment.py"
        ).read_text(),
    )


def _process_file(path: str, processor: Callable):
    if path.startswith("gs://") or path.startswith("gcs://"):
        import google.auth

        credentials, project_id = google.auth.default()
        bare_path = path[path.index("/") + 2 :]  # noqa: E203

        import gcsfs

        fs = gcsfs.GCSFileSystem(project=project_id)
    else:
        bare_path = path

        from fsspec.implementations import local

        fs = local.LocalFileSystem(auto_mkdir=True)

    with fs.open(bare_path, "wt") as f:
        processor(f)


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
    template_stream = _create_template_stream(ctx.obj["context_helper"])

    package_name = ctx.obj["context_helper"].context.package_name
    dag_filename = f"{package_name}.py"
    target_path = Path(target_path)
    target_path = target_path / dag_filename

    _process_file(str(target_path), lambda f: template_stream.dump(f))


@airflow_group.command()
@click.option(
    "-o",
    "--output",
    "output",
    type=str,
    help="Location where DAG file should be uploaded, for GCS use gs:// or "
    "gcs:// prefix, other notations indicate locally mounted filesystem",
)
@click.pass_context
def upload_pipeline(ctx, output):
    """
    Uploads pipeline to Airflow DAG location
    """
    context_helper = ctx.obj["context_helper"]
    template_stream = _create_template_stream(context_helper)
    package_name = context_helper.context.package_name
    dag_filename = f"{package_name}.py"

    _process_file(
        f"{output}/{dag_filename}", lambda f: template_stream.dump(f)
    )
