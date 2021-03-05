from collections import defaultdict
from pathlib import Path

import click
import jinja2
from slugify import slugify

from kedro_airflow_k8s.context_helper import ContextHelper


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
def generate(ctx, target_path="dags/"):
    """Create an Airflow DAG for a project"""
    loader = jinja2.FileSystemLoader(str(Path(__file__).parent))
    jinja_env = jinja2.Environment(
        autoescape=True, loader=loader, lstrip_blocks=True
    )
    jinja_env.filters["slugify"] = slugify
    template = jinja_env.get_template("airflow_dag_template.j2")

    package_name = ctx.obj["context_helper"].context.package_name
    dag_filename = f"{package_name}.py"

    target_path = Path(target_path)
    target_path = target_path / dag_filename

    target_path.parent.mkdir(parents=True, exist_ok=True)
    pipeline = ctx.obj["context_helper"].context.pipelines.get("__default__")
    dependencies = defaultdict(list)

    nodes_with_no_deps = set(node.name for node in pipeline.nodes)
    for node, parent_nodes in pipeline.node_dependencies.items():
        for parent in parent_nodes:
            dependencies[parent].append(node)
            nodes_with_no_deps = nodes_with_no_deps - set([node.name])

    template.stream(
        dag_name=package_name,
        dependencies=dependencies,
        project_name=ctx.obj["context_helper"].project_name,
        pipeline=pipeline,
        config=ctx.obj["context_helper"].config,
        git_info=ctx.obj["context_helper"].session.store["git"],
        base_nodes=nodes_with_no_deps,
        mlflow_url=ctx.obj["context_helper"].mlflow_config[
            "mlflow_tracking_uri"
        ],
        env=ctx.obj["context_helper"].env,
    ).dump(str(target_path))
