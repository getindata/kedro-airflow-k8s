from collections import defaultdict
from pathlib import Path
from typing import Optional

import jinja2
import kedro
from jinja2.environment import TemplateStream
from slugify import slugify

from kedro_airflow_k8s import version


def _get_mlflow_url(context_helper):
    try:
        import importlib

        importlib.import_module("kedro_mlflow")
        return context_helper.mlflow_config["mlflow_tracking_uri"]
    except (ModuleNotFoundError, kedro.config.config.MissingConfigException):
        return None


def _create_template_stream(
    context_helper,
    dag_name: Optional[str] = None,
    schedule_interval: Optional[str] = None,
    image: Optional[str] = None,
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
        image=image or context_helper.config.image,
        git_info=context_helper.session.store["git"],
        base_nodes=nodes_with_no_deps,
        bottom_nodes=bottom_nodes,
        mlflow_url=_get_mlflow_url(context_helper),
        env=context_helper.env,
        schedule_interval=schedule_interval,
        include_start_mlflow_experiment_operator=(
            Path(__file__).parent / "operators/start_mlflow_experiment.py"
        ).read_text(),
        kedro_airflow_k8s_version=version,
    )


def get_dag_filename_and_template_stream(
    ctx,
    cron_expression: Optional[str] = None,
    dag_name: Optional[str] = None,
    image: Optional[str] = None,
):
    context_helper = ctx.obj["context_helper"]
    package_name = context_helper.context.package_name
    dag_filename = f"{dag_name or package_name}.py"
    template_stream = _create_template_stream(
        context_helper,
        dag_name=dag_name,
        schedule_interval=cron_expression,
        image=image,
    )
    return dag_filename, template_stream
