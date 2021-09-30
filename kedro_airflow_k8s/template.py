from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional, List

import jinja2
import kedro
from jinja2.environment import TemplateStream
from slugify import slugify

from kedro_airflow_k8s import version
from kedro_airflow_k8s.config import ResourceConfig


def _get_mlflow_url(context_helper):
    try:
        import importlib

        importlib.import_module("kedro_mlflow")
        return context_helper.mlflow_config["mlflow_tracking_uri"]
    except (ModuleNotFoundError, kedro.config.config.MissingConfigException):
        return None


def _get_jinja_template(name: str):
    loader = jinja2.FileSystemLoader(str(Path(__file__).parent))
    jinja_env = jinja2.Environment(
        autoescape=True, loader=loader, lstrip_blocks=True
    )
    jinja_env.filters["slugify"] = slugify
    template = jinja_env.get_template(name)
    return template


def _node_resources(nodes, config) -> Dict[str, ResourceConfig]:
    result = {}
    default_config = config.__default__
    for node in nodes:
        resources = [
            tag[len("resources:"):]  # noqa: E203
            for tag in node.tags
            if "resources:" in tag
        ]
        result[node.name] = (
            config[resources[0]] if resources else default_config
        )
    return result


def _create_template_stream(
        context_helper,
        dag_name: str,
        schedule_interval: str,
        image: str,
        with_external_dependencies: bool,
) -> TemplateStream:
    template = _get_jinja_template("airflow_dag_template.j2")

    pipeline = context_helper.pipeline
    dependencies = defaultdict(list)
    for node, parent_nodes in pipeline.node_dependencies.items():
        for parent in parent_nodes:
            dependencies[parent].append(node)

    dependencies_grouped = defaultdict(list)
    spark_task_groups = [tg for tg in context_helper.pipeline_grouped if
                         tg.group_type == "spark"]

    spark_tasks = dict()
    for tg in spark_task_groups:
        task_group_name = tg.name
        for node in tg.task_group:
            node_name = node.name
            spark_tasks[node_name] = task_group_name

    for node, parent_nodes in pipeline.node_dependencies.items():
        for parent in parent_nodes:
            if node.name not in spark_tasks.keys():
                dependencies_grouped[
                    parent.name if parent.name not in spark_tasks.keys() else
                    spark_tasks[parent.name]].append(node.name)

    return template.stream(
        pipeline=pipeline,
        pipeline_grouped=context_helper.pipeline_grouped,
        dependencies=dependencies,
        dependencies_grouped=dependencies_grouped,
        with_external_dependencies=with_external_dependencies,
        config=context_helper.config,
        resources=_node_resources(
            pipeline.nodes, context_helper.config.run_config.resources
        ),
        mlflow_url=_get_mlflow_url(context_helper),
        env=context_helper.env,
        project_name=context_helper.project_name,
        dag_name=dag_name,
        image=image,
        pipeline_name=context_helper.pipeline_name,
        schedule_interval=schedule_interval,
        git_info=context_helper.session.store["git"],
        kedro_airflow_k8s_version=version,
        include_start_mlflow_experiment_operator=(
                Path(__file__).parent / "operators/start_mlflow_experiment.py"
        ).read_text(),
        include_create_pipeline_storage_operator=(
                Path(__file__).parent / "operators/create_pipeline_storage.py"
        ).read_text(),
        include_delete_pipeline_storage_operator=(
                Path(__file__).parent / "operators/delete_pipeline_storage.py"
        ).read_text(),
        include_data_volume_init_operator=(
                Path(__file__).parent / "operators/data_volume_init.py"
        ).read_text(),
        include_node_pod_operator=(
                Path(__file__).parent / "operators/node_pod.py"
        ).read_text(),
        secrets=context_helper.config.run_config.secrets,
        macro_params=context_helper.config.run_config.macro_params,
        variables_params=context_helper.config.run_config.variables_params,
    )


def _create_spark_tasks_template_stream(
        context_helper,
        dag_name: str
) -> List[TemplateStream]:
    spark_task_templates = []
    spark_task_groups = [tg for tg in context_helper.pipeline_grouped if
                         tg.group_type == "spark"]
    for tg in spark_task_groups:
        from_node = tg.task_group[0].name
        to_node = tg.task_group[-1].name
        template = _get_jinja_template("airflow_spark_task_template.j2")
        spark_task_templates.append(
            template.stream(
                from_node=from_node,
                to_node=to_node
            )
        )
    return spark_task_templates


def get_cron_expression(
        ctx, cron_expression: Optional[str] = None
) -> Optional[str]:
    config = ctx.obj["context_helper"].config
    return cron_expression or config.run_config.cron_expression


def get_dag_filename_and_template_stream(
        ctx,
        cron_expression: Optional[str] = None,
        dag_name: Optional[str] = None,
        image: Optional[str] = None,
        with_external_dependencies: bool = True,
):
    config = ctx.obj["context_helper"].config
    dag_name = dag_name or config.run_config.run_name

    template_stream = _create_template_stream(
        ctx.obj["context_helper"],
        dag_name=dag_name,
        schedule_interval=cron_expression,
        image=image or config.run_config.image,
        with_external_dependencies=with_external_dependencies,
    )
    spark_template_streams = _create_spark_tasks_template_stream(
        ctx.obj["context_helper"],
        dag_name=dag_name,
    )
    return dag_name, template_stream, spark_template_streams
