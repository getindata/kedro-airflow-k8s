from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional

import jinja2
import kedro
from jinja2.environment import TemplateStream
from slugify import slugify

from kedro_airflow_k8s import version
from kedro_airflow_k8s.config import KubernetesPodTemplate, ResourceConfig


def get_commit_sha(context_helper):
    try:
        return context_helper.session.store["git"]["commit_sha"]
    except KeyError:
        return "UNKNOWN"


def get_mlflow_url(context_helper):
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
            tag[len("resources:") :]  # noqa: E203
            for tag in node.tags
            if "resources:" in tag
        ]
        result[node.name] = (
            config[resources[0]] if resources else default_config
        )
    return result


def _pod_templates(nodes, config) -> Dict[str, KubernetesPodTemplate]:
    result = defaultdict(lambda: None)

    default_config = config.__default__
    for node in nodes:
        templates = [
            tag[len("k8s_template:") :]  # noqa: E203
            for tag in node.tags
            if "k8s_template:" in tag
        ]
        result[node.name] = (
            config[templates[0]] if templates else default_config
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

    custom_spark_factory = None
    if context_helper.config.run_config.spark.operator_factory:
        factory = context_helper.config.run_config.spark.operator_factory
        pkg = factory[: factory.rindex(".")]
        clazz = factory[factory.rindex(".") + 1 :]  # noqa: E203
        mod = __import__(pkg, fromlist=[clazz])
        custom_spark_factory = getattr(mod, clazz)()

    return template.stream(
        custom_spark_factory=custom_spark_factory,
        pipeline=context_helper.pipeline,
        pipeline_grouped=context_helper.pipeline_grouped,
        with_external_dependencies=with_external_dependencies,
        config=context_helper.config,
        resources=_node_resources(
            context_helper.pipeline.nodes,
            context_helper.config.run_config.resources,
        ),
        mlflow_url=get_mlflow_url(context_helper),
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
        k8s_templates=_pod_templates(
            context_helper.pipeline.nodes,
            context_helper.config.run_config.kubernetes_pod_templates,
        ),
    )


def _create_spark_tasks_template_stream(
    context_helper,
) -> Dict[str, TemplateStream]:
    spark_task_templates = {}
    spark_task_groups = [
        tg
        for tg in context_helper.pipeline_grouped
        if tg.group_type == "pyspark"
    ]
    for tg in spark_task_groups:
        node_names = [node.name for node in tg.task_group]
        template = _get_jinja_template("airflow_spark_task_template.j2")
        spark_task_templates[tg.name] = template.stream(
            node_names=node_names,
            project_name=context_helper.project_name,
            kedro_env=context_helper.env,
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
    )
    return dag_name, template_stream, spark_template_streams
