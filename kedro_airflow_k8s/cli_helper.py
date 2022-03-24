import logging
import tarfile
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Optional

import fsspec
import jinja2
from jinja2.environment import TemplateStream

from kedro_airflow_k8s.template import get_commit_sha, get_mlflow_url


class CliHelper:
    @staticmethod
    def dump_templates(
        dag_name: str, target_path: str, template_stream: TemplateStream
    ):
        dag_filename = f"{dag_name}.py"
        dag_target_path = (
            str((Path(target_path) / dag_filename))
            if "://" not in target_path
            else f"{target_path}/{dag_filename}"
        )
        with fsspec.open(dag_target_path, "wt") as f:
            template_stream.dump(f)

    @staticmethod
    def dump_spark_templates(
        target_path: str,
        project_name: str,
        commit_sha: str,
        template_steams: Dict[str, TemplateStream],
    ):
        for name, spark_template in template_steams.items():
            spark_task_target_path = (
                target_path + f"/{project_name}-{commit_sha}-{name}.py"
            )
            with fsspec.open(str(spark_task_target_path), "wt") as f:
                spark_template.dump(f)

    @staticmethod
    def _read_resource(source_dir: str, location: str) -> Optional[str]:
        return (Path(source_dir) / location).read_text() if location else ""

    @staticmethod
    def dump_spark_artifacts(
        ctx,
        target_path: str,
        spark_template_streams: Dict[str, TemplateStream],
    ):
        commit_sha = get_commit_sha(ctx.obj["context_helper"])
        source_dir = ctx.obj["context_helper"].source_dir
        project_name = ctx.obj["context_helper"].project_name
        spark_config = ctx.obj["context_helper"].config.run_config.spark
        is_mlflow_enabled = bool(get_mlflow_url(ctx.obj["context_helper"]))

        if spark_config.requires_artifacts_dump:
            user_init = CliHelper._read_resource(
                source_dir, spark_config.user_init_path
            )
            user_post_init = CliHelper._read_resource(
                source_dir, spark_config.user_post_init_path
            )

            CliHelper.dump_spark_templates(
                target_path, project_name, commit_sha, spark_template_streams
            )
            CliHelper.dump_project_as_archive(
                source_dir, target_path, project_name, commit_sha
            )
            CliHelper.dump_init_script(
                target_path,
                project_name,
                spark_config.artifacts_path,
                is_mlflow_enabled,
                user_init,
                user_post_init,
                commit_sha,
            )

    @staticmethod
    def dump_project_as_archive(
        source_path: str, target_path: str, project_name: str, commit_sha: str
    ):
        with NamedTemporaryFile(
            prefix="kedro-airflow-k8s-project", suffix=".tar.gz"
        ) as t:
            logging.info("Packing " + str(Path(source_path).parent))
            logging.info("Compressing to " + t.name)
            with tarfile.open(t.name, mode="w:gz") as tf:
                tf.add(
                    str(Path(source_path).parent),
                    arcname=project_name,
                    filter=lambda t: t
                    if not t.name.startswith(project_name + "/venv/")
                    else None,
                )

            target_name = target_path + f"/{project_name}-{commit_sha}.tar.gz"
            with fsspec.open(target_name, "wb") as f:
                f.write(Path(t.name).read_bytes())

    @staticmethod
    def dump_init_script(
        target_path: str,
        project_name: str,
        artifacts_path: str,
        is_mlflow_enabled: bool,
        user_init: str,
        user_post_init: str,
        commit_sha: str,
    ):  # pylint: disable=too-many-arguments
        loader = jinja2.FileSystemLoader(str(Path(__file__).parent))
        jinja_env = jinja2.Environment(
            autoescape=True, loader=loader, lstrip_blocks=True
        )
        template = jinja_env.get_template("dataproc_init_script_template.j2")

        template_stream = template.stream(
            gcs_path=artifacts_path,
            archive_name=f"{project_name}-{commit_sha}.tar.gz",
            project_name=project_name,
            is_mlflow_enabled=is_mlflow_enabled,
            user_init=user_init,
            user_post_init=user_post_init,
        )

        target_name = target_path + f"/{project_name}-{commit_sha}.sh"
        with fsspec.open(target_name, "wt") as f:
            template_stream.dump(f)

    @staticmethod
    def conditionally_handle_spark_artifacts(
        spark_template_streams: Dict[str, TemplateStream], ctx
    ):
        if spark_template_streams:
            target_path = ctx.obj[
                "context_helper"
            ].config.run_config.spark.artifacts_path
            CliHelper.dump_spark_artifacts(
                ctx, target_path, spark_template_streams
            )
