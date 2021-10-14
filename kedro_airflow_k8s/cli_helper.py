import sys
import tarfile
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict

import fsspec
import jinja2
from jinja2.environment import TemplateStream
from kedro.framework.cli.utils import call

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

        if spark_config.user_init_path:
            user_init = (
                Path(source_dir) / spark_config.user_init_path
            ).read_text()
        else:
            user_init = ""

        CliHelper.dump_spark_templates(
            target_path, project_name, commit_sha, spark_template_streams
        )
        CliHelper.dump_project_as_package(
            source_dir, target_path, project_name, commit_sha
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
            commit_sha,
        )

    @staticmethod
    def spawn_package(source_path: str):
        call(
            [sys.executable, "setup.py", "clean", "--all", "bdist_wheel"],
            cwd=str(source_path),
        )

    @staticmethod
    def dump_project_as_package(
        source_path: str, target_path: str, project_name: str, commit_sha: str
    ):
        CliHelper.spawn_package(source_path)

        wheel_location = Path(source_path) / "dist"
        wheels = [
            whl
            for whl in wheel_location.iterdir()
            if whl.name.endswith(".whl")
        ]
        wheels.sort(key=lambda wh: wh.stat().st_mtime, reverse=True)

        target_name = (
            target_path + f"/{project_name}-{commit_sha}-py3-none-any.whl"
        )
        with fsspec.open(target_name, "wb") as f:
            f.write(wheels[0].read_bytes())

    @staticmethod
    def dump_project_as_archive(
        source_path: str, target_path: str, project_name: str, commit_sha: str
    ):
        with NamedTemporaryFile(
            prefix="kedro-airflow-k8s-project", suffix=".tar.gz"
        ) as t:
            with tarfile.open(t.name, mode="w:gz") as tf:
                tf.add(source_path, arcname=project_name)

            target_name = target_path + f"/{project_name}-{commit_sha}.tar.gz"
            with fsspec.open(target_name, "wb") as f:
                f.write(Path(t.name).read_bytes())

    @staticmethod
    def dump_init_script(
        target_path: str,
        project_name: str,
        gcs_path: str,
        is_mlflow_enabled: bool,
        user_init: str,
        commit_sha: str,
    ):
        loader = jinja2.FileSystemLoader(str(Path(__file__).parent))
        jinja_env = jinja2.Environment(
            autoescape=True, loader=loader, lstrip_blocks=True
        )
        template = jinja_env.get_template("dataproc_init_script_template.j2")

        template_stream = template.stream(
            gcs_path=gcs_path,
            archive_name=f"{project_name}-{commit_sha}.tar.gz",
            project_name=project_name,
            package_name=f"{project_name}-{commit_sha}-py3-none-any.whl",
            is_mlflow_enabled=is_mlflow_enabled,
            user_init=user_init,
        )

        target_name = target_path + f"/{project_name}-{commit_sha}.sh"
        with fsspec.open(target_name, "wt") as f:
            template_stream.dump(f)
