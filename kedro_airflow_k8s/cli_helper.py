import fsspec
from pathlib import Path
from typing import List
from jinja2.environment import TemplateStream


class CliHelper:
    @staticmethod
    def dump_templates(dag_name: str, target_path: str, template_stream: TemplateStream,
                       spark_template_streams: List[TemplateStream]):
        dag_filename = f"{dag_name}.py"
        dag_target_path = str((Path(target_path) / dag_filename)) if '://' not in target_path else f"{target_path}/{dag_filename}"
        with fsspec.open(dag_target_path, "wt") as f:
            template_stream.dump(f)

        spark_id = 0
        for spark_template in spark_template_streams:
            spark_task_target_path = str(Path(target_path) / f"{dag_name}_spark_{spark_id}.py") \
                if '://' not in target_path else f"{target_path}/{dag_name}_spark_{spark_id}.py"
            with fsspec.open(str(spark_task_target_path), "wt") as f:
                spark_template.dump(f)
            spark_id = spark_id + 1
