from typing import Dict

from kedro_airflow_k8s.config import PluginConfig
from kedro_airflow_k8s.template_helper import SparkOperatorFactoryBase


class TestOperatorFactory(SparkOperatorFactoryBase):
    def create_cluster_operator(
        self,
        project_name: str,
        config: PluginConfig,
        init_script_path: str,
        cluster_config: Dict,
    ) -> str:
        return f"""CreateClusterOperator("{project_name}")"""

    def delete_cluster_operator(
        self, project_name: str, config: PluginConfig
    ) -> str:
        return f"""DeleteClusterOperator("{project_name}")"""

    @property
    def imports_statement(self) -> str:
        return (
            "from test import CreateClusterOperator, DeleteClusterOperator,"
            " SubmitOperator"
        )

    def submit_operator(
        self, project_name, node, pipeline, config, main_python_file_path: str
    ):
        return f"""SubmitOperator("{project_name}", "{node.name}")"""
