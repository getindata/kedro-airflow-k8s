import abc
from typing import Dict

from kedro_airflow_k8s.config import PluginConfig


class SparkOperatorFactoryBase(abc.ABC):
    @abc.abstractmethod
    def submit_operator(
        self,
        project_name: str,
        node_name: str,
        config: PluginConfig,
        main_python_file_path: str,
    ) -> str:
        pass

    @abc.abstractmethod
    def create_cluster_operator(
        self,
        project_name: str,
        config: PluginConfig,
        init_script_path: str,
        cluster_config: Dict,
    ) -> str:
        pass

    @abc.abstractmethod
    def delete_cluster_operator(
        self, project_name: str, config: PluginConfig
    ) -> str:
        pass

    @property
    @abc.abstractmethod
    def imports_statement(self) -> str:
        pass
