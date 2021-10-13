import abc

from kedro_airflow_k8s.config import PluginConfig


class SparkOperatorFactoryBase(abc.ABC):
    @abc.abstractmethod
    def submit_operator(
        self, project_name: str, node_name: str, config: PluginConfig
    ) -> str:
        pass

    @property
    @abc.abstractmethod
    def imports_list(self):
        pass
