from functools import lru_cache
from pathlib import Path

from kedro import __version__ as kedro_version
from semver import VersionInfo

from .config import PluginConfig

CONFIG_FILE_PATTERN = "airflow-k8s*"


class ContextHelper(object):
    def __init__(self, metadata, env, pipeline_name):
        self._metadata = metadata
        self._env = env
        self._session = None
        self._pipeline_name = pipeline_name

    @property
    def env(self):
        return self._env

    @property
    def project_name(self):
        return self._metadata.project_name

    @property
    def context(self):
        return self.session.load_context()

    @property
    def pipeline(self):
        return self.context.pipelines.get(self._pipeline_name)

    @property
    def pipeline_name(self):
        return self._pipeline_name

    @property
    def session(self):
        from kedro.framework.session import KedroSession

        if self._session is None:
            self._session = KedroSession.create(
                self._metadata.package_name, env=self._env
            )

        return self._session

    @property
    @lru_cache()
    def config(self) -> PluginConfig:
        raw = self.context.config_loader.get(CONFIG_FILE_PATTERN)
        return PluginConfig(raw)

    @property
    @lru_cache()
    def mlflow_config(self):
        return self.context.config_loader.get("mlflow*")

    @staticmethod
    def init(metadata, env, pipeline_name="__default__"):
        version = VersionInfo.parse(kedro_version)
        if version.match(">=0.17.0"):
            return ContextHelper(metadata, env, pipeline_name)
        else:
            return ContextHelper16(metadata, env, pipeline_name)


class ContextHelper16(ContextHelper):
    """Variant for compatibility with Kedro 1.6"""

    @property
    def project_name(self):
        return self.context.project_name

    @property
    def context(self):
        from kedro.framework.context import load_context

        return load_context(Path.cwd(), env=self._env)

    @property
    def session(self):
        from kedro.framework.session import KedroSession

        return KedroSession.create("", Path.cwd(), env=self._env)
