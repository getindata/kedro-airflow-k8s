import os
from typing import Any, Dict, Iterable

from kedro.config import ConfigLoader, TemplatedConfigLoader
from kedro.framework.hooks import hook_impl


class EnvTemplatedConfigLoader(TemplatedConfigLoader):
    """Config loader that can substitute $(commit_id) and $(branch_name)
    placeholders with information taken from env variables."""

    VAR_PREFIX = "KEDRO_CONFIG_"
    # defaults provided so default variables ${commit_id|dirty} work for some entries
    ENV_DEFAULTS = {"commit_id": None, "branch_name": None}

    def __init__(
        self,
        conf_source: str,
        env: str = None,
        runtime_params: Dict[str, Any] = None,
        *,
        base_env: str = "base",
        default_run_env: str = "local"
    ):
        super().__init__(
            conf_source,
            env=env,
            runtime_params=runtime_params,
            globals_dict=self.read_env(),
            base_env=base_env,
            default_run_env=default_run_env,
        )

    def read_env(self) -> Dict:
        config = EnvTemplatedConfigLoader.ENV_DEFAULTS.copy()
        overrides = dict(
            [
                (k.replace(EnvTemplatedConfigLoader.VAR_PREFIX, "").lower(), v)
                for k, v in os.environ.copy().items()
                if k.startswith(EnvTemplatedConfigLoader.VAR_PREFIX)
            ]
        )
        config.update(**overrides)
        return config


class KedoAirflowK8SLoaderHook:
    @hook_impl
    def register_config_loader(
        self, conf_paths: Iterable[str]
    ) -> ConfigLoader:
        return EnvTemplatedConfigLoader(conf_paths)


env_templated_config_loader_hook = KedoAirflowK8SLoaderHook()
