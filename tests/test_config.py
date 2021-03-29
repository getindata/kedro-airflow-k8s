import unittest

import yaml
from kedro.config.config import MissingConfigException

from kedro_airflow_k8s.config import PluginConfig

CONFIG_YAML = """
host: test.host.com
output: /data/ariflow/dags
run_config:
    image: test.image:1234
    image_pull_policy: Always
    namespace: airflow-test
    experiment_name: test-experiment
    run_name: test-experiment-branch
    cron_expression: "@hourly"
    description: "test pipeline"
    volume:
        storageclass: kms
        size: 3Gi
        access_modes: [ReadWriteMany]
        skip_init: True
        owner: 1000
"""


class TestPluginConfig(unittest.TestCase):
    def test_plugin_config(self):
        cfg = PluginConfig(yaml.safe_load(CONFIG_YAML))

        assert cfg.host == "test.host.com"
        assert cfg.output == "/data/ariflow/dags"
        assert cfg.run_config
        assert cfg.run_config.image == "test.image:1234"
        assert cfg.run_config.image_pull_policy == "Always"
        assert cfg.run_config.namespace == "airflow-test"
        assert cfg.run_config.experiment_name == "test-experiment"
        assert cfg.run_config.run_name == "test-experiment-branch"
        assert cfg.run_config.cron_expression == "@hourly"
        assert cfg.run_config.description == "test pipeline"
        assert cfg.run_config.volume
        assert cfg.run_config.volume.storageclass == "kms"
        assert cfg.run_config.volume.size == "3Gi"
        assert cfg.run_config.volume.access_modes == ["ReadWriteMany"]
        assert cfg.run_config.volume.skip_init is True
        assert cfg.run_config.volume.owner == 1000

    def test_defaults(self):
        cfg = PluginConfig({"run_config": {}})

        assert cfg.run_config
        assert cfg.run_config.image_pull_policy == "IfNotPresent"
        assert cfg.run_config.cron_expression == "@daily"
        assert cfg.run_config.description is None

        assert cfg.run_config.volume
        assert cfg.run_config.volume.storageclass is None
        assert cfg.run_config.volume.size == "1Gi"
        assert cfg.run_config.volume.access_modes == ["ReadWriteOnce"]
        assert cfg.run_config.volume.skip_init is False
        assert cfg.run_config.volume.owner == 0

    def test_run_name_is_experiment_name_by_default(self):
        cfg = PluginConfig(
            {"run_config": {"experiment_name": "test-experiment"}}
        )

        assert cfg.run_config
        assert cfg.run_config.experiment_name == "test-experiment"
        assert cfg.run_config.run_name == cfg.run_config.experiment_name

    def test_missing_required_config(self):
        cfg = PluginConfig({})
        with self.assertRaises(MissingConfigException):
            print(cfg.host)
