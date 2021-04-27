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
    startup_timeout: 120
    volume:
        storageclass: kms
        size: 3Gi
        access_modes: [ReadWriteMany]
        skip_init: True
        owner: 1000
        disabled: True
    resources:
        __default__:
            node_selectors:
                size: mammoth
            labels:
                running: airflow
            tolerations:
                - key: "group"
                  value: "data-processing"
                  effect: "NoExecute"
            annotations:
                iam.amazonaws.com/role: airflow
            requests:
                cpu: "1"
                memory: "1Gi"
            limits:
                cpu: "2"
                memory: "2Gi"
        custom_resource_config_name:
            requests:
                cpu: "8"
"""


class TestPluginConfig(unittest.TestCase):
    def test_plugin_config(self):
        cfg = PluginConfig(yaml.safe_load(CONFIG_YAML))

        assert cfg.host == "test.host.com"
        assert cfg.output == "/data/ariflow/dags"
        assert cfg.run_config
        assert cfg.run_config.image == "test.image:1234"
        assert cfg.run_config.image_pull_policy == "Always"
        assert cfg.run_config.startup_timeout == 120
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
        assert cfg.run_config.volume.disabled is True
        assert cfg.run_config.resources
        resources = cfg.run_config.resources
        assert resources.__default__
        assert resources.__default__.node_selectors
        assert resources.__default__.node_selectors["size"] == "mammoth"
        assert resources.__default__.labels
        assert resources.__default__.labels["running"] == "airflow"
        assert resources.__default__.tolerations
        assert resources.__default__.tolerations[0] == {"key": "group","value": "data-processing","effect": "NoExecute",}
        assert resources.__default__.annotations
        assert resources.__default__.annotations['iam.amazonaws.com/role'] == "airflow"
        assert resources.__default__.requests
        assert resources.__default__.requests.cpu == "1"
        assert resources.__default__.requests.memory == "1Gi"
        assert resources.__default__.limits
        assert resources.__default__.limits.cpu == "2"
        assert resources.__default__.limits.memory == "2Gi"
        assert resources.custom_resource_config_name
        assert not resources.custom_resource_config_name.node_selectors
        assert not resources.custom_resource_config_name.labels
        assert not resources.custom_resource_config_name.tolerations
        assert resources.custom_resource_config_name.requests
        assert resources.custom_resource_config_name.requests.cpu == "8"
        assert not resources.custom_resource_config_name.requests.memory
        assert not resources.custom_resource_config_name.limits.memory
        assert not resources.custom_resource_config_name.limits.cpu

    def test_defaults(self):
        cfg = PluginConfig({"run_config": {}})

        assert cfg.run_config
        assert cfg.run_config.image_pull_policy == "IfNotPresent"
        assert cfg.run_config.startup_timeout == 600
        assert cfg.run_config.cron_expression == "@daily"
        assert cfg.run_config.description is None

        assert cfg.run_config.volume
        assert cfg.run_config.volume.disabled is False
        assert cfg.run_config.volume.storageclass is None
        assert cfg.run_config.volume.size == "1Gi"
        assert cfg.run_config.volume.access_modes == ["ReadWriteOnce"]
        assert cfg.run_config.volume.skip_init is False
        assert cfg.run_config.volume.owner == 0
        assert cfg.run_config.resources

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
