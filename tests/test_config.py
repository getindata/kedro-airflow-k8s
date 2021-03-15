import unittest

import yaml
from kedro.config.config import MissingConfigException

from kedro_airflow_k8s.config import PluginConfig

CONFIG_YAML = """
image: "gcr.io/project-image/test"
image_pull_policy: "Always"
requestStorage: 3Gi
namespace: "airflow"
"""


class TestPluginConfig(unittest.TestCase):
    def test_plugin_config(self):

        cfg = PluginConfig(yaml.safe_load(CONFIG_YAML))

        assert cfg.image == "gcr.io/project-image/test"
        assert cfg.image_pull_policy == "Always"
        assert cfg.request_storage == "3Gi"
        assert cfg.namespace == "airflow"

    def test_defaults(self):
        cfg = PluginConfig({})
        assert cfg.image_pull_policy == "Always"
        assert cfg.request_storage == "1Gi"

    def test_missing_required_config(self):
        cfg = PluginConfig({})
        with self.assertRaises(MissingConfigException):
            print(cfg.image)
