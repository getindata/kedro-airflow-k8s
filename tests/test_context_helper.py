import unittest
from unittest.mock import MagicMock, Mock, patch

from kedro.framework.session import KedroSession

from kedro_airflow_k8s.config import PluginConfig
from kedro_airflow_k8s.context_helper import ContextHelper


class TestContextHelper(unittest.TestCase):
    def test_project_name(self):
        metadata = Mock()
        metadata.project_name = "test_project"

        helper = ContextHelper(metadata, "test")
        assert helper.project_name == "test_project"

    def test_context(self):
        metadata = Mock()
        metadata.package_name = "test_package"
        kedro_session = MagicMock(KedroSession)
        kedro_session.load_context.return_value = "sample_context"

        with patch.object(KedroSession, "create") as create:
            create().load_context.return_value = "sample_context"
            helper = ContextHelper(metadata, "test")
            assert helper.context == "sample_context"
            create.assert_called_with("test_package", env="test")

    def test_config(self):
        metadata = Mock()
        metadata.package_name = "test_package"
        context = MagicMock()
        context.config_loader.return_value.get.return_value = ["one", "two"]
        with patch.object(KedroSession, "create", context) as create:
            create().load_context().config_loader.get.return_value = {}
            helper = ContextHelper(metadata, "test")
            assert helper.config == PluginConfig({})

    def test_pipeline_selection(self):
        metadata = Mock()
        metadata.package_name = "test_package"
        context = MagicMock()
        context.pipelines = {"feature_engineering": "pipeline_mock"}
        with patch.object(KedroSession, "create") as create:
            create().load_context.return_value = context
            helper = ContextHelper(metadata, "test", "feature_engineering")
            assert helper.pipeline == "pipeline_mock"
