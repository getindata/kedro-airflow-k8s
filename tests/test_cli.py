import unittest

from click.testing import CliRunner

from kedro_airflow_k8s.cli import hello


class TestPluginCLI(unittest.TestCase):
    def test_hello(self):
        runner = CliRunner()
        result = runner.invoke(hello, [])

        assert result.exit_code == 0
