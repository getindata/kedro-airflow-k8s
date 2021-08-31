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
    start_date: 20200102
    image_pull_secrets: pull1,pull2
    service_account_name: service_account
    volume:
        storageclass: kms
        size: 3Gi
        access_modes: [ReadWriteMany]
        skip_init: True
        owner: 1000
        disabled: True
    secrets:
        -   secret: "foo"
        -   deploy_type: "env"
            deploy_target: "SQL_CONN"
            secret: "airflow-secrets"
            key: "sql_alchemy_conn"
        -   deploy_type: "volume"
            deploy_target: "/etc/sql_conn"
            secret: "airflow-secrets"
            key: "sql_alchemy_con"
    macro_params: [ds,prev_ds]
    variables_params: [env]
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
    external_dependencies:
        - dag_id: test-parent-dag
        - dag_id: test-another-parent-dag
          task_id: test-parent-task
          timeout: 2
          check_existence: False
          execution_delta: 10
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
        assert cfg.run_config.start_date == "20200102"
        assert cfg.run_config.service_account_name == "service_account"
        assert cfg.run_config.image_pull_secrets == "pull1,pull2"
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
        assert resources.__default__.tolerations[0] == {
            "key": "group",
            "value": "data-processing",
            "effect": "NoExecute",
        }
        assert resources.__default__.annotations
        assert (
            resources.__default__.annotations["iam.amazonaws.com/role"]
            == "airflow"
        )
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
        dependencies = cfg.run_config.external_dependencies
        assert len(dependencies) == 2
        assert dependencies[0].dag_id == "test-parent-dag"
        assert dependencies[0].task_id is None
        assert dependencies[0].check_existence
        assert dependencies[0].timeout == 60 * 24
        assert dependencies[0].execution_delta == 0
        assert dependencies[1].dag_id == "test-another-parent-dag"
        assert dependencies[1].task_id == "test-parent-task"
        assert dependencies[1].check_existence is False
        assert dependencies[1].timeout == 2
        assert dependencies[1].execution_delta == 10

        assert cfg.run_config.secrets
        secrets = cfg.run_config.secrets
        assert len(secrets) == 3
        first_secret = secrets[0]
        assert first_secret.secret == "foo"
        assert first_secret.deploy_type == "env"
        second_secret = secrets[1]
        assert second_secret.secret == "airflow-secrets"
        assert second_secret.deploy_type == "env"
        assert second_secret.deploy_target == "SQL_CONN"
        assert second_secret.key == "sql_alchemy_conn"

        third_secret = secrets[2]
        assert third_secret.secret == "airflow-secrets"
        assert third_secret.deploy_type == "volume"
        assert third_secret.deploy_target == "/etc/sql_conn"
        assert third_secret.key == "sql_alchemy_con"

        assert cfg.run_config.macro_params == ["ds", "prev_ds"]
        assert cfg.run_config.variables_params == ["env"]

    def test_defaults(self):
        cfg = PluginConfig({"run_config": {}})

        assert cfg.run_config
        assert cfg.run_config.image_pull_policy == "IfNotPresent"
        assert cfg.run_config.startup_timeout == 600
        assert cfg.run_config.cron_expression == "@daily"
        assert cfg.run_config.description is None
        assert cfg.run_config.start_date is None
        assert cfg.run_config.auth_config.type == "Null"

        assert cfg.run_config.volume
        assert cfg.run_config.volume.disabled is False
        assert cfg.run_config.volume.storageclass is None
        assert cfg.run_config.volume.size == "1Gi"
        assert cfg.run_config.volume.access_modes == ["ReadWriteOnce"]
        assert cfg.run_config.volume.skip_init is False
        assert cfg.run_config.volume.owner == 0
        assert cfg.run_config.resources

        assert not cfg.run_config.external_dependencies

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
