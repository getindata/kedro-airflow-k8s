import os

from kedro.config import MissingConfigException

DEFAULT_CONFIG_TEMPLATE = """
# Base url of the Apache Airflow, should include the schema (http/https)
host: {url}

# Directory from where Apache Airflow is reading DAGs definitions
output: {output}

# Configuration used to run the pipeline
run_config:

    # Name of the image to run as the pipeline steps
    image: {image}

    # Pull policy to be used for the steps. Use Always if you push the images
    # on the same tag, or Never if you use only local images
    image_pull_policy: IfNotPresent

    # Pod startup timeout in seconds
    startup_timeout: 600

    # Namespace for Airflow pods to be created
    namespace: airflow

    # Name of the Airflow experiment to be created
    experiment_name: {project}

    # Name of the dag as it's presented in Airflow
    run_name: {run_name}

    # Apache Airflow cron expression for scheduled runs
    cron_expression: "@daily"

    # Optional pipeline description
    #description: "Very Important Pipeline"

    # Optional volume specification
    volume:
        # Storage class - use null (or no value) to use the default storage
        # class deployed on the Kubernetes cluster
        storageclass: # default
        # The size of the volume that is created. Applicable for some storage
        # classes
        size: 1Gi
        # Access mode of the volume used to exchange data. ReadWriteMany is
        # preferred, but it is not supported on some environements (like GKE)
        # Default value: ReadWriteOnce
        #access_modes: [ReadWriteMany]
        # Flag indicating if the data-volume-init step (copying raw data to the
        # fresh volume) should be skipped
        skip_init: False
        # Allows to specify fsGroup executing pipelines within containers
        # Default: root user group (to avoid issues with volumes in GKE)
        owner: 0
        # Tells if volume should not be used at all, false by default
        disabled: False

    # List of optional secrets specification
    secrets:
            # deploy_type: The type of secret deploy in Kubernetes, either `env` or
            `volume`
        -   deploy_type: "env"
            # deploy_target: (Optional) The environment variable when `deploy_type` `env`
            # or file path when `deploy_type` `volume` where expose secret. If `key` is
            # not provided deploy target should be None.
            deploy_target: "SQL_CONN"
            # secret: Name of the secrets object in Kubernetes
            secret: "airflow-secrets"
            # key: (Optional) Key of the secret within the Kubernetes Secret if not
            # provided in `deploy_type` `env` it will mount all secrets in object
            key: "sql_alchemy_conn"

    # Apache Airflow macros to be exposed for the parameters
    # List of macros can be found here:
    # https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
    macro_params: [ds, prev_ds]

    # Apache Airflow variables to be exposed for the parameters
    variables_params: [env]

    # Optional resources specification
    #resources:
        # Default configuration used by all nodes that do not declare the
        # resource configuration. It's optional. If node does not declare the resource
        # configuration, __default__ is assigned by default, otherwise cluster defaults
        # will be used.
        #__default__:
            # Optional labels to be put into pod node selector
            #node_selectors:
              #Labels are user provided key value pairs
              #node_pool_label/k8s.io: example_value
            # Optional labels to apply on pods
            #labels:
              #running: airflow
            # Optional annotations to apply on pods
            #annotations:
              #iam.amazonaws.com/role: airflow
            # Optional list of kubernetes tolerations
            #tolerations:
                #- key: "group"
                  #value: "data-processing"
                  #effect: "NoExecute"
                #- key: "group"
                  #operator: "Equal",
                  #value: "data-processing",
                  #effect: "NoSchedule"
            #requests:
                #Optional amount of cpu resources requested from k8s
                #cpu: "1"
                #Optional amount of memory resource requested from k8s
                #memory: "1Gi"
            #limits:
                #Optional amount of cpu resources limit on k8s
                #cpu: "1"
                #Optional amount of memory resource limit on k8s
                #memory: "1Gi"
        # Other arbitrary configurations to use
        #custom_resource_config_name:
            # Optional labels to be put into pod node selector
            #labels:
                #Labels are user provided key value pairs
                #label_key: label_value
            #requests:
                #Optional amount of cpu resources requested from k8s
                #cpu: "1"
                #Optional amount of memory resource requested from k8s
                #memory: "1Gi"
            #limits:
                #Optional amount of cpu resources limit on k8s
                #cpu: "1"
                #Optional amount of memory resource limit on k8s
                #memory: "1Gi"

    # Optional external dependencies configuration
    #external_dependencies:
        # Can just select dag as a whole
        #- dag_id: upstream-dag
        # or detailed
        #- dag_id: another-upstream-dag
        # with specific task to wait on
        #  task_id: with-precise-task
        # Maximum time (minute) to wait for the external dag to finish before this
        # pipeline fails, the default is 1440 == 1 day
        #  timeout: 2
        # Checks if the external dag exists before waiting for it to finish. If it
        # does not exists, fail this pipeline. By default is set to true.
        #  check_existence: False
        # Time difference with the previous execution to look at (minutes),
        # the default is 0 meaning no difference
        #  execution_delta: 10
"""


class Config(object):
    def __init__(self, raw):
        self._raw = raw

    def _get_or_default(self, prop, default):
        return self._raw.get(prop, default)

    def _get_or_fail(self, prop):
        if prop in self._raw.keys():
            return self._raw[prop]
        else:
            raise MissingConfigException(
                f"Missing required configuration: '{self._get_prefix()}{prop}'."
            )

    def _get_prefix(self):
        return ""

    def __eq__(self, other):
        return self._raw == other._raw


class ResourceNodeConfig(Config):
    @property
    def cpu(self):
        return self._get_or_default("cpu", None)

    @property
    def memory(self):
        return self._get_or_default("memory", None)


class ResourceConfig(Config):
    @property
    def annotations(self):
        return self._get_or_default("annotations", {})

    @property
    def tolerations(self):
        return self._get_or_default("tolerations", {})

    @property
    def node_selectors(self):
        return self._get_or_default("node_selectors", {})

    @property
    def labels(self):
        return self._get_or_default("labels", {})

    @property
    def requests(self):
        return ResourceNodeConfig(self._get_or_default("requests", {}))

    @property
    def limits(self):
        return ResourceNodeConfig(self._get_or_default("limits", {}))


class ResourcesConfig(Config):
    def __getattr__(self, item):
        return self[item]

    def __getitem__(self, item):
        return ResourceConfig(self._get_or_default(item, {}))


class ExternalDependenyConfig(Config):
    @property
    def dag_id(self):
        return self._get_or_fail("dag_id")

    @property
    def task_id(self):
        return self._get_or_default("task_id", None)

    @property
    def check_existence(self):
        return self._get_or_default("check_existence", True)

    @property
    def execution_delta(self):
        return self._get_or_default("execution_delta", 0)

    @property
    def timeout(self):
        return self._get_or_default("timeout", 60 * 24)


class RunConfig(Config):
    @property
    def image(self):
        return self._get_or_fail("image")

    @property
    def image_pull_policy(self):
        return self._get_or_default("image_pull_policy", "IfNotPresent")

    @property
    def startup_timeout(self):
        return self._get_or_default("startup_timeout", 600)

    @property
    def namespace(self):
        return self._get_or_fail("namespace")

    @property
    def experiment_name(self):
        return self._get_or_fail("experiment_name")

    @property
    def run_name(self):
        return self._get_or_default("run_name", self.experiment_name)

    @property
    def cron_expression(self):
        return self._get_or_default("cron_expression", "@daily")

    @property
    def description(self):
        return self._get_or_default("description", None)

    @property
    def volume(self):
        cfg = self._get_or_default("volume", {})
        return VolumeConfig(cfg)

    @property
    def secrets(self):
        cfg = self._get_or_default("secrets", [])
        return [SecretConfig(secret) for secret in cfg]

    @property
    def macro_params(self):
        return self._get_or_default("macro_params", [])

    @property
    def variables_params(self):
        return self._get_or_default("variables_params", [])

    @property
    def resources(self):
        cfg = self._get_or_default("resources", {})
        return ResourcesConfig(cfg)

    @property
    def external_dependencies(self):
        deps = self._get_or_default("external_dependencies", [])
        return [ExternalDependenyConfig(cfg) for cfg in deps]

    def _get_prefix(self):
        return "run_config."


class VolumeConfig(Config):
    @property
    def disabled(self):
        return self._get_or_default("disabled", False)

    @property
    def storageclass(self):
        return self._get_or_default("storageclass", None)

    @property
    def size(self):
        return self._get_or_default("size", "1Gi")

    @property
    def access_modes(self):
        return self._get_or_default("access_modes", ["ReadWriteOnce"])

    @property
    def skip_init(self):
        return self._get_or_default("skip_init", False)

    @property
    def owner(self):
        return self._get_or_default("owner", 0)

    def _get_prefix(self):
        return "run_config.volume."


class SecretConfig(Config):
    @property
    def deploy_type(self):
        return self._get_or_default("deploy_type", "env")

    @property
    def deploy_target(self):
        return self._get_or_default("deploy_target", None)

    @property
    def secret(self):
        return self._get_or_fail("secret")

    @property
    def key(self):
        return self._get_or_default("key", None)

    def _get_prefix(self):
        return "run_config.secrets."


class PluginConfig(Config):
    @property
    def host(self):
        return self._get_or_fail("host")

    @property
    def output(self):
        return self._get_or_fail("output")

    @property
    def run_config(self):
        cfg = self._get_or_default("run_config", {})
        return RunConfig(cfg)

    @staticmethod
    def sample_config(**kwargs):
        return DEFAULT_CONFIG_TEMPLATE.format(**kwargs)

    @staticmethod
    def initialize_github_actions(project_name, where, templates_dir):
        os.makedirs(where / ".github/workflows", exist_ok=True)
        for template in ["on-merge-to-master.yml", "on-push.yml"]:
            file_path = where / ".github/workflows" / template
            template_file = templates_dir / f"github-{template}"
            with open(template_file, "r") as tfile, open(file_path, "w") as f:
                f.write(tfile.read().format(project_name=project_name))
