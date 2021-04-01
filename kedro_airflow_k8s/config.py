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


class RunConfig(Config):
    @property
    def image(self):
        return self._get_or_fail("image")

    @property
    def image_pull_policy(self):
        return self._get_or_default("image_pull_policy", "IfNotPresent")

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

    def _get_prefix(self):
        return "run_config."


class VolumeConfig(Config):
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
