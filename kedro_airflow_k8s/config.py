from kedro.config import MissingConfigException

DEFAULT_CONFIG_TEMPLATE = """
image: {image}
imagePullPolicy: Always
namespace: {namespace}
accessMode: {accessMode}
requestStorage: {requestStorage}
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


class PluginConfig(Config):
    @property
    def image(self):
        return self._get_or_fail("image")

    @property
    def image_pull_policy(self):
        return self._get_or_default("imagePullPolicy", "Always")

    @property
    def namespace(self):
        return self._get_or_fail("namespace")

    @property
    def access_mode(self):
        return self._get_or_default("accessMode", "ReadWriteOnce")

    @property
    def request_storage(self):
        return self._get_or_default("requestStorage", "1Gi")

    @staticmethod
    def sample_config(**kwargs):
        return DEFAULT_CONFIG_TEMPLATE.format(**kwargs)
