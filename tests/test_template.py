import os
from contextlib import contextmanager
import unittest
from unittest.mock import Mock, MagicMock, patch

from kedro.framework.session.session import KedroSession

from kedro_airflow_k8s.context_helper import ContextHelper

from kedro_airflow_k8s.template import get_commit_sha


@contextmanager
def environment(env):
    original_environ = os.environ.copy()
    os.environ.update(env)
    yield
    os.environ = original_environ


class TestTemplate(unittest.TestCase):

    def test_get_commit_sha_from_env(self):
        with environment({
            "KEDRO_CONFIG_COMMIT_ID": "sha_from_commit"
        }):
            result = get_commit_sha(None)
            assert result == "sha_from_commit"

    def test_get_commit_sha_exception(self):
        context_helper = MagicMock()
        context_helper.session.store = {"git": {"commit_sha": "sha_from_session"}}
        result = get_commit_sha(context_helper)

        assert result == "sha_from_session"

    def test_sha_unknown(self):
        context_helper = MagicMock()
        context_helper.session.store = {"git": {}}
        result = get_commit_sha(context_helper)

        assert result == "UNKNOWN"
