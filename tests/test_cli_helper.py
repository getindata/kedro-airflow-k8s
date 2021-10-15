import tarfile
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from kedro_airflow_k8s.cli_helper import CliHelper


class TestCliHelper(unittest.TestCase):

    COMMIT_SHA = "abcdab"

    def test_dump_project_as_archive(self):
        with TemporaryDirectory(prefix="kedro-airflow-k8s-tests") as dir_name:
            (Path(dir_name) / "src").mkdir(parents=True, exist_ok=True)
            (Path(dir_name) / "src/file_1").write_text("12345")
            (Path(dir_name) / "src/file_2").write_text("67890")
            (Path(dir_name) / "src/some_dir").mkdir(
                parents=True, exist_ok=True
            )
            (Path(dir_name) / "src/some_dir/file_3").write_text("abcdef")

            CliHelper.dump_project_as_archive(
                dir_name + "/src",
                dir_name,
                "test-kedro-project",
                self.COMMIT_SHA,
            )

            assert (
                Path(dir_name) / "test-kedro-project-abcdab.tar.gz"
            ).exists()
            assert (
                Path(dir_name) / "test-kedro-project-abcdab.tar.gz"
            ).stat().st_size > 0

            with tarfile.open(
                str(Path(dir_name) / "test-kedro-project-abcdab.tar.gz"),
                mode="r:gz",
            ) as tf:
                assert "test-kedro-project/src/file_1" in tf.getnames()
                assert "test-kedro-project/src/file_2" in tf.getnames()
                assert (
                    "test-kedro-project/src/some_dir/file_3" in tf.getnames()
                )

    def test_dump_init_script(self):
        init_script = """echo "User injected init script"
touch "/home/kedro/hello_world\""""
        with TemporaryDirectory(prefix="kedro-airflow-k8s-tests") as dir_name:
            CliHelper.dump_init_script(
                dir_name,
                "test-kedro-project",
                gcs_path="gs://test-bucket/packages",
                is_mlflow_enabled=False,
                user_init=init_script,
                commit_sha=self.COMMIT_SHA,
            )

            assert (Path(dir_name) / "test-kedro-project-abcdab.sh").exists()

            full_script = (
                Path(dir_name) / "test-kedro-project-abcdab.sh"
            ).read_text()
            print(full_script)
            assert init_script in full_script
            assert (
                "gsutil cp gs://test-bucket/packages/test-kedro-project-abcdab.tar.gz"
                " /tmp/" in full_script
            )
            assert (
                "gsutil cp "
                "gs://test-bucket/packages/test-kedro-project-abcdab-py3-none-any.whl "
                "/tmp/" in full_script
            )
            assert """pip install -U /tmp/test-kedro-project""" in full_script
            assert (
                """tar zxvf /tmp/test-kedro-project-abcdab.tar.gz -C /opt"""
                in full_script
            )
            assert (
                """cd /opt/test-kedro-project && kedro install"""
                in full_script
            )
            assert """kedro mlflow init""" not in full_script
