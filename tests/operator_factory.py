from kedro_airflow_k8s.template_helper import SparkOperatorFactoryBase


class TestOperatorFactory(SparkOperatorFactoryBase):
    def submit_operator(self, project_name, node_name, config):
        return f"""SubmitOperator("{project_name}", "{node_name}")"""

    @property
    def imports_list(self):
        return [("test", "SubmitOperator"), ("test", "CreateClusterOperator")]
