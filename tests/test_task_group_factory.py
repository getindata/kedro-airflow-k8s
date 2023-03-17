import unittest

import pyspark
from kedro.extras.datasets.pandas import CSVDataSet
from kedro.extras.datasets.spark import SparkDataSet
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline, node

from kedro_airflow_k8s.task_group import TaskGroupFactory


class TestTaskGroupFactory(unittest.TestCase):
    def test_empty_pipeline(self):
        pipeline = Pipeline(nodes=[])

        data_catalog = DataCatalog(data_sets={})

        task_groups = TaskGroupFactory().create(pipeline, data_catalog)

        assert len(task_groups) == 0

    def test_create_dag_only_spark(self):
        pipeline = Pipeline(
            nodes=[
                node(
                    self,
                    inputs=["sparkframe1"],
                    outputs=["sparkframe2"],
                    name="node1",
                ),
                node(
                    self,
                    inputs=["sparkframe2"],
                    outputs=["sparkframe3"],
                    name="node2",
                ),
            ]
        )

        data_catalog = DataCatalog(
            data_sets={
                "sparkframe1": SparkDataSet("/tmp/dummy1"),
                "sparkframe2": SparkDataSet("/tmp/dummy2"),
                "sparkframe3": SparkDataSet("/tmp/dummy3"),
            }
        )

        task_groups = TaskGroupFactory().create(pipeline, data_catalog)

        assert len(task_groups) == 1
        assert task_groups[0].group_type == "pyspark"
        assert len(task_groups[0].task_group) == 2
        assert task_groups[0].name.startswith("pyspark_")
        assert len(task_groups[0].children) == 0

    def test_create_dag_only_default(self):
        pipeline = Pipeline(
            nodes=[
                node(
                    self,
                    inputs=["pandasframe1"],
                    outputs=["pandasframe2"],
                    name="node1",
                ),
                node(
                    self,
                    inputs=["pandasframe2"],
                    outputs=["pandasframe3"],
                    name="node2",
                ),
            ]
        )

        data_catalog = DataCatalog(
            data_sets={
                "pandasframe1": CSVDataSet("/tmp/dummy1"),
                "pandasframe2": CSVDataSet("/tmp/dummy2"),
                "pandasframe3": CSVDataSet("/tmp/dummy3"),
            }
        )

        task_groups = TaskGroupFactory().create(pipeline, data_catalog)
        task_groups.sort(key=lambda x: x.name)

        assert len(task_groups) == 2
        assert task_groups[0].group_type == "default"
        assert len(task_groups[0].task_group) == 1
        assert task_groups[0].name.startswith("node1")
        assert len(task_groups[0].children) == 1
        assert task_groups[1] in task_groups[0].children
        assert task_groups[1].group_type == "default"
        assert len(task_groups[1].task_group) == 1
        assert task_groups[1].name.startswith("node2")
        assert len(task_groups[1].children) == 0

    def test_create_dag_intermediate_spark_frames(self):
        def node2(
            input_param: pyspark.sql.dataframe.DataFrame,
        ) -> pyspark.sql.dataframe.DataFrame:
            pass

        pipeline = Pipeline(
            nodes=[
                node(
                    self,
                    inputs=["sparkframe1"],
                    outputs=["sparkframe2"],
                    name="node1",
                ),
                node(
                    node2,
                    inputs=["sparkframe2"],
                    outputs=["sparkframe3"],
                    name="node2",
                ),
                node(
                    self,
                    inputs=["sparkframe3"],
                    outputs=["sparkframe4"],
                    name="node3",
                ),
            ]
        )

        data_catalog = DataCatalog(
            data_sets={
                "sparkframe1": SparkDataSet("/tmp/dummy1"),
                "sparkframe4": SparkDataSet("/tmp/dummy4"),
            }
        )

        task_groups = TaskGroupFactory().create(pipeline, data_catalog)

        assert len(task_groups) == 1
        assert task_groups[0].group_type == "pyspark"
        assert len(task_groups[0].task_group) == 3
        assert task_groups[0].name.startswith("pyspark_")

    def test_create_dag_mixed_datasets(self):
        pipeline = Pipeline(
            nodes=[
                node(
                    self,
                    inputs=["sparkframe1"],
                    outputs=["sparkframe2"],
                    name="node1",
                ),
                node(
                    self,
                    inputs=["sparkframe2"],
                    outputs=["sparkframe3"],
                    name="node2",
                ),
                node(
                    self,
                    inputs=["pandasframe1"],
                    outputs=["pandasframe2"],
                    name="node3",
                ),
            ]
        )

        data_catalog = DataCatalog(
            data_sets={
                "sparkframe1": SparkDataSet("/tmp/dummy1"),
                "sparkframe3": SparkDataSet("/tmp/dummy3"),
                "pandasframe1": CSVDataSet("/tmp/dummy4"),
                "pandasframe2": CSVDataSet("/tmp/dummy5"),
            }
        )

        task_groups = TaskGroupFactory().create(pipeline, data_catalog)
        task_groups.sort(reverse=True, key=lambda x: x.name)

        assert len(task_groups) == 2
        assert task_groups[0].group_type == "pyspark"
        assert len(task_groups[0].task_group) == 2
        assert len(task_groups[0].children) == 0
        assert task_groups[0].name.startswith("pyspark_")
        assert task_groups[1].group_type == "default"
        assert len(task_groups[1].task_group) == 1
        assert task_groups[1].name == "node3"
        assert len(task_groups[1].children) == 0

    def test_create_dag_interleaved_datasets(self):
        pipeline = Pipeline(
            nodes=[
                node(
                    self,
                    inputs=["pandasframe0"],
                    outputs=["sparkframe1"],
                    name="node1",
                ),
                node(
                    self,
                    inputs=["sparkframe1", "pandasframe2"],
                    outputs=["sparkframe2"],
                    name="node2",
                ),
                node(
                    self,
                    inputs=["pandasframe1"],
                    outputs=["pandasframe2"],
                    name="node3",
                ),
            ]
        )

        data_catalog = DataCatalog(
            data_sets={
                "sparkframe1": SparkDataSet("/tmp/dummy1"),
                "sparkframe2": SparkDataSet("/tmp/dummy2"),
                "pandasframe0": CSVDataSet("/tmp/dummy3"),
                "pandasframe1": CSVDataSet("/tmp/dummy4"),
                "pandasframe2": CSVDataSet("/tmp/dummy5"),
            }
        )

        task_groups = TaskGroupFactory().create(pipeline, data_catalog)
        task_groups.sort(reverse=True, key=lambda x: x.name)

        assert len(task_groups) == 2
        assert task_groups[0].group_type == "pyspark"
        assert len(task_groups[0].task_group) == 2
        assert task_groups[0] in task_groups[1].children
        assert task_groups[0].name.startswith("pyspark_")
        assert task_groups[1].group_type == "default"
        assert len(task_groups[1].task_group) == 1
        assert task_groups[1].name == "node3"

    def test_create_dag_multiple_spark_subdags(self):
        pipeline = Pipeline(
            nodes=[
                node(
                    self,
                    inputs=["sparkframe1"],
                    outputs=["sparkframe2"],
                    name="node1",
                ),
                node(
                    self,
                    inputs=["sparkframe2"],
                    outputs=["pandasframe1"],
                    name="node2",
                ),
                node(
                    self,
                    inputs=["pandasframe1"],
                    outputs=["pandasframe2"],
                    name="node3",
                ),
                node(
                    self,
                    inputs=["pandasframe2"],
                    outputs=["sparkframe3"],
                    name="node4",
                ),
                node(
                    self,
                    inputs=["sparkframe3"],
                    outputs=["sparkframe4"],
                    name="node5",
                ),
            ]
        )

        data_catalog = DataCatalog(
            data_sets={
                "sparkframe1": SparkDataSet("/tmp/dummy1"),
                "sparkframe2": SparkDataSet("/tmp/dummy2"),
                "pandasframe1": CSVDataSet("/tmp/dummy3"),
                "pandasframe2": CSVDataSet("/tmp/dummy4"),
                "pandasframe3": CSVDataSet("/tmp/dummy5"),
                "sparkframe3": SparkDataSet("/tmp/dummy6"),
                "sparkframe4": SparkDataSet("/tmp/dummy7"),
            }
        )

        task_groups = TaskGroupFactory().create(pipeline, data_catalog)
        task_groups.sort(key=lambda x: x.name)

        assert len(task_groups) == 3
        assert task_groups[0].group_type == "default"
        assert len(task_groups[0].task_group) == 1
        assert task_groups[0].name == "node3"
        assert len(task_groups[0].children) == 1

        assert task_groups[1].group_type == "pyspark"
        assert len(task_groups[1].task_group) == 2
        assert task_groups[1].name.startswith("pyspark_")
        assert task_groups[2].group_type == "pyspark"
        assert len(task_groups[2].task_group) == 2
        assert task_groups[2].name.startswith("pyspark_")

        assert task_groups[2].name != task_groups[1].name

    def test_create_dag_from_tags(self):
        pipeline = Pipeline(
            nodes=[
                node(
                    self,
                    inputs=["pandasframe1"],
                    outputs=["pandasframe2"],
                    name="node3",
                    tags=["kedro-airflow-k8s:group:pyspark"],
                ),
                node(
                    self,
                    inputs=["pandasframe2"],
                    outputs=["pandasframe3"],
                    name="node4",
                ),
            ]
        )

        data_catalog = DataCatalog(
            data_sets={
                "pandasframe1": CSVDataSet("/tmp/dummy1"),
                "pandasframe3": CSVDataSet("/tmp/dummy3"),
            }
        )

        task_groups = TaskGroupFactory().create(pipeline, data_catalog)
        task_groups.sort(reverse=True, key=lambda x: x.name)

        assert len(task_groups) == 2
        assert task_groups[0].group_type == "pyspark"
        assert len(task_groups[0].task_group) == 1
        assert task_groups[1] in task_groups[0].children
        assert task_groups[0].name.startswith("pyspark_")
        assert task_groups[1].group_type == "default"
        assert len(task_groups[1].task_group) == 1
        assert task_groups[1].name == "node4"

    def test_non_pyspark_dependency(self):
        pipeline = Pipeline(
            nodes=[
                node(
                    self,
                    inputs=["sparkframe1"],
                    outputs=["sparkframe2", "pandasframe1"],
                    name="node1",
                ),
                node(
                    self,
                    inputs=["sparkframe2", "pandasframe2"],
                    outputs=["sparkframe3"],
                    name="node2",
                ),
                node(
                    self,
                    inputs=["pandasframe1"],
                    outputs=["pandasframe2"],
                    name="node3",
                ),
            ]
        )

        data_catalog = DataCatalog(
            data_sets={
                "sparkframe1": SparkDataSet("/tmp/dummy1"),
                "sparkframe2": SparkDataSet("/tmp/dummy2"),
                "pandasframe1": CSVDataSet("/tmp/dummy3"),
                "pandasframe2": CSVDataSet("/tmp/dummy4"),
                "sparkframe3": SparkDataSet("/tmp/dummy6"),
            }
        )

        task_groups = TaskGroupFactory().create(pipeline, data_catalog)

        assert len(task_groups) == 1
        assert task_groups[0].group_type == "pyspark"
        assert len(task_groups[0].task_group) == 3
        assert task_groups[0].name.startswith("pyspark_")

    def test_create_dag_only_spark_ungrouped(self):
        pipeline = Pipeline(
            nodes=[
                node(
                    self,
                    inputs=["sparkframe1"],
                    outputs=["sparkframe2"],
                    name="node1",
                ),
                node(
                    self,
                    inputs=["sparkframe2"],
                    outputs=["sparkframe3"],
                    name="node2",
                ),
                node(
                    self,
                    inputs=None,
                    outputs=["pandasframe1"],
                    name="node3",
                ),
            ]
        )

        data_catalog = DataCatalog(
            data_sets={
                "sparkframe1": SparkDataSet("/tmp/dummy1"),
                "sparkframe2": SparkDataSet("/tmp/dummy2"),
                "sparkframe3": SparkDataSet("/tmp/dummy6"),
                "pandasframe1": CSVDataSet("/tmp/dummy3"),
            }
        )

        task_groups = TaskGroupFactory().create_ungrouped(
            pipeline, data_catalog
        )
        task_groups.sort(key=lambda x: x.name)

        assert len(task_groups) == 3
        assert task_groups[0].group_type == "pyspark"
        assert len(task_groups[0].task_group) == 1
        assert task_groups[0].name.startswith("node1")
        assert len(task_groups[0].children) == 1
        assert task_groups[1] in task_groups[0].children
        assert task_groups[1].group_type == "pyspark"
        assert len(task_groups[1].task_group) == 1
        assert task_groups[1].name.startswith("node2")
        assert len(task_groups[1].children) == 0
        assert task_groups[2].group_type == "default"
