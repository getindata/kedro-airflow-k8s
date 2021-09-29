from inspect import signature
from typing import List

import logging
from kedro.pipeline.node import Node
from kedro.pipeline.pipeline import Pipeline
from kedro.io.data_catalog import DataCatalog


KEDRO_SPARK_DATASET_TYPES = ['SparkDataSet', 'SparkHiveDataSet', 'SparkJDBCDataSet']


class TaskGroup:
    def __init__(self, name: str, task_group: List[Node], group_type: str):
        self._name = name
        self._task_group = task_group
        self._group_type = group_type  # could be default, spark, etc

    @property
    def name(self):
        return self._name

    @property
    def task_group(self):
        return self._task_group

    @property
    def group_type(self):
        return self._group_type

    def append_task(self, node: Node):
        self._task_group.append(node)


class TaskGroupFactory:
    @staticmethod
    def create(pipeline: Pipeline, catalog: DataCatalog) -> List[TaskGroup]:
        task_groups = []
        pyspark_task_group_id = -1
        task_group_id = -1
        for node in pipeline.nodes: # FIXME: change naive approch (TBD)
            for node_input in node.inputs:
                # find first node in a subdag that uses any kind of Kedro SparkDataset
                if node_input in catalog._data_sets.keys() and type(
                        catalog._data_sets[node_input]).__name__ in KEDRO_SPARK_DATASET_TYPES:
                    pyspark_task_group_id = pyspark_task_group_id + 1
                    task_group_id = task_group_id + 1
                    tg = TaskGroup(f"spark_{pyspark_task_group_id}", [node], "spark")
                    logging.info(f"Initializing Spark task group {tg.name}")
                    task_groups.append(tg)
                # assume that each node needs to have at least 1 Spark DataFrame input
                elif 'DataFrame' in [p.annotation.__name__ for p in
                                     signature(node.func).parameters.values() if
                                     p.annotation.__class__.__name__ == 'type']:
                    task_groups[task_group_id].append_task(node)
                else:
                    tg = TaskGroup(f"default_{task_group_id}", [node], "default")
                    task_groups.append(tg)
                    task_group_id = task_group_id + 1

        logging.info(f"Detected {pyspark_task_group_id + 1} PySpark task groups")
        logging.info(f"Detected total number of {task_group_id + 1} task groups")
        return task_groups



