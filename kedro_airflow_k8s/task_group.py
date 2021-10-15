import logging
from collections import defaultdict
from inspect import signature
from typing import Dict, List, Optional, Set

from kedro.io.data_catalog import DataCatalog
from kedro.pipeline.node import Node
from kedro.pipeline.pipeline import Pipeline

KEDRO_SPARK_DATASET_TYPES = [
    "SparkDataSet",
    "SparkHiveDataSet",
    "SparkJDBCDataSet",
]


class TaskGroup:
    def __init__(self, name: str, task_group: List[Node], group_type: str):
        self._name = name
        self._task_group = set(task_group)
        self._group_type = group_type  # could be default, spark, etc
        self._children = set()

    @property
    def name(self):
        return self._name

    @property
    def task_group(self):
        return self._task_group

    @property
    def group_type(self):
        return self._group_type

    @property
    def children(self):
        return self._children

    def __contains__(self, item):
        return item in self._task_group

    def merge(self, group):
        self._task_group = self._task_group.union(group._task_group)
        self._children = self._children.union(group._children)

    def append_task(self, node: Node):
        self._task_group.add(node)

    def set_children(self, children):
        self._children = children

    def renamed(self, new_name):
        tg = TaskGroup(new_name, set(self._task_group), self._group_type)
        tg._children = set(self._children)
        return tg

    def __eq__(self, other):
        return self._name == other._name

    def __hash__(self):
        return hash(self._name)

    def __repr__(self):
        return f"{self.name}/{self.group_type}"


class TaskGroupFactory:
    def __init__(self, group_counter: int = 0):
        self.group_counter = group_counter

    @staticmethod
    def _is_pyspark(node: Node, catalog: DataCatalog) -> bool:
        parameter_types = [
            ".".join([p.annotation.__module__, p.annotation.__name__])
            for p in signature(node.func).parameters.values()
            if p.annotation.__class__.__name__ == "type"
        ]
        if "pyspark.sql.dataframe.DataFrame" in parameter_types:
            return True

        return_annotation = signature(node.func).return_annotation
        if return_annotation:
            return_type = ".".join(
                [return_annotation.__module__, return_annotation.__name__]
            )
            if "pyspark.sql.dataframe.DataFrame" == return_type:
                return True

        for node_input in node.inputs:
            spark_input_in_catalog = (
                node_input in catalog._data_sets.keys()
                and type(catalog._data_sets[node_input]).__name__
                in KEDRO_SPARK_DATASET_TYPES
            )
            if spark_input_in_catalog:
                return True

        for node_output in node.outputs:
            spark_output_in_catalog = (
                node_output in catalog._data_sets.keys()
                and type(catalog._data_sets[node_output]).__name__
                in KEDRO_SPARK_DATASET_TYPES
            )
            if spark_output_in_catalog:
                return True
        return False

    @staticmethod
    def _extract_groups(
        pipeline: Pipeline, catalog: DataCatalog
    ) -> Dict[str, Set[Node]]:
        def extract_group(node: Node) -> str:
            tagged_as_pyspark = set(
                [
                    tag
                    for tag in node.tags
                    if tag == "kedro-airflow-k8s:group:pyspark"
                ]
            )
            if tagged_as_pyspark or TaskGroupFactory._is_pyspark(
                node, catalog
            ):
                return "pyspark"
            return "default"

        groups = defaultdict(set)
        for node in pipeline.nodes:
            groups[extract_group(node)].add(node)
        return groups

    @staticmethod
    def _get_group(
        pyspark_dep: Node, pyspark_groups: Set[TaskGroup]
    ) -> Optional[TaskGroup]:
        for group in pyspark_groups:
            if pyspark_dep in group:
                return group
        return None

    def _merge_groups(self, groups: Set[TaskGroup]) -> TaskGroup:
        final_group = None
        for g in groups:
            if not final_group:
                final_group = g
            else:
                final_group.merge(g)
        if not final_group:
            final_group = TaskGroup(
                f"pyspark_{self.group_counter}", [], "pyspark"
            )
            self.group_counter += 1
        return final_group

    def create(
        self, pipeline: Pipeline, catalog: DataCatalog
    ) -> List[TaskGroup]:
        all_groups = TaskGroupFactory._extract_groups(pipeline, catalog)
        logging.info(f"Found user groups: [{all_groups.keys()}]")

        nodes_parent_deps = pipeline.node_dependencies
        nodes_child_deps = defaultdict(set)
        for node, parent_nodes in pipeline.node_dependencies.items():
            for parent in parent_nodes:
                nodes_child_deps[parent].add(node)

        pyspark_groups = set()
        marked_as_pyspark = all_groups["pyspark"]
        for pyspark_node in marked_as_pyspark:
            deps = nodes_parent_deps[pyspark_node].union(
                nodes_child_deps[pyspark_node]
            )
            pyspark_deps = marked_as_pyspark.intersection(deps)

            task_groups = set()
            for pyspark_dep in pyspark_deps:
                task_group = TaskGroupFactory._get_group(
                    pyspark_dep, pyspark_groups
                )
                if task_group:
                    task_groups.add(task_group)

            final_group = self._merge_groups(task_groups)
            final_group.append_task(pyspark_node)
            pyspark_groups = pyspark_groups.difference(task_groups)
            pyspark_groups.add(final_group)

        tmp_pyspark_groups = set()
        rest_counter = 0
        for g in pyspark_groups:
            tmp_pyspark_groups.add(g.renamed(f"pyspark_{str(rest_counter)}"))
            rest_counter += 1
        pyspark_groups = tmp_pyspark_groups

        default_nodes = all_groups["default"]
        default_groups = set()
        for dn in default_nodes:
            match_to_pyspark_group = False
            for psg in pyspark_groups:
                input_in_pyspark_group = False
                output_in_pyspark_group = False
                for tg in psg.task_group:
                    if set(dn.inputs).intersection(set(tg.outputs)):
                        input_in_pyspark_group = True
                    if set(dn.outputs).intersection(set(tg.inputs)):
                        output_in_pyspark_group = True
                if input_in_pyspark_group and output_in_pyspark_group:
                    psg.append_task(dn)
                    match_to_pyspark_group = True
                    break

            if not match_to_pyspark_group:
                default_groups.add(TaskGroup(dn.name, [dn], "default"))

        for d in default_groups.union(pyspark_groups):
            groups_deps = set()
            for group_node in d.task_group:
                groups_deps = groups_deps.union(nodes_child_deps[group_node])

            task_group_deps = set()
            for group in default_groups.union(pyspark_groups):
                if group is not d and groups_deps.intersection(
                    group.task_group
                ):
                    task_group_deps.add(group)

            d.set_children(task_group_deps)

        logging.info(f"Found pyspark groups: {pyspark_groups}")
        return list(default_groups.union(pyspark_groups))