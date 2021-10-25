import logging
from collections import defaultdict
from inspect import signature
from typing import Callable, Dict, List, Optional, Set

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
    def _node_get_func(node: Node) -> Callable:
        try:
            return node.func
        except AttributeError:
            return node._func  # support for kedro<0.17

    @staticmethod
    def _is_any_parameter_pyspark_frame(node: Node) -> bool:
        parameter_types = [
            ".".join([p.annotation.__module__, p.annotation.__name__])
            for p in signature(
                TaskGroupFactory._node_get_func(node)
            ).parameters.values()
            if p.annotation.__class__.__name__ == "type"
        ]
        return "pyspark.sql.dataframe.DataFrame" in parameter_types

    @staticmethod
    def _is_return_value_pyspark_frame(node: Node) -> bool:
        return_annotation = signature(
            TaskGroupFactory._node_get_func(node)
        ).return_annotation
        return (
            return_annotation
            and "pyspark.sql.dataframe.DataFrame"
            == ".".join(
                [return_annotation.__module__, return_annotation.__name__]
            )
        )

    @staticmethod
    def _is_any_parameter_pyspark_dataset(
        datasets: List[str], catalog: DataCatalog
    ) -> bool:
        for dataset in datasets:
            if (
                dataset in catalog._data_sets.keys()
                and type(catalog._data_sets[dataset]).__name__
                in KEDRO_SPARK_DATASET_TYPES
            ):
                return True

        return False

    @staticmethod
    def _is_tagged_as_pyspark(node: Node):
        tagged_as_pyspark = set(
            [
                tag
                for tag in node.tags
                if tag == "kedro-airflow-k8s:group:pyspark"
            ]
        )
        return tagged_as_pyspark

    @staticmethod
    def _is_pyspark(node: Node, catalog: DataCatalog) -> bool:
        node_is_pyspark = (
            TaskGroupFactory._is_any_parameter_pyspark_frame(node)
            or TaskGroupFactory._is_return_value_pyspark_frame(node)
            or TaskGroupFactory._is_any_parameter_pyspark_dataset(
                node.inputs, catalog
            )
            or TaskGroupFactory._is_any_parameter_pyspark_dataset(
                node.outputs, catalog
            )
            or TaskGroupFactory._is_tagged_as_pyspark(node)
        )

        return node_is_pyspark

    @staticmethod
    def _extract_groups(
        pipeline: Pipeline, catalog: DataCatalog
    ) -> Dict[str, Set[Node]]:
        def extract_group(node: Node) -> str:
            return (
                "pyspark"
                if TaskGroupFactory._is_pyspark(node, catalog)
                else "default"
            )

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

    @staticmethod
    def _get_nodes_child_deps(pipeline: Pipeline) -> Dict[Node, Set[Node]]:
        nodes_child_deps = defaultdict(set)
        for node, parent_nodes in pipeline.node_dependencies.items():
            for parent in parent_nodes:
                nodes_child_deps[parent].add(node)
        return nodes_child_deps

    @staticmethod
    def _get_deps_task_groups(
        pyspark_deps: Set[Node], pyspark_groups: Set[TaskGroup]
    ) -> Set[TaskGroup]:
        task_groups = set()
        for pyspark_dep in pyspark_deps:
            task_group = TaskGroupFactory._get_group(
                pyspark_dep, pyspark_groups
            )
            if task_group:
                task_groups.add(task_group)
        return task_groups

    def _create_pyspark_groups(
        self,
        marked_as_pyspark: Set[Node],
        nodes_parent_deps: Dict[Node, Set[Node]],
        nodes_child_deps: Dict[Node, Set[Node]],
    ) -> Set[TaskGroup]:
        pyspark_groups = set()
        for pyspark_node in marked_as_pyspark:
            deps = nodes_parent_deps[pyspark_node].union(
                nodes_child_deps[pyspark_node]
            )
            pyspark_deps = marked_as_pyspark.intersection(deps)
            task_groups = TaskGroupFactory._get_deps_task_groups(
                pyspark_deps, pyspark_groups
            )

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

        return pyspark_groups

    @staticmethod
    def _set_children_dependencies(
        any_group: TaskGroup,
        all_groups: Set[TaskGroup],
        nodes_child_deps: Dict[Node, Set[Node]],
    ):
        groups_deps = set()
        for group_node in any_group.task_group:
            groups_deps = groups_deps.union(nodes_child_deps[group_node])

        task_group_deps = set()
        for group in all_groups:
            if group is not any_group and groups_deps.intersection(
                group.task_group
            ):
                task_group_deps.add(group)

        any_group.set_children(task_group_deps)

    @staticmethod
    def _is_default_node_part_of_pyspark_group(
        default_node: Node, pyspark_group: TaskGroup
    ) -> bool:
        input_in_pyspark_group = False
        output_in_pyspark_group = False
        for task_group in pyspark_group.task_group:
            if set(default_node.inputs).intersection(set(task_group.outputs)):
                input_in_pyspark_group = True
            if set(default_node.outputs).intersection(set(task_group.inputs)):
                output_in_pyspark_group = True
        return input_in_pyspark_group and output_in_pyspark_group

    @staticmethod
    def _is_default_node_part_of_pyspark_groups(
        default_node: Node, pyspark_groups: Set[TaskGroup]
    ):
        for pyspark_group in pyspark_groups:
            if TaskGroupFactory._is_default_node_part_of_pyspark_group(
                default_node, pyspark_group
            ):
                return True, pyspark_group
        return False, None

    @staticmethod
    def _create_default_groups(
        default_nodes: Set[Node], pyspark_groups: Set[TaskGroup]
    ) -> Set[TaskGroup]:
        default_groups = set()
        for default_node in default_nodes:
            (
                match_to_pyspark_group,
                pyspark_group,
            ) = TaskGroupFactory._is_default_node_part_of_pyspark_groups(
                default_node, pyspark_groups
            )
            if match_to_pyspark_group:
                pyspark_group.append_task(default_node)
            else:
                default_groups.add(
                    TaskGroup(default_node.name, [default_node], "default")
                )
        return default_groups

    def create(
        self, pipeline: Pipeline, catalog: DataCatalog
    ) -> List[TaskGroup]:
        all_groups = TaskGroupFactory._extract_groups(pipeline, catalog)
        logging.info(f"Found user groups: [{all_groups.keys()}]")

        nodes_child_deps = TaskGroupFactory._get_nodes_child_deps(pipeline)
        nodes_parent_deps = pipeline.node_dependencies

        pyspark_groups = self._create_pyspark_groups(
            all_groups["pyspark"], nodes_parent_deps, nodes_child_deps
        )

        default_groups = TaskGroupFactory._create_default_groups(
            all_groups["default"], pyspark_groups
        )

        every_group = default_groups.union(pyspark_groups)
        for any_group in default_groups.union(pyspark_groups):
            TaskGroupFactory._set_children_dependencies(
                any_group, every_group, nodes_child_deps
            )

        logging.info(f"Found pyspark groups: {pyspark_groups}")
        return list(default_groups.union(pyspark_groups))
