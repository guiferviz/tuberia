from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import fields
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import networkx as nx
from pydantic.dataclasses import Callable

from tuberia.exceptions import TuberiaException


def is_private_attribute_name(name):
    return name.startswith("_")


def is_task(value: Any) -> bool:
    return isinstance(value, Task)


def is_non_empty_list_of_tasks(value: Any) -> bool:
    return isinstance(value, List) and len(value) > 0 and is_task(value[0])


def is_non_empty_dict_of_tasks(value: Any) -> bool:
    return (
        isinstance(value, Dict)
        and len(value) > 0
        and is_task(next(value.values().__iter__()))
    )


def object_to_tuple_of_tuples(
    obj: Any,
    include_class_name: bool = True,
    fields: Optional[List[str]] = None,
) -> Tuple[Tuple[str, str], ...]:
    if fields is None:
        fields = list(vars(obj).keys())
    class_name: Tuple[Tuple[str, str], ...] = tuple()
    if include_class_name:
        class_name = tuple(
            [
                (
                    "__class__",
                    f"{obj.__class__.__module__}::{obj.__class__.__qualname__}",
                )
            ]
        )
    return class_name + tuple((i, getattr(obj, i)) for i in fields)


def to_hashable_data_structure(obj: Any):
    if isinstance(obj, dict):
        return frozenset(
            [(k, to_hashable_data_structure(v)) for k, v in obj.items()]
        )
    elif isinstance(obj, (set, list, tuple)):
        return tuple([to_hashable_data_structure(i) for i in obj])
    elif inspect.ismethod(obj) or inspect.isfunction(obj):
        return inspect.getsource(obj)
    elif hasattr(obj, "__dict__"):
        return object_to_tuple_of_tuples(obj)
    return obj


def public_fields(dataclass):
    return [
        i.name
        for i in fields(dataclass)
        if not is_private_attribute_name(i.name)
    ]


class TaskDescriptor:
    @classmethod
    def get_public_fields(cls, task: Task):
        if getattr(task, "__dataclass_type__", None) is None:
            all_fields = list(vars(task).keys())
        else:
            all_fields = [i.name for i in fields(task)]
        return [i for i in all_fields if not is_private_attribute_name(i)]

    @classmethod
    def get_public_tuple(cls, task: Task):
        return object_to_tuple_of_tuples(
            task,
            include_class_name=True,
            fields=cls.get_public_fields(task),
        )

    @classmethod
    def get_json(cls, task: Task, sort=False):
        class CustomEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, (set, frozenset)):
                    return list(sorted(obj))
                elif is_task(obj):
                    return obj.id
                elif inspect.ismethod(obj) or inspect.isfunction(obj):
                    return inspect.getsource(obj)
                # TODO: serialize arbitrary objects.
                # elif isinstance(obj, object):
                #    return vars(obj)
                return json.JSONEncoder.default(self, obj)

        try:
            json_str = json.dumps(
                task._task_descriptor.get_public_tuple(task),
                sort_keys=sort,
                ensure_ascii=True,
                cls=CustomEncoder,
            )
        except TypeError as e:
            raise TuberiaException(
                f"Not able to hash {type(task)} in a deterministic way."
                "Overwrite the id property or make objects JSON serializable."
            ) from e
        return json_str

    @classmethod
    def get_sha1(cls, task: Task) -> str:
        return sha1(task._task_descriptor.get_json(task, sort=True))

    @classmethod
    def get_dependencies(cls, task: Task) -> List[Task]:
        tasks = []
        for i in task._task_descriptor.get_public_fields(task):
            value = getattr(task, i)
            if is_task(value):
                tasks.append(value)
            elif is_non_empty_list_of_tasks(value):
                for i in value:
                    tasks.append(i)
            elif is_non_empty_dict_of_tasks(value):
                for i in value.values():
                    tasks.append(i)
        return tasks

    @classmethod
    def get_hashable_data_structure(cls, task: Task) -> Any:
        return to_hashable_data_structure(
            task._task_descriptor.get_public_tuple(task)
        )


class Task:
    _task_descriptor: TaskDescriptor = TaskDescriptor()

    @property
    @lru_cache
    def id(self) -> str:
        return self._task_descriptor.get_sha1(self)

    def run(self):
        raise NotImplementedError()

    def __eq__(self, other: Any):
        if self.__class__ == other.__class__:
            self_tuple = self._task_descriptor.get_public_tuple(self)
            other_tuple = other._task_descriptor.get_public_tuple(other)
            return self_tuple == other_tuple
        raise NotImplementedError

    def __hash__(self):
        return hash(self._task_descriptor.get_hashable_data_structure(self))


class FunctionTask(Task):
    def __init__(self, function: Callable):
        self.function = function
        self.full_name = function.__name__

    def run(self):
        self.function()


def sha1(value: str) -> str:
    m = hashlib.sha1()
    m.update(value.encode())
    return m.hexdigest()


def dependency_graph(tasks: List[Task]) -> nx.DiGraph:
    G = nx.DiGraph()
    pending_tasks = tasks
    visited = set()
    visited_ids = set()
    while len(pending_tasks):
        task = pending_tasks.pop()
        if task in visited:
            continue
        G.add_node(task)
        if task.id in visited_ids:
            raise ValueError(
                f"A different task with the same ID already exists: `{task.id}`"
            )
        for i in task._task_descriptor.get_dependencies(task):
            G.add_edge(i, task)
            pending_tasks.append(i)
        visited.add(task)
    return G


def dag(tasks: List[Task]) -> nx.DiGraph:
    G = dependency_graph(tasks)
    if not nx.is_directed_acyclic_graph(G):
        raise ValueError("not a directed acyclic graph")
    return G


def run(tasks: List[Task]):
    G = dag(tasks)
    for i in topological_sort_grouped(G):
        for j in i:
            j.run()


def topological_sort_grouped(G):
    # Taken from https://stackoverflow.com/questions/56802797/digraph-parallel-ordering
    indegree_map = {v: d for v, d in G.in_degree() if d > 0}
    zero_indegree = [v for v, d in G.in_degree() if d == 0]
    while zero_indegree:
        yield zero_indegree
        new_zero_indegree = []
        for v in zero_indegree:
            for _, child in G.edges(v):
                indegree_map[child] -= 1
                if not indegree_map[child]:
                    new_zero_indegree.append(child)
        zero_indegree = new_zero_indegree
