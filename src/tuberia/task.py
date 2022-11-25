from __future__ import annotations

from typing import Any, List

import networkx as nx
from networkx.classes.digraph import DiGraph


def is_private_attribute_name(name):
    return name.startswith("_")


def is_task(value: Any) -> bool:
    return isinstance(value, Task)


def to_hashable_type(obj: Any):
    if isinstance(obj, dict):
        return frozenset([(k, to_hashable_type(v)) for k, v in obj.items()])
    elif isinstance(obj, (set, list, tuple)):
        return tuple([to_hashable_type(i) for i in obj])
    return obj


class Task:
    def run(self):
        raise NotImplementedError()

    def _public_dict(self):
        return {
            k: v
            for k, v in vars(self).items()
            if not is_private_attribute_name(k)
        }

    def _public_tuple(self):
        return tuple([self.__class__.__name__]) + tuple(
            i for i in self._public_dict().values()
        )

    def dependencies(self) -> List[Task]:
        return [v for _, v in self._public_dict().items() if is_task(v)]

    def __eq__(self, other):
        return self._public_tuple() == other._public_tuple()

    def __hash__(self):
        return hash(to_hashable_type(self._public_tuple()))


def dependency_tree(tasks: List[Task]) -> DiGraph:
    G = nx.DiGraph()
    pending_tasks = tasks
    visited = set()
    while len(pending_tasks):
        task = pending_tasks.pop()
        if task in visited:
            continue
        for i in task.dependencies():
            G.add_edge(i, task)
            pending_tasks.append(i)
        visited.add(task)
    return G
