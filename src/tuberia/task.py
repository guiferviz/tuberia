from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import dataclass as python_dataclass
from dataclasses import fields
from functools import lru_cache
from typing import Any, List, Literal, Optional

import networkx as nx
from networkx.classes.digraph import DiGraph
from pydantic.dataclasses import dataclass as pydantic_dataclass
from typing_extensions import dataclass_transform

from tuberia.exceptions import TuberiaException


def is_private_attribute_name(name):
    return name.startswith("_")


def is_task(value: Any) -> bool:
    return isinstance(value, Task)


def to_hashable_type(obj: Any):
    if isinstance(obj, dict):
        return frozenset([(k, to_hashable_type(v)) for k, v in obj.items()])
    elif isinstance(obj, (set, list, tuple)):
        return tuple([to_hashable_type(i) for i in obj])
    # If we use the following code we can make any object hashable:
    # elif hasattr(obj, "__dict__"):
    #     vars_obj = vars(obj).copy()
    #     assert "__class__" not in vars_obj
    #     vars_obj["__class__"] = obj.__class__.__name__
    #     return to_hashable_type(vars_obj)
    elif isinstance(obj, object):
        implemented = False
        for c in inspect.getmro(obj.__class__):
            if c != object:
                if [v for k, v in vars(c).items() if k == "__hash__"]:
                    implemented = True
        if not implemented:
            raise RuntimeError(f"__hash__ method not implemented by {obj}")
    return obj


def public_fields(dataclass):
    return [
        i.name
        for i in fields(dataclass)
        if not is_private_attribute_name(i.name)
    ]


@dataclass_transform()
class Task:
    __dataclass_type__: Optional[Literal["python", "pydantic"]]

    def __init_subclass__(
        cls, dataclass: Optional[Literal["python", "pydantic"]] = None
    ):
        cls.__dataclass_type__ = dataclass
        if dataclass == "python":
            return python_dataclass(eq=False)(cls)
        elif dataclass == "pydantic":
            return pydantic_dataclass(eq=False)(cls)
        elif dataclass is None:
            return cls
        raise ValueError(f"Unknown dataclass value `{dataclass}`")

    @property
    @lru_cache
    def id(self):
        return self._sha1()

    def run(self):
        raise NotImplementedError()

    def dependencies(self) -> List[Task]:
        tasks = []
        for i in self._public_fields():
            value = getattr(self, i)
            if is_task(value):
                tasks.append(value)
        return tasks

    def _public_fields(self):
        if self.__dataclass_type__ is None:
            all_fields = list(vars(self).keys())
        else:
            all_fields = [i.name for i in fields(self)]
        return [i for i in all_fields if not is_private_attribute_name(i)]

    def _public_tuple(self):
        return tuple([self.__class__.__name__]) + tuple(
            getattr(self, i) for i in self._public_fields()
        )

    def __eq__(self, other):
        return self._public_tuple() == other._public_tuple()

    def __hash__(self):
        return hash(to_hashable_type(self._public_tuple()))

    def _sha1(self):
        class SetEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, (set, frozenset)):
                    return list(sorted(obj))
                # TODO: serialize arbitrary objects.
                # elif isinstance(obj, object):
                #    return vars(obj)
                return json.JSONEncoder.default(self, obj)

        try:
            json_str = json.dumps(
                self._public_tuple(),
                sort_keys=True,
                ensure_ascii=True,
                cls=SetEncoder,
            )
        except TypeError as e:
            raise TuberiaException(
                f"Not able to hash {type(self)} in a deterministic way."
                "Overwrite the id property or make objects JSON serializable."
            ) from e
        return sha1(json_str)


def sha1(value: str):
    m = hashlib.sha1()
    m.update(value.encode())
    return m.hexdigest()


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
