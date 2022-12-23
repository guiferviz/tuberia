from __future__ import absolute_import

import inspect
import functools
import json
import hashlib
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Type,
    TypeVar,
    Tuple,
    Optional,
)

from loguru import logger
from tuberia.exceptions import TuberiaException

from tuberia.utils import list_or_generator_or_value_to_list

T = TypeVar("T")


class IdPlugin:
    def __call__(self, _: Any) -> str:
        raise NotImplementedError()


class DependencyPlugin(Generic[T]):
    def __init__(self, type_: Type[T]):
        self.type_ = type_

    def __call__(self, _: T) -> List[T]:
        raise NotImplementedError()


class Task:
    id_plugin: IdPlugin
    dependency_plugin: DependencyPlugin


class StaticId(IdPlugin):
    def __init__(self, static_id: str):
        self.static_id = static_id

    def __call__(self, _: Any) -> str:
        return self.static_id


class ClassNameId(IdPlugin):
    def __call__(self, task_obj: Any) -> str:
        return task_obj.__class__.__name__


class NumberedClassNameId(IdPlugin):
    def __init__(self):
        self.n = 0

    @functools.lru_cache
    def __call__(self, task_obj: Any) -> str:
        id_ = f"{task_obj.__class__.__name__}_{self.n}"
        self.n += 1
        return id_


class FunctionId(IdPlugin):
    def __init__(self, function: Callable):
        self.function = function

    def __call__(self, task_obj: Any) -> str:
        return self.function(task_obj)


class Sha1Id(IdPlugin):
    def __call__(self, task_obj: Any) -> str:
        deterministic_representation = self._to_deterministic_representation(
            task_obj
        )
        logger.debug(deterministic_representation)
        return self._sha1(deterministic_representation)

    def _sha1(self, value: str) -> str:
        m = hashlib.sha1()
        m.update(value.encode())
        return m.hexdigest()

    def _object_to_dict(self, task_obj: Any) -> dict:
        obj = dict(
            __class__=f"{task_obj.__class__.__module__}::{task_obj.__class__.__qualname__}"
        )
        obj_vars = dict(vars(task_obj))
        assert "__class__" not in obj_vars
        obj.update(obj_vars)
        return obj

    def _to_deterministic_representation(self, task_obj: Any) -> str:
        _object_to_dict = self._object_to_dict

        class CustomEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, set):
                    return ["set"] + list(sorted(obj))
                if isinstance(obj, frozenset):
                    return ["frozenset"] + list(sorted(obj))
                # elif is_task(obj):
                #    return obj.id
                elif inspect.ismethod(obj) or inspect.isfunction(obj):
                    return inspect.getsource(obj)
                elif isinstance(obj, object):
                    return _object_to_dict(obj)
                return json.JSONEncoder.default(self, obj)

        try:
            json_str = json.dumps(
                _object_to_dict(task_obj),
                sort_keys=True,
                ensure_ascii=True,
                cls=CustomEncoder,
            )
        except TypeError as e:
            raise TuberiaException(
                f"Not able to hash {type(task_obj)} in a deterministic way."
                "Overwrite the id property or make objects JSON serializable."
            ) from e
        return json_str


class MethodDependencyPlugin(DependencyPlugin, Generic[T]):
    def __init__(self, type_: Type[T], method_name: str = "get_dependencies"):
        self.type_ = type_
        self.method_name = method_name

    def __call__(self, task_obj: T) -> List[T]:
        if hasattr(task_obj, self.method_name):
            return self._get_dependencies_from_method(task_obj)
        return []

    def _get_dependencies_from_method(self, task_obj: Any):
        dependencies = list_or_generator_or_value_to_list(
            # We assume the method exists if this function is called.
            getattr(task_obj, self.method_name)()
        )
        if not all(isinstance(d, self.type_) for d in dependencies):
            invalid_dependencies = [
                str(d) for d in dependencies if not isinstance(d, self.type_)
            ]
            raise ValueError(
                f"`get_dependencies` of task `{task_obj}` should return only objects of type `{self.type_.__name__}`."
                f" Found invalid dependencies: {invalid_dependencies}"
            )
        return dependencies


class DynamicDependencyPlugin(DependencyPlugin, Generic[T]):
    def __init__(self, type_: Type[T] = Task):
        self.type_ = type_

    def __call__(self, task_obj: T) -> List[T]:
        return self._get_dependencies_from_attributes(task_obj)

    def _get_public_attributes(self, task: T) -> List[str]:
        all_fields = list(vars(task).keys())
        return [i for i in all_fields if not i.startswith("_")]

    def _get_dependencies_from_attributes(self, task: T) -> List[T]:
        dependencies = []
        attributes = self._get_public_attributes(task)
        for attr in attributes:
            value = getattr(task, attr)
            if isinstance(value, List):
                dependencies.extend(self._get_dependencies_from_list(value))
            elif isinstance(value, Dict):
                dependencies.extend(self._get_dependencies_from_dict(value))
            elif isinstance(value, self.type_):
                dependencies.append(value)
        return dependencies

    def _get_dependencies_from_list(self, lst: list) -> List[str]:
        dependencies = list(filter(lambda x: isinstance(x, self.type_), lst))
        if len(dependencies) > 0 and len(lst) != len(dependencies):
            for i in filter(lambda x: not isinstance(x, self.type_), lst):
                logger.warning(
                    f"Object of type {type(i)} is not a task and will be ignored."
                    " This happens when a list contains tasks mixed with other type of objects."
                )
        return dependencies

    def _get_dependencies_from_dict(self, dct: dict) -> List[str]:
        dependencies = list(
            filter(lambda x: isinstance(x, self.type_), dct.values())
        )
        if len(dependencies) > 1 and len(dct) != len(dependencies):
            for i in filter(
                lambda x: not isinstance(x, self.type_), dct.values()
            ):
                logger.warning(
                    f"Object of type {type(i)} is not a task and will be ignored."
                    " This happens when a dictionary contains tasks mixed with other type of objects."
                )
        return dependencies


def get_id(task_obj: Any):
    """Get the ID of an existing object that represents a task.

    Get id using the plugin stored in `id_plugin`. If the attribute is not
    defined then use the `ClassNameId` plugin.
    """
    if hasattr(task_obj, "id_plugin"):
        if not callable(task_obj.id_plugin):
            raise TypeError(f"`id_plugin` of `{task_obj}` is not callable")
        try:
            return task_obj.id_plugin(task_obj)
        except TypeError as err:
            if str(err).endswith(
                "takes 1 positional argument but 2 were given"
            ):
                raise TypeError(
                    "`id_plugin` cannot be a method, use the @staticmethod decorator"
                ) from err
            raise
    else:
        return ClassNameId()(task_obj)


def get_dependencies(task_obj: Any):
    if hasattr(task_obj, "dependency_plugin"):
        if not callable(task_obj.dependency_plugin):
            raise TypeError(
                f"`dependency_plugin` of `{task_obj}` is not callable"
            )
        try:
            return task_obj.dependency_plugin(task_obj)
        except TypeError as err:
            if str(err).endswith(
                "takes 1 positional argument but 2 were given"
            ):
                raise TypeError(
                    "`dependency_plugin` cannot be a method, use the @staticmethod decorator"
                ) from err
            raise
    else:
        return DynamicDependencyPlugin(Task)(task_obj)
