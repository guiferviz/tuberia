from typing import Callable, Any


class IdPlugin:
    def __call__(self, _: Any) -> str:
        raise NotImplementedError()


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
        self.cache = {}

    def __call__(self, task_obj: Any) -> str:
        if task_obj in self.cache:
            return self.cache[task_obj]
        id_ = f"{task_obj.__class__.__name__}_{self.n}"
        self.cache[task_obj] = id_
        self.n += 1
        return id_


class FunctionId(IdPlugin):
    def __init__(self, function: Callable):
        self.function = function

    def __call__(self, task_obj: Any) -> str:
        return self.function(task_obj)


class Task:
    id_plugin: IdPlugin = ClassNameId()


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
            if str(err).endswith("takes 1 positional argument but 2 were given"):
                raise TypeError("`id_plugin` cannot be a method, use the @staticmethod decorator") from err
            raise
    else:
        return Task.id_plugin(task_obj)
