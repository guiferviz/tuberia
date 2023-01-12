import importlib
from typing import Any, Iterator, List, TypeVar, Union

T = TypeVar("T")


def get_module_member(module_name, member_name):
    m = importlib.import_module(module_name)
    return getattr(m, member_name)


def freeze(obj: Any) -> Union[tuple, frozenset]:
    """Converts any object into a immutable data structure.

    An immutable data structure is hashable, so it can be used as a key in
    a dictionary or in a set. If you have a list of mutable objects you can
    easily check if there are duplicates creating a set of a freeze version of
    each object.

    It also works with objects (objects that have `__dict__` method). An extra
    attribute `__class__` with the name of the class es added to the output
    just to distinguish between two different object with the same parameters
    but from different class.

    """
    if isinstance(obj, dict):
        return frozenset([(k, freeze(v)) for k, v in obj.items()])
    elif isinstance(obj, (set, list, tuple)):
        return tuple([freeze(i) for i in obj])
    elif hasattr(obj, "__dict__"):
        vars_obj = vars(obj).copy()
        assert "__class__" not in vars_obj
        vars_obj["__class__"] = obj.__class__.__name__
        return freeze(vars_obj)
    return obj


def list_or_generator_or_value_to_list(
    gen_or_value_or_list: Union[T, List[T], Iterator[T]]
) -> List[T]:
    """Convert a type T, an Iterator[T] or a List[T] into a List[T].

    Look at the following example:

    ```python
    def just_one_value():
        return 1

    def return_list():
        return [1, 2]

    def yield_values():
        yield 1
        yield 2

    assert list_or_generator_or_value_to_list(just_one_value()) == [1]
    assert list_or_generator_or_value_to_list(return_list()) == [1, 2]
    assert list_or_generator_or_value_to_list(yield_values()) == [1, 2]
    ```

    The usage of `list_or_generator_or_value_to_list` allows a lot of
    flexibility on the definition of the functions. Users of your framework can
    define their functions in any format and your code will always get a list.

    """
    if isinstance(gen_or_value_or_list, Iterator):
        return list(gen_or_value_or_list)
    elif isinstance(gen_or_value_or_list, List):
        return gen_or_value_or_list
    return [gen_or_value_or_list]
