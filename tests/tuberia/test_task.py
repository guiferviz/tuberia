from typing import Any

import pydantic
import pytest

from tuberia.exceptions import TuberiaException
from tuberia.task import Task, dependency_graph


@pytest.fixture(params=["python", "pydantic"])
def dataclass(request):
    return request.param


@pytest.fixture(params=["python", "pydantic", "none"])
def dataclass_or_none(request):
    return None if request.param == "none" else request.param


@pytest.fixture
def value_task():
    class ValueTask(Task):
        def __init__(self, value):
            self.value = value

    return ValueTask


def test_task_without_dataclass():
    class ValueTask(Task, dataclass=None):
        def __init__(self, value: int):
            self.value = value

    task = ValueTask(3)
    assert task.value == 3


def test_task_with_python_dataclass():
    class ValueTask(Task, dataclass="python"):
        value: int

    task = ValueTask(3)
    assert task.value == 3


def test_task_with_pydantic_dataclass():
    class ValueTask(Task, dataclass="pydantic"):
        value: int

    task = ValueTask("3")  # type: ignore
    assert task.value == 3


def test_task_with_pydantic_dataclass_and_validators():
    class ValueTask(Task, dataclass="pydantic"):
        value: int

        @pydantic.validator("value")
        def double_value(cls, value):
            return value * 2

    task = ValueTask(3)
    assert task.value == 6


def test_task_with_pydantic_dataclass_with_errors():
    class ValueTask(Task, dataclass="pydantic"):
        value: int

    with pytest.raises(pydantic.ValidationError) as exception:
        ValueTask("a")  # type: ignore
    assert len(exception.value.errors()) == 1
    assert exception.value.errors()[0]["type"] == "type_error.integer"
    assert exception.value.errors()[0]["loc"] == ("value",)


def test_task_with_pydantic_and_reserved_names():
    with pytest.raises(NameError, match="shadows a BaseModel attribute"):

        class InvalidSchemaTask(Task, dataclass="pydantic"):
            schema: str

    class ValidSchemaTask(Task, dataclass="pydantic"):
        class schema:
            a = "a"


def test_task_dataclass_with_private_attributes(dataclass):
    class MyTask(Task, dataclass=dataclass):
        public: int

        def __post_init__(self):
            self.private = 1

    task = MyTask(3)
    assert task.public == 3
    assert task.private == 1


def test_task_inheritance_from_none_dataclass_to_python_dataclass():
    class MySuperTask(Task, dataclass=None):
        def __init__(self, super_value: int, another_super_value: int = -1):
            self.super_value = super_value
            self.another_super_value = another_super_value

        def super_method(self):
            return self.super_value * 2

        def __hash__(self):
            return 0

    class MySubClass(MySuperTask, dataclass="python"):
        sub_value: int
        another_super_value: int

        def __post_init__(self):
            super().__init__(self.sub_value * 2, self.another_super_value)

    task = MySubClass(3, 4)
    assert task.sub_value == 3
    assert task.super_value == 6
    assert task.super_method() == 12
    assert task.another_super_value == 4
    assert hash(task) == 0


def test_task_inheritance_from_python_dataclass_to_none_dataclass():
    class MySuperTask(Task, dataclass="python"):
        super_value: int

    class MySubClass(MySuperTask, dataclass=None):
        def __init__(self, super_value: int, sub_value: int):
            super().__init__(super_value)
            self.sub_value = sub_value

    for task in [MySubClass(super_value=3, sub_value=4), MySubClass(3, 4)]:
        assert task.super_value == 3
        assert task.sub_value == 4


def test_task_inheritance_from_python_dataclass_to_pydantic_dataclass():
    class MySuperTask(Task, dataclass="python"):
        super_value: int

    class MySubClass(MySuperTask, dataclass="pydantic"):
        sub_value: int

    for task in [MySubClass(super_value=3, sub_value=4), MySubClass(3, 4)]:
        assert task.super_value == 3
        assert task.sub_value == 4


def test_dependencies_with_python_dataclass(dataclass):
    class MyTask0(Task, dataclass=dataclass):
        pass

    class MyTask1(Task, dataclass=dataclass):
        pass

    class MyTask2(Task, dataclass=dataclass):
        my_task_0: MyTask0
        _my_task_1: MyTask1

    my_task_0 = MyTask0()
    my_task_1 = MyTask1()
    my_task_2 = MyTask2(my_task_0, my_task_1)
    assert my_task_2._task_descriptor.get_dependencies(my_task_2) == [my_task_0]


def test_hash_with_dataclass(dataclass):
    class MyTask(Task, dataclass=dataclass):
        a: int
        b: str

    task0 = MyTask(a=3, b="b")
    task1 = MyTask(b="b", a=3)
    assert hash(task0) == hash(task1)


def test_hash_with_none_dataclass():
    class MyTask(Task, dataclass=None):
        def __init__(self, a: int, b: str):
            self.a = a
            self.b = b

    task0 = MyTask(a=3, b="b")
    task1 = MyTask(b="b", a=3)
    assert hash(task0) == hash(task1)


def test_eq_with_dataclass(dataclass):
    class MyTask(Task, dataclass=dataclass):
        a: int
        b: str

    task0 = MyTask(a=3, b="b")
    task1 = MyTask(b="b", a=3)
    assert task0 == task1


def test_eq_with_none_dataclass():
    class MyTask(Task, dataclass=None):
        def __init__(self, a: int, b: str):
            self.a = a
            self.b = b

    task0 = MyTask(a=3, b="b")
    task1 = MyTask(b="b", a=3)
    assert task0 == task1


def test_hash_with_nested_tasks():
    class MyTaskA(Task):
        def __init__(self, a):
            self.a = a

    class MyTaskB(Task):
        def __init__(self, my_task_a, b):
            self.my_task_a = my_task_a
            self.b = b

    my_task_a_0 = MyTaskA(3)
    task0 = MyTaskB(my_task_a_0, "b")
    my_task_a_1 = MyTaskA(3)
    task1 = MyTaskB(my_task_a_1, "b")
    assert hash(task0) == hash(task1)


@pytest.mark.parametrize(
    "non_hashable",
    [
        pytest.param([0, 1], id="list"),
        pytest.param({0, 1}, id="set"),
        pytest.param({0: 1}, id="dictionary"),
        pytest.param([0, [1, 2]], id="nested_list"),
        pytest.param({0: {1: 2}}, id="nested_dictionary"),
        pytest.param(
            {
                0: [
                    1,
                    2,
                    {
                        3: (
                            4,
                            {
                                5,
                            },
                        )
                    },
                ]
            },
            id="crazy_nested",
        ),
    ],
)
def test_hash_with_non_hashable_types(dataclass, non_hashable):
    class MyTask(Task, dataclass=dataclass):
        a: Any

    task0 = MyTask(non_hashable)
    task1 = MyTask(non_hashable)
    assert hash(task0) == hash(task1)
    task2 = MyTask("hashable")
    assert hash(task1) != hash(task2)


def test_hash_with_non_hashable_types_different_order():
    class MyTask(Task):
        def __init__(self, a):
            self.a = a

    task0 = MyTask({"a": 1, "b": 2})
    task1 = MyTask({"b": 2, "a": 1})
    assert hash(task0) == hash(task1)


def test_eq():
    class MyTask(Task):
        def __init__(self, a, b):
            self.a = a
            self.b = b

    task0 = MyTask(a=3, b="b")
    task1 = MyTask(a=3, b="b")
    assert task0 == task1


def test_sha1_sortable_keys():
    class MyTask(Task):
        def __init__(self, value):
            self.value = value

    task0 = MyTask({"a": 1, "b": 2})
    task1 = MyTask({"b": 2, "a": 1})
    expected_sha1 = "7b4107fb188b57159236465554f2da9da3f769f7"
    assert (
        task0._task_descriptor.get_sha1(task0)
        == task1._task_descriptor.get_sha1(task1)
        == expected_sha1
    )


def test_sha1_sets():
    class MyTask(Task):
        def __init__(self, value):
            self.value = value

    task0 = MyTask({"b", "a"})
    task1 = MyTask({"a", "b"})
    expected_sha1 = "75c451fdcfe55993ecdc664e6c7641809ffd51dc"
    assert (
        task0._task_descriptor.get_sha1(task0)
        == task1._task_descriptor.get_sha1(task1)
        == expected_sha1
    )


def test_sha1_frozensets():
    class MyTask(Task):
        def __init__(self, value):
            self.value = value

    task0 = MyTask(frozenset(["b", "a"]))
    task1 = MyTask(frozenset(["a", "b"]))
    expected_sha1 = "85abc54e96173bb75c24b9ca3a93e8b8e578d7c6"
    assert (
        task0._task_descriptor.get_sha1(task0)
        == task1._task_descriptor.get_sha1(task1)
        == expected_sha1
    )


def test_sha1_fails_when_no_sortable_elements_are_found():
    class MyTask(Task):
        def __init__(self, value):
            self.value = value

    class HashableButNotSortable:
        def __init__(self, value):
            self.value = value

        def __hash__(self):
            return hash(i for i in self.value.items())

    task = MyTask(
        {HashableButNotSortable({"a": "b"}), HashableButNotSortable({"a": "b"})}
    )
    with pytest.raises(TuberiaException):
        task._task_descriptor.get_sha1(task)


def test_sha1_float_is_not_eq_to_int(value_task):
    task0 = value_task(1)
    task1 = value_task(1.0)
    assert task0._task_descriptor.get_sha1(
        task0
    ) != task1._task_descriptor.get_sha1(task1)


def test_id(value_task, mocker):
    task = value_task(1)
    assert task.id == "1072d23672244cbcdda765dd1c58791ad19cde6a"
    task.id
    task.value = 2
    assert task.id == "ee78355c16d5b1163be14f9aa0b6bea56e110e10"


def test_dependency_tree():
    class Task0(Task):
        def __init__(self, value: int):
            self.value = value

    class Task1(Task):
        def __init__(self, previous_task: Task0, another_previous_task: Task0):
            self.previous_task = previous_task
            self.another_previous_task = another_previous_task

    task0_0 = Task0(0)
    task0_1 = Task0(1)
    task1 = Task1(Task0(0), Task0(1))
    tree = dependency_graph([task1])
    assert set(tree.nodes) == {task0_0, task0_1, task1}
    assert set(tree.edges) == {(task0_0, task1), (task0_1, task1)}


def test_dataclasses_are_not_used_by_default():
    class MyTask(Task):
        pass

    task = MyTask()
    assert task.__dataclass_type__ is None


def test_hash_with_different_classes_different_fields():
    class MyTask(Task, dataclass="python"):  # type: ignore
        a: str

    task0 = MyTask("a")

    class MyTask(Task, dataclass="python"):
        b: str

    task1 = MyTask("a")

    assert hash(task0) != hash(task1)
    with pytest.raises(NotImplementedError):
        assert task0 == task1


def test_hash_with_different_classes_same_fields():
    def my_task_0():
        class MyTask(Task, dataclass="python"):
            a: str

        return MyTask

    def my_task_1():
        class MyTask(Task, dataclass="python"):
            a: str

        return MyTask

    task0 = my_task_0()("a")
    task1 = my_task_1()("a")

    assert hash(task0) != hash(task1)
    with pytest.raises(NotImplementedError):
        assert task0 == task1