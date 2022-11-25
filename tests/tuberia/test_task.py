import pytest

from tuberia.base_model import BaseModel
from tuberia.task import Task, dependency_tree
from tuberia.visualization import open_in_browser


def test_dependencies():
    class MyTask0(Task):
        pass

    class MyTask1(Task):
        pass

    class MyTask2(Task):
        my_task_0: MyTask0
        _my_task_1: MyTask1

        def __init__(self, my_task_0: MyTask0, my_task_1: MyTask1):
            self.my_task_0 = my_task_0
            self._my_task_1 = my_task_1

    my_task_0 = MyTask0()
    my_task_1 = MyTask1()
    my_task_2 = MyTask2(my_task_0, my_task_1)
    assert my_task_2.dependencies() == [my_task_0]


def test_hash():
    class MyTask(Task):
        def __init__(self, a, b):
            self.a = a
            self.b = b

    task0 = MyTask(a=3, b="b")
    task1 = MyTask(a=3, b="b")
    assert hash(task0) == hash(task1)


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
                            [
                                5,
                            ],
                        )
                    },
                ]
            },
            id="crazy_nested",
        ),
    ],
)
def test_hash_with_non_hashable_types(non_hashable):
    class MyTask(Task):
        def __init__(self, a):
            self.a = a

    task0 = MyTask(non_hashable)
    task1 = MyTask(non_hashable)
    assert hash(task0) == hash(task1)
    task2 = MyTask("hashable")
    assert hash(task1) != hash(task2)


def test_eq():
    class MyTask(Task):
        def __init__(self, a, b):
            self.a = a
            self.b = b

    task0 = MyTask(a=3, b="b")
    task1 = MyTask(a=3, b="b")
    assert task0 == task1


def test_dependency_tree():
    class Task0(Task):
        def __init__(self, value: int):
            self.value = value

    class Task1(Task, BaseModel):
        def __init__(self, previous_task: Task0, another_previous_task: Task0):
            self.previous_task = previous_task
            self.another_previous_task = another_previous_task

    tree = dependency_tree([Task1(Task0(0), Task0(1))])
    open_in_browser(tree)
