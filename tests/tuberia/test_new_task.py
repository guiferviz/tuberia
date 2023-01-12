import logging

import pytest
from tuberia.exceptions import TuberiaException

from tuberia.new_task import (
    ClassNameId,
    DynamicDependencyPlugin,
    FunctionId,
    MethodDependencyPlugin,
    NumberedClassNameId,
    Sha1Id,
    StaticId,
    get_dependencies,
    get_id,
    Task,
)


class TestGetId:
    def test_empty_class(self):
        class MyClass:
            pass

        assert get_id(MyClass) == "type"
        assert get_id(MyClass()) == "MyClass"

    def test_static_lambda_callable(self):
        class MyClass:
            id_plugin = staticmethod(lambda _: "my_id")

        assert get_id(MyClass) == "my_id"
        assert get_id(MyClass()) == "my_id"

    def test_def_method_callable(self):
        class MyClass:
            def id_plugin(self):
                return "my_id"

        assert get_id(MyClass) == "my_id"
        with pytest.raises(TypeError, match=".*cannot be a method.*"):
            get_id(MyClass())

    def test_static_def_callable(self):
        class MyClass:
            @staticmethod
            def id_plugin(_):
                return "my_id"

        assert get_id(MyClass) == "my_id"
        assert get_id(MyClass()) == "my_id"

    def test_value_error_when_not_callable(self):
        class MyClass:
            id_plugin = 1

        with pytest.raises(TypeError, match=".*is not callable"):
            get_id(MyClass)


class TestFunctionId:
    def test_def_function(self):
        def my_id(task):
            return task.__class__.__name__

        class MyClass:
            pass

        id_plugin = FunctionId(my_id)
        assert id_plugin(MyClass) == "type"
        assert id_plugin(MyClass()) == "MyClass"

    def test_lambda_function(self):
        class MyClass:
            pass

        id_plugin = FunctionId(lambda x: x.__class__.__name__)
        assert id_plugin(MyClass) == "type"
        assert id_plugin(MyClass()) == "MyClass"


class TestStaticId:
    def test_returns_always_same_id(self):
        class MyClass:
            pass

        id_plugin = StaticId("my_id")
        assert id_plugin(MyClass()) == "my_id"
        assert id_plugin(MyClass()) == "my_id"


class TestSha1Id:
    @pytest.fixture
    def value_task(self):
        class ValueTask:
            def __init__(self, value):
                self.value = value

        return ValueTask

    def test_sha1_with_int(self, value_task):
        id_plugin = Sha1Id()
        assert (
            id_plugin._to_deterministic_representation(value_task(3))
            == '{"__class__": "tests.tuberia.test_new_task::TestSha1Id.value_task.<locals>.ValueTask", "value": 3}'
        )
        assert (
            id_plugin(value_task(3))
            == "b0b9a5dff01d0956ef8e46af61b0ec85d1633476"
        )

    def test_sha1_with_list(self, value_task):
        id_plugin = Sha1Id()
        assert (
            id_plugin._to_deterministic_representation(value_task([3, 2]))
            == '{"__class__": "tests.tuberia.test_new_task::TestSha1Id.value_task.<locals>.ValueTask", "value": [3, 2]}'
        )
        assert (
            id_plugin(value_task([3, 2]))
            == "5c234d07ee22ed3a24ecb44e9a5a99a45d7b5fc0"
        )

    def test_sha1_with_set(self, value_task):
        id_plugin = Sha1Id()
        assert (
            id_plugin._to_deterministic_representation(value_task({3, 2}))
            == '{"__class__": "tests.tuberia.test_new_task::TestSha1Id.value_task.<locals>.ValueTask", "value": ["set", 2, 3]}'
        )
        assert (
            id_plugin(value_task({3, 2}))
            == "a570ad2e15c800c85a0f64427aee32c7f56e224a"
        )
        assert (
            id_plugin(value_task({2, 3}))
            == "a570ad2e15c800c85a0f64427aee32c7f56e224a"
        )

    def test_sha1_with_set_not_sortable(self, value_task):
        id_plugin = Sha1Id()
        with pytest.raises(
            TuberiaException, match="Not able to hash.*in a deterministic way.*"
        ):
            id_plugin._to_deterministic_representation(
                value_task({Task(), Task()})
            )

    def test_sha1_with_dict(self, value_task):
        id_plugin = Sha1Id()
        task = value_task({"b": 1, "a": 2})
        assert (
            id_plugin._to_deterministic_representation(task)
            == '{"__class__": "tests.tuberia.test_new_task::TestSha1Id.value_task.<locals>.ValueTask", "value": {"a": 2, "b": 1}}'
        )
        assert id_plugin(task) == "5270ee1c6aadf72b965a8b578c2dbfdd6153bb01"
        assert (
            id_plugin(value_task({"a": 2, "b": 1}))
            == "5270ee1c6aadf72b965a8b578c2dbfdd6153bb01"
        )

    def test_sha1_with_objects(self, value_task):
        class MyTask:
            pass

        id_plugin = Sha1Id()
        task = value_task(MyTask())
        assert (
            id_plugin._to_deterministic_representation(task)
            == '{"__class__": "tests.tuberia.test_new_task::TestSha1Id.value_task.<locals>.ValueTask", "value": {"__class__": "tests.tuberia.test_new_task::TestSha1Id.test_sha1_with_objects.<locals>.MyTask"}}'
        )
        assert id_plugin(task) == "ea1d39caf57506e4f9fd902c8c3023b15bc2fd79"



class TestClassNameId:
    def test_several_calls_with_objects_of_the_same_class(self):
        class MyClass:
            pass

        id_plugin = ClassNameId()
        assert id_plugin(MyClass()) == "MyClass"
        assert id_plugin(MyClass()) == "MyClass"


class TestNumberedClassNameId:
    def test_several_calls_with_new_objects(self):
        class MyClass:
            pass

        id_plugin = NumberedClassNameId()
        assert id_plugin(MyClass()) == "MyClass_0"
        assert id_plugin(MyClass()) == "MyClass_1"
        assert id_plugin(MyClass()) == "MyClass_2"

    def test_several_calls_same_object(self):
        class MyClass:
            pass

        id_plugin = NumberedClassNameId()
        my_class_0 = MyClass()
        assert id_plugin(my_class_0) == "MyClass_0"
        assert id_plugin(MyClass()) == "MyClass_1"
        assert id_plugin(my_class_0) == "MyClass_0"


class TestDynamicDependencyPlugin:
    @pytest.fixture
    def dependency_plugin(self):
        return DynamicDependencyPlugin(Task)

    def test_get_dependencies_from_list_empty(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        assert dependency_plugin._get_dependencies_from_list([]) == []

    def test_get_dependencies_from_list_one_task(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task = Task()
        assert dependency_plugin._get_dependencies_from_list([task]) == [task]

    def test_get_dependencies_from_list_multiple_tasks(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task1 = Task()
        task2 = Task()
        assert dependency_plugin._get_dependencies_from_list(
            [task1, task2]
        ) == [
            task1,
            task2,
        ]

    def test_get_dependencies_from_list_multiple_tasks_keep_order(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task1 = Task()
        task2 = Task()
        assert dependency_plugin._get_dependencies_from_list(
            [task2, task1]
        ) == [
            task2,
            task1,
        ]

    def test_get_dependencies_from_list_with_other_types(
        self, dependency_plugin: DynamicDependencyPlugin, caplog
    ):
        with caplog.at_level(logging.WARNING):
            assert (
                dependency_plugin._get_dependencies_from_list([1, object()])
                == []
            )
            assert len(caplog.records) == 0

    def test_get_dependencies_from_list_mixed_objects(
        self, dependency_plugin: DynamicDependencyPlugin, caplog
    ):
        task1 = Task()
        task2 = Task()
        with caplog.at_level(logging.WARNING):
            assert dependency_plugin._get_dependencies_from_list(
                [task1, 1, task2, object()]
            ) == [task1, task2]
            assert len(caplog.records) == 2
            assert [i.message for i in caplog.records] == [
                "Object of type <class 'int'> is not a task and will be ignored."
                " This happens when a list contains tasks mixed with other type of objects.",
                "Object of type <class 'object'> is not a task and will be ignored."
                " This happens when a list contains tasks mixed with other type of objects.",
            ]

    def test_get_dependencies_from_list_nested_structure(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task1 = Task()
        task2 = Task()
        task3 = Task()
        assert dependency_plugin._get_dependencies_from_list(
            [[task1, task2], task3, {task1: task2}]
        ) == [task3]

    def test_get_dependencies_from_dict_empty(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        assert dependency_plugin._get_dependencies_from_dict({}) == []

    def test_get_dependencies_from_dict_one_task_as_value(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task = Task()
        assert dependency_plugin._get_dependencies_from_dict(
            {"value": task}
        ) == [task]

    def test_get_dependencies_from_dict_one_task_as_key(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task = Task()
        assert (
            dependency_plugin._get_dependencies_from_dict({task: "value"}) == []
        )

    def test_get_dependencies_from_dict_multiple_tasks(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task1 = Task()
        task2 = Task()
        assert dependency_plugin._get_dependencies_from_dict(
            {"value1": task1, "value2": task2}
        ) == [
            task1,
            task2,
        ]

    def test_get_dependencies_from_dict_multiple_tasks_keep_order(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task1 = Task()
        task2 = Task()
        assert dependency_plugin._get_dependencies_from_dict(
            {"value2": task2, "value1": task1}
        ) == [
            task2,
            task1,
        ]

    def test_get_dependencies_from_dict_with_other_types(
        self, dependency_plugin: DynamicDependencyPlugin, caplog
    ):
        with caplog.at_level(logging.WARNING):
            assert (
                dependency_plugin._get_dependencies_from_dict(
                    {"value1": 1, "value2": object()}
                )
                == []
            )
            assert len(caplog.records) == 0

    def test_get_dependencies_from_dict_mixed_objects(
        self, dependency_plugin: DynamicDependencyPlugin, caplog
    ):
        task1 = Task()
        task2 = Task()
        with caplog.at_level(logging.WARNING):
            assert dependency_plugin._get_dependencies_from_dict(
                {
                    "value1": task1,
                    "value2": 1,
                    "value3": task2,
                    "value4": object(),
                }
            ) == [task1, task2]
            assert len(caplog.records) == 2
            assert [i.message for i in caplog.records] == [
                "Object of type <class 'int'> is not a task and will be ignored."
                " This happens when a dictionary contains tasks mixed with other type of objects.",
                "Object of type <class 'object'> is not a task and will be ignored."
                " This happens when a dictionary contains tasks mixed with other type of objects.",
            ]

    def test_get_dependencies_from_dict_nested_structure(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task1 = Task()
        task2 = Task()
        task3 = Task()
        assert dependency_plugin._get_dependencies_from_dict(
            {"value1": [task1], "value2": task3, "value3": {task1: task2}}
        ) == [task3]

    def test_get_dependencies_from_attributes_simple_attribute(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task1 = Task()

        class TestTask(Task):
            def __init__(self):
                self.attribute = task1

        assert dependency_plugin(TestTask()) == [task1]

    def test_get_dependencies_from_attributes_list(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task1 = Task()

        class TestTask(Task):
            def __init__(self):
                self.attribute = [task1]

        assert dependency_plugin(TestTask()) == [task1]

    def test_get_dependencies_from_attributes_dict(
        self, dependency_plugin: DynamicDependencyPlugin
    ):
        task1 = Task()

        class TestTask(Task):
            def __init__(self):
                self.attribute = {"tuberia": task1}

        assert dependency_plugin(TestTask()) == [task1]


class TestMethodDependencyPlugin:
    def test_get_dependencies_from_method_that_returns_a_list(self):
        task1 = Task()

        class TestTask(Task):
            def get_dependencies(self):
                return [task1]

        assert MethodDependencyPlugin(Task)(TestTask()) == [task1]

    def test_get_dependencies_from_method_that_yields_a_task(self):
        task1 = Task()

        class TestTask(Task):
            def get_dependencies(self):
                yield task1

        assert MethodDependencyPlugin(Task)(TestTask()) == [task1]

    def test_get_dependencies_from_method_that_returns_one_task(self):
        task1 = Task()

        class TestTask(Task):
            def get_dependencies(self):
                return task1

        assert MethodDependencyPlugin(Task)(TestTask()) == [task1]

    def test_get_dependencies_from_method_that_returns_a_invalid_value(self):
        task1 = Task()

        class InvalidTask(Task):
            def get_dependencies(self):
                return {"value": task1}

        with pytest.raises(
            ValueError, match="should return only objects of type `Task`"
        ):
            MethodDependencyPlugin(Task)(InvalidTask())

    def test_it_works_with_different_type_and_name(self):
        class MyTask:
            pass

        my_task = MyTask()

        class TestTask(Task):
            def get_my_dependencies(self):
                return my_task

        assert MethodDependencyPlugin(MyTask, "get_my_dependencies")(
            TestTask()
        ) == [my_task]


class TestGetDependencies:
    def test_get_dependencies_with_task_objects(self):
        my_task_0 = Task()

        class MyTask1(Task):
            def __init__(self):
                self.attribute = my_task_0

        assert get_dependencies(MyTask1()) == [my_task_0]

    def test_get_dependencies_with_custom_task_class(self):
        class MyTask:
            pass

        class MyTask0(MyTask):
            pass

        class MyTask1:
            pass

        my_task_0 = MyTask0()
        my_task_1 = MyTask1()

        class MyTask2:
            dependency_plugin = DynamicDependencyPlugin(MyTask)

            def __init__(self):
                self.attribute_task = my_task_0
                self.attribute_obj = my_task_1

        assert get_dependencies(MyTask2()) == [my_task_0]
