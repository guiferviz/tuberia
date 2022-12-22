import pytest

from tuberia.new_task import FunctionId, NumberedClassNameId, get_id


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
