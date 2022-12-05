from typing import Any, Optional

import pydantic
import pytest

from tuberia.base_model import BaseModel, get_attributes, get_validators


def test_get_attributes_ignores_private():
    """get_attributes ignores private and dunder methods."""

    class A:
        a: str = "hi"
        _a: str = "bye"
        __dunder__: int = 1

    assert get_attributes(A) == dict(
        a=(str, "hi"),
    )


def test_get_attributes_in_order():
    """get_attributes should return fields in the right order.

    The right order is the order in which they where defined in the inheritance
    tree. `inspect.getmembers` return the fields sorted by name, so it is
    better to use `vars` because it keeps the definition order.

    """

    class A:
        a: str = "hi"

    class B(A):
        b: int = 2

    class C(B):
        c1: float = 1.0
        c0: float = 0.0

    assert list(get_attributes(C).items()) == list(
        dict(
            a=(str, "hi"),
            b=(int, 2),
            c1=(float, 1.0),
            c0=(float, 0.0),
        ).items()
    )


def test_get_attributes_fields_without_value():
    """get_attributes should work with fields with no default value.

    This test invalidates an implementation of `get_attributes` that only uses
    `vars(type_object)` because it only returns values defined in the class. We
    also need to use `__annotations__` to get all the fields without value.

    Values without a value should have an `...` in the output dict. Using None
    can be confusing because a default value of None is different from not
    having any value.

    """

    class A:
        a: Optional[str] = None

    class B(A):
        b: int

    class C(B):
        c1: float
        c0: float

    assert list(get_attributes(C).items()) == list(
        dict(
            a=(Optional[str], None),
            b=(int, ...),
            c1=(float, ...),
            c0=(float, ...),
        ).items()
    )


def test_get_attributes_fields_in_order_when_overload():
    """get_attributes should reorder fields when overloaded in subclasses."""

    class A:
        a: str = "a"
        value: str = "a"

    class B(A):
        b: str = "b"
        value: str = "b"

    assert list(get_attributes(B).items()) == list(
        dict(
            a=(str, "a"),
            b=(str, "b"),
            value=(str, "b"),
        ).items()
    )


def test_get_attributes_works_when_datatype_overwrite():
    """get_attributes works when fields are overloaded with different types."""

    class A:
        value: str = "a"

    class B(A):
        value: int = 1

    assert list(get_attributes(B).items()) == list(
        dict(
            value=(int, 1),
        ).items()
    )


def test_get_attributes_return_properties_with_annotations():
    class A:
        value: str = "a"

        @property
        def property_value(self) -> int:
            return 3

    assert list(get_attributes(A).items()) == list(
        dict(
            value=(str, "a"),
            property_value=(int, property),
        ).items()
    )


def test_get_attributes_return_properties_without_annotations():
    class A:
        @property
        def property_value(self):
            return 3

    assert list(get_attributes(A).items()) == list(
        dict(
            property_value=(Any, property),
        ).items()
    )


def test_get_attributes_return_properties_at_the_end():
    class A:
        @property
        def property_value(self):
            return 3

        a: str = "a"

    class B(A):
        b: str = "b"

    assert list(get_attributes(B).items()) == list(
        dict(
            a=(str, "a"),
            property_value=(Any, property),
            b=(str, "b"),
        ).items()
    )


def test_get_validators():
    class A:
        a: str = "a"

        @pydantic.validator("a")
        def validate_a(cls, value):
            return value

    validators = get_validators(A)
    assert len(validators) == 1
    assert list(validators.keys()) == ["validate_a"]
    assert [i[0] for i in validators.values()] == [("a",)]


class TestsBaseModel:
    def test_create_model_with_default_values(self):
        class MyBaseModel(BaseModel):
            a: int = 1
            b: str = "b"

        model = MyBaseModel()
        assert model.a == 1
        assert model.b == "b"

    def test_create_model_overwriting_default_values(self):
        class MyBaseModel(BaseModel):
            a: int = 1
            b: str = "b"

        model = MyBaseModel(b="B")
        assert model.a == 1
        assert model.b == "B"

    def test_create_model_with_required_values(self):
        class MyBaseModel(BaseModel):
            a: int = 1
            b: str

        model = MyBaseModel(b="b")
        assert model.a == 1
        assert model.b == "b"

    def test_create_model_fails_with_wrong_data_types(self):
        class MyBaseModel(BaseModel):
            a: int = 1

        with pytest.raises(
            pydantic.ValidationError, match="value is not a valid integer"
        ) as exception:
            MyBaseModel(a="a")  # type: ignore
        errors = exception.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "type_error.integer"

    def test_create_model_fails_if_not_required_values_provided(self):
        class MyBaseModel(BaseModel):
            a: int

        with pytest.raises(pydantic.ValidationError) as exception:
            MyBaseModel()  # type: ignore
        errors = exception.value.errors()
        assert len(errors) == 1
        assert errors[0]["type"] == "value_error.missing"

    @pytest.mark.xfail(strict=True)
    def test_no_error_if_overwrite_schema_attribute(self):
        class MyBaseModel(BaseModel):
            schema: int = 1
            json: str = ""

        MyBaseModel()

    def test_type_annotations(self):
        class MyBaseModel(BaseModel):
            a: int

        MyBaseModel(a="1", b=3)

    def test_validators_are_working_fine(self):
        class MyBaseModel(BaseModel):
            a: str

            @pydantic.validator("a")
            def upper_case_a(cls, value):
                return value.upper()

        model = MyBaseModel(a="hi!")
        assert model.a == "HI!"

    def test_validators_are_working_fine_with_subclasses(self):
        class MyBaseModel(BaseModel):
            name: str

            @pydantic.validator("name", always=True)
            def format_name(cls, value, values):
                return value.format(**values)

        class MySubBaseModel(MyBaseModel):
            country: str = "esp"
            name: str = "my_name_{country}"

        model = MySubBaseModel()
        assert model.name == "my_name_esp"

    def test_root_validators_are_working_fine(self):
        class MyBaseModel(BaseModel):
            name: str

            @pydantic.root_validator()
            def format_name(cls, values):
                if "name" in values:
                    values["name"] = values.get("name").format(**values)
                return values

        class MySubBaseModel(MyBaseModel):
            country: str = "esp"
            name: str = "my_name_{country}"

        model = MySubBaseModel()
        assert model.name == "my_name_esp"

    def test_create_attributes_in_init(self):
        class MyBaseModel(BaseModel):
            name: str = pydantic.Field(default_factory=lambda: "dynamic")

            def __init__(self, name_init: str):
                # If we do not call super init here name would be a class
                # attribute instead of the string "dynamic".
                super().__init__()
                self.name_init = name_init

        model = MyBaseModel("name_init")
        assert model.name == "dynamic"
        assert model.name_init == "name_init"

    def test_create_property_in_subclass_with_validator(self):
        class MySuper(BaseModel):
            name: str = "super"

            @pydantic.validator("name", always=True)
            def format_name(cls, value):
                return value.upper()

        class MyBase(MySuper):
            @property
            def name(self):
                return "base"

        model = MyBase(name="init")
        assert model.name == "base"

    def test_create_property_in_subclass_with_value_overwrite(self):
        class MySuper(BaseModel):
            name: str = "super"

            @pydantic.validator("name", always=True)
            def format_name(cls, value):
                return value.upper()

        class MyBase(MySuper):
            _name: Optional[str] = None

            @property
            def name(self):
                if self._name is not None:
                    return self._name
                return "base"

            @name.setter
            def name(self, name):
                self._name = name

        model = MyBase(name="init")
        assert model.name == "init"
        model = MyBase()
        assert model.name == "base"

    def test_linter_not_complain_about_args_with_defaults(self):
        class MySuper(BaseModel):
            name0: str = "name0"

        class _(MySuper):
            # If dataclass_transform does not have kw_only_default=True the
            # next line will raise a linter error.
            name1: str
