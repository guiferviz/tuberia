import inspect
from typing import Any

import pydantic
from pydantic.class_validators import (
    ROOT_VALIDATOR_CONFIG_KEY,
    VALIDATOR_CONFIG_KEY,
)


def get_annotations(type_object):
    if getattr(inspect, "get_annotations", None):
        return inspect.get_annotations(type_object)
    return getattr(type_object, "__annotations__", {})


def is_private_attribute_name(name):
    return name.startswith("_")


def is_property(member):
    return isinstance(member, property)


def get_attributes(type_object):
    """Return all attributes defined in `task_class` and all superclasses.

    The output is a dictionary with the name of the attribute as key and a
    tuple as value. The tuple contains two elements, the data type of the
    attribute and the default value defined in the class definition.

    """
    attributes = {}
    for superclass in reversed(inspect.getmro(type_object)):
        superclass_vars = vars(superclass)
        for k, v in get_annotations(superclass).items():
            if is_private_attribute_name(k):
                continue
            if k in attributes:
                del attributes[k]
            attributes[k] = (v, superclass_vars.get(k, ...))
        for i in inspect.getmembers(superclass, is_property):
            property_name = i[0]
            return_type = get_annotations(i[1].fget).get("return", Any)
            attributes[property_name] = (return_type, property)
    return attributes


def get_validators(type_object):
    validators = {}
    for superclass in reversed(inspect.getmro(type_object)):
        for k, v in vars(superclass).items():
            validator = getattr(v, VALIDATOR_CONFIG_KEY, None)
            root_validator = getattr(v, ROOT_VALIDATOR_CONFIG_KEY, None)
            validator = validator or root_validator
            if validator:
                validators[k] = validator
    return validators


class BaseModel:
    __pydantic_model__: pydantic.BaseModel

    def __init__(self, **kwargs):
        attributes = get_attributes(self.__class__)
        validators = get_validators(self.__class__)
        validators = {
            k: pydantic.validator(*v[0], allow_reuse=True, always=v[1].always)(
                v[1].func
            )
            for k, v in validators.items()
        }
        self.__pydantic_model__ = pydantic.create_model(
            self.__class__.__name__,
            **{k: v for k, v in attributes.items() if v[1] != property},
            __validators__=validators,
        )(**{k: v for k, v in kwargs.items()})
        for i in attributes:
            setattr(self, i, getattr(self.__pydantic_model__, i))
