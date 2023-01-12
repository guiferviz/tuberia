import inspect
from typing import Any, List, Optional

import pydantic
from pydantic.class_validators import (
    ROOT_VALIDATOR_CONFIG_KEY,
    VALIDATOR_CONFIG_KEY,
)
from typing_extensions import dataclass_transform


def get_annotations(type_object):
    if getattr(inspect, "get_annotations", None):
        return inspect.get_annotations(type_object)
    return getattr(type_object, "__annotations__", {})


def is_private_attribute_name(name):
    return name.startswith("_")


def is_property(member):
    return isinstance(member, property)


def get_attributes(
    type_object, properties=True, only_properties_with_set=False, reorder=True
):
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
            if reorder:
                if k in attributes:
                    del attributes[k]
            attributes[k] = (v, superclass_vars.get(k, ...))
        for i in inspect.getmembers(superclass, is_property):
            property_name = i[0]
            if is_private_attribute_name(property_name):
                continue
            return_type = get_annotations(i[1].fget).get("return", Any)
            if property_name in attributes and attributes[property_name] != (
                return_type,
                property,
            ):
                del attributes[property_name]
            if properties and (
                not only_properties_with_set or i[1].fset is not None
            ):
                attributes[property_name] = (return_type, property)
    return attributes


def get_validators(
    type_object, root=False, attributes: Optional[List[str]] = None
):
    validators = {}
    for superclass in reversed(inspect.getmro(type_object)):
        for k, v in vars(superclass).items():
            validator_config_key = (
                ROOT_VALIDATOR_CONFIG_KEY if root else VALIDATOR_CONFIG_KEY
            )
            validator = getattr(v, validator_config_key, None)
            if validator and root:
                validators[k] = validator
            elif validator and not root:
                if attributes is not None:
                    validator = list(validator)
                    validator[0] = tuple(
                        i for i in validator[0] if i in attributes
                    )
                    validator = tuple(validator)
                if validator[0]:
                    validators[k] = validator
    return validators


class Config:
    arbitrary_types_allowed: bool = True


@dataclass_transform(kw_only_default=True)
class BaseModel:
    __pydantic_model__: pydantic.BaseModel

    def __init__(self, **kwargs):
        attributes = get_attributes(self.__class__, properties=False)
        validators = get_validators(
            self.__class__, attributes=list(attributes.keys())
        )
        validators = {
            k: pydantic.validator(*v[0], allow_reuse=True, always=v[1].always)(
                v[1].func
            )
            for k, v in validators.items()
        }
        root_validators = get_validators(self.__class__, root=True)
        root_validators = {
            k: pydantic.root_validator(allow_reuse=True)(v.func)
            for k, v in root_validators.items()
        }
        validators.update(root_validators)
        self.__pydantic_model__ = pydantic.create_model(
            self.__class__.__name__,
            **{k: v for k, v in attributes.items() if v[1] != property},
            __validators__=validators,
            __config__=Config,  # type: ignore
        )(**{k: v for k, v in kwargs.items()})
        for i, (_, value) in get_attributes(
            self.__class__, only_properties_with_set=True
        ).items():
            try:
                validated_value = getattr(self.__pydantic_model__, i)
            except AttributeError as error:
                if i in kwargs:
                    validated_value = kwargs[i]
                elif value == property:
                    continue
                else:
                    raise error
            setattr(self, i, validated_value)
