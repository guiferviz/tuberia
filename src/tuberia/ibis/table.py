from typing import Type

import ibis
import inflection
import pydantic
from ibis.expr.types.core import Expr

from tuberia import schema
from tuberia.base_model import BaseModel
from tuberia.task import Task


def default_name(type_table: Type) -> str:
    name = type_table.__name__
    name = inflection.underscore(name)
    return name


class Table(Task, BaseModel):
    database: str
    name: str = ""
    prefix_name: str = ""
    suffix_name: str = ""
    full_name: str = ""
    _connection: ibis.BaseBackend = None  # type: ignore

    class schema:
        pass

    @pydantic.validator("name", always=True)
    def default_name(cls, name):
        if not name:
            name = default_name(cls)
        return name

    @pydantic.validator("full_name", always=True)
    def defaul_full_name(cls, value, values):
        database = values.get("database")
        name = values.get("name")
        prefix = values.get("prefix_name")
        suffix = values.get("suffix_name")
        if (
            not value
            and database
            and name
            and prefix is not None
            and suffix is not None
        ):
            value = f"{database}.{prefix}{name}{suffix}"
        return value

    @property
    def ibis_schema(self) -> ibis.Schema:
        schema.to_ibis(self.schema)

    def define(self) -> Expr:
        raise NotImplementedError()
