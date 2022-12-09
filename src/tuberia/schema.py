import inspect
from typing import Any, List, Optional, Type

import ibis
import pyspark.sql.types as T
from varname import varname


class Column(str):
    def __new__(
        cls,
        dtype: Any = str,
        alias: Optional[str] = None,
        key: bool = False,
    ):
        if alias is None:
            alias = str(varname())
        return str.__new__(cls, alias)

    def __init__(
        self,
        dtype: Any = str,
        alias: Optional[str] = None,
        key: bool = False,
    ):
        self.dtype = dtype
        self.key = key


def to_ibis(schema: type) -> ibis.Schema:
    columns = {}
    for _, v in vars(schema).items():
        if isinstance(v, Column):
            columns[v] = v.dtype
    return ibis.Schema.from_dict(columns)


def to_pyspark(schema: type) -> T.StructType:
    struct_fields: List[T.StructField] = []
    for _, v in vars(schema).items():
        if isinstance(v, Column):
            pyspark_type = v.dtype
            if inspect.isclass(pyspark_type) and issubclass(
                pyspark_type, T.DataType
            ):
                pyspark_type = pyspark_type()
            elif not isinstance(pyspark_type, T.DataType):
                pyspark_type = python_type_to_pyspark_type(pyspark_type)()
            struct_fields.append(T.StructField(v, pyspark_type))
    return T.StructType(struct_fields)


def python_type_to_pyspark_type(python_type: type) -> Type[T.DataType]:
    try:
        # Ignoring linter error, if the types is not in the mappings the error
        # will be catch.
        return T._type_mappings[python_type]  # type: ignore
    except KeyError as err:
        raise KeyError(
            f"Unknown translation from python type `{python_type}` to pyspark type"
        ) from err
