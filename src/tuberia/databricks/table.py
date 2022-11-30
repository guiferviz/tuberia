from __future__ import annotations

import inspect
import re
from typing import List, Optional, Type

import inflection
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from tuberia.databricks.expectation import Expectation
from tuberia.databricks.settings import TuberiaDatabricksSettings
from tuberia.schema import column
from tuberia.spark import get_spark
from tuberia.task import Task


def format_name(name, task: Task):
    return name.format(vars(task))


def default_name(table: Table) -> str:
    name = table.__class__.__name__
    if TuberiaDatabricksSettings().default_table_name_underscore:
        name = inflection.underscore(name)
    return name


def validate_full_name(full_name: str):
    if not re.fullmatch(r"\w+.\w+", full_name):
        raise ValueError(f"Invalid table full name: {full_name}")


def python_type_to_pyspark_type(python_type: type) -> Type[T.DataType]:
    try:
        # Ignoring linter error, if the types is not in the mappings the error
        # will be catch.
        return T._type_mappings[python_type]  # type: ignore
    except KeyError as err:
        raise KeyError(
            f"Unknown translation from python type `{python_type}` to pyspark type"
        ) from err


class Table(Task):
    """A PySpark table.

    Attributes:
        database: Database name.
        name: Table name.
        prefix_name: Prefix that will be prepended to the table name.
        suffix_name: Suffix that will be appended to the table name.
        path: Optional path to the root directory that will be used to save the
            table files.
        full_name: Database and table name with prefix and suffix. The full
            name has the form `{database}.{prefix}{name}{suffix}`.
        full_path: The default full path has the form
            `{path}/{database}/{prefix}{name}{suffix}`.
        spark: Easy way to access the spark session.

    """

    class schema:
        pass

    database: str
    name: str
    prefix_name: str = ""
    suffix_name: str = ""
    path: Optional[str] = None

    def __init__(
        self,
        database: str,
        name: Optional[str] = None,
        prefix_name: str = "",
        suffix_name: str = "",
        path: Optional[str] = None,
    ):
        if name is None:
            name = default_name(self)
        self.database = database
        self.name = name
        self.prefix_name = prefix_name
        self.suffix_name = suffix_name
        self.path = path
        validate_full_name(self.full_name)

    @property
    def full_name(self) -> str:
        return (
            f"{self.database}.{self.prefix_name}{self.name}{self.suffix_name}"
        )

    @property
    def full_path(self) -> str:
        return f"{self.path}/{self.database}/{self.prefix_name}{self.name}{self.suffix_name}"

    @property
    def spark(self) -> SparkSession:
        return get_spark()

    def expect(self) -> List[Expectation]:
        return []

    def run(self):
        df = self.df()
        self.write(df)
        for i in self.expect():
            report = i.run(self)
            if not report.success:
                raise RuntimeError(f"Expectation failed: {report}")

    def df(self):
        raise NotImplementedError()

    def read(self) -> DataFrame:
        return self.spark.table(self.full_name)

    def write(self, df):
        writer = df.write.format("delta")
        if self.path:
            writer = writer.option("path", self.full_path)
        writer.saveAsTable(self.full_name)

    def pyspark_schema(self) -> T.StructType:
        struct_fields: List[T.StructField] = []
        for _, v in vars(self.schema).items():
            if isinstance(v, column):
                pyspark_type = v.dtype
                if inspect.isclass(pyspark_type) and issubclass(
                    pyspark_type, T.DataType
                ):
                    pyspark_type = pyspark_type()
                elif not isinstance(pyspark_type, T.DataType):
                    pyspark_type = python_type_to_pyspark_type(pyspark_type)()
                struct_fields.append(T.StructField(v, pyspark_type))
        return T.StructType(struct_fields)
