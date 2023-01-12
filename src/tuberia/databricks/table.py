from __future__ import annotations

import re
from typing import List, Optional, Type

import inflection
import pydantic
import pyspark.sql.types as T
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from tuberia import schema
from tuberia.base_model import BaseModel
from tuberia.databricks.expectation import Expectation
from tuberia.databricks.settings import TuberiaDatabricksSettings
from tuberia.spark import get_spark
from tuberia.task import Task


def format_name(name, task: Task):
    return name.format(vars(task))


def default_name(type_table: Type) -> str:
    name = type_table.__name__
    if TuberiaDatabricksSettings().default_table_name_underscore:
        name = inflection.underscore(name)
    return name


def validate_full_name(full_name: str):
    if not re.fullmatch(r"\w+.\w+", full_name):
        raise ValueError(f"Invalid table full name: {full_name}")


class Table(Task, BaseModel):
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

    database: str
    name: str = ""
    prefix_name: str = ""
    suffix_name: str = ""
    path: Optional[str] = None

    class schema:
        pass

    @pydantic.validator("name", always=True)
    def default_name(cls, value):
        if not value:
            value = default_name(cls)
        return value

    """
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
    """

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
        expectations = []
        for _, v in vars(self.schema).items():
            if isinstance(v, Expectation):
                expectations.append(v)
        return expectations

    def run(self):
        logger.info(f"Running table `{self.full_name}`")
        df = self.df()
        self.write(df)
        for i in self.expect():
            logger.info(f"Running expectation `{i}`")
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

    @property
    def pyspark_schema(self) -> T.StructType:
        return schema.to_pyspark(self.schema)
