from __future__ import annotations

from typing import Any, Dict, List, Optional

import inflection
import prefect
import pydantic
from pyspark.sql import DataFrame

from tuberia.expectation import Expectation
from tuberia.spark import get_spark
from tuberia.utils import freeze


class MetaTable(pydantic.main.ModelMetaclass):
    def __new__(cls, name, bases, dct):
        table_object = super().__new__(cls, name, bases, dct)
        return table_object


class Table(pydantic.BaseModel, metaclass=MetaTable):
    class Config:
        arbitrary_types_allowed = True

    database: str
    prefix_name: str = ""
    name: str = None  # type: ignore
    suffix_name: str = ""
    path: Optional[str] = None

    schema_: Optional[Any] = None
    # FIXME: It uses an underscore because schema already exist in pydantic.BaseModel.

    @pydantic.validator("name", always=True)
    def default_name(cls, name):
        if name is None:
            return inflection.underscore(cls.__name__)  # type: ignore
        return name

    @property
    def full_name(self) -> str:
        return (
            f"{self.database}.{self.prefix_name}{self.name}{self.suffix_name}"
        )

    @property
    def id(self) -> str:
        return self.full_name

    def expect(self) -> List[Expectation]:
        return []

    def run(self):
        df = self.define()
        self.write(df)
        for i in self.expect():
            report = i.run(self)
            if not report.success:
                raise RuntimeError(f"Expectation failed: {report}")

    def define(self):
        raise NotImplementedError()

    def read(self) -> DataFrame:
        return get_spark().table(self.full_name)

    def write(self, df):
        writer = df.write.format("delta")
        if self.path:
            writer = writer.option(
                "path",
                f"{self.path}/{self.database}/{self.prefix_name}{self.name}{self.suffix_name}",
            )
        writer.saveAsTable(self.full_name)

    def freeze(self):
        return freeze(self)

    def _dependencies(self) -> List[Table]:
        dependencies: Dict[Any, Table] = {}
        for _, v in self:
            if isinstance(v, Table):
                dependencies[v.freeze()] = v
            elif isinstance(v, List) and len(v) > 0 and isinstance(v[0], Table):
                for i in v:
                    dependencies[i.freeze()] = i
            elif (
                isinstance(v, Dict)
                and len(v) > 0
                and isinstance(next(v.values().__iter__()), Table)
            ):
                for _, vv in v:
                    dependencies[vv.freeze()] = vv
        return list(dependencies.values())


class TableTask(prefect.Task):
    def __init__(self, table: Table, **kwargs):
        super().__init__(name=table.full_name, **kwargs)
        self.table = table

    def run(self) -> Table:
        self.table.run()
        return self.table
