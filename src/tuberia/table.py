from typing import Optional

import prefect
import pydantic


class MetaTable(pydantic.main.ModelMetaclass):
    def __new__(cls, name, bases, dct):
        return super().__new__(cls, name, bases, dct)


class Table(pydantic.BaseModel, metaclass=MetaTable):
    class Config:
        arbitrary_types_allowed = True

    database: str
    prefix_name: str = ""
    name: str
    suffix_name: str = ""
    path: Optional[str] = None

    @property
    def full_name(self) -> str:
        return (
            f"{self.database}.{self.prefix_name}{self.name}{self.suffix_name}"
        )

    @property
    def id(self) -> str:
        return self.full_name

    def create(self):
        df = self.define()
        self.write(df)

    def define(self):
        raise NotImplementedError()

    def write(self, df):
        writer = df.write
        if self.path:
            writer = writer.option("path", f"{self.path}/{self.name}")
        writer.saveAsTable(self.full_name)


class TableTask(prefect.Task):
    def __init__(self, table: Table, **kwargs):
        super().__init__(name=table.full_name, **kwargs)
        self.table = table

    def run(self) -> Table:
        self.table.create()
        return self.table
