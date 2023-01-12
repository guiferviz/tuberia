from datetime import timedelta
from typing import Optional

import pydantic
from prefect import Task, flow, task
from prefect.results import LiteralResult
from prefect.task_runners import BaseTaskRunner, TaskConcurrencyType
from prefect.tasks import task_input_hash


class MockTaskRunner(BaseTaskRunner):
    def __init__(self):
        super().__init__()
        self.register = {}

    @property
    def concurrency_type(self) -> TaskConcurrencyType:
        return TaskConcurrencyType.SEQUENTIAL

    async def submit(self, key, call) -> None:
        self.register[key] = call

    async def wait(self, key, timeout: float = None):
        return LiteralResult(value=self.register[key])


class Quarter(pydantic.BaseModel):
    year: int
    quarter: int

    def previous(self):
        if self.quarter == 1:
            return Quarter(year=self.year - 1, quarter=4)
        return Quarter(year=self.year, quarter=self.quarter - 1)


class MetaTask(type):
    def __call__(cls, *args, **kwargs):
        kwargs_defaults = dict(
            cache_key_fn=task_input_hash,
            cache_expiration=timedelta(seconds=20),
        )
        kwargs_defaults.update(kwargs)
        obj = super().__call__(*args, **kwargs_defaults)
        obj.cache_key_fn = lambda a, b: obj.name
        obj.submit()
        return obj


class Database(Task, metaclass=MetaTask):
    def __init__(self, name: str, path: Optional[str] = None, **kwargs):
        super().__init__(fn=self.run, **kwargs)
        self.name = name
        self.path = path

    def run(self):
        print(f"creating {self.name} database")
        from time import sleep

        sleep(3)

    def __repr__(self) -> str:
        return f"Database({self.name})"


class Table(Task, metaclass=MetaTask):
    def __init__(
        self, database: Database, name: Optional[str] = None, **kwargs
    ):
        name = name or self.__class__.__name__
        super().__init__(fn=self.run, name=name, **kwargs)
        self.database = database

    def run(self):
        df = self.df()
        print(f"creating table {self.name} in {self.database}")
        self.write(df)

    def df(self):
        pass

    def write(self, df):
        pass


class TableA(Table):
    def df(self):
        print("creating df transformations for TableA")


class TableB(Table):
    def df(self):
        print("creating df transformations for TableB")


class TableC(Table):
    def __init__(self, table_a: TableA, table_b: TableB, **kwargs):
        super().__init__(**kwargs)
        self.table_a = table_a
        self.table_b = table_b

    def df(self):
        print("creating df transformations for TableC")


class TableD(Table):
    def df(self):
        print("creating df transformations for TableD")


@flow()
def my_sub_flow():
    database = Database("my_sub_database")
    table_d = TableD(database)
    return table_d


@flow()
def my_flow():
    database = Database("my_database")
    table_a = TableA(database)
    table_b = TableB(database)
    table_c = TableC(table_a, table_b, database=database)
    my_sub_flow()
    return table_a, table_b, table_c


def main():
    breakpoint()
    my_mocked_flow = my_flow
    my_mocked_flow.task_runner = MockTaskRunner()
    # my_mocked_sub_flow = my_sub_flow
    # my_mocked_sub_flow.task_runner = MockTaskRunner()
    state = my_mocked_flow(return_state=True)


if __name__ == "__main__":
    main()
