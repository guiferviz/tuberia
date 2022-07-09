from typing import Any, Callable, Optional, Union, overload

import prefect
import pydantic
from makefun import wraps
from prefect.core.task import _validate_run_signature
from prefect.tasks.core.function import FunctionTask
from pydantic.typing import NoneType
from pyspark.sql import DataFrame


class Table(pydantic.BaseModel):
    database: str
    name: str

    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.name}"


class TableTask(prefect.Task):
    def run(self, **_: Any) -> Table:
        raise NotImplementedError()


class TableFunctionTask(TableTask, FunctionTask):
    def __init__(self, fun: Callable[..., Table], **kwargs):
        super().__init__(fun, **kwargs)


class DataFrameTableTask(TableTask):
    def get_table_name(self, **kwargs) -> str:
        return self.name.format(**kwargs)

    def define(self, **_: Any) -> DataFrame:
        raise NotImplementedError()

    def persist(self, _: DataFrame) -> Table:
        raise NotImplementedError()

    def validate(self, _: Table):
        pass

    def run(self, **kwargs: Any) -> Table:
        df = self.define(**kwargs)
        table = self.persist(df)
        self.validate(table)
        return table


class DataFrameTableFunctionTask(TableTask):
    def __init__(
        self,
        define: Optional[Callable[..., DataFrame]] = None,
        persist: Optional[Callable[[DataFrame], Table]] = None,
        validate: Optional[Callable[[Table], NoneType]] = None,
        **kwargs: Any,
    ):
        if define is not None:
            _validate_run_signature(define)
            self.run = wraps(define)(self.run)
            self.define = define
        if persist is not None:
            self.persist = persist
        if validate is not None:
            self.validate = validate
        super().__init__(**kwargs)


# Taken from prefect/utilities/tasks.py:
# To support type checking with optional arguments to `table`, we need to make
# use of `typing.overload`
@overload
def table(fun: Callable[..., Table]) -> TableFunctionTask:  # type: ignore
    pass


@overload
def table(
    **task_init_kwargs: Any,
) -> Callable[[Callable[..., Table]], TableFunctionTask]:  # type: ignore
    pass


def table(  # type: ignore
    fun: Callable[..., Table] = None, **task_init_kwargs: Any
) -> Union[
    TableFunctionTask, Callable[[Callable[..., Table]], TableFunctionTask]
]:
    if fun is None:
        return lambda fun: TableFunctionTask(fun=fun, **task_init_kwargs)
    return TableFunctionTask(fun=fun, **task_init_kwargs)
