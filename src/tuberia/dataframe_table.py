from typing import Any, Callable, Optional, Union, overload

from makefun import wraps
from prefect.core.task import _validate_run_signature
from pyspark.sql import DataFrame

from tuberia.table import Table, TableTask


class DataFrameTableTask(TableTask):
    def get_table_name(self, **kwargs) -> str:
        return self.name.format(**kwargs)

    def define(self, **_: Any) -> DataFrame:
        raise NotImplementedError()

    def persist(self, _: DataFrame, /) -> Table:
        raise NotImplementedError()

    def validate(self, _: Table, /) -> None:
        pass

    def run(self, **kwargs: Any) -> Table:
        df = self.define(**kwargs)
        table = self.persist(df)
        self.validate(table)
        return table


class DataFrameTableFunctionTask(DataFrameTableTask):
    def __init__(
        self,
        define: Optional[Callable[..., DataFrame]] = None,
        persist: Optional[Callable[[DataFrame], Table]] = None,
        validate: Optional[Callable[[Table], None]] = None,
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
def df_table(fun: Callable[..., DataFrame]) -> DataFrameTableFunctionTask:  # type: ignore
    pass


@overload
def df_table(
    define: Optional[Callable[..., DataFrame]] = None,
    persist: Optional[Callable[[DataFrame], Table]] = None,
    validate: Optional[Callable[[Table], None]] = None,
    **task_init_kwargs: Any,
) -> Callable[[Callable[..., DataFrame]], DataFrameTableFunctionTask]:  # type: ignore
    pass


def df_table(  # type: ignore
    fun: Callable[..., DataFrame] = None, **task_init_kwargs: Any
) -> Union[
    DataFrameTableFunctionTask,
    Callable[[Callable[..., DataFrame]], DataFrameTableFunctionTask],
]:
    if fun is None:
        return lambda fun: DataFrameTableFunctionTask(
            define=fun, **task_init_kwargs
        )
    return DataFrameTableFunctionTask(define=fun, **task_init_kwargs)
