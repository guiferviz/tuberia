from typing import Any, Callable, Union, overload

import prefect
import pydantic
from prefect.tasks.core.function import FunctionTask


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
