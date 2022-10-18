from typing import Dict, List, Optional

import prefect
import pydantic


class MetaTable(pydantic.main.ModelMetaclass):
    def __new__(cls, name, bases, dct):
        return super().__new__(cls, name, bases, dct)


class Table(pydantic.BaseModel, metaclass=MetaTable):
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
        pass


class TableTask(prefect.Task):
    def __init__(self, table: Table, **kwargs):
        super().__init__(name=table.full_name, **kwargs)
        self.table = table

    def run(self) -> Table:
        self.table.create()
        return self.table


def make_flow(tables: List[Table]):
    with prefect.Flow("make_tables") as flow:
        existing_tasks: Dict[str, prefect.Task] = {}
        for i in tables:
            make_table_task(i, existing_tasks)
    return flow


def make_table_task(
    table: Table, existing_tasks: Dict[str, prefect.Task]
) -> prefect.Task:
    print(f"analyzing table {table.full_name}")
    if table.full_name in existing_tasks:
        return existing_tasks[table.full_name]
    dependencies: List[prefect.Task] = []
    for k, v in table:
        if isinstance(v, Table):
            print(f"table found in {k}")
            dependencies.append(make_table_task(v, existing_tasks))
        elif isinstance(v, List) and len(v) > 0 and isinstance(v[0], Table):
            for i in v:
                dependencies.append(make_table_task(i, existing_tasks))
        # TODO: support dictionaries.
    task = TableTask(table)
    task.set_dependencies(upstream_tasks=dependencies)
    existing_tasks[table.full_name] = task
    return task


def run_flow(flow: prefect.Flow):
    state = flow.run()
    if state is not None and not state.is_successful():
        state_list, state_summary = "", ""
        for task, result in state.result.items():
            state_list += f"\n{task.name} = {result}"
            state_summary += f"\n{task.name} --> {type(result).__name__}"
        raise Exception(
            f"errors running the flow: {state.message}{state_summary}"
        )
    return state
