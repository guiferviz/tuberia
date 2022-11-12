import abc
from typing import Any, Dict, List, Union

import inflection
import prefect
import pydantic

from tuberia import utils
from tuberia.base_settings import BaseSettings
from tuberia.table import Table, TableTask
from tuberia.visualization import open_mermaid_flow_in_browser


class DependencyNode:
    def __init__(self, node, dependencies):
        self.node = node
        self.dependencies = dependencies


class Flow(abc.ABC, BaseSettings):
    class Config:
        env_prefix = "tuberia_flow_"
        env_json_config_name = "json_settings"

    name: str = None  # type: ignore

    @pydantic.validator("name", always=True)
    def default_name(cls, name):
        if name is None:
            return inflection.underscore(cls.__name__)  # type: ignore
        return name

    @property
    def full_name(self):
        cls = self.__class__
        return f"{cls.__module__}.{cls.__qualname__}"

    @staticmethod
    def from_qualified_name(name: str) -> "Flow":
        module_name, class_name = name.rsplit(".", 1)
        return utils.get_module_member(module_name, class_name)

    @abc.abstractmethod
    def define(self):
        raise NotImplementedError()

    def pre_run(self):
        pass

    def run(self):
        tables = self.define()
        flow = make_prefect_flow(tables)
        # TODO: improve the way we are calling pre and post run hooks.
        self.pre_run()
        run_flow(flow)
        self.post_run()

    def post_run(self):
        pass

    def plot(self):
        self.visualize()

    def visualize(self):
        tables = self.define()
        flow = make_prefect_flow(tables)
        open_mermaid_flow_in_browser(flow)

    def make_dependency_tree(self) -> List[DependencyNode]:
        return []

    def dict_tables(self) -> Dict[Union[tuple, frozenset], Table]:
        pending_tables = self.define()
        tables: Dict[Any, Table] = {}
        while len(pending_tables) > 0:
            table = pending_tables.pop()
            table_freeze = table.freeze()
            if table_freeze not in tables:
                pending_tables.extend(table._dependencies())
                tables[table_freeze] = table
        return tables

    def list_tables(self) -> List[Table]:
        return list(self.dict_tables().values())


def _add_table_object(
    table: Table, existing_tables_set: set, existing_tables_list: list
):
    table_freeze = table.freeze()
    if table_freeze not in existing_tables_set:
        for i in table._dependencies():
            _add_table_object(i, existing_tables_set, existing_tables_list)
        existing_tables_set.add(table_freeze)
        existing_tables_list.append(table)


def run(tables: List[Table]):
    run_flow(make_prefect_flow(tables))


def make_prefect_flow(tables: List[Table]):
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
