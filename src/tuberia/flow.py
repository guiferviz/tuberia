import abc
from typing import Dict, List

import prefect

from tuberia.base_settings import BaseSettings
from tuberia.table import Table, TableTask
from tuberia.visualization import open_mermaid_flow_in_browser


class Flow(abc.ABC, BaseSettings):
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
