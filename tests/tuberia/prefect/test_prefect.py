from typing import List

from prefect import Flow, Task, task


def test_prefect():
    class Table0(Task):
        def run(self):
            print("creating table 0")

    class Table1(Task):
        def run(self):
            print("creating table 1")

    class Table2(Task):
        def __init__(self, my_set: set):
            super().__init__()
            self.my_set = my_set

        def run(self):
            print(f"creating table 2 using set {self.my_set}")

    @task
    def join_tables(table_left, table_right, keys: List[str]):
        print(f"join table0 with table1 using keys {keys}")

    @task
    def join_set_of_tables(tables: set, keys: List[str]):
        print(f"joining tables {tables} using keys {keys}")

    class JoinTables(Task):
        def __init__(self, table0: Table0, table1: Table1):
            super().__init__()
            self.table0 = table0
            self.table1 = table1
            self.keys = ["key"]
            self.__call__()

        def run(self):
            print(f"join table0 with table1 using keys {self.keys}")

    my_creation_set = {"1", "2"}
    with Flow("test_flow") as flow:
        table0 = Table0()()
        table1 = Table1()()
        tables2 = []
        for i in my_creation_set:
            print("creating", i)
            tables2.append(Table2({i})())
        joined = join_tables(table0, table1, ["key"])
        joined_set = join_set_of_tables({table0, table1}, ["key"])
        # joined = JoinTables(table1, table0)()

    flow.run()
    print(flow.get_tasks())
    for i in tables2:
        print(i.name, i.my_set, flow.slugs[i])
    assert (
        flow.serialized_hash()
        == "752392446328f7f78336fd07ecda32f466fe6020d3c116788a4716dc2305ea73"
    )
