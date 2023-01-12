from typing import List

from tuberia.databricks.table import Table
from tuberia.flow import Flow


def test_get_dict_tables():
    class Table0(Table):
        database = ""

    class Table1(Table):
        database = ""
        table0: Table0

    class Table2(Table):
        database = ""
        table0: Table0

    class Table3(Table):
        database = ""
        table1: Table1
        table2: Table2

    class MyFlow(Flow):
        def define(self) -> List[Table]:
            table0 = Table0()
            table1 = Table1(table0=table0)
            table2 = Table2(table0=table0)
            table3 = Table3(table1=table1, table2=table2)
            return [table3]

    flow = MyFlow()
    tables = flow.list_tables()
    assert {type(i) for i in tables} == {Table0, Table1, Table2, Table3}
