from tuberia.table import Table


def test_table():
    table = Table(database="my_database", name="my_table")
    assert table.full_name == "my_database.my_table"


def test_table_dependencies():
    class Table0(Table):
        name: str = "table_0"

        def create(self):
            print("creating table 0")

    class Table1(Table):
        name: str = "table_1"
        previous_table: Table

        def create(self):
            print("creating table 1 from table 0")

    class Table2(Table):
        name: str = "table_2"
        previous_table: Table

        def create(self):
            print("creating table 2 from table 0")

    class Table3(Table):
        name: str = "table_3"
        previous_table_1: Table
        previous_table_2: Table

        def create(self):
            print("creating table 3 from table 1 and 2")

    table_0 = Table0(database="my_database")
    table_1 = Table1(database="my_database", previous_table=table_0)
    table_2 = Table2(database="my_database", previous_table=table_0)
    Table3(
        database="my_database",
        previous_table_1=table_1,
        previous_table_2=table_2,
    )
