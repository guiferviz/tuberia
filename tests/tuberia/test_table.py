from typing import List

import pytest
from prefect import Flow

from tuberia.table import Table, TableTask, table


def my_table(rows: int) -> Table:
    print(f"creating table with {rows} rows")
    return Table(database="my_database", name="my_table")


@pytest.fixture
def my_table_decorator_no_params() -> TableTask:
    return table(my_table)


@pytest.fixture
def my_table_decorator_with_params() -> TableTask:
    return table(name="my_super_table")(my_table)


def test_table_decorator_no_params(my_table_decorator_no_params, capsys):
    my_table_decorator_no_params.run(rows=10)
    my_table_decorator_no_params.run(10)
    captured = capsys.readouterr()
    assert captured.out == "creating table with 10 rows\n" * 2


def test_table_decorator_with_params(my_table_decorator_with_params, capsys):
    assert my_table_decorator_with_params.name == "my_super_table"
    my_table_decorator_with_params.run(rows=5)
    my_table_decorator_with_params.run(5)
    captured = capsys.readouterr()
    assert captured.out == "creating table with 5 rows\n" * 2


def test_table_in_flow(my_table_decorator_no_params: TableTask, capsys):
    with Flow("test") as flow:
        my_table_decorator_no_params(rows=10)
        my_table_decorator_no_params(10)
    flow.run()
    captured = capsys.readouterr()
    assert captured.out == "creating table with 10 rows\n" * 2


def test_flow_with_dependencies(capsys):
    @table
    def one() -> Table:
        print("table one created")
        return Table(database="my_database", name="one")

    @table
    def two(one: Table, letter: str) -> Table:
        print(f"table two created from {one.full_name} and letter={letter}")
        return Table(database="my_database", name=f"two_{letter}")

    @table
    def concat(tables: List[Table]) -> Table:
        print(f"table concat created from {', '.join(i.name for i in tables)}")
        return Table(database="my_database", name="concat")

    with Flow("test") as flow:
        one_table = one()
        two_a_table = two(one_table, "a")
        two_b_table = two(one_table, "b")
        concat([two_a_table, two_b_table])

    flow.run()
    captured = capsys.readouterr().out.split("\n")
    assert len(captured) == 5
    assert captured[0] == "table one created"
    assert set(captured[1:3]) == set(
        [
            "table two created from my_database.one and letter=a",
            "table two created from my_database.one and letter=b",
        ]
    )
    assert captured[3] == "table concat created from two_a, two_b"
    assert captured[4] == ""
