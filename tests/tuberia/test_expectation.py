import pyspark.sql.functions as F
import pytest

from tuberia.expectation import PrimaryKey
from tuberia.flow import run
from tuberia.spark import get_spark
from tuberia.table import Table


def test_check(test_database: str):
    class MyTable(Table):
        database: str = test_database

        def expect(self):
            return [
                PrimaryKey(columns=["id"]),
            ]

        def define(self):
            return get_spark().range(10)

    my_table = MyTable()
    run([my_table])


def test_check_fail(test_database: str):
    class MyTable(Table):
        database: str = test_database

        def expect(self):
            return [
                PrimaryKey(columns=["id"]),
            ]

        def define(self):
            return (
                get_spark()
                .range(10)
                .withColumn("id", (F.col("id") / F.lit(2)).cast("int"))
            )

    my_table = MyTable()
    with pytest.raises(Exception, match="errors running the flow"):
        run([my_table])


def test_primary_key_fails_when_unique_but_have_nulls(spark, mocker):
    df = spark.createDataFrame(
        [
            [0],
            [1],
            [None],
            [None],
        ],
        "id int",
    )
    mocker.patch.object(Table, "read", return_value=df)
    expectation = PrimaryKey(columns={"id"})
    report = expectation.run(Table.construct())
    assert not report.success
    assert report.expectation == expectation
    assert report.element_count == 4
    assert report.unexpected_list == [{"id": None, "__count__": 2}]
    assert report.unexpected_keys_list == [{"id": None, "__count__": 2}]
    assert report.unexpected_count == 2
    assert report.unexpected_percent == 0.5
    assert report.unexpected_percent_nonmissing == 0
    assert report.missing_count == 2
    assert report.missing_percent == 0.5


def test_primary_key_ignores_duplicates_in_non_key_columns(spark, mocker):
    df = spark.createDataFrame(
        [
            [0, "a"],
            [1, "a"],
            [3, "b"],
        ],
        "id int, value string",
    )
    mocker.patch.object(Table, "read", return_value=df)
    expectation = PrimaryKey(columns={"id"})
    report = expectation.run(Table.construct())
    assert report.success
    assert report.expectation == expectation
    assert report.element_count == 3
    assert report.unexpected_list == []
    assert report.unexpected_keys_list == []
    assert report.unexpected_count == 0
    assert report.unexpected_percent == 0
    assert report.unexpected_percent_nonmissing == 0
    assert report.missing_count == 0
    assert report.missing_percent == 0


def test_primary_key_works_with_multi_column_keys(spark, mocker):
    df = spark.createDataFrame(
        [
            [0, "a"],
            [1, "a"],
            [3, "b"],
            [0, "a"],
        ],
        "id_int int, id_str string",
    )
    mocker.patch.object(Table, "read", return_value=df)
    expectation = PrimaryKey(columns={"id_int", "id_str"})
    report = expectation.run(Table.construct())
    assert not report.success
    assert report.expectation == expectation
    assert report.element_count == 4
    assert report.unexpected_list == [
        {"id_int": 0, "id_str": "a", "__count__": 2}
    ]
    assert report.unexpected_keys_list == [
        {"id_int": 0, "id_str": "a", "__count__": 2}
    ]
    assert report.unexpected_count == 2
    assert report.unexpected_percent == 0.5
    assert report.unexpected_percent_nonmissing == 0.5
    assert report.missing_count == 0
    assert report.missing_percent == 0
