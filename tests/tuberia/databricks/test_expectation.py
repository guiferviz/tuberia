import pyspark.sql.functions as F
import pytest

from tuberia.databricks.expectation import Expression, PrimaryKey
from tuberia.databricks.table import Table
from tuberia.schema import Column
from tuberia.spark import get_spark
from tuberia.task import run


def test_check(test_database: str):
    class MyTable(Table):
        database: str = test_database

        class Schema:
            id = Column(int)
            _pk = PrimaryKey(id)
            _no_zero = Expression(F.col(id) > 0)

        def df(self):
            return get_spark().range(10)

    my_table = MyTable()
    run([my_table])


def test_check_fail(test_database: str):
    class MyTable(Table):
        database: str = test_database

        class schema:
            id = Column(int)
            _pk = PrimaryKey(id)

        def df(self):
            return (
                get_spark()
                .range(10)
                .withColumn(
                    self.schema.id,
                    (F.col("id") / F.lit(2)).cast("int"),
                )
            )

    my_table = MyTable()
    with pytest.raises(Exception, match="Expectation failed"):
        run([my_table])


class TestPrimaryKey:
    def test_fails_when_unique_but_have_nulls(self, spark, mocker):
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
        expectation = PrimaryKey("id")
        report = expectation.run(Table(database=""))
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

    def test_ignores_duplicates_in_non_key_columns(self, spark, mocker):
        df = spark.createDataFrame(
            [
                [0, "a"],
                [1, "a"],
                [3, "b"],
            ],
            "id int, value string",
        )
        mocker.patch.object(Table, "read", return_value=df)
        expectation = PrimaryKey("id")
        report = expectation.run(Table(database=""))
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

    def test_works_with_multi_column_keys(self, spark, mocker):
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
        expectation = PrimaryKey("id_int", "id_str")
        report = expectation.run(Table(database=""))
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


class TestExpression:
    @pytest.fixture(params=["string_expr", "column_expr"])
    def expr(self, spark, request):
        if request.param == "string_expr":
            return "LENGTH(name) BETWEEN 1 AND 2"
        elif request.param == "column_expr":
            return F.length("name").between(1, 2)

    def test_works_without_nulls(self, spark, mocker, expr):
        df = spark.createDataFrame(
            [
                [0, ""],
                [1, "a"],
                [2, "aa"],
                [3, "aaa"],
            ],
            "id int, name string",
        )
        mocker.patch.object(Table, "read", return_value=df)
        expectation = Expression(expr)
        report = expectation.run(Table(database=""))
        assert not report.success
        assert report.expectation == expectation
        assert report.element_count == 4
        assert report.unexpected_list == [
            {"id": 0, "name": ""},
            {"id": 3, "name": "aaa"},
        ]
        assert report.unexpected_keys_list == []
        assert report.unexpected_count == 2
        assert report.unexpected_percent == 0.5
        assert report.unexpected_percent_nonmissing == 0.5
        assert report.missing_count == 0
        assert report.missing_percent == 0

    def test_works_with_nulls(self, spark, mocker, expr):
        df = spark.createDataFrame(
            [
                [0, None],
                [1, "a"],
                [2, "aa"],
                [3, "aaa"],
            ],
            "id int, name string",
        )
        mocker.patch.object(Table, "read", return_value=df)
        expectation = Expression(expr)
        report = expectation.run(Table(database=""))
        assert not report.success
        assert report.expectation == expectation
        assert report.element_count == 4
        assert report.unexpected_list == [{"id": 3, "name": "aaa"}]
        assert report.unexpected_keys_list == []
        assert report.unexpected_count == 1
        assert report.unexpected_percent == 0.25
        assert report.unexpected_percent_nonmissing == 1 / 3
        assert report.missing_count == 1
        assert report.missing_percent == 0.25

    def test_success(self, spark, mocker, expr):
        df = spark.createDataFrame(
            [
                [0, "a"],
                [1, "a"],
                [2, "ab"],
                [3, "bc"],
            ],
            "id int, name string",
        )
        mocker.patch.object(Table, "read", return_value=df)
        expectation = Expression(expr)
        report = expectation.run(Table(database=""))
        assert report.success
        assert report.expectation == expectation
        assert report.element_count == 4
        assert report.unexpected_list == []
        assert report.unexpected_keys_list == []
        assert report.unexpected_count == 0
        assert report.unexpected_percent == 0
        assert report.unexpected_percent_nonmissing == 0
        assert report.missing_count == 0
        assert report.missing_percent == 0
