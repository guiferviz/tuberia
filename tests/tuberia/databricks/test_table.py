from datetime import date

import pyspark.sql.types as T
import pytest

from tuberia.databricks.table import Table
from tuberia.schema import column


class TestsTable:
    def test_full_name_with_database_and_name(self):
        table = Table("my_database", "my_table")
        assert table.full_name == "my_database.my_table"

    def test_full_name_with_prefix(self):
        table = Table("my_database", "my_table", prefix_name="pre_")
        assert table.full_name == "my_database.pre_my_table"

    def test_full_name_with_suffix(self):
        table = Table("my_database", "my_table", suffix_name="_suf")
        assert table.full_name == "my_database.my_table_suf"

    def test_full_name_with_prefix_and_suffix(self):
        table = Table(
            "my_database", "my_table", prefix_name="pre_", suffix_name="_suf"
        )
        assert table.full_name == "my_database.pre_my_table_suf"

    def test_database_with_spaces(self):
        with pytest.raises(ValueError):
            Table(database="my database")

    def test_default_name_without_value(self):
        assert Table(database="my_database").name == "table"

    def test_default_name_with_value(self):
        class MyTable(Table):
            pass

        assert Table(database="my_database", name="my_table").name == "my_table"
        assert Table(database="my_database", name="MyTable").name == "MyTable"
        assert MyTable(database="my_database").name == "my_table"

    def test_full_path(self):
        table = Table(
            database="my_database",
            path="my_path",
            prefix_name="pre_",
            suffix_name="_suf",
        )
        assert table.full_path == "my_path/my_database/pre_table_suf"

    def test_pyspark_schema(self):
        class MyTable(Table):
            class schema:
                id = column(int)
                value0 = column(str)
                value1 = column(date, alias="Value1")
                value2 = column(T.IntegerType)
                value3 = column(T.IntegerType())

        table = MyTable(database="my_database")
        assert table.pyspark_schema() == T.StructType(
            [
                T.StructField("id", T.LongType()),
                T.StructField("value0", T.StringType()),
                T.StructField("Value1", T.DateType()),
                T.StructField("value2", T.IntegerType()),
                T.StructField("value3", T.IntegerType()),
            ]
        )

    def test_pyspark_dynamic_schema(self):
        class MyTable(Table):
            def __init__(self, values: int, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.values = values

            @property
            def schema(self):
                class meta_schema(type):
                    # This __getattribute__ makes linter happy. It basically
                    # accepts any column name, even if it does not exists. It
                    # is not ideal, but *dynamic* schema does not play well
                    # with *static* type checks...
                    def __getattribute__(self, value):
                        return super().__getattribute__(value)

                class schema(metaclass=meta_schema):
                    id = column(int)

                for i in range(self.values):
                    name = f"value{i}"
                    setattr(schema, name, column(int, alias=name))
                return schema

        table = MyTable(database="my_database", values=4)
        assert table.schema.value0 == column(int, alias="value0")
        assert table.pyspark_schema() == T.StructType(
            [
                T.StructField("id", T.LongType()),
                T.StructField("value0", T.LongType()),
                T.StructField("value1", T.LongType()),
                T.StructField("value2", T.LongType()),
                T.StructField("value3", T.LongType()),
            ]
        )


class TestsTableFlows:
    def test_table_flow(self):
        from typing import List

        import pyspark.sql.functions as F

        from tuberia.databricks.table import Table
        from tuberia.expectation import PrimaryKey
        from tuberia.schema import column
        from tuberia.task import Task, dependency_tree
        from tuberia.visualization import open_in_browser

        class Range(Table):
            """Table with numbers from 1 to `n`.

            Attributes:
                n: Max number in table.

            """

            def __init__(self, n: int = 10, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.n = n

            class schema:
                id = column(int)

            def expect(self):
                yield PrimaryKey({self.schema.id})

            def df(self):
                return self.spark.range(self.n).withColumn(
                    "id", F.col(self.schema.id)
                )

        class MultiplyFactorRange(Table):
            """Double a Range table.

            Attributes:
                range: Table to double.

            """

            class schema:
                id = column(int)

            def __init__(
                self, range: Range, factor: float = 2, *args, **kwargs
            ):
                super().__init__(*args, **kwargs)
                self.range = range
                self.factor = factor
                self.name = f"{self.name}_factor_{factor}"

            def expect(self):
                yield PrimaryKey({self.schema.id})

            def df(self):
                return self.range.read().withColumn(
                    self.schema.id, F.col(self.range.schema.id) * self.factor
                )

        class Flow(Task):
            def __init__(self, database: str, factors: List[int]):
                double_ranges = []
                for i in factors:
                    range_table = Range(n=10, database=database)
                    double_ranges.append(
                        MultiplyFactorRange(
                            range_table, factor=i, database=database
                        )
                    )
                self.double_ranges = double_ranges

        G = dependency_tree([Flow("db", factors=[2, 4, 6])])
        for i, attr in G.nodes.items():
            full_name = getattr(i, "full_name", None)
            if full_name:
                attr["name"] = full_name
        open_in_browser(G)
