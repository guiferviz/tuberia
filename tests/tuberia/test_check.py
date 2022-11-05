import pyspark.sql.functions as F
import pytest

from tuberia.check import Unique
from tuberia.flow import run
from tuberia.spark import get_spark
from tuberia.table import Table


def test_check(test_database: str):
    class MyTable(Table):
        database: str = test_database

        def checks(self):
            return [
                Unique(columns=["id"]),
            ]

        def define(self):
            return get_spark().range(10)

    my_table = MyTable()
    run([my_table])


def test_check_fail(test_database: str):
    class MyTable(Table):
        database: str = test_database

        def checks(self):
            return [
                Unique(columns=["id"]),
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
