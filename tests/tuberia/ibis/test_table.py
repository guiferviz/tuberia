import ibis
import pydantic
from pyspark.sql import SparkSession

from tuberia.ibis.table import Table
from tuberia.schema import Column
from tuberia.task import run


def test_table_attributes():
    class MyTable(Table):
        database: str = "my_database"

    table = MyTable()
    assert table.name == "my_table"
    assert table.full_name == "my_database.my_table"


def test_table(test_database: str, spark: SparkSession):
    spark.range(10).write.saveAsTable(f"{test_database}.my_range")

    class MyRange(Table):
        database: str = test_database

        class schema:
            id = Column("int")

        def define(self):
            pass

    class MyTable(Table):
        database: str = test_database
        my_range: MyRange = pydantic.Field(default_factory=MyRange)

        class schema:
            id = Column("int")
            id_double = Column("int")

        def define(self):
            my_range = self._connection.table(self.my_range.full_name)
            breakpoint()
            return my_range[
                (my_range[self.my_range.schema.id]).name(self.schema.id),
                (my_range[self.my_range.schema.id] * 2).name(
                    self.schema.id_double
                ),
            ]

    connection = ibis.pyspark.connect(spark)
    table = MyTable()
    table._connection = connection
    run([table])
