import pytest
from pyspark.sql.utils import AnalysisException

from pytest_tuberia.utils import database_generator


def test_database_generator(spark):
    iterator = database_generator(spark, "test_database")
    with pytest.raises(AnalysisException):
        spark.catalog.listTables("test_database")
    next(iterator)  # Create database
    spark.catalog.listTables("test_database")
    with pytest.raises(StopIteration):
        next(iterator)  # Delete database
    with pytest.raises(AnalysisException):
        spark.catalog.listTables("test_database")
