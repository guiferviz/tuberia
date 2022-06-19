import pytest
from pyspark.sql.session import SparkSession

from tuberia.spark import get_dbutils, get_spark, is_databricks


def test_get_spark_with_existing_value(mocker):
    mocker.patch("tuberia.spark._SPARK", "mock_spark_session")
    spark = get_spark()
    assert spark == "mock_spark_session"


def test_get_spark_without_value_in_databricks(mocker):
    mocker.patch("tuberia.spark._SPARK", None)
    mocker.patch("tuberia.spark.is_databricks", return_value=True)
    spark = get_spark()
    assert isinstance(spark, SparkSession)


def test_get_spark_without_value_in_local(mocker):
    mocker.patch("tuberia.spark._SPARK", None)
    mocker.patch("tuberia.spark.is_databricks", return_value=False)
    spark = get_spark()
    assert isinstance(spark, SparkSession)


def test_get_dbutils_in_databricks(spark, mocker):
    DBUtils = mocker.patch("tuberia.spark.DBUtils", return_value="dbutils")
    assert get_dbutils() == "dbutils"
    assert DBUtils.called_once_with(spark)


def test_get_dbutils_in_local(mocker):
    mocker.patch("tuberia.spark.DBUtils", None)
    with pytest.raises(Exception) as excinfo:
        get_dbutils()
    assert "no DBUtils in your current env" in str(excinfo.value)


def test_is_databricks_true(mocker):
    mocker.patch("os.environ", {"DATABRICKS_RUNTIME_VERSION": "databricks"})
    assert is_databricks()


def test_is_databricks_false(mocker):
    mocker.patch("os.environ", {})
    assert not is_databricks()
