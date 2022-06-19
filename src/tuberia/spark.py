import os
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

try:
    from pyspark.dbutils import DBUtils  # type: ignore
except ModuleNotFoundError:
    DBUtils = None


LOCAL_SPARK_DATA_DIR = "./spark_data"

_SPARK: Optional[SparkSession] = None


def set_spark(spark: Optional[SparkSession]):
    global _SPARK
    _SPARK = spark


def get_spark() -> SparkSession:
    global _SPARK
    if _SPARK is None:
        _SPARK = get_spark_session_depend_on_environment(LOCAL_SPARK_DATA_DIR)
    return _SPARK


def get_spark_session_depend_on_environment(data_dir: str) -> SparkSession:
    if is_databricks():
        return get_default_spark_session()
    return get_local_spark_session(data_dir)


def get_dbutils():
    if DBUtils is None:
        raise Exception("no DBUtils in your current env :(")
    return DBUtils(get_spark())


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_default_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def get_local_spark_session(data_dir: Optional[str] = None) -> SparkSession:
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("tuberia_project")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:1.1.0",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
            "1",
        )
    )
    if data_dir is not None:
        data_dir = str(Path(data_dir).resolve())
        builder = builder.config("spark.sql.warehouse.dir", data_dir).config(
            "spark.driver.extraJavaOptions",
            f"-Dderby.system.home={data_dir}",
        )
    return builder.enableHiveSupport().getOrCreate()
