import os
from pathlib import Path
from typing import Optional

import delta
from pyspark.sql import SparkSession

from tuberia.typing import PathLike

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
        _SPARK = setup_spark_session_depend_on_environment(LOCAL_SPARK_DATA_DIR)
    return _SPARK


def setup_spark_session_depend_on_environment(
    data_dir: Optional[PathLike],
) -> SparkSession:
    if is_databricks():
        return get_existing_spark_session()
    return setup_local_spark_session(data_dir)


def get_dbutils():
    if DBUtils is None:
        raise Exception("no DBUtils in your current env :(")
    return DBUtils(get_spark())


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_existing_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def setup_local_spark_session(
    data_dir: Optional[PathLike] = None, delta: bool = True
) -> SparkSession:
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("tuberia_project")
        # TODO: move spark properties to a settings object.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config(
            "spark.sql.adaptive.coalescePartitions.initialPartitionNum", "1"
        )
    )
    if delta:
        builder = config_spark_builder_with_delta(builder)
    if data_dir is not None:
        builder = config_spark_builder_with_data_dir(builder, data_dir)
    return builder.enableHiveSupport().getOrCreate()


def config_spark_builder_with_data_dir(
    builder: SparkSession.Builder, data_dir: PathLike
) -> SparkSession.Builder:
    # Windows also requires posix "/" paths.
    data_dir = Path(data_dir).resolve().as_posix()
    return builder.config("spark.sql.warehouse.dir", data_dir).config(
        "spark.driver.extraJavaOptions",
        f"-Dderby.system.home={data_dir}",
    )


def config_spark_builder_with_delta(
    builder: SparkSession.Builder,
) -> SparkSession.Builder:
    # https://docs.delta.io/latest/quick-start.html#python
    builder = builder.config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    ).config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    # Add spark.jars.packages with a version that matches the installed
    # delta-spark Python package.
    return delta.configure_spark_with_delta_pip(builder)
