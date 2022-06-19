from typing import Iterator

from pyspark.sql import SparkSession


def database_generator(
    spark: SparkSession, database_name: str
) -> Iterator[str]:
    spark.sql(f"CREATE DATABASE {database_name}")
    yield database_name
    spark.sql(f"DROP DATABASE {database_name} CASCADE")
