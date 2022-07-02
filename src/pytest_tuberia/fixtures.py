import random
import string
from typing import Callable, Iterator

import pytest
from pyspark.sql import SparkSession

from pytest_tuberia.utils import database_generator
from tuberia.spark import set_spark, setup_spark_session_depend_on_environment


@pytest.fixture(scope="session")
def spark(tmp_path_factory) -> Iterator[SparkSession]:
    temporal_dir = str(tmp_path_factory.mktemp("spark_data"))
    spark_session = setup_spark_session_depend_on_environment(temporal_dir)
    set_spark(spark_session)
    yield spark_session
    set_spark(None)


@pytest.fixture(scope="session")
def random_str() -> Callable[[int], str]:
    def generate(k: int) -> str:
        return "".join(
            random.choices(string.ascii_lowercase + string.digits, k=k)
        )

    return generate


@pytest.fixture
def test_database(spark, random_str: Callable[[int], str]) -> Iterator[str]:
    yield from database_generator(spark, f"test_database_{random_str(8)}")
