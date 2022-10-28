import pytest
from movies.flows.movies import Movies


@pytest.fixture
def database_dir(tmp_path_factory, test_database) -> str:
    return str(tmp_path_factory.mktemp(test_database))


@pytest.fixture
def movies_flow(database_dir, random_str) -> Movies:
    return Movies(
        input_credits_path="./data/tmdb_5000_credits.csv",
        input_movies_path="./data/tmdb_5000_movies.csv",
        database=f"test_database_{random_str(8)}",
        database_dir=database_dir,
    )


def test_movies(spark, movies_flow):
    movies_flow.visualize()
    movies_flow.run()
    final_tables = movies_flow.define()
    for i in final_tables:
        print(i.full_name)
        spark.table(i.full_name).show()
