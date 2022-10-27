import pytest
from movies.flows.movies import Movies


@pytest.fixture
def database_dir(tmp_path_factory, test_database) -> str:
    return str(tmp_path_factory.mktemp(test_database))


@pytest.fixture
def movies_flow(database_dir, test_database) -> Movies:
    return Movies(
        input_credits_path="./data/tmdb_5000_credits.csv",
        input_movies_path="./data/tmdb_5000_movies.csv",
        database=test_database,
        database_dir=database_dir,
    )


def test_movies(spark, test_database, movies_flow):
    print(f"{test_database=}, {movies_flow=}")
    movies_flow.plot()
    movies_flow.run()
    final_tables = movies_flow.define()
    input_movies = final_tables[0]
    spark.table(input_movies.full_name).select("id", "title").show()
    input_credits = final_tables[1]
    spark.table(input_credits.full_name).select("id", "cast").show()
    exploded_credits = final_tables[2]
    spark.table(exploded_credits.full_name).show()
