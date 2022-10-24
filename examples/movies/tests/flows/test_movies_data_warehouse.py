import pytest
from movies.flows.movies import Movies
from movies.settings import MoviesSettings

from tuberia.table import make_flow, run_flow
from tuberia.visualization import open_mermaid_flow_in_browser


@pytest.fixture
def database_dir(tmp_path_factory, test_database) -> str:
    return str(tmp_path_factory.mktemp(test_database))


@pytest.fixture
def movies_flow(database_dir, test_database) -> MoviesSettings:
    return Movies(
        input_credits_path="./data/tmdb_5000_credits.csv",
        input_movies_path="./data/tmdb_5000_movies.csv",
        database=test_database,
        database_dir=database_dir,
    )


def test_movies_data_warehouse(spark, test_database, movies_flow):
    print(f"{test_database=}, {movies_flow=}")
    final_tables = movies_flow.create()
    open_mermaid_flow_in_browser(make_flow(final_tables))
    run_flow(make_flow(final_tables))
    input_movies = final_tables[0]
    spark.table(input_movies.full_name).select("id", "title").show()
    input_credits = final_tables[1]
    spark.table(input_credits.full_name).select("id", "cast").show()
    exploded_credits = final_tables[2]
    spark.table(exploded_credits.full_name).show()
