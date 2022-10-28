from typing import List

from movies.tables.actor import Actor
from movies.tables.acts_in_movie import ActsInMovie
from movies.tables.exploded_credits import ExplodedCredits
from movies.tables.input_credits import InputCredits
from movies.tables.input_movies import InputMovies
from movies.tables.movie import Movie

from tuberia.flow import Flow
from tuberia.spark import get_spark
from tuberia.table import Table


class Movies(Flow):
    input_credits_path: str
    input_movies_path: str
    database: str
    database_dir: str

    def pre_run(self):
        get_spark().sql(f"CREATE DATABASE {self.database}")

    def define(self) -> List[Table]:
        defaults = dict(
            database=self.database,
            path=self.database_dir,
        )
        input_movies = InputMovies(**defaults, input_file_path=self.input_movies_path)
        input_credits = InputCredits(
            **defaults, input_file_path=self.input_credits_path
        )
        exploded_credits = ExplodedCredits(**defaults, input_credits=input_credits)
        actor = Actor(**defaults, exploded_credits=exploded_credits)
        movie = Movie(**defaults, input_movies=input_movies)
        acts_in_movie = ActsInMovie(
            **defaults, exploded_credits=exploded_credits, actor=actor, movie=movie
        )
        return [
            actor,
            movie,
            acts_in_movie,
        ]
