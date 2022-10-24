from typing import List

from movies.tables.exploded_credits import ExplodedCredits
from movies.tables.input_credits import InputCredits
from movies.tables.input_movies import InputMovies

from tuberia.flow import Flow
from tuberia.table import Table


class Movies(Flow):
    input_credits_path: str
    input_movies_path: str
    database: str
    database_dir: str

    def create(self) -> List[Table]:
        defaults = dict(
            database=self.database,
            path=self.database_dir,
        )
        input_movies = InputMovies(**defaults, input_file_path=self.input_movies_path)
        input_credits = InputCredits(
            **defaults, input_file_path=self.input_credits_path
        )
        exploded_credits = ExplodedCredits(**defaults, input_credits=input_credits)
        return [input_movies, input_credits, exploded_credits]
