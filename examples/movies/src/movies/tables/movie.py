from tuberia.spark import get_spark
from tuberia.table import Table

from movies.tables.input_movies import InputMovies


class Movie(Table):
    input_movies: InputMovies

    def define(self):
        return (
            get_spark()
            .table(self.input_movies.full_name)
            .select("id", "title")
            .distinct()
        )
