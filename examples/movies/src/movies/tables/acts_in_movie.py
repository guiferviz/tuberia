import pyspark.sql.types as T

from tuberia.spark import get_spark
from tuberia.table import Table

from movies.tables.actor import Actor
from movies.tables.exploded_credits import ExplodedCredits
from movies.tables.movie import Movie


class ActsInMovie(Table):
    exploded_credits: ExplodedCredits
    actor: Actor
    movie: Movie
    schema_: T.StructType = T.StructType(
        [
            T.StructField("movie_id", T.IntegerType()),
            T.StructField("actor_id", T.LongType()),
        ]
    )

    def define(self):
        return get_spark().sql(
            f"""
                SELECT ec.id as movie_id, a.id as actor_id
                FROM {self.exploded_credits.full_name} ec
                INNER JOIN {self.actor.full_name} a
                    ON ec.actor = a.name
                INNER JOIN {self.movie.full_name} m
                    ON ec.id = m.id
            """
        )
