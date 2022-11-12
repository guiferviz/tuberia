import pyspark.sql.types as T
from tuberia.expectation import PrimaryKey

from tuberia.spark import get_spark
from tuberia.table import Table

from movies.tables.exploded_credits import ExplodedCredits


class Actor(Table):
    """An actor dimension.

    A table in which we assign an ID to each person like these:
    <img src="https://imagenes.elpais.com/resizer/LafH8tTghcjGBTIUUk_i2Nddqg0=/1960x1470/arc-anglerfish-eu-central-1-prod-prisa.s3.amazonaws.com/public/M5WQSP3N5H2RLPE32VELYT6NEY.jpg"></img>

    Attributes:
        exploded_credits: ExplodedCredits table, used to take the distinct of
            the actors.

    """

    exploded_credits: ExplodedCredits
    schema_: T.StructType = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("name", T.StringType()),
        ]
    )

    def expect(self):
        return [
            PrimaryKey({"id"}),
        ]

    def define(self):
        return (
            get_spark()
            .table(self.exploded_credits.full_name)
            .select("actor")
            .distinct()
            .selectExpr("monotonically_increasing_id() AS id", "actor AS name")
        )
