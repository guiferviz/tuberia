import pyspark.sql.functions as F
from movies.tables.input_credits import InputCredits

from tuberia.spark import get_spark
from tuberia.table import Table


class ExplodedCredits(Table):
    name = "exploded_credits"
    input_credits: InputCredits

    def define(self):
        return (
            get_spark()
            .table(self.input_credits.full_name)
            .select("id", "cast")
            .withColumn("cast", F.explode("cast"))
            .selectExpr("id", "cast.character as character", "cast.name as actor")
        )
