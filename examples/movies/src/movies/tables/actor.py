from movies.tables.exploded_credits import ExplodedCredits

from tuberia.spark import get_spark
from tuberia.table import Table


class Actor(Table):
    exploded_credits: ExplodedCredits

    def define(self):
        return (
            get_spark()
            .table(self.exploded_credits.full_name)
            .select("actor")
            .distinct()
            .selectExpr("monotonically_increasing_id() AS id", "actor AS name")
        )
