from movies.tables.exploded_credits import ExplodedCredits

from tuberia.spark import get_spark
from tuberia.table import Table


class Actor(Table):
    name = "actor"
    exploded_credits: ExplodedCredits

    def define(self):
        return (
            get_spark()
            .table(self.exploded_credits.full_name)
            .select("actor")
            .distinct()
        )
