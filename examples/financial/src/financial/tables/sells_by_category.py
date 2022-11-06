import pyspark.sql.types as T

from tuberia.spark import get_spark
from tuberia.table import Table

from financial.tables.input_categories import InputCategories
from financial.tables.input_products import InputProducts
from financial.tables.input_sells import InputSells


class SellsByCategory(Table):
    input_products: InputProducts
    input_categories: InputCategories
    input_sells: InputSells
    schema_: T.StructType = T.StructType(
        [
            T.StructField("category", T.IntegerType()),
            T.StructField("total_price", T.LongType()),
        ]
    )

    def define(self):
        return get_spark().sql(
            f"""
                SELECT c.category, sum(p.unit_price)
                FROM {self.input_sells.full_name} s
                INNER JOIN {self.input_products.full_name} p
                    ON s.product = p.id
                INNER JOIN {self.input_categories.full_name} c
                    ON p.category = c.id
            """
        )
