import pyspark.sql.types as T
from tuberia.spark import get_spark
from tuberia.table import Table


class InputCategories(Table):
    input_file_path: str
    products_schema: T.StructType = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("category", T.StringType()),
        ]
    )
    input_file_schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("category", T.StringType()),
        ]
    )

    def define(self):
        return (
            get_spark()
            .read.format("csv")
            .schema(self.input_file_schema)
            .option("quote", '"')
            .option("escape", '"')
            .option("header", "true")
            .load(self.input_file_path)
        )
