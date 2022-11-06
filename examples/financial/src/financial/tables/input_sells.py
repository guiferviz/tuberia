import pyspark.sql.types as T
from tuberia.spark import get_spark
from tuberia.table import Table


class InputSells(Table):
    input_file_path: str
    sells_schema: T.StructType = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("client", T.IntegerType()),
            T.StructField("product", T.IntegerType()),
        ]
    )
    input_file_schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("client", T.IntegerType()),
            T.StructField("product", T.IntegerType()),
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
