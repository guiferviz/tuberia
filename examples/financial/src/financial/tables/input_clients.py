import pyspark.sql.functions as F
import pyspark.sql.types as T
from tuberia.spark import get_spark
from tuberia.table import Table


class InputClients(Table):
    input_file_path: str
    clients_schema: T.StructType = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("surname", T.StringType()),
            T.StructField("zip_code", T.StringType()),
            T.StructField("country", T.StringType()),
        ]
    )
    input_file_schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("surname", T.StringType()),
            T.StructField("zip_code", T.StringType()),
            T.StructField("country", T.StringType()),
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
