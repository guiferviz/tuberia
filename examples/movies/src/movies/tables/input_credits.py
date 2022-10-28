import pyspark.sql.functions as F
import pyspark.sql.types as T
from tuberia.spark import get_spark
from tuberia.table import Table


class InputCredits(Table):
    input_file_path: str
    cast_schema = T.ArrayType(
        T.StructType(
            [
                T.StructField("cast_id", T.StringType()),
                T.StructField("character", T.StringType()),
                T.StructField("credit_id", T.StringType()),
                T.StructField("gender", T.StringType()),
                T.StructField("id", T.StringType()),
                T.StructField("name", T.StringType()),
                T.StructField("order", T.StringType()),
            ]
        )
    )
    input_file_schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("title", T.StringType()),
            T.StructField("cast", T.StringType()),
            T.StructField("crew", T.StringType()),
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
            .withColumn("cast", F.from_json(F.col("cast"), self.cast_schema))
        )
