import pyspark.sql.types as T

from tuberia.spark import get_spark
from tuberia.table import Table


class InputMovies(Table):
    input_file_path: str
    input_file_schema: T.StructType = T.StructType(
        [
            T.StructField("budget", T.IntegerType()),
            T.StructField("genres", T.StringType()),
            T.StructField("homepage", T.StringType()),
            T.StructField("id", T.IntegerType()),
            T.StructField("keywords", T.StringType()),
            T.StructField("original_language", T.StringType()),
            T.StructField("original_title", T.StringType()),
            T.StructField("overview", T.StringType()),
            T.StructField("popularity", T.DoubleType()),
            T.StructField("production_companies", T.StringType()),
            T.StructField("production_countries", T.StringType()),
            T.StructField("release_date", T.DateType()),
            T.StructField("revenue", T.LongType()),
            T.StructField("runtime", T.IntegerType()),
            T.StructField("spoken_languages", T.StringType()),
            T.StructField("status", T.StringType()),
            T.StructField("tagline", T.StringType()),
            T.StructField("title", T.StringType()),
            T.StructField("vote_average", T.FloatType()),
            T.StructField("vote_count", T.IntegerType()),
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
