from typing import List, Optional

from pyspark.sql import DataFrame


class Check:
    success: Optional[bool] = None

    def run(self, df: DataFrame):
        raise NotImplementedError()

    def report(self) -> dict:
        return {}


class Unique(Check):
    columns: List[str]

    def __init__(self, columns):
        self.columns = columns

    def run(self, df: DataFrame):
        df_unique_columns = df.select(*self.columns)
        self._total_rows = df_unique_columns.count()
        self._unique_rows = df_unique_columns.distinct().count()
        self.success = self._total_rows == self._unique_rows

    def sort_description(self):
        return "Columns should be unique."

    def description(self):
        return f"Columns {self.columns} should be unique."

    def report(self):
        return {
            "name": self.__class__.__name__,
            "description": self.description(),
            "success": self.success,
            "total_rows": self._total_rows,
            "unique_rows": self._unique_rows,
            "ok_percentage": self._unique_rows / self._total_rows,
            "fail_percentage": 1 - (self._unique_rows / self._total_rows),
        }
