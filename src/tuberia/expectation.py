from __future__ import annotations

import abc
import operator
from functools import reduce
from typing import TYPE_CHECKING, Any, List, Set

import pydantic
import pyspark.sql.functions as F

if TYPE_CHECKING:
    from tuberia.table import Table


class Expectation(abc.ABC):
    @abc.abstractmethod
    def run(self, table: Table) -> Report:
        raise NotImplementedError()

    @abc.abstractmethod
    def description(self) -> str:
        raise NotImplementedError()


class Report(pydantic.BaseModel, arbitrary_types_allowed=True):
    """Base model for expectation reports.

    Attributes:
        success: `True` if the expectation ran OK.
        expectation: Expectation object that generated this report object.

    """

    success: bool
    expectation: Expectation


class RowLevelValidationReport(Report):
    """Report for expectations that evaluate a condition row by row.

    Taking most of the report fields from great expectations.

    Attributes:
        unexpected_list: A list of all values that violate the expectation.
        unexpected_keys_list: A list of the indices of the unexpected values
            in the column.
        element_count: The total number of values in the column.
        unexpected_count: The total count of unexpected values in the column.
        unexpected_percent: The overall percent of unexpected values.
        unexpected_percent_nonmissing: The percent of unexpected values,
            excluding missing values from the denominator.
        missing_count: The number of missing values in the column.
        missing_percent: The total percent of missing values in the column.

    """

    unexpected_list: List[Any]
    unexpected_keys_list: List[Any]
    element_count: int
    unexpected_count: int
    unexpected_percent: float
    unexpected_percent_nonmissing: float
    missing_count: float
    missing_percent: float


class TableLevelValidationReport(Report):
    """Report for expectations that run aggregations over the full table.

    Taking report fields from great expectations.

    Attributes:
        observed_value: The aggregate statistic computed for the column.
        element_count: The total number of values in the column.
        missing_count: The number of missing values in the column.
        missing_percent: The total percent of missing values in the column.

    """

    observed_value: float
    element_count: int
    missing_count: int
    missing_percent: float


class CustomReport(Report, extra="allow"):
    pass


class PrimaryKey(Expectation):
    """Expect a set of columns to uniquely identify a row.

    Nulls are not considered valid values for key columns.

    Attributes:
        columns: Set of columns to check.

    """

    columns: Set[str]

    def __init__(self, columns):
        self.columns = columns

    def run(self, table: Table):
        df_key_columns = table.read().select(*self.columns)
        element_count = df_key_columns.count()
        any_key_column_with_nulls = reduce(
            operator.__or__, [F.col(i).isNull() for i in self.columns]
        )
        unique_count = (
            df_key_columns.groupBy(*self.columns)
            .agg(F.count("*").alias("__count__"))
            .filter(~any_key_column_with_nulls & (F.col("__count__") == 1))
            .count()
        )
        missing_count = df_key_columns.filter(any_key_column_with_nulls).count()
        success = element_count == unique_count
        unexpected_count = element_count - unique_count
        if not success:
            unexpected_list = (
                df_key_columns.groupBy(*self.columns)
                .agg(F.count("*").alias("__count__"))
                .filter(any_key_column_with_nulls | (F.col("__count__") > 1))
                .take(20)
            )
            unexpected_list = [i.asDict() for i in unexpected_list]
        else:
            unexpected_list = []
        return RowLevelValidationReport(
            success=success,
            expectation=self,
            unexpected_list=unexpected_list,
            unexpected_keys_list=unexpected_list,
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_percent=unexpected_count / element_count,
            unexpected_percent_nonmissing=(unexpected_count - missing_count)
            / (element_count - missing_count),
            missing_count=missing_count,
            missing_percent=missing_count / element_count,
        )

    def description(self) -> str:
        return f"Columns {self.columns} should uniquely identify a row."
