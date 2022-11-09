from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, List, Set

import pydantic

if TYPE_CHECKING:
    from tuberia.table import Table


class Expectation(abc.ABC):
    @abc.abstractmethod
    def run(self, table: Table) -> Report:
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

    Taking report fields from great expectations.

    Attributes:
        unexpected_list: A list of all values that violate the expectation.
        unexpected_index_list: A list of the indices of the unexpected values
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
    unexpected_index_list: List[Any]
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

    Attributes:
        columns: Set of columns to check.

    """

    columns: Set[str]

    def __init__(self, columns):
        self.columns = columns

    def run(self, table: Table):
        df_key_columns = table.read().select(*self.columns)
        element_count = df_key_columns.count()
        unique_count = df_key_columns.distinct().count()
        success = element_count == unique_count
        unexpected_count = element_count - unique_count
        # return RowLevelValidationReport(
        return CustomReport(
            success=success,
            expectation=self,
            unexpected_list=None,  # type:ignore
            unexpected_index_list=None,  # type:ignore
            element_count=element_count,  # type:ignore
            unexpected_count=unexpected_count,  # type:ignore
            unexpected_percent=unexpected_count / element_count,  # type:ignore
            unexpected_percent_nonmissing=None,  # type:ignore
            missing_count=None,  # type:ignore
            missing_percent=None,  # type:ignore
        )

    def description(self):
        return f"Columns {self.columns} should uniquely identify a row."
