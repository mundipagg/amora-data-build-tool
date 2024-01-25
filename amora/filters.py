from datetime import date, datetime
from typing import Any, List, Optional, Type

from pydantic import BaseModel
from sqlalchemy import TIMESTAMP, Column, Date, DateTime, String, func, select
from sqlalchemy.sql.type_api import TypeEngine

from amora.providers import bigquery
from amora.questions import Question, parse_name


class Filter(BaseModel):
    """
    Base class for filters

    Attributes:
        field: name of the column to be filtered.
        title: title of the filter for display purposes.
        default: the default value for the filter.
        _field_class: the SQLAlchemy type of the column to be filtered.
    """

    field: str
    title: Optional[str] = None
    default: Optional[Any] = None
    _field_class: Type[TypeEngine]

    def is_valid_for(self, question: Question) -> bool:
        """
        Checks whether the `Filter` object is valid for the given `Question` object.

        Args:
            question: the `Question` object to check against.

        Returns:
            `True` if the filter is valid for the given question, `False` otherwise.
        """
        for col in question.answer_columns():
            if col.name == self.field:
                if isinstance(col.type, self._field_class):
                    return True
        return False

    def filter(self, question: Question, value: Any) -> Question:
        """
        Filters the `Question` object with the given `value` for the filter.

        Args:
            question: the `Question` object to filter.
            value: the value to filter by.

        Returns:
            A new `Question` object with the filter applied.

        Raises:
            ValueError: if the filter is invalid for the given question
        """
        if not self.is_valid_for(question):
            raise ValueError("Invalid filter")

        def question_func():
            base = question.question_func().cte(question.question_func.__name__)
            return select(base).where(base.c[self.field] == value)

        question_func.__doc__ = question.question_func.__doc__
        question_func.__name__ = f"{question.question_func.__name__}_filtered_by_{self.field}_{parse_name(value)}"

        return Question(question_func=question_func)


class DateFilter(Filter):
    """
    A filter for date columns.

    Attributes:
        field: name of the column to be filtered.
        title: title of the filter for display purposes.
        default: the default value for the filter. Defaults to today's date.
        min_selectable_date: the earliest selectable date. Defaults to unix epoch.
        max_selectable_date: the latest selectable date. Defaults to today's date.
    """

    _field_class = Date
    default: date = date.today()
    min_selectable_date: Optional[date] = datetime.utcfromtimestamp(0)
    max_selectable_date: Optional[date] = datetime.utcnow()

    def is_valid_for(self, question: Question) -> bool:
        """
        Checks whether the `Filter` object is valid for the given `Question` object.

        Args:
            question: the `Question` object to check against.

        Returns:
            `True` if the filter is valid for the given question, `False` otherwise.
        """
        for col in question.answer_columns():
            if col.name == self.field:
                if isinstance(col.type, (Date, DateTime, TIMESTAMP)):
                    return True
        return False

    def filter(self, question: Question, value: date) -> Question:
        if not self.is_valid_for(question):
            raise ValueError("Invalid filter")

        def question_func():
            base = question.question_func().cte(question.question_func.__name__)
            return select(base).where(
                func.date(base.c[self.field]) == func.date(value, type_=Date)
            )

        question_func.__doc__ = question.question_func.__doc__
        question_func.__name__ = (
            f"{question.question_func.__name__}_at_{value.isoformat()}"
        )

        return Question(question_func=question_func)


class ValueFilter(Filter):
    _field_class = String


class DateRangeFilter(Filter):
    """
    A filter for date ranges.

    Attributes:
        field: name of the column to be filtered.
        title: title of the filter for display purposes.
        default: the default value for the filter. Defaults to today's date.
        min_selectable_date: the earliest selectable date. Defaults to unix epoch.
        max_selectable_date: the latest selectable date. Defaults to today's date.
    """

    class Range(BaseModel):
        start: date
        end: date

    _field_class = Date
    min_selectable_date: Optional[date] = date(1970, 1, 1)
    max_selectable_date: Optional[date] = date.today()

    def is_valid_for(self, question: Question) -> bool:
        """
        Checks whether the `Filter` object is valid for the given `Question` object.

        Args:
            question: the `Question` object to check against.

        Returns:
            `True` if the filter is valid for the given question, `False` otherwise.
        """
        for col in question.answer_columns():
            if col.name == self.field:
                if isinstance(col.type, (Date, DateTime, TIMESTAMP)):
                    return True
        return False

    def filter(self, question: Question, value: Range) -> Question:
        if not self.is_valid_for(question):
            raise ValueError("Invalid filter")

        def question_func():
            base = question.question_func().cte(question.question_func.__name__)
            return select(base).where(
                func.date(base.c[self.field]).between(value.start, value.end)
            )

        question_func.__doc__ = question.question_func.__doc__
        question_func.__name__ = f"{question.question_func.__name__}_{value.start.isoformat()}_to_{value.end.isoformat()}"

        return Question(question_func=question_func)


class AcceptedValuesFilter(Filter):
    """
    This filter performs OR query on the selected options.

    Attributes:
        field: name of the column to be filtered.
        title: title of the filter for display purposes.
        default: the default value for the filter.
        selectable_values: the list of values to be displayed in the filter.
    """

    _field_class = String
    default: Optional[str] = None
    selectable_values: Optional[List[str]] = None

    @classmethod
    def from_column_values(cls, column: Column, **kwargs) -> "AcceptedValuesFilter":
        result = bigquery.run(select(column.distinct().label("values")))
        # fixme: validate distinct count before
        df = result.to_dataframe()
        return cls(field=column.key, selectable_values=df["values"].tolist(), **kwargs)

    def filter(self, question: Question, values: List[str]) -> Question:
        if not self.is_valid_for(question):
            raise ValueError("Invalid filter")

        values.sort()

        def question_func():
            base = question.question_func().cte(question.question_func.__name__)
            return select(base).where(base.c[self.field].in_(values))

        question_func.__doc__ = question.question_func.__doc__
        question_func.__name__ = f"{question.question_func.__name__}_{'_'.join((parse_name(v) for v in values))}"

        return Question(question_func=question_func)
