from datetime import date, datetime
from typing import Any, List, Optional, Type

from pydantic import BaseModel
from sqlalchemy import TIMESTAMP, Date, DateTime, String, func, select
from sqlalchemy.sql.type_api import TypeEngine

from amora.questions import Question


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
        question_func.__name__ = f"{question.question_func.__name__}_{value}"

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
            f"{question.question_func.__name__}_{value.isoformat()}"
        )

        return Question(question_func=question_func)


class ValueFilter(Filter):
    _field_class = String
