from datetime import date
from typing import List, Optional

from pydantic import BaseModel

from amora.questions import Question


class Filter(BaseModel):
    type: str
    id: str
    default: str
    title: str


class DateFilter(Filter):
    type = "date"
    default: date = date.today()
    python_type = date
    min_selectable_date: Optional[date] = None
    max_selectable_date: Optional[date] = None


class AcceptedValuesFilter(Filter):
    type = "accepted_values"
    values: List[str]
    default: Optional[str] = None

    # todo: validate that "self.default in self.values"


class Dashboard(BaseModel):
    id: str
    name: str
    questions: List[List[Question]]
    filters: List[Filter]

    class Config:
        arbitrary_types_allowed = True
