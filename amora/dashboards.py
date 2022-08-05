from datetime import date
from typing import Any, List, Optional

from pydantic import BaseModel

from amora.questions import Question


class Filter(BaseModel):
    type: str
    id: str
    default: Any
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

def dashboard_for_path(path: Path) -> Dashboard:
    spec = spec_from_file_location(".".join(["amoradashboard", path.stem]), path)
    if spec is None:
        raise ValueError(f"Invalid path `{path}`. Not a valid Python file.")

    module = module_from_spec(spec)

    if spec.loader is None:
        raise ValueError(f"Invalid Dashboard path `{path}`. Unable to load module.")

    try:
        spec.loader.exec_module(module)  # type: ignore
    except Exception as e:
        raise ValueError(
            f"Invalid Dashboard path `{path}`. Unable to execute module."
        ) from e

    dashboard = getattr(module, "dashboard")
    assert isinstance(dashboard, Dashboard)
    return dashboard
