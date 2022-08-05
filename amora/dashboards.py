from datetime import date
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from amora.config import settings
from amora.logger import logger
from amora.questions import Question
from amora.utils import list_files


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


DashboardUid = str


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


def list_dashboards(
    path: Path = settings.DASHBOARDS_PATH,
) -> Dict[DashboardUid, Dashboard]:
    """
    Searches for python files with
    Args:
        path: The path to search for Dashboard definitions on files

    Returns:
        A dict of all available dashboards
    """
    for file_path in list_files(path, suffix=".py"):
        if file_path.stem.startswith("_"):
            continue

        try:
            dashboard = dashboard_for_path(file_path)
        except ValueError:
            logger.exception(f"Unable to load dashboard")
        else:
            DASHBOARDS[dashboard.uid] = dashboard

    return DASHBOARDS
