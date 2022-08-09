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
    """
    A set of one or more questions, organized and arranged into one or more rows,
    that provide an at-a-glance view of related information. Dashboards can optionally
    contain `filters`, that act upon `questions`, refining it's answers.

    A new dashboard should be defined as a Python module with a `dashboard: Dashboard`
    attribute. E.g:

    ```python
    # $AMORA_PROJECT_PATH/dashboards/steps.py

    from amora.dashboards import AcceptedValuesFilter, Dashboard, DateFilter
    from examples.amora_project.models.step_count_by_source import (
        how_many_data_points_where_acquired,
        what_are_the_available_data_sources,
        what_are_the_values_observed_on_the_iphone,
        what_is_the_current_estimated_walked_distance,
        what_is_the_latest_data_point,
        what_is_the_total_step_count_to_date,
    )

    dashboard = Dashboard(
        uid="1",
        name="Health :: Step Analysis",
        questions=[
            [
                what_is_the_latest_data_point,
                what_is_the_current_estimated_walked_distance,
                how_many_data_points_where_acquired,
            ],
            [
                what_is_the_total_step_count_to_date,
                what_are_the_values_observed_on_the_iphone,
                what_are_the_available_data_sources,
            ],
        ],
        filters=[
            DateFilter(
                default="2021-01-01", title="data de início", id="start-date-filter"
            ),
            DateFilter(default="2023-01-01", title="data fim", id="end-date-filter"),
            AcceptedValuesFilter(
                default="iPhone",
                values=["Diogo's iPhone", "iPhone"],
                title="Source device",
                id="source-device-filter",
            ),
        ],
    )
    ```
    """

    uid: DashboardUid
    name: str
    questions: List[List[Question]]
    filters: List[Filter]

    class Config:
        arbitrary_types_allowed = True


DASHBOARDS: Dict[DashboardUid, Dashboard] = dict()


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
    path: Path = settings.dashboards_path,
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
            logger.exception("Unable to load dashboard")
        else:
            DASHBOARDS[dashboard.uid] = dashboard

    return DASHBOARDS
