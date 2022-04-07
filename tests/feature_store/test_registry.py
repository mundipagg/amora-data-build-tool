from datetime import datetime

from feast import Feature, ValueType
from amora.feature_store import registry
from amora.feature_store.decorators import feature_view
from amora.models import AmoraModel, Field


def test_get_repo_contents():
    repo_contents = registry.get_repo_contents()
    assert len(repo_contents.feature_views) == 1
    assert len(repo_contents.entities) == 1

    feature_view = repo_contents.feature_views.pop()
    assert feature_view.features == [
        Feature(name="value_avg", dtype=ValueType.FLOAT),
        Feature(name="value_sum", dtype=ValueType.FLOAT),
        Feature(name="value_count", dtype=ValueType.FLOAT),
    ]
    assert feature_view.entities == ["source_name"]


def test_get_repo_contents_with_multiple_calls():
    repo_contents = registry.get_repo_contents()
    assert len(repo_contents.feature_views) == 1
    assert len(repo_contents.entities) == 1

    repo_contents = registry.get_repo_contents()
    assert len(repo_contents.feature_views) == 1
    assert len(repo_contents.entities) == 1

    @feature_view
    class DriverActivity(AmoraModel, table=True):
        datetime_trunc_day: datetime = Field(primary_key=True)
        driver: str = Field(primary_key=True)
        rating: float
        trips_today: int

        @classmethod
        def feature_view_entities(cls):
            return [cls.driver]

        @classmethod
        def feature_view_features(cls):
            return [cls.trips_today, cls.rating]

        @classmethod
        def feature_view_event_timestamp(cls) -> str:
            return cls.datetime_trunc_day

    repo_contents = registry.get_repo_contents()
    assert len(repo_contents.feature_views) == 2
    assert len(repo_contents.entities) == 2
