from datetime import datetime

import pytest
from feast import Entity, FeatureService, FeatureView

from amora.feature_store import registry
from amora.feature_store.decorators import feature_view
from amora.feature_store.feature_view import name_for_model
from amora.models import AmoraModel, Field

from tests.models.step_count_by_source import StepCountBySource


def test_get_repo_contents():
    repo_contents = registry.get_repo_contents()

    assert len(repo_contents.feature_views) > 0
    for fv in repo_contents.feature_views:
        assert isinstance(fv, FeatureView)

    assert len(repo_contents.entities) > 0
    for entity in repo_contents.entities:
        assert isinstance(entity, Entity)


def test_get_repo_contents_with_multiple_calls():
    repo_contents = registry.get_repo_contents()
    first_call_fvs = repo_contents.feature_views

    repo_contents = registry.get_repo_contents()
    assert len(first_call_fvs) == len(repo_contents.feature_views)

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
    expected_fv_name = name_for_model(DriverActivity)
    for fv in repo_contents.feature_views:
        if fv.name == expected_fv_name:
            return

    pytest.fail(
        f"{expected_fv_name} not found in Feast's RepoContents. Repo feature views: {repo_contents.feature_views}"
    )


def test_get_feature_service():
    feature_service = registry.get_feature_service(StepCountBySource)

    assert isinstance(feature_service, FeatureService)
    assert feature_service.name == "amora_fs__step_count_by_source"
    assert len(feature_service.feature_view_projections) == 1

    feature_view_projection = feature_service.feature_view_projections[0]
    assert feature_view_projection.name == StepCountBySource.__tablename__
