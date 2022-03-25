from typing import Dict

from feast import FeatureView
from feast.repo_contents import RepoContents

from amora.models import Model, list_models

FEATURE_REGISTRY: Dict[Model, FeatureView] = {}


def get_repo_contents() -> RepoContents:
    # fixme: making sure that we've collected all Feature Views
    _models = list(list_models())

    return RepoContents(
        feature_views=set(FEATURE_REGISTRY.values()),
        entities=set(),
        feature_services=set(),
        on_demand_feature_views=set(),
        request_feature_views=set(),
    )
