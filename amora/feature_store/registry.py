from typing import Dict, Iterable, List, Tuple

from feast import Entity, FeatureService, FeatureView
from feast.repo_contents import RepoContents
from sqlalchemy.orm import InstrumentedAttribute

from amora.feature_store.feature_view import name_for_model
from amora.feature_store.type_mapping import value_type_for_column
from amora.models import Model, list_models

FEATURE_REGISTRY: Dict[str, Tuple[FeatureView, FeatureService, Model]] = {}


def get_entities() -> Iterable[Entity]:
    for fv, _service, model in FEATURE_REGISTRY.values():
        for entity_name in fv.entities:
            entity_column: InstrumentedAttribute = getattr(model, entity_name)

            yield Entity(
                name=entity_name,
                value_type=value_type_for_column(entity_column),
                description=entity_column.comment or "",
            )


def get_feature_views() -> List[FeatureView]:
    return [fv for (fv, _service, _model) in FEATURE_REGISTRY.values()]


def get_feature_service(model: Model) -> FeatureService:
    (_fv, service, _model) = FEATURE_REGISTRY[name_for_model(model)]
    return service


def get_feature_services() -> List[FeatureService]:
    return [service for (_fv, service, _model) in FEATURE_REGISTRY.values()]


def get_repo_contents() -> RepoContents:
    # fixme: making sure that we've collected all Feature Views
    _models = list(list_models())

    feature_views = list(set(get_feature_views()))
    entities = list(set(get_entities()))
    feature_services = list(set(get_feature_services()))
    data_sources = [fv.batch_source for fv in feature_views]

    return RepoContents(
        data_sources=data_sources,
        feature_views=feature_views,
        entities=entities,
        feature_services=feature_services,
        on_demand_feature_views=[],
        request_feature_views=[],
        stream_feature_views=[],
    )
