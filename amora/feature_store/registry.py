from typing import Dict, Iterable, List, Tuple

from feast import Entity, FeatureService, FeatureView
from feast.diff.property_diff import TransitionType
from feast.diff.registry_diff import RegistryDiff
from feast.repo_contents import RepoContents
from pydantic import BaseModel
from sqlalchemy.orm import InstrumentedAttribute

from amora.feature_store.feature_view import name_for_model
from amora.feature_store.type_mapping import VALUE_TYPE_TO_NAME, value_type_for_column
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


class ObjectDiff(BaseModel):
    name: str
    transition_type: str
    object_type: str


class PropertyDiff(BaseModel):
    object_name: str
    property_name: str
    declared: str
    existing: str


class FeatureDiff(BaseModel):
    name: str
    diff: Iterable


class Diff(BaseModel):
    objects: List[ObjectDiff]
    properties: List[PropertyDiff]
    features: List[FeatureDiff]


def parse_feature_schema_diff(existing: str, declared: str):
    def parse_diff_as_feature_records(value):
        records = set()
        for item in value:
            if hasattr(item, "feature_columns"):
                return parse_diff_as_feature_records(item.feature_columns)

            records.add((item.name, VALUE_TYPE_TO_NAME[item.value_type]))

        return records

    existing_features, declared_features = parse_diff_as_feature_records(
        existing
    ), parse_diff_as_feature_records(declared)

    removed = existing_features - declared_features
    added = declared_features - existing_features

    for name, value_type in added:
        yield {"name": name, "value_type": value_type, "change": "added"}

    for name, value_type in removed:
        yield {"name": f"~~{name}~~", "value_type": value_type, "change": "removed"}


def parse_diff(registry_diff: RegistryDiff):
    objects = []
    properties = []
    features = []

    for object_diff in registry_diff.feast_object_diffs:
        if object_diff.transition_type is TransitionType.UNCHANGED:
            continue

        objects.append(
            ObjectDiff(
                name=object_diff.name,
                transition_type=object_diff.transition_type.name,
                object_type=object_diff.feast_object_type.name,
            )
        )

        for property_diff in object_diff.feast_object_property_diffs:
            if property_diff.property_name == "features":
                features.append(
                    FeatureDiff(
                        name=object_diff.name,
                        diff=parse_feature_schema_diff(
                            property_diff.val_existing,
                            property_diff.val_declared,
                        ),
                    )
                )

            else:
                properties.append(
                    PropertyDiff(
                        object_name=object_diff.name,
                        property_name=property_diff.property_name,
                        declared=str(property_diff.val_declared),
                        existing=str(property_diff.val_existing),
                    )
                )

    return Diff(
        objects=objects,
        properties=properties,
        features=features,
    )
