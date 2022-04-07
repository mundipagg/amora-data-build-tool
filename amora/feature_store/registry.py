import sqlmodel

from typing import Dict, Iterable, Tuple, List
from feast import FeatureView, Entity, ValueType
from feast.repo_contents import RepoContents
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql import sqltypes
from amora.models import Model, list_models


PYTHON_TYPES_TO_FS_TYPES = {
    sqltypes.Float: ValueType.FLOAT,
    sqltypes.String: ValueType.STRING,
    sqlmodel.AutoString: ValueType.STRING,
    sqltypes.Integer: ValueType.INT64,
    bytes: ValueType.BYTES,
    sqltypes.Boolean: ValueType.BOOL,
    sqltypes.Date: ValueType.UNIX_TIMESTAMP,
    sqltypes.DateTime: ValueType.UNIX_TIMESTAMP,
}

FEATURE_REGISTRY: Dict[str, Tuple[FeatureView, Model]] = {}


def get_entities() -> Iterable[Entity]:
    for _, (fv, model) in FEATURE_REGISTRY.items():
        for entity_name in fv.entities:
            entity_column: InstrumentedAttribute = getattr(model, entity_name)

            yield Entity(
                name=entity_name,
                value_type=PYTHON_TYPES_TO_FS_TYPES[entity_column.type.__class__],
                description=entity_column.comment,
            )


def get_feature_views() -> List[FeatureView]:
    return [fv for (fv, _model) in FEATURE_REGISTRY.values()]


def get_repo_contents() -> RepoContents:
    # fixme: making sure that we've collected all Feature Views
    _models = list(list_models())

    return RepoContents(
        feature_views=set(get_feature_views()),
        entities=set(get_entities()),
        feature_services=set(),
        on_demand_feature_views=set(),
        request_feature_views=set(),
    )
