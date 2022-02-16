from amora.config import settings
from amora.models import Model
from amora.providers.bigquery import get_fully_qualified_id
from datetime import datetime
from feast import (
    FeatureView,
    FeatureStore,
    BigQuerySource,
    Feature,
    ValueType,
    RepoConfig,
)
from google.protobuf.duration_pb2 import Duration
from typing import Dict


FEATURE_REGISTRY: Dict[Model, FeatureView] = {}
PYTHON_TYPES_TO_FS_TYPES = {
    float: ValueType.FLOAT,
    str: ValueType.STRING,
    int: ValueType.INT64,
    bytes: ValueType.BYTES,
    bool: ValueType.BOOL,
    datetime: ValueType.UNIX_TIMESTAMP,
}

repo_config = RepoConfig(
    registry=settings.FEATURE_STORE_REGISTRY,
    project="amora",
    provider=settings.FEATURE_STORE_PROVIDER,
    online_store={
        "type": settings.FEATURE_STORE_ONLINE_STORE_TYPE,
        **settings.FEATURE_STORE_ONLINE_STORE_CONFIG,
    },
    offline_store={
        "type": settings.FEATURE_STORE_OFFLINE_STORE_TYPE,
        **settings.FEATURE_STORE_OFFLINE_STORE_CONFIG,
    },
)

fs = FeatureStore(config=repo_config)


def feature_view(model: Model):
    columns = model.__table__.columns

    FEATURE_REGISTRY[model] = FeatureView(
        name=model.unique_name,
        entities=[col for col in columns if col.primary_key],
        features=[
            Feature(
                name=col.name,
                dtype=PYTHON_TYPES_TO_FS_TYPES[col.type.python_type],
            )
            for col in columns
        ],
        batch_source=BigQuerySource(table_ref=get_fully_qualified_id(model)),
        ttl=Duration(seconds=settings.FS_DEFAULT_FEATURE_TTL_IN_SECONDS),
    )

    return model
