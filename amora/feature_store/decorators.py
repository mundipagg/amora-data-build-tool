from feast import FeatureView, Feature, BigQuerySource
from google.protobuf.duration_pb2 import Duration
from amora.feature_store.config import settings
from amora.feature_store.registry import FEATURE_REGISTRY, PYTHON_TYPES_TO_FS_TYPES
from amora.feature_store.protocols import FeatureViewSourceProtocol
from amora.models import Model
from amora.providers.bigquery import get_fully_qualified_id


def feature_view(model: Model) -> Model:
    if not isinstance(model, FeatureViewSourceProtocol):
        raise ValueError(
            f"Feature view models (`@feature_view`) must implement the "
            f"{FeatureViewSourceProtocol.__name__} protocol. "
            f"{model} failed the check"
        )

    FEATURE_REGISTRY[model] = FeatureView(
        name=model.__tablename__,
        entities=[col.name for col in model.feature_view_entities()],
        features=[
            Feature(
                name=col.name,
                dtype=PYTHON_TYPES_TO_FS_TYPES[col.type.__class__],
            )
            for col in model.feature_view_features()
        ],
        batch_source=BigQuerySource(
            table_ref=get_fully_qualified_id(model),
            event_timestamp_column=model.feature_view_event_timestamp(),
        ),
        ttl=Duration(seconds=settings.DEFAULT_FEATURE_TTL_IN_SECONDS),
    )

    return model