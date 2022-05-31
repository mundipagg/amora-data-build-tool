from datetime import timedelta

from feast import BigQuerySource, FeatureView, Field

from amora.feature_store import settings
from amora.feature_store.protocols import FeatureViewSourceProtocol
from amora.feature_store.type_mapping import SQLALCHEMY_TYPES_TO_FS_DTYPES
from amora.models import Model
from amora.providers.bigquery import get_fully_qualified_id


def name_for_model(model: Model) -> str:
    """
    Feature View Name is the name of the group of features.
    """
    # fixme: type ignore não deveria ser necessário aqui
    return model.__tablename__  # type: ignore


def feature_view_for_model(model: Model) -> FeatureView:
    """
    A feature view is an object that represents a logical group of time-series
    feature data as it is found in a model.
    """
    if not isinstance(model, FeatureViewSourceProtocol):
        raise ValueError(
            f"Feature view models (`@feature_view`) must implement the "
            f"{FeatureViewSourceProtocol.__name__} protocol. "
            f"{model} failed the check"
        )

    return FeatureView(
        name=name_for_model(model),
        entities=[col.name for col in model.feature_view_entities()],
        schema=[
            Field(
                name=col.name,
                dtype=SQLALCHEMY_TYPES_TO_FS_DTYPES[col.type.__class__],
            )
            for col in model.feature_view_features()
        ],
        source=BigQuerySource(
            table=get_fully_qualified_id(model),
            timestamp_field=model.feature_view_event_timestamp().key,
        ),
        ttl=timedelta(seconds=settings.DEFAULT_FEATURE_TTL_IN_SECONDS),
    )
