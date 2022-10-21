from datetime import timedelta

from feast import BigQuerySource, Entity, FeatureView, Field

from amora.feature_store import settings
from amora.feature_store.protocols import FeatureViewSourceProtocol
from amora.feature_store.type_mapping import feast_type_for_colum
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
        entities=[Entity(name=col.name) for col in model.feature_view_entities()],
        schema=[
            Field(
                name=col.name,
                dtype=feast_type_for_colum(col),
            )
            for col in model.feature_view_features()
        ],
        source=BigQuerySource(
            table=get_fully_qualified_id(model),
            timestamp_field=model.feature_view_event_timestamp().name,
        ),
        ttl=timedelta(seconds=settings.DEFAULT_FEATURE_TTL_IN_SECONDS),
        owner=model.owner(),
        description=model.__model_config__.description,
    )
