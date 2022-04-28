from feast import FeatureService

from amora.feature_store.feature_view import feature_view_for_model
from amora.feature_store.registry import FEATURE_REGISTRY
from amora.models import Model


def feature_view(model: Model) -> Model:
    """
    Generates a Feature View and a Feature Service for the decorated model.
    Models decorated with `@feature_view` must implement the `FeatureViewSourceProtocol`

    """
    fv = feature_view_for_model(model)

    service = FeatureService(
        name=f"amora_fs__{fv.name}",
        features=[fv],
    )

    FEATURE_REGISTRY[fv.name] = (fv, service, model)

    return model
