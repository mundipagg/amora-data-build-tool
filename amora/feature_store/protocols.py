from typing import Protocol, runtime_checkable, List

from amora.models import Column


@runtime_checkable
class FeatureViewSourceProtocol(Protocol):
    @classmethod
    def feature_view_entities(cls) -> List[Column]:
        ...

    @classmethod
    def feature_view_features(cls) -> List[Column]:
        ...

    @classmethod
    def feature_view_event_timestamp(cls) -> Column:
        ...
