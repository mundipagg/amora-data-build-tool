from typing import Protocol, runtime_checkable


@runtime_checkable
class FeatureViewSourceProtocol(Protocol):
    @classmethod
    def feature_view_entities(cls):
        ...

    @classmethod
    def feature_view_features(cls):
        ...

    @classmethod
    def feature_view_event_timestamp(cls) -> str:
        ...
