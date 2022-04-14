from typing import Protocol, runtime_checkable, List

from amora.models import Column


@runtime_checkable
class FeatureViewSourceProtocol(Protocol):
    """
    The contract needed to expose the `AmoraModel` definition as a [Feature View](feature-view.md)
    """

    @classmethod
    def feature_view_entities(cls) -> List[Column]:
        """
        Returns a list of the model columns that should be used as an [entity](feature-view.md#entities). E.g:

        An `AmoraModel` with a single `customer_id` entity:

        ```python
        @classmethod
        def feature_view_entities(cls) -> List[Column]:
            return [cls.customer_id]
        ```

        An `AmoraModel` with multiple entities, `customer_id` and `company_id`:

        ```python
        @classmethod
        def feature_view_entities(cls) -> List[Column]:
            return [cls.customer_id, cls.company_id]
        ```

        A model with no entities:

        ```python
        @classmethod
        def feature_view_entities(cls) -> List[Column]:
            return []
        ```
        """
        ...

    @classmethod
    def feature_view_features(cls) -> List[Column]:
        """
        Returns a list of the model columns that should be used as [features](feature-view.md#features). E.g:

        Features of a customer entity could be _the number of transactions
        they have made on a month_ as `count_transactions_last_30d` and _the sum
        of the transactions amounts they have made on a month_
        as `sum_transactions_last_30d`:

        ```python
        @classmethod
        def feature_view_features(cls) -> List[Column]:
            return [
                cls.count_transactions_last_30d,
                cls.sum_transactions_last_30d
            ]
        ```
        """
        ...

    @classmethod
    def feature_view_event_timestamp(cls) -> Column:
        """
        Event timestamp column used for point-in-time joins of feature values. E.g:

        ```python
        @classmethod
        def feature_view_event_timestamp(cls) -> Column:
            return cls.event_timestamp
        ```

        At your Amora Model, the column should be defined as such:

        ```python
        from sqlalchemy import Column, TIMESTAMP
        from amora.models import AmoraModel, Field


        @feature_view
        class AModel(AmoraModel, table=True):
            ...
            event_timestamp: datetime = Field(sa_column=Column(TIMESTAMP))
        ```

        """
        ...
