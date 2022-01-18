from datetime import datetime
from typing import Optional

from amora.models import AmoraModel, Field, MetaData
from amora.storage import local_storage
from amora.config import settings
from amora.version import VERSION


class AuditLog(AmoraModel, table=True):
    """
    Stores test log data
    """

    metadata = MetaData(schema=None)

    test_run_id: str = Field(
        primary_key=True,
        description="Unique id of the test run",
        nullable=False,
    )
    test_node_id: str = Field(
        primary_key=True,
        description="pytest full node id of the item",
        nullable=False,
    )
    bytes_billed: int = Field(
        description="Total billable scanned bytes during query executions"
    )
    estimated_cost_in_usd: float = Field(
        description="Estimated cost of query executions"
    )
    query: str = Field(description="SQL query executed for data assertion")
    user_email: Optional[str] = Field(
        description="GCP user email that performed the query", nullable=True
    )
    execution_time_in_ms: int = Field(
        description="Query execution time in miliseconds"
    )
    inserted_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC Datetime of the insert",
    )
    referenced_tables: str = Field(
        description="JSON encoded referenced tables on the query", nullable=True
    )
    settings: str = Field(
        description="JSON encoded current`amora.config.settings`",
        default=settings.json(),
    )
    amora_version: str = Field(
        description="Current version of the amora package", default=VERSION
    )


AuditLog.__table__.create(bind=local_storage, checkfirst=True)
