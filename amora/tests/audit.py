import os
from datetime import datetime
from typing import Iterable, Optional

from sqlalchemy import func

from amora.config import settings as default_settings
from amora.models import (
    AmoraModel,
    Field,
    MaterializationTypes,
    ModelConfig,
    Session,
    select,
)
from amora.storage import local_engine, local_metadata
from amora.types import Compilable
from amora.version import VERSION


class AuditLog(AmoraModel, table=True):
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        description="Stores test log data",
    )
    metadata = local_metadata

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
    execution_time_in_ms: int = Field(description="Query execution time in miliseconds")
    inserted_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC Datetime of the insert",
    )
    referenced_tables: str = Field(
        description="JSON encoded referenced tables on the query", nullable=True
    )
    settings: str = Field(
        description="JSON encoded current`amora.config.settings`",
        default=default_settings.json(),
    )
    amora_version: str = Field(
        description="Current version of the amora package", default=VERSION
    )
    xdist_worker_id: Optional[str] = Field(
        description="The name of the `pytest-xdist` worker. E.g.: gw2",
        default=os.getenv("PYTEST_XDIST_WORKER"),
    )

    @classmethod
    def get_all(cls, test_run_id: str) -> Iterable["AuditLog"]:
        with Session(local_engine) as session:
            statement = select(cls).where(cls.test_run_id == test_run_id)
            return (log for (log, *_) in session.exec(statement).all())  # type: ignore


class AuditReport(AmoraModel, table=True):
    __depends_on__ = [AuditLog]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.view,
        description="Stores test run reports",
    )
    metadata = local_metadata

    test_run_id: str = Field(primary_key=True)
    amora_version: str = Field(
        description="Current version of the amora package", default=VERSION
    )
    user_email: Optional[str] = Field(
        description="GCP user email that performed the query", nullable=True
    )

    total_query_time: int
    total_cost: float
    total_bytes_billed: int

    @classmethod
    def source(cls) -> Optional[Compilable]:
        raise NotImplementedError  # WIP

        return select(
            AuditLog.test_run_id,
            AuditLog.amora_version,
            AuditLog.user_email,
            func.sum(AuditLog.execution_time_in_ms).label(cls.total_query_time.key),
            func.sum(AuditLog.estimated_cost_in_usd).label(cls.total_cost.key),
            func.sum(AuditLog.bytes_billed).label(cls.total_bytes_billed.key),
        ).group_by(AuditLog.test_run_id, AuditLog.amora_version, AuditLog.user_email)


AuditLog.__table__.create(bind=local_engine, checkfirst=True)
