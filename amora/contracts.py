from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List


@dataclass
class TimingInfo:
    name: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


@dataclass
class BaseResult:
    """
    Base dataclass for query execution result data
    """

    total_bytes: int
    query: str
    job_id: str
    referenced_tables: List[str]
    user_email: str
    execution_time_in_ms: int
