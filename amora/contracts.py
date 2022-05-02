from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


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
    query: Optional[str]
    job_id: Optional[str]
    referenced_tables: List[str]
    user_email: Optional[str]
