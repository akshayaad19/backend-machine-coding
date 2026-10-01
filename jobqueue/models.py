from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional, Union

MIN_PRIORITY = 0
MAX_PRIORITY = 9


class JobState(enum.Enum):
    PENDING = "pending"
    CLAIMED = "claimed"
    RETRY_SCHEDULED = "retry_scheduled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ---------- errors ----------

class JobServiceError(Exception):
    pass


class ValidationError(JobServiceError, ValueError):
    pass


class JobNotFound(JobServiceError, KeyError):
    pass


class UnknownWorker(JobServiceError, KeyError):
    pass


class LeaseLost(JobServiceError):
    """The caller's lease token is no longer the job's current lease (expired, re-leased, or job finished)."""

class Cancelled(JobServiceError):
    """The job cannot be cancelled."""
# ---------- results ----------

@dataclass(frozen=True)
class Success:
    pass


@dataclass(frozen=True)
class TransientFailure:
    error: str = ""


@dataclass(frozen=True)
class PermanentFailure:
    error: str = ""


Result = Union[Success, TransientFailure, PermanentFailure]
RESULT_TYPES = (Success, TransientFailure, PermanentFailure)


# ---------- jobs ----------

@dataclass(frozen=True)
class JobSpec:
    type: str
    payload: Any = None
    priority: int = 5
    max_retries: int = 3

    def validate(self) -> None:
        if not isinstance(self.type, str) or not self.type.strip():
            raise ValidationError("type must be a non-empty string")
        if not _is_int(self.priority) or not MIN_PRIORITY <= self.priority <= MAX_PRIORITY:
            raise ValidationError(f"priority must be an int in [{MIN_PRIORITY}, {MAX_PRIORITY}]")
        if not _is_int(self.max_retries) or self.max_retries < 0:
            raise ValidationError("max_retries must be an int >= 0")


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass
class Lease:
    token: str
    worker_id: str
    expires_at: float


@dataclass
class Job:
    id: str
    spec: JobSpec
    created_at: float
    state: JobState = JobState.PENDING
    attempts: int = 0  # number of times the job has been claimed
    lease: Optional[Lease] = None
    claimed_at: Optional[float] = None
    ready_at: float = 0.0
    last_error: Optional[str] = None


@dataclass(frozen=True)
class LeasedJob:
    """What a worker receives from claim(). Plain data so it could cross a network boundary."""
    job_id: str
    type: str
    payload: Any
    attempt: int
    lease_token: str
    lease_duration: float
    expires_at: float


@dataclass(frozen=True)
class DeadLetter:
    job_id: str
    type: str
    payload: Any
    priority: int
    attempts: int
    reason: str
    error: Optional[str]
    failed_at: float


@dataclass
class WorkerStats:
    worker_id: str
    types: frozenset
    current_job_id: Optional[str] = None
    succeeded: int = 0
    failed: int = 0
    leases_lost: int = 0


# ---------- handler interface ----------

@dataclass(frozen=True)
class JobContext:
    """Passed to handlers. job_id is stable across attempts, so it is the idempotency key."""
    job_id: str
    attempt: int


class JobHandler(ABC):
    """Handlers MUST be idempotent: delivery is at-least-once, so run() may execute more than once per job."""

    @abstractmethod
    def run(self, payload: Any, ctx: JobContext) -> Result:
        ...
