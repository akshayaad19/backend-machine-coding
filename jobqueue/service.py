from __future__ import annotations

import copy
import heapq
import itertools
import threading
import uuid
from collections import defaultdict, deque
from typing import Dict, Iterable, List, Optional, Tuple

from .models import (
    RESULT_TYPES, DeadLetter, Job, JobNotFound, JobSpec, JobState, Lease, LeasedJob,
    LeaseLost, PermanentFailure, Result, Success, TransientFailure, UnknownWorker,
    ValidationError, WorkerStats,
)
from .policies import (
    Clock, ExponentialBackoff, PriorityScheduler, RetryPolicy, SystemClock,
    WeightedRoundRobinScheduler,
)

DEFAULT_LEASE_DURATION = 5.0


class JobService:
    """In-process job queue with leases, retries, DLQ and stats.

    All state lives behind one lock. Expired leases and due retries are processed
    lazily at the start of every public call, so correctness never depends on a
    background timer. The in-memory dicts/queues below are the persistence boundary:
    a durable store would replace them behind the same public API.
    """

    def __init__(self, clock: Optional[Clock] = None, retry_policy: Optional[RetryPolicy] = None,
                 scheduler: Optional[PriorityScheduler] = None,
                 default_lease_duration: float = DEFAULT_LEASE_DURATION):
        self._clock = clock or SystemClock()
        self._retry_policy = retry_policy or ExponentialBackoff()
        self._scheduler = scheduler or WeightedRoundRobinScheduler()
        self._default_lease_duration = default_lease_duration
        self._lease_durations: Dict[str, float] = {}

        self._lock = threading.RLock()
        self._jobs: Dict[str, Job] = {}
        self._ready: Dict[Tuple[str, int], deque] = defaultdict(deque)  # (type, priority) -> FIFO of (seq, job_id)
        self._delayed: List[Tuple[float, int, str]] = []  # min-heap of (ready_at, seq, job_id)
        self._in_flight: Dict[str, Job] = {}
        self._dlq: List[DeadLetter] = []
        self._workers: Dict[str, WorkerStats] = {}
        self._seq = itertools.count()

    # ---------- configuration ----------

    def register_job_type(self, job_type: str, lease_duration: float) -> None:
        if lease_duration <= 0:
            raise ValidationError("lease_duration must be > 0")
        with self._lock:
            self._lease_durations[job_type] = lease_duration

    def register_worker(self, worker_id: str, types: Iterable[str]) -> None:
        types = frozenset(types)
        if not worker_id:
            raise ValidationError("worker_id must be non-empty")
        if not types:
            raise ValidationError("worker must handle at least one type")
        with self._lock:
            self._workers[worker_id] = WorkerStats(worker_id=worker_id, types=types)

    # ---------- producer API ----------

    def submit(self, spec: JobSpec) -> str:
        spec.validate()
        with self._lock:
            self._housekeeping()
            now = self._clock.now()
            job = Job(id=uuid.uuid4().hex, spec=spec, created_at=now, ready_at=now)
            self._jobs[job.id] = job
            self._enqueue_ready(job)
            return job.id

    # ---------- worker API ----------

    def claim(self, worker_id: str) -> Optional[LeasedJob]:
        with self._lock:
            self._housekeeping()
            worker = self._get_worker(worker_id)
            eligible = {p for (t, p), q in self._ready.items() if q and t in worker.types}
            if not eligible:
                return None
            priority = self._scheduler.pick(eligible)

            # FIFO across all of this worker's types at the chosen priority.
            queues = [self._ready[(t, priority)] for t in worker.types if self._ready.get((t, priority))]
            queue = min(queues, key=lambda q: q[0][0])
            _, job_id = queue.popleft()
            job = self._jobs[job_id]

            now = self._clock.now()
            duration = self._lease_duration_for(job.spec.type)
            job.state = JobState.CLAIMED
            job.attempts += 1
            job.claimed_at = now
            job.lease = Lease(token=uuid.uuid4().hex, worker_id=worker_id, expires_at=now + duration)
            self._in_flight[job.id] = job
            worker.current_job_id = job.id

            return LeasedJob(job_id=job.id, type=job.spec.type, payload=job.spec.payload,
                             attempt=job.attempts, lease_token=job.lease.token,
                             lease_duration=duration, expires_at=job.lease.expires_at)

    def renew(self, job_id: str, lease_token: str) -> float:
        """Extend the lease by the type's lease duration. Returns the new expiry."""
        with self._lock:
            self._housekeeping()
            job = self._check_lease(job_id, lease_token)
            job.lease.expires_at = self._clock.now() + self._lease_duration_for(job.spec.type)
            return job.lease.expires_at

    def cancel_job(self, job_id: str, lease_token: str) -> None:
       """Cancel a job. If it is in-flight, the lease is released."""
       with self._lock:
           self._housekeeping()
           job = self._get_job(job_id)
           if job.state is JobState.PENDING or job.state is JobState.RETRY_SCHEDULED:
                  job.state = JobState.CANCELLED
           elif job.state is JobState.CLAIMED and job.lease is not None:
               return copy.error("cannot cancel a job that is currently claimed")
        

    def complete(self, job_id: str, lease_token: str, result: Result) -> None:
        """Record a job's outcome. Any stale lease (success or failure) is rejected with LeaseLost."""
        if not isinstance(result, RESULT_TYPES):
            raise ValidationError(f"result must be one of {[t.__name__ for t in RESULT_TYPES]}")
        with self._lock:
            self._housekeeping()
            job = self._check_lease(job_id, lease_token)
            worker = self._workers.get(job.lease.worker_id)
            self._release_lease(job)

            if isinstance(result, Success):
                job.state = JobState.SUCCEEDED
                if worker:
                    worker.succeeded += 1
                return

            if worker:
                worker.failed += 1
            job.last_error = result.error
            if isinstance(result, PermanentFailure):
                self._dead_letter(job, "permanent_failure")
            elif job.attempts > job.spec.max_retries:
                self._dead_letter(job, "retries_exhausted")
            else:
                self._schedule_retry(job)

    # ---------- observability ----------

    def pending_count_by_type(self) -> Dict[str, int]:
        with self._lock:
            self._housekeeping()
            counts: Dict[str, int] = defaultdict(int)
            for (job_type, _), q in self._ready.items():
                if q:
                    counts[job_type] += len(q)
            return dict(counts)

    def pending_count_by_priority(self) -> Dict[int, int]:
        with self._lock:
            self._housekeeping()
            counts: Dict[int, int] = defaultdict(int)
            for (_, priority), q in self._ready.items():
                if q:
                    counts[priority] += len(q)
            return dict(counts)

    def retry_scheduled_count(self) -> int:
        with self._lock:
            self._housekeeping()
            return len(self._delayed)

    def in_flight_count(self) -> int:
        with self._lock:
            self._housekeeping()
            return len(self._in_flight)

    def dlq_size(self) -> int:
        with self._lock:
            self._housekeeping()
            return len(self._dlq)

    def list_dlq(self, limit: Optional[int] = None) -> List[DeadLetter]:
        with self._lock:
            self._housekeeping()
            return list(self._dlq if limit is None else self._dlq[:limit])

    def worker_stats(self) -> Dict[str, WorkerStats]:
        with self._lock:
            self._housekeeping()
            return {w_id: copy.copy(w) for w_id, w in self._workers.items()}

    def stuck_jobs(self, running_longer_than: float) -> List[str]:
        """Claimed jobs held (and renewed) for longer than the threshold."""
        with self._lock:
            self._housekeeping()
            now = self._clock.now()
            return [j.id for j in self._in_flight.values() if now - j.claimed_at > running_longer_than]

    def get_job(self, job_id: str) -> Job:
        with self._lock:
            self._housekeeping()
            return copy.deepcopy(self._get_job(job_id))

    # ---------- internals ----------

    def _housekeeping(self) -> None:
        now = self._clock.now()
        self._reap_expired_leases(now)
        self._promote_due_retries(now)

    def _reap_expired_leases(self, now: float) -> None:
        expired = [j for j in self._in_flight.values() if j.lease.expires_at <= now]
        for job in expired:
            worker = self._workers.get(job.lease.worker_id)
            if worker:
                worker.leases_lost += 1
            self._release_lease(job)
            job.last_error = "lease expired"
            # A lease expiry counts as an attempt, so a job that keeps crashing workers ends up in the DLQ.
            if job.attempts > job.spec.max_retries:
                self._dead_letter(job, "lease_expired_retries_exhausted")
            else:
                self._enqueue_ready(job)

    def _promote_due_retries(self, now: float) -> None:
        while self._delayed and self._delayed[0][0] <= now:
            _, _, job_id = heapq.heappop(self._delayed)
            self._enqueue_ready(self._jobs[job_id])

    def _enqueue_ready(self, job: Job) -> None:
        job.state = JobState.PENDING
        self._ready[(job.spec.type, job.spec.priority)].append((next(self._seq), job.id))

    def _schedule_retry(self, job: Job) -> None:
        job.state = JobState.RETRY_SCHEDULED
        job.ready_at = self._clock.now() + self._retry_policy.next_delay(job.attempts)
        heapq.heappush(self._delayed, (job.ready_at, next(self._seq), job.id))

    def _dead_letter(self, job: Job, reason: str) -> None:
        job.state = JobState.FAILED
        self._dlq.append(DeadLetter(
            job_id=job.id, type=job.spec.type, payload=job.spec.payload,
            priority=job.spec.priority, attempts=job.attempts, reason=reason,
            error=job.last_error, failed_at=self._clock.now(),
        ))

    def _release_lease(self, job: Job) -> None:
        self._in_flight.pop(job.id, None)
        worker = self._workers.get(job.lease.worker_id)
        if worker and worker.current_job_id == job.id:
            worker.current_job_id = None
        job.lease = None

    def _check_lease(self, job_id: str, lease_token: str) -> Job:
        job = self._get_job(job_id)
        if job.state is not JobState.CLAIMED or job.lease is None or job.lease.token != lease_token:
            raise LeaseLost(f"lease for job {job_id} is no longer held by this token")
        return job

    def _lease_duration_for(self, job_type: str) -> float:
        return self._lease_durations.get(job_type, self._default_lease_duration)

    def _get_job(self, job_id: str) -> Job:
        try:
            return self._jobs[job_id]
        except KeyError:
            raise JobNotFound(job_id) from None

    def _get_worker(self, worker_id: str) -> WorkerStats:
        try:
            return self._workers[worker_id]
        except KeyError:
            raise UnknownWorker(worker_id) from None
