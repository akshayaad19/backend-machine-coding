from __future__ import annotations

import logging
import threading
from typing import Dict, Optional

from .models import RESULT_TYPES, JobContext, JobHandler, LeasedJob, LeaseLost, Result, TransientFailure
from .service import JobService

log = logging.getLogger(__name__)


class Worker:
    """Pulls jobs for its types, renews the lease in the background while the handler runs, then completes."""

    def __init__(self, service: JobService, worker_id: str, handlers: Dict[str, JobHandler],
                 poll_interval: float = 0.05, renew_fraction: float = 1 / 3):
        self.worker_id = worker_id
        self._service = service
        self._handlers = dict(handlers)
        self._poll_interval = poll_interval
        self._renew_fraction = renew_fraction
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        service.register_worker(worker_id, self._handlers.keys())

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name=f"worker-{self.worker_id}", daemon=True)
        self._thread.start()

    def stop(self, timeout: Optional[float] = None) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout)

    def run_once(self) -> bool:
        """Claim and process one job. Returns False if nothing was available."""
        leased = self._service.claim(self.worker_id)
        if leased is None:
            return False

        stop_renewing = threading.Event()
        renewer = threading.Thread(target=self._renew_loop, args=(leased, stop_renewing), daemon=True)
        renewer.start()
        try:
            result = self._execute(leased)
        finally:
            stop_renewing.set()
            renewer.join()

        try:
            self._service.complete(leased.job_id, leased.lease_token, result)
        except LeaseLost:
            log.warning("worker %s lost lease on job %s; result discarded", self.worker_id, leased.job_id)
        return True

    def _loop(self) -> None:
        while not self._stop.is_set():
            if not self.run_once():
                self._stop.wait(self._poll_interval)

    def _execute(self, leased: LeasedJob) -> Result:
        handler = self._handlers[leased.type]
        try:
            result = handler.run(leased.payload, JobContext(job_id=leased.job_id, attempt=leased.attempt))
        except Exception as exc:  # a crashing handler is treated as a transient failure
            return TransientFailure(f"{type(exc).__name__}: {exc}")
        if not isinstance(result, RESULT_TYPES):
            return TransientFailure(f"handler returned non-Result: {result!r}")
        return result

    def _renew_loop(self, leased: LeasedJob, stop: threading.Event) -> None:
        interval = leased.lease_duration * self._renew_fraction
        while not stop.wait(interval):
            try:
                self._service.renew(leased.job_id, leased.lease_token)
            except LeaseLost:
                return
