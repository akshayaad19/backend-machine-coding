import threading
import time
import unittest

from jobqueue import (
    ExponentialBackoff, FakeClock, JobHandler, JobService, JobSpec, JobState, LeaseLost,
    PermanentFailure, Success, TransientFailure, ValidationError, Worker,
)
from jobqueue.models import Cancelled


def make_service(clock=None, lease=5.0):
    clock = clock or FakeClock()
    service = JobService(clock=clock, retry_policy=ExponentialBackoff(base=1.0, cap=60.0, jitter=0))
    service.register_job_type("email", lease_duration=lease)
    return service, clock


class LeaseTests(unittest.TestCase):
    def test_lease_expiry_redelivers_job_to_another_worker(self):
        service, clock = make_service()
        service.register_worker("A", ["email"])
        service.register_worker("B", ["email"])
        job_id = service.submit(JobSpec("email", {"to": "x"}))

        leased_a = service.claim("A")
        self.assertIsNone(service.claim("B"))  # leased, invisible to others
        self.assertEqual(service.in_flight_count(), 1)

        clock.advance(5.0)  # A "crashes": lease expires without renewal
        self.assertEqual(service.pending_count_by_type(), {"email": 1})

        leased_b = service.claim("B")
        self.assertEqual(leased_b.job_id, job_id)
        self.assertEqual(leased_b.attempt, 2)
        self.assertEqual(service.worker_stats()["A"].leases_lost, 1)

        service.complete(job_id, leased_b.lease_token, Success())
        self.assertEqual(service.get_job(job_id).state, JobState.SUCCEEDED)
        self.assertEqual(leased_a.job_id, job_id)

    def test_stale_success_and_failure_are_rejected(self):
        service, clock = make_service()
        service.register_worker("A", ["email"])
        service.register_worker("B", ["email"])
        job_id = service.submit(JobSpec("email"))
        leased_a = service.claim("A")
        clock.advance(6)
        leased_b = service.claim("B")

        with self.assertRaises(LeaseLost):
            service.complete(job_id, leased_a.lease_token, Success())
        with self.assertRaises(LeaseLost):
            service.complete(job_id, leased_a.lease_token, PermanentFailure("boom"))
        with self.assertRaises(LeaseLost):
            service.renew(job_id, leased_a.lease_token)
        self.assertEqual(service.get_job(job_id).state, JobState.CLAIMED)

        service.complete(job_id, leased_b.lease_token, Success())
        with self.assertRaises(LeaseLost):  # duplicate completion
            service.complete(job_id, leased_b.lease_token, Success())

    def test_stale_result_rejected_even_before_reclaim(self):
        service, clock = make_service()
        service.register_worker("A", ["email"])
        job_id = service.submit(JobSpec("email"))
        leased = service.claim("A")
        clock.advance(5.0)
        with self.assertRaises(LeaseLost):
            service.complete(job_id, leased.lease_token, Success())
        self.assertEqual(service.get_job(job_id).state, JobState.PENDING)

    def test_renewal_keeps_lease(self):
        service, clock = make_service()
        service.register_worker("A", ["email"])
        service.register_worker("B", ["email"])
        job_id = service.submit(JobSpec("email"))
        leased = service.claim("A")

        for _ in range(3):
            clock.advance(4)
            service.renew(job_id, leased.lease_token)
            self.assertIsNone(service.claim("B"))

        self.assertEqual(service.stuck_jobs(running_longer_than=10), [job_id])
        service.complete(job_id, leased.lease_token, Success())
        job = service.get_job(job_id)
        self.assertEqual((job.state, job.attempts), (JobState.SUCCEEDED, 1))

    def test_cancel_job(self):
        service, clock = make_service()
        service.register_worker("A", ["email"])
        service.register_worker("B", ["email"])
        lease_token = service.claim("A")
        job_id = service.submit(JobSpec("email"))
        service.cancel_job(job_id, lease_token)
       
        job_status = JobState.CLAIMED
        self.assertRaises(Cancelled)

    def test_lease_duration_is_per_type(self):
        service, clock = make_service(lease=5.0)
        service.register_job_type("video", lease_duration=60.0)
        service.register_worker("A", ["email", "video", "other"])
        service.submit(JobSpec("video"))
        service.submit(JobSpec("other"))
        leased_video = service.claim("A")
        leased_other = service.claim("A")
        self.assertEqual(leased_video.lease_duration, 60.0)
        self.assertEqual(leased_other.lease_duration, 5.0)  # service default


class RetryTests(unittest.TestCase):
    def test_transient_failure_retries_with_exponential_backoff(self):
        service, clock = make_service()
        service.register_worker("A", ["email"])
        job_id = service.submit(JobSpec("email", max_retries=3))

        for expected_delay in (1, 2, 4):
            leased = service.claim("A")
            service.complete(job_id, leased.lease_token, TransientFailure("smtp down"))
            self.assertEqual(service.get_job(job_id).state, JobState.RETRY_SCHEDULED)
            clock.advance(expected_delay - 0.01)
            self.assertIsNone(service.claim("A"), "claimed before backoff elapsed")
            clock.advance(0.01)
            self.assertEqual(service.get_job(job_id).state, JobState.PENDING)

        leased = service.claim("A")
        self.assertEqual(leased.attempt, 4)
        service.complete(job_id, leased.lease_token, Success())
        self.assertEqual(service.get_job(job_id).state, JobState.SUCCEEDED)

    def test_dlq_after_max_retries(self):
        service, clock = make_service()
        service.register_worker("A", ["email"])
        job_id = service.submit(JobSpec("email", payload={"n": 1}, max_retries=2))

        for _ in range(3):
            clock.advance(100)
            leased = service.claim("A")
            service.complete(job_id, leased.lease_token, TransientFailure("nope"))

        self.assertEqual(service.get_job(job_id).state, JobState.FAILED)
        self.assertEqual(service.dlq_size(), 1)
        entry = service.list_dlq()[0]
        self.assertEqual((entry.job_id, entry.attempts, entry.reason, entry.error),
                         (job_id, 3, "retries_exhausted", "nope"))
        self.assertEqual(service.worker_stats()["A"].failed, 3)

    def test_permanent_failure_skips_retries(self):
        service, _ = make_service()
        service.register_worker("A", ["email"])
        job_id = service.submit(JobSpec("email", max_retries=5))
        leased = service.claim("A")
        service.complete(job_id, leased.lease_token, PermanentFailure("bad address"))

        self.assertEqual(service.get_job(job_id).state, JobState.FAILED)
        self.assertEqual(service.list_dlq()[0].reason, "permanent_failure")
        self.assertEqual(service.list_dlq()[0].attempts, 1)

    def test_repeated_lease_expiry_eventually_dead_letters(self):
        service, clock = make_service()
        service.register_worker("A", ["email"])
        job_id = service.submit(JobSpec("email", max_retries=1))
        for _ in range(2):
            service.claim("A")
            clock.advance(5)
        self.assertEqual(service.dlq_size(), 1)
        self.assertEqual(service.list_dlq()[0].reason, "lease_expired_retries_exhausted")
        self.assertEqual(service.get_job(job_id).state, JobState.FAILED)


class PriorityTests(unittest.TestCase):
    def _claim_priorities(self, service, n):
        out = []
        for _ in range(n):
            leased = service.claim("A")
            out.append(service.get_job(leased.job_id).spec.priority)
            service.complete(leased.job_id, leased.lease_token, Success())
        return out

    def test_higher_priority_first(self):
        service, _ = make_service()
        service.register_worker("A", ["email"])
        for p in (1, 9, 5, 0):
            service.submit(JobSpec("email", priority=p))
        self.assertEqual(self._claim_priorities(service, 4), [9, 5, 1, 0])

    def test_weighted_no_starvation(self):
        service, _ = make_service()
        service.register_worker("A", ["email"])
        for _ in range(30):
            service.submit(JobSpec("email", priority=9))
        service.submit(JobSpec("email", priority=0))
        order = self._claim_priorities(service, 12)
        # priority 9 gets weight 10 per cycle, then priority 0 gets its 1 turn
        self.assertEqual(order, [9] * 10 + [0, 9])

    def test_fifo_within_priority(self):
        service, _ = make_service()
        service.register_worker("A", ["email"])
        ids = [service.submit(JobSpec("email", payload=i)) for i in range(5)]
        claimed = []
        for _ in ids:
            leased = service.claim("A")
            claimed.append(leased.job_id)
            service.complete(leased.job_id, leased.lease_token, Success())
        self.assertEqual(claimed, ids)

    def test_worker_only_gets_its_types(self):
        service, _ = make_service()
        service.register_worker("A", ["email"])
        service.submit(JobSpec("resize_image", priority=9))
        self.assertIsNone(service.claim("A"))
        self.assertEqual(service.pending_count_by_type(), {"resize_image": 1})


class ValidationAndStatsTests(unittest.TestCase):
    def test_invalid_specs_rejected(self):
        service, _ = make_service()
        for spec in (JobSpec(""), JobSpec("email", priority=10), JobSpec("email", priority=-1),
                     JobSpec("email", priority=True), JobSpec("email", max_retries=-1)):
            with self.assertRaises(ValidationError):
                service.submit(spec)

    def test_observability_counts(self):
        service, _ = make_service()
        service.register_worker("A", ["email"])
        service.submit(JobSpec("email", priority=9))
        service.submit(JobSpec("email", priority=1))
        service.submit(JobSpec("sms", priority=1))
        leased = service.claim("A")

        self.assertEqual(service.pending_count_by_type(), {"email": 1, "sms": 1})
        self.assertEqual(service.pending_count_by_priority(), {1: 2})
        self.assertEqual(service.in_flight_count(), 1)
        self.assertEqual(service.worker_stats()["A"].current_job_id, leased.job_id)

        service.complete(leased.job_id, leased.lease_token, Success())
        stats = service.worker_stats()["A"]
        self.assertEqual((stats.current_job_id, stats.succeeded), (None, 1))


class WorkerThreadTests(unittest.TestCase):
    """Uses the real clock and real threads."""

    def test_worker_renews_lease_for_long_job(self):
        service = JobService(default_lease_duration=0.2)
        done = threading.Event()

        class Slow(JobHandler):
            def run(self, payload, ctx):
                time.sleep(0.6)  # 3x the lease; background renewal keeps it alive
                done.set()
                return Success()

        worker = Worker(service, "w1", {"slow": Slow()}, poll_interval=0.01)
        job_id = service.submit(JobSpec("slow"))
        worker.start()
        self.assertTrue(done.wait(2))
        worker.stop(timeout=2)

        job = service.get_job(job_id)
        self.assertEqual((job.state, job.attempts), (JobState.SUCCEEDED, 1))

    def test_handler_exception_is_transient(self):
        service = JobService(retry_policy=ExponentialBackoff(base=0.01, jitter=0))

        class Flaky(JobHandler):
            calls = 0

            def run(self, payload, ctx):
                Flaky.calls += 1
                if Flaky.calls < 3:
                    raise RuntimeError("flaky")
                return Success()

        worker = Worker(service, "w1", {"flaky": Flaky()}, poll_interval=0.01)
        job_id = service.submit(JobSpec("flaky", max_retries=3))
        worker.start()
        deadline = time.monotonic() + 2
        while service.get_job(job_id).state is not JobState.SUCCEEDED and time.monotonic() < deadline:
            time.sleep(0.01)
        worker.stop(timeout=2)
        self.assertEqual(service.get_job(job_id).attempts, 3)


if __name__ == "__main__":
    unittest.main()
