"""Run: .venv/bin/python demo.py"""
import random
import time

from jobqueue import (
    ExponentialBackoff, JobHandler, JobService, JobSpec, PermanentFailure, Success,
    TransientFailure, Worker,
)


class SendEmail(JobHandler):
    def run(self, payload, ctx):
        if payload.get("to") == "invalid":
            return PermanentFailure("invalid address")
        if random.random() < 0.3:
            return TransientFailure("smtp timeout")
        time.sleep(0.05)
        return Success()


class ResizeImage(JobHandler):
    def run(self, payload, ctx):
        time.sleep(0.4)  # longer than the 0.2s lease; the worker renews it in the background
        return Success()


def main():
    service = JobService(retry_policy=ExponentialBackoff(base=0.1, cap=1.0))
    service.register_job_type("send_email", lease_duration=0.5)
    service.register_job_type("resize_image", lease_duration=0.2)

    workers = [
        Worker(service, "email-1", {"send_email": SendEmail()}),
        Worker(service, "mixed-1", {"send_email": SendEmail(), "resize_image": ResizeImage()}),
    ]
    for i in range(10):
        service.submit(JobSpec("send_email", {"to": f"user{i}@x.com"}, priority=i % 10))
    service.submit(JobSpec("send_email", {"to": "invalid"}))
    service.submit(JobSpec("resize_image", {"path": "a.png"}, priority=9))

    for w in workers:
        w.start()
    time.sleep(3)
    for w in workers:
        w.stop()

    print("pending by type:   ", service.pending_count_by_type())
    print("in flight:         ", service.in_flight_count())
    print("retry scheduled:   ", service.retry_scheduled_count())
    print("dlq size:          ", service.dlq_size())
    for entry in service.list_dlq():
        print("   dlq:", entry.job_id[:8], entry.type, entry.reason, entry.error)
    for stats in service.worker_stats().values():
        print(f"worker {stats.worker_id}: ok={stats.succeeded} failed={stats.failed} "
              f"leases_lost={stats.leases_lost}")


if __name__ == "__main__":
    main()
