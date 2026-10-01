# Background Job Service — PLAN

## Decisions

- **Lease duration** — per job type (`register_job_type(type, lease_duration)`), default **5s**.
  Short enough to detect crashed workers quickly; long-running jobs are expected to renew.
- **Lease overrun** — **never accept a stale result**. Every claim issues a new lease token.
  `complete`/`renew` with a token that is no longer the job's current lease (expired, re-leased,
  or job already finished) raises `LeaseLost` and changes nothing — success or failure alike.
  Only the current lease holder decides the job's outcome.
- **Priority** — weighted round-robin, `weight = priority + 1`. Each cycle, priority 9 gets up to
  10 claims, 8 gets 9, … 0 gets 1, highest first. Empty levels are skipped. When all eligible
  levels have used their credits, a new cycle starts.
  Guarantee: any non-empty priority level is served at least once per cycle (≤ 55 claims) — no starvation.
- **Same-priority ordering** — FIFO by time the job became ready (a retried job rejoins at the back).
- **Delivery** — **at-least-once**. A job can run more than once (worker crashes after doing the
  work but before `complete`). Handlers **must be idempotent**; they receive `ctx.job_id`
  (stable across attempts) to use as an idempotency key, and `ctx.attempt`.
- **Results** — `Success` → succeeded. `PermanentFailure` → DLQ immediately.
  `TransientFailure` (or a handler exception / non-Result return) → retry until `max_retries`
  is exhausted, then DLQ. `max_retries = N` means up to N+1 attempts.
- **Lease expiry counts as an attempt**, so a job that keeps crashing workers ends up in the DLQ
  instead of looping forever.
- **Backoff** — `delay = min(60s, 1s * 2^(attempt-1)) * (1 + U(0, 0.1))`. Exponential to relieve
  a failing downstream, capped to bound latency, jittered to avoid synchronized retry storms.

## Design

| Component | Role |
|---|---|
| `JobService` (`jobqueue/service.py`) | State machine, leases, retries, DLQ, observability; one lock |
| `Worker` (`jobqueue/worker.py`) | Thread: claim → background renew → run handler → complete |
| `RetryPolicy` / `ExponentialBackoff` | Pluggable backoff |
| `PriorityScheduler` / `WeightedRoundRobinScheduler` | Pluggable priority selection |
| `Clock` / `SystemClock` / `FakeClock` | Injected time for deterministic tests |
| `JobHandler`, `Result`, `JobSpec`, `Job`, `Lease`, `DeadLetter`, `WorkerStats` | Models (`jobqueue/models.py`) |

- Expired leases and due retries are processed lazily at the start of every public call, so
  correctness does not depend on a background timer.
- **Persistence boundary**: the in-memory structures inside `JobService` (jobs map, ready queues
  per (type, priority), delayed heap, in-flight map, DLQ). A durable store would replace them.
- **Network boundary**: workers only use `claim / renew / complete` with ids and tokens (plain
  data), so a transport layer can sit in front of `JobService` unchanged.

## Not implemented (scope cut)
Per-submission lease override, DLQ replay, background reaper thread, worker deregistration.

## Running
```
.venv/bin/python -m unittest discover -s tests -t . -v
.venv/bin/python demo.py
```
