# One-chunk read-only SNAP-PERF profile

This throwaway branch is never merged. The production owner runs the reviewed
script once after 21:00 UTC, in an isolated container using the verified
production image. WSL does not run it against production.

Require an independent 600-second bound on the exact container/cgroup lifetime,
a bounded memory/CPU limit,
the existing private `DB_URL` supplied through the established owner workflow,
and no application entrypoint or writer lifecycle. The script's parent gives its
worker 580 seconds, including application imports. One absolute shutdown deadline
allows at most ten further seconds, checks the process group even after normal
leader exit and records leader-reaped/group-empty evidence honestly. A nonempty
group refuses success. Killing only an attached Docker CLI is not a container
lifetime bound. Nothing installs a scheduler, timer or service.

Use `snapperf_profile_20261116.py` as the standard-library-only launcher and keep
`snapperf_profile_bootstrap_20261116.py` and
`snapperf_profile_worker_20261116.py` beside it. The bootstrap installs class-only
exception reporting and disables logging sinks before importing application code
inside the already bounded child. Copy all three files from the exact reviewed branch.

All database connections start with read-only transactions, a 60-second
statement timeout, a one-second lock timeout and application name
`snapperf-profile`. The script verifies those settings. A second statement guard
refuses DML, DDL, advisory locks and selected side-effect SQL function families.
It permits only the exact audited `SET TRANSACTION ISOLATION LEVEL REPEATABLE
READ, READ ONLY`, alongside read statements. It is a guard for the frozen workload,
not a sandbox for arbitrary SQL. It never calls `_tick_once`, reconciliation,
sample writers, anchor creation or credential readers.

The owner first uses `--list-scopes --output NEW_DIRECTORY` to obtain only hashed
live USD anchor identities, if an existing verified receipt has no matching hash.
The actual run requires `--scope-hash HASH --venues VERIFIED_SPOT_VENUES
--output DIFFERENT_NEW_DIRECTORY`. The expected venue set comes from an existing
runtime/topology receipt; the script does not decrypt credentials to rediscover
it. Both invocations go through the bounded parent. A direct worker invocation
is refused. Never pass a DB URL or wallet identifier on the command line.

Four installed source-file hashes and calculation version 5C.3 must match the
frozen `28d34dbe` source witness. A changed image or source requires a new review.
The selected epoch is rebound through the real repository. Start is immediately
after its latest current-calculation sample, or after the anchor if none exists;
the existing tip must contain a full 1,440-minute correction chunk.

The script measures scope preparation, a **chunk-bounded** persisted read, the
real `_series` call, causal prior peak and `_plan_chunk`, including its per-minute
observation/evidence reads. It stops before reconciliation or inserts. The bounded
persisted read deliberately does not reproduce the production loop's full-suffix
preload or the earliest late-fill/self-heal suffix; those costs remain separate
limitations. The real offload/join behavior
is retained, and cProfile statistics include the offloaded valuation worker.

Artifacts contain code hashes, hashed scopes, UTC window boundaries, phase/SQL
timings and aggregate counts. SQL logs store only a hash, phase, ordinal, duration
and outcome, with no SQL text, literals, parameters or rows. This conservative
change from the original prefix request needs the owner's acceptance before
execution. The cProfile top 30 contains function
metadata only. Exceptions retain their class rather than their text. Artifacts
use a new owner-only directory; existing receipts are never overwritten.

Offline verification covered source pinning, scope hashing, hash-only SQL logs,
exact read-only transaction admission, generic SET/side-effect refusal, actual
entrypoint source-mismatch/import/render failures, and cleanup of a TERM-resistant
descendant after its leader exits. Status and query-log closure are attempted before
optional profiler rendering. No production execution or performance acceptance is claimed.
