# Backtesting

Snapper ships two backtest execution engines, both producing the same
artifacts (signals, trades, equity points) so a strategy author can pick
either path with confidence.

## Execution modes

The `execution_mode` field on `BacktestConfig` selects which engine the
runner instantiates:

- `direct_db` — `DirectDbEngine` reads candles directly from the
  repository and feeds them into the strategy in-process. Lowest
  overhead; recommended for fast iteration.
- `zmq_replay` — `ZmqReplayEngine` spins up a private XPUB/XSUB broker
  on OS-assigned local ports, attaches the strategy as a SUB, and
  publishes candles via a `ReplayPublisher`. Exercises the full live
  message-bus path, so any production wiring bug surfaces in the
  backtest.

Both engines call the same `process_time_batch` helper for fill
simulation, signal recording, and equity sampling, so artifact parity
is structural rather than aspirational.

### Choosing between modes

| Concern | direct_db | zmq_replay |
|---|---|---|
| Speed | fastest (no IPC) | slower (per-candle ZMQ hop) |
| Fidelity | bypasses message bus | mirrors live wire path |
| Concurrency | unlimited | at most one active run (DB-enforced) |
| Cancel SLA | bounded by `cancel_poll_ms` | bounded by `cancel_poll_ms` |

## At-most-one-running invariant

Migration `0004_backtest_single_running` adds a partial unique index
`uq_bt_single_running` on `backtest_runs(status)` filtered to
`status='running' AND known_to=KNOWN_TO_MAX`. This is a declarative DB
guarantee — a second runner trying to transition `pending → running`
receives `IntegrityError` at COMMIT, the runner catches it via
`is_single_running_conflict` and transitions to `failed` with a clear
error message. No engine, no broker, no ports consumed for the loser.

The constraint is dialect-aware (PostgreSQL via asyncpg + SQLite via
aiosqlite); see `snapper.data.backtest_conflict` for detection details.

## Cancellation

`CancelProbe` polls `backtest_runs.status` between time batches
(Direct-DB) or per processed candle (ZMQ replay). Throttled by
`config.cancel_poll_ms` (default 500 ms), each DB read bounded by
`probe_timeout_s` (default 1.0 s). A stuck DB logs a warning and the
next probe re-tries — cancel detection degrades gracefully under SQLite
lock contention rather than hanging behind the driver's 30 s lock
timeout.

End-to-end cancel SLA target: ≤ 5 s from the API call
`POST /api/backtests/{run_id}/cancel` (which sets status to
`cancel_requested`) to terminal status `cancelled`.

## start_date warmup gating

Candles before `config.start_date` feed strategy indicators (so SMA/MACD
warm up properly) but do **not** generate fills, signals, or equity
points. The `process_time_batch` helper enforces this with a single
`if batch[0].open_at < config.start_date: return` guard before fill
simulation.

Both engines honour the gate identically; the parity test
`tests/integration/test_backtest_hardening.py::test_warmup_gating_parity_between_engines`
asserts field-for-field equality.

## ZMQ replay implementation notes

The replay path adds a few primitives the live system does not need:

- **Echo-ack handshake**: the publisher sends a sentinel
  `WARMUP_PUBLIC_ID` candle on every expected topic each round; the
  strategy mixin's `_listen_loop` ACKs each topic into
  `state.acked_topics` and only sets `state.subscriber_ready` when the
  set equals `state.expected_topics`. Up to `WARMUP_MAX_RETRIES=5`
  rounds of `WARMUP_READY_TIMEOUT_S=1.0 s` each; failure raises
  `BacktestReadinessTimeoutError`.
- **Drain coordinator**: counts published vs processed candles. After
  the publisher streams the last candle it calls
  `mark_done_publishing()` and waits on `drained.wait()` bounded by
  `DRAIN_TIMEOUT_S=10.0 s`. Failure raises `BacktestDrainTimeoutError`
  with both counters in the message so an operator can tell publisher-
  stall from strategy-death.
- **Bounded cleanup**: the engine cancels publisher and listen tasks
  then `asyncio.wait(timeout=2.0)` — tasks that refuse to terminate are
  logged as leaks rather than silently kept alive.
- **Shielded DB writes**: the runner wraps every terminal-status DB
  write (`cancelled`, `failed`, fail-event insert) in `asyncio.shield`
  so a second cancel arriving mid-write does not interrupt the write
  coroutine.
