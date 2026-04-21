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

Migration `0001_init` includes a partial unique index
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

## Phase 2c — advanced metrics, live progress, comparison

Phase 2c ships three interlocking surfaces:

### Advanced metrics (migration 0005)

Eight additional metric columns on `backtest_results` — five
promoted from the `extra_metrics` JSON blob
(`sortino_ratio`, `cagr`, `calmar_ratio`, `expectancy`,
`avg_trade_pnl`) plus three net-new metrics computed in
`snapper.application.backtest.metrics`:

- **`max_drawdown_duration_seconds`** — longest peak-to-recovery
  window; unrecovered runs use the final suffix from last peak to
  end-of-curve. Degenerate input (fewer than two points, flat
  curve, strictly monotone) returns `None` + a
  `metric_warning` event.
- **`exposure_ratio`** — fraction of total run duration during
  which the portfolio held a non-zero position; leading-edge
  interval attribution. Zero-trade runs with non-zero duration
  return `0.0` (not degenerate).
- **`turnover_ratio`** — total notional traded divided by mean
  equity. Zero-trade returns `0.0`; non-positive mean equity
  returns `None` + warning.

Read-side fallback at `GET /api/backtests/{id}` collapses pre-0005
`extra_metrics` JSON into the typed slots using explicit `is not
None` coalescing (never Python truthiness — preserves legitimate
`0.0` values). The response strips the five promoted names from the
emitted `extra_metrics` so mixed-vintage rows never surface the
same metric twice.

### Live progress (4-segment WS topic family)

Runner emits `BacktestProgressData` events on
`backtest.{wallet_public_id}.{run_public_id}.{event}` where
`event ∈ {started, progress, milestone, completed, failed,
cancelled}`. The existing ZMQ→WS bridge forwards the envelope to
every subscribed client.

- `BacktestProgressEvent` is the canonical Literal source of truth
  in `snapper.messaging.schemas.data`; topic validator, emitter,
  and frontend handlers all derive from it.
- `BacktestProgressEmitter` throttles `progress` at 250 ms,
  emits `milestone` events exactly once per 25/50/75 pct bucket
  (bypassing the throttle so milestones + progress at the same
  step do not collide), and fires `started` / terminal events
  once. Degrades gracefully when `total_candles=None` by disabling
  milestones and pinning `progress_pct=0.0`.
- Per-subscription RBAC: admins subscribe to any wallet prefix;
  non-admin roles subscribe only to prefixes matching their
  `active_wallet_public_id`. Foreign-wallet or bare `backtest.`
  subscriptions are denied at the handler before the bridge is
  touched.
- `BacktestProgressData.milestone` carries a `@model_validator`
  enforcing the cross-field invariant `milestone is not None iff
  event == 'milestone'` so a malformed payload cannot reach the
  frontend chip logic.

### Config hash for auto-pair (migration 0006)

Every new run persists a `config_hash` via
`compute_fingerprint(config, for_pairing=True)` — SHA-256 over the
10 pairing-stable fields
(`strategy_class`, `instruments`, `start_date`, `end_date`,
`initial_balance`, `strategy_params`, `timeframe`, `fill_model`,
`slippage_bps`, `commission_bps`). `execution_mode`,
`snapshot_as_of`, `warmup_bars`, and `buffer_size` are explicitly
excluded so Direct-DB and ZMQ replay runs on the same config share
a hash.

`GET /api/backtests?config_hash={hash}` returns sibling runs for
the auto-pair UI; the repository query is wallet-scoped and
indexed on `(wallet_public_id, config_hash, timestamp)`.

### Comparison (migration 0007)

`POST /api/backtests/compare` creates (or returns existing) a
comparison row; `GET /api/backtests/compare/{id}` returns the
comparison metadata plus the diff **recomputed from current
artifact rows** so metric-schema changes never stale a persisted
diff. The diff surfaces four shapes:

- `metrics_diff` — per-name `{run_a, run_b, delta, pct}` with
  explicit `is not None` precedence.
- `equity_overlay` — full-outer-joined equity samples on
  `point_time` with nullable per-leg fields.
- `trades_diff` — multiset matching on `(instrument, executed_at,
  side, quantized_qty, quantized_price)` at `1e-8` precision so
  IEEE drift collapses but real ticks stay distinct. Common
  entries carry `pnl_a`/`pnl_b`/`pnl_delta`.
- `signals_diff` — multiset on `(instrument, signal_time,
  signal_type)`.

Route ordering: compare endpoints are registered **before**
`/{run_id}` so `/compare` is not captured as `id="compare"`. Pair
is normalised to lexical `(min, max)` before insert. Duplicate
submit is idempotent via SELECT-then-INSERT with an
`IntegrityError` rollback+re-SELECT race recovery path.

### No-active-wallet fail-closed

Every backtest read endpoint (list, detail, trades, signals,
events, equity, compare) returns **400 `no active wallet selected`**
when `principal.active_wallet_public_id is None`. WS subscribes
return `topic_denied`. The picker triggers a
`selectWalletAndRefresh` on change that mints a new JWT carrying
the chosen wallet claim before swapping the client scope, so REST
and WS both authorise against the same wallet after the picker
moves.
