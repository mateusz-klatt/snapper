# Backtesting

Snapper ships two backtest execution engines, both producing the same
artifacts (signals, trades, equity points) so a strategy author can pick
either path with confidence.

## Authorization

Authorization uses the token's effective permissions from Snapper's
34-permission catalogue. The frontend opens the Backtests resource with
`read:backtests` and independently hides and rechecks each mutation.

| Surface | Required permission | Current named permission sets |
| --- | --- | --- |
| List strategy classes, runs, artifacts, and comparisons | `read:backtests` | `ai_reviewer`, `ai_delegate`, `viewer`, `operator`, `admin` |
| Create, cancel, or rerun a backtest | `manage:backtests` | `operator`, `admin` |
| Create a comparison | `create:backtest_comparisons` | `ai_reviewer`, `ai_delegate`, `operator`, `admin` |
| Subscribe to backtest progress | `read:backtests` | `ai_reviewer`, `ai_delegate`, `viewer`, `operator`, `admin` |

The current `viewer` set therefore has the full operator read surface but no
backtest mutation. Wallet-bound REST endpoints revalidate the token's active
wallet on every request. Reads accept operator scope grants plus personal
`wallet_user_read_grants`; mutations require operator scope grants. A personal
read grant therefore permits inspection without authorizing a run or comparison
write. Neither named set receives global wallet visibility.

## Execution modes

The `execution_mode` field on `BacktestConfig` selects which engine the
runner instantiates:

- `direct_db` — `DirectDbEngine` reads candles directly from the
  repository and feeds them into the strategy in-process. Lowest
  overhead; recommended for fast iteration.
- `zmq_replay` — `ZmqReplayEngine` spins up a private XPUB/XSUB broker
  on OS-assigned local ports, attaches the strategy as a SUB, and
  publishes candles via a `ReplayPublisher`. Exercises the full live
  candle publication/subscription path on the private broker. It does not
  exercise production order dispatch, venue execution, or deployment wiring.

Both engines call the same `process_time_batch` helper for fill
simulation, signal recording, and equity sampling, so artifact parity
is structural rather than aspirational.

### Choosing between modes

| Concern | direct_db | zmq_replay |
|---|---|---|
| Speed | fastest (no IPC) | slower (per-candle ZMQ hop) |
| Fidelity | bypasses message bus | mirrors live wire path |
| Concurrency | at most one active run (global `uq_bt_single_running` invariant — see below) | at most one active run (same DB-enforced invariant) |
| Cancellation checks | between batches, throttled by `cancel_poll_ms` | per received candle, throttled by `cancel_poll_ms` |

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
`config.cancel_poll_ms` (default 500 ms), each DB read uses `asyncio.wait_for` with
`probe_timeout_s` (default 1.0 s). A timed-out probe logs a warning and
the next safe point retries. Cancellation is cooperative: strategy work,
database-driver cancellation cleanup, and terminal-status persistence can
extend latency beyond those settings.

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
`tests/integration/test_backtest_hardening.py::TestHardeningIntegration::test_warmup_gating_parity_between_engines`
asserts equality of the per-signal (signal_time, signal_type, instrument)
tuples and per-point (point_time, equity) tuples between the two engines.

## Signal and fill artifacts

`process_time_batch` is the single execution/fill path for both engines.
For every timestamp batch it updates `latest_closes` for all candles
first, feeds each candle through the strategy, drops pre-`start_date`
signals, then records kept signals/trades and one equity point for the
timestamp.

Each kept signal receives a fresh UUID7 `public_id` before any fill is
simulated. When a trade is produced, `backtest_trades.signal_public_id`
carries that same value so Direct-DB and ZMQ replay artifacts are
FK-linkable in parity tests. The signal row records:

- `signal_time` from the candle batch timestamp (`open_at`);
- `signal_type` from `StrategySignal.side`;
- `instrument` from `StrategySignal.instrument`;
- `price` from the resolved target close when fill attribution succeeds,
  or from `StrategySignal.price` on the missing-target-close fallback;
- `timestamp` from the run's `snapshot_as_of` bus-time anchor.

### Live-execution fidelity boundary

Both backtest engines share the same simulator, but that simulator does not
implement the live coordinator's absolute-position-target contract. A BUY
spends a fraction of remaining cash; a SELL closes an existing position and
cannot open a short from flat. A zero-strength BUY produces no fill. Repeated
BUY targets can therefore buy repeatedly in a backtest even though the live
engine would treat an unchanged target as a no-op. Multi-leg signals are
simulated independently, without the paired-execution guard. Direct-DB/ZMQ
artifact parity is not evidence of live sizing, shorting, or compensation
parity.

The only current fill model is `market`. `simulate_market_fill` executes
at the relevant close price adjusted by `slippage_bps`; commission is
charged in basis points. Buy size is
`signal_strength * cash / cost_per_unit` (default strength `1.0`), and a
sell flattens the current position quantity. Invalid close prices,
zero/negative buy strength, no cash, or a sell with no position return
`None`; the signal is still recorded, but no trade row is written.

When a fill exists, the trade row records:

- `executed_at = fill.fill_at`, which is the candle `open_at` timestamp
  passed into the fill model;
- `instrument`, `side`, `quantity`, `price`, `fee`, and `pnl` from the
  simulated fill (`pnl` is populated on sells from realized PnL delta);
- `position_after` from the portfolio after the fill;
- `signal_public_id` linking back to the triggering signal;
- `timestamp` from the same `snapshot_as_of` bus-time anchor.

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

## Advanced metrics, live progress, comparison

The backtest surface ships three interlocking capabilities:

### Advanced metrics

Eight additional metric columns on `backtest_results` — five
promoted from the `extra_metrics` JSON blob
(`sortino_ratio`, `cagr`, `calmar_ratio`, `expectancy`,
`avg_trade_pnl`) plus three net-new metrics computed in
`snapper.application.backtest.metrics`:

- **`max_drawdown_duration_seconds`** — longest peak-to-recovery
  window; unrecovered runs use the final suffix from last peak to
  end-of-curve. Degenerate input (fewer than two points, flat
  curve, monotonically non-decreasing (no drawdown ever
  observed)) returns `None` + a `metric_warning` event.
- **`exposure_ratio`** — fraction of total run duration during
  which the portfolio held a non-zero position; leading-edge
  interval attribution. Zero-trade runs with non-zero duration
  return `0.0` (not degenerate).
- **`turnover_ratio`** — total notional traded divided by mean
  equity. Zero-trade returns `0.0`; non-positive mean equity
  returns `None` + warning.

Read-side fallback at `GET /api/backtests/{id}` collapses older
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
- Per-subscription wallet scope: `read:backtests` admits the topic category,
    then effective `impersonate:operator` permits subscription to any wallet
    prefix. Every other caller may subscribe only to prefixes matching its
    `active_wallet_public_id`. Foreign-wallet or bare `backtest.` subscriptions
    are denied at the handler before the bridge is touched.
- `BacktestProgressData.milestone` carries a `@model_validator`
  enforcing the cross-field invariant `milestone is not None iff
  event == 'milestone'` so a malformed payload cannot reach the
  frontend chip logic.

### Config hash for auto-pair

Every new run persists a `config_hash` via
`compute_fingerprint(config, for_pairing=True)` — SHA-256 over the
10 pairing-stable fields
(`strategy_class`, `instruments`, `start_date`, `end_date`,
`initial_balance`, `strategy_params`, `timeframe`, `fill_model`,
`slippage_bps`, `commission_bps`), plus
`target_execution_exchange` when it is set on the config
(see "Cross-asset execution" → "Fingerprint + pairing" below —
default-`None` runs keep their exact prior hash). `execution_mode`,
`snapshot_as_of`, `warmup_bars`, and `buffer_size` are explicitly
excluded so Direct-DB and ZMQ replay runs on the same config share
a hash.

`GET /api/backtests?config_hash={hash}` returns sibling runs for
the auto-pair UI; the repository query is wallet-scoped and
indexed on `(wallet_public_id, config_hash, timestamp)`.

### Comparison

`POST /api/backtests/compare` creates (or returns existing) a
comparison row and requires `create:backtest_comparisons`. The current
`ai_reviewer`, `ai_delegate`, `operator`, and `admin` sets contain that
permission; `viewer` and `ai_researcher` do not. This preserves historical
comparison creation for review and delegate principals without widening the
read-only viewer. `GET /api/backtests/compare/{id}` requires `read:backtests`
and returns the comparison metadata plus the diff **recomputed from current
artifact rows** so metric-schema changes never stale a persisted diff. The diff
surfaces four shapes:

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

Every wallet-bound backtest read endpoint (list, detail, trades, signals,
events, equity, and comparison reads) returns **400 `no active wallet selected`**
when `principal.active_wallet_public_id is None`. WS subscribes
respond with a `subscription_success` frame whose `status` is
`denied` (or `partial` when some requested topics succeeded), with
the offending topics surfaced in `denied_topics`. The picker
triggers a `selectWalletAndRefresh` on change that mints a new JWT
carrying the chosen wallet claim before swapping the client
scope, so REST and WS both authorise against the same wallet
after the picker moves.

`GET /api/backtests/strategy-classes` is the exception: it requires
`read:backtests` but is a global registry read and does not require an active
wallet.

## Cross-asset execution

Cross-asset strategies observe market data on one venue / instrument
and emit signals that execute on a *different* venue / instrument
(for example `TradFiObserveCryptoExecute` observes MNQM6-CME candles
on `kraken_equities` and emits BUY/SELL signals on `BTC-USD` / `kraken`).
The backtest engine supports this through a single config field.

`TradFiObserveCryptoExecute` is an illustrative, non-runnable example:
it is **not** auto-registered, so `BacktestConfig.validate_strategy_class`
rejects the config below as written. To run it, first copy the example
module to the strategies package root and decorate it with
`@register_strategy`, or substitute a registered strategy class
(`RSIReversion`, `MACDCrossover`, or `CointegrationPairs`). The
current registry is queryable at
`GET /api/backtests/strategy-classes`, which returns the sorted
`StrategyFactory` keys accepted as `strategy_class` values (see
[api.md](api.md)):

```python
BacktestConfig(
    strategy_class="TradFiObserveCryptoExecute",
    instruments={
        "kraken_equities": ["MNQM6-CME"],
        "kraken": ["BTC-USD"],
    },
    target_execution_exchange="kraken",
    start_date=..., end_date=...,
    wallet_public_id=...,
    initial_balance=10_000.0,
    strategy_params={"fast_period": 12, "slow_period": 26, "min_candles": 30},
)
```

### Attribution semantics

When `target_execution_exchange is not None`, the batch processor
substitutes at simulated-fill time:

- `target_exchange = str(config.target_execution_exchange)` — carries
  the *venue label* only. It is NOT validated against the feed
  populating `latest_closes[target_instrument]`; the caller is
  responsible for configuring `instruments` so the target symbol's
  feed matches the target venue.
- `target_instrument = signal.instrument` — comes directly from the
  strategy's `StrategySignal`, unchanged since pre-cross-asset.
- `target_close = latest_closes[target_instrument]` — the *price* at
  which the simulated fill executes. The source feed's close price is
  no longer used on cross-asset runs.

When `target_execution_exchange is None` (default), the simulator passes
`event.exchange` into the in-memory fill and uses `signal.instrument`
plus `latest_closes[signal.instrument]` for execution. Persisted backtest
trade and signal artifacts expose instrument and price only; source
`exchange` and optional `target_execution_exchange` live on the run row,
not per trade/signal. The single-leg emitters (`rsi.py`, `macd.py`)
always set `signal.instrument == event.instrument`, so for those strategies
the in-memory fill's exchange / instrument / price are byte-identical with
the pre-v1.2 path. `CointegrationPairs` does not
follow that convention: its `on_candle` returns a
`[primary, hedge]` pair whose hedge leg targets the *partner*
instrument, so that leg is attributed to `event.exchange` plus the
partner symbol and fills at `latest_closes[partner]` (see
"Multi-leg signal groups" below).

### Multi-leg signal groups

A strategy callback may return a `list[StrategySignal]` (a leg
group) instead of a single signal. The backtest path handles
groups as follows:

- **Fail-closed group preflight** — `_handle_candle_data` runs the
  whole group through `BaseStrategy._normalize_signal_group` before
  returning anything: every element must be a `StrategySignal`, no
  two legs may share an instrument, and every leg must map to a
  configured output topic. A malformed group raises instead of
  emitting, so a backtest can never record half a spread.
- **Per-leg fan-out** — `process_time_batch` pairs each leg with
  the candle event that produced the group and processes the legs
  independently: each leg gets its own `signal_public_id`, fill
  simulation, and signal/trade rows.
- **Hedge-leg pricing** — the hedge leg fills at
  `latest_closes[partner]`, the partner instrument's latest close
  from its own feed, not the observing candle's close. Because
  `process_time_batch` updates `latest_closes` for every candle in
  the timestamp batch before feeding the strategy, both legs price
  off same-timestamp data when both feeds emit. A partner symbol
  absent from `latest_closes` falls into the missing-target-close
  policy below.

`CointegrationPairs` additionally refuses to enter a one-sided
position at the source: `_build_paired_entry` returns `None` (no
signals at all) when the partner leg has no buffered price yet.

### Missing-target-close policy

If `signal.instrument` has no entry in `latest_closes` (typically
during the warmup overlap window where the strategy fires before the
target feed has primed), the engine:

1. Skips `simulate_market_fill` + the trade row (no fill attempted).
2. Increments `collector.cross_asset_blocked_fills` by one, logged at
   DEBUG with `reason='missing_target_close'`.
3. Still records the signal row with `price = float(signal.price)`
   (source-close fallback) so downstream analytics see the strategy's
   intent.

At run end the counter surfaces through
`BacktestResultInsertRow.extra_metrics["cross_asset_blocked_fills"]`
*only when positive*. Single-feed runs keep `extra_metrics == {}`
byte-identical with pre-cross-asset behaviour.

### Scope limits

Two explicit non-goals in the current implementation:

- **Same-symbol multi-venue** — `PortfolioTracker.positions` is keyed by
  instrument only, so running BTC-USD on Kraken Spot + Kraken Futures
  simultaneously in one backtest is NOT supported. Cross-asset
  strategies in scope use distinct symbols across feeds.
- **Multi-target cross-asset** — a single
  `target_execution_exchange` per run means one strategy cannot emit
  signals for different target venues inside the same backtest.

### Supported: cross-asset via REST

`POST /api/backtests` accepts an optional `target_execution_exchange`
field on `BacktestCreateBody`. When set, simulated fills are
attributed to that order-capable venue while candles still feed from
`exchange`. When unset, the run stays single-exchange and
byte-identical to pre-cross-asset behaviour. The same field round-trips
through DB persistence + the rerun endpoint, and surfaces on
`BacktestRunData` for frontend display. Allowed target values:
`paper` / `kraken` / `kraken_futures` / `walutomat`.

### Fingerprint + pairing

`compute_fingerprint` includes `target_execution_exchange` in the
payload **only when non-None**, so default-None runs keep their exact
pre-cross-asset hash in both the default and
`for_pairing=True` paths. This preserves dedup cache validity and
the auto-pair UI grouping logic in the `_resolve_auto_pair` resolver
in `backtest_routes.py`.
Explicit cross-asset runs (field set to a concrete venue like
`"kraken"`) generate distinct fingerprints so they never collide
with single-venue baselines.
