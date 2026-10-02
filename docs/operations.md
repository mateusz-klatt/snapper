# Operations runbook — multi-instance trade coordinator

The trade runtime supports static-hash shard partitioning. At
`SNAPPER_COORDINATOR_INSTANCE_COUNT=1` (the default) a single
`TraderCoordinator` owns every shard. Scaling to `N >= 2` deploys
multiple coordinators against the same DB and ZMQ broker, each
owning `~1/N` of the shards deterministically via SHA-256 of the
shard key.

This document covers operating the N-instance trade runtime:
scale-up, scale-down, and crash recovery. It is intentionally
opinionated about systemd because systemd template units are the
stable recipe. A contract for alternative orchestrators (Docker
Compose, Kubernetes, Nomad) is also specified so operators can
adopt them with empirical verification.

## Prerequisites: systemd template unit

All subsequent recipes assume a template unit
`snapper-trade-zmq@.service` that consumes `%i` as the instance id:

```ini
# /etc/systemd/system/snapper-trade-zmq@.service
[Unit]
Description=Snapper Trade Coordinator (instance %i)
After=network.target snapper-broker.service

[Service]
Type=simple
User=snapper
EnvironmentFile=/etc/snapper/coordinator.env
Environment=SNAPPER_COORDINATOR_INSTANCE_ID=%i
ExecStart=/opt/snapper/bin/snapper trade-zmq
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
```

With this template, `snapper-trade-zmq@0.service` and
`snapper-trade-zmq@1.service` are distinct systemd units with
separate cgroups and separate journal streams on the same host.
Per-instance PID is queried via
`systemctl show -p MainPID snapper-trade-zmq@0.service`.

`/etc/snapper/coordinator.env` holds the shared bootstrap env,
including `SNAPPER_COORDINATOR_INSTANCE_COUNT=2` during N=2
operation:

```bash
DB_URL=postgresql+asyncpg://snapper:...@db.internal/snapper
SNAPPER_ENV=production
ZMQ_BROKER_XSUB=tcp://broker.internal:7500
ZMQ_BROKER_XPUB=tcp://broker.internal:7501
MASTER_PASSWORD=...
SNAPPER_COORDINATOR_INSTANCE_COUNT=2
```

`SNAPPER_ENV=production` (also `prod` / `staging`) arms the
placeholder-secret gate: every process loading this env file —
coordinators included — refuses to start while `MASTER_PASSWORD` is
unset or still the development placeholder
(`snapper_default_master_password_v1`); `BootstrapSettingsLoader`
raises `ValueError` naming the variable before anything binds. That is
the whole gate: the JWT, CSRF, and control-plane signing keys are all
DERIVED from the master password (one-root-secret model,
`src/snapper/infrastructure/security/kdf.py`), so no separate secret
rows exist to validate. Remediation: set a real `MASTER_PASSWORD` in
the env file before flipping `SNAPPER_ENV` to a production-like value.

Rotating `MASTER_PASSWORD` (change the `.env` value, rebuild/restart):

- Derived keys (JWT, CSRF, command signing) change in each process when it
  restarts with the new value. Recreate all participating containers with the
  same environment; Compose does not switch the fleet atomically, and old-key
  and new-key processes cannot verify each other's signatures.
- Every session/JWT is invalidated: all users re-log-in, and MCP
  delegate long-lived tokens MUST be re-minted (they are signed with
  the derived auth key).
- CSRF cookies refresh transparently on the next page load.
- Fernet-ENCRYPTED rows do NOT re-encrypt themselves: run
  `snapper settings-rotate-encryption` for settings rows FIRST, and
  recreate wallet credentials separately (they share the Fernet key
  but are not rewritten by that command) — see `.env.example` for the
  step-by-step order.

`SNAPPER_COORDINATOR_INSTANCE_COUNT >= 2` requires a PostgreSQL
`DB_URL`. On a SQLite backend the coordinator refuses to start with a
count above 1, raising `ValueError` before any signal is dispatched:
SQLite compiles `SELECT ... FOR UPDATE` to a plain SELECT, so
cross-instance row claims cannot serialize — use PostgreSQL for
multi-instance deployments. A single-instance SQLite coordinator
starts but logs a writer-concurrency warning.

## Scale up N=1 → N=2 (systemd)

```bash
# 1. Edit /etc/snapper/coordinator.env — set SNAPPER_COORDINATOR_INSTANCE_COUNT=2.

# 2. Stop the currently-running single-instance coordinator.
sudo systemctl stop snapper-trade-zmq@0.service

# 3. Verify graceful shutdown.
sudo systemctl status snapper-trade-zmq@0.service | grep -E "Active:|Main PID:"

# 4. Start both instances.
sudo systemctl start snapper-trade-zmq@0.service
sudo systemctl start snapper-trade-zmq@1.service

# 5. Verify both are live.
sudo systemctl is-active snapper-trade-zmq@0.service  # expect: active
sudo systemctl is-active snapper-trade-zmq@1.service  # expect: active
sudo journalctl -u snapper-trade-zmq@0.service -n 20 | grep "instance 0/2"
sudo journalctl -u snapper-trade-zmq@1.service -n 20 | grep "instance 1/2"
```

## Scale down N=2 → N=1 (systemd)

```bash
# 1. Stop BOTH coordinator instances first — full cutover is required.
sudo systemctl stop snapper-trade-zmq@0.service
sudo systemctl stop snapper-trade-zmq@1.service

# 2. Verify nothing remains.
sudo systemctl list-units 'snapper-trade-zmq@*' --state=active --no-legend
# (expect empty output)

# 3. Edit /etc/snapper/coordinator.env — set SNAPPER_COORDINATOR_INSTANCE_COUNT=1.

# 4. Start instance 0 only.
sudo systemctl start snapper-trade-zmq@0.service
```

## Why full cutover is required

Scale up and scale down both require stopping ALL old-N
coordinators BEFORE starting any new-N coordinator. Overlapping
old-N and new-N instances is the failure mode to prevent.

**Scenario: improper restart with overlap.** If an operator skipped
the cutover step and left instance 0 running as `count=1` while
starting instance 1 as `count=2`:

- Instance 0 believes it owns ALL shards (count=1 → every `owns()`
  returns True).
- Instance 1 believes it owns shards where `hash(shard_key) % 2 ==
  1`.
- Both coordinators dispatch the same `TradeCommand` for shards
  where `hash() % 2 == 1` → duplicate venue orders OR a race on
  `status='created' → 'dispatched'`.

The `~10s` dispatch pause for owned shards during the cutover
window is the explicit no-HA trade-off. HA was excluded from
scope by design.

## Coordinator failure and shutdown

The required signal receiver is supervised together with the coordinator's
background workers. An unhandled receive, validation or handler error, or an
unexpected receiver exit, fails the owning run. Check its failure and the
configured process restart policy when intake stops. Receiver failure initiates
the owner's shutdown instead of silently leaving its sibling workers running.

Shutdown cancels and awaits owned workers before releasing the coordinator's
private socket contexts. The same cleanup applies to partial startup and
recovery failures. Concurrent stop callers join that shutdown, and a later
stop does not repeat successful cleanup. If context destruction failed, later
stop reports the retained cleanup failure without automatically retrying it.
An earlier operational failure remains the primary run error. Zero socket
linger does not provide a hard deadline for native resource teardown.

No message replay or independent receiver restart is performed by this
cleanup. The systemd and application process-manager restart policies retain
their existing roles; a replacement coordinator performs its ordinary
startup recovery before receiving signals.

The API container probes `/api/health` directly with curl. Its connection budget
is one second and its total request budget is two seconds, within Docker's
three-second healthcheck timeout. The probe runs without a shell wrapper so
curl is the process Docker supervises. A timeout reports an unhealthy probe;
it does not diagnose the cause of an unresponsive API or automatically restart
the container.

## Crash recovery (systemd)

If instance K crashes:

```bash
# 1. Diagnose.
sudo journalctl -u snapper-trade-zmq@1.service -n 100 --no-pager

# 2. Check whether Restart=always brought it back.
sudo systemctl is-active snapper-trade-zmq@1.service

# 3a. If active: confirm the coordinator resumed with its original ownership.
sudo journalctl -u snapper-trade-zmq@1.service -n 200 | grep "instance 1/2"
sudo journalctl -u snapper-trade-zmq@1.service -n 200 | grep "recovery complete"

# 3b. If inactive (Restart=always gave up): manually restart.
sudo systemctl restart snapper-trade-zmq@1.service
```

During the outage window, shards owned by the crashed instance
pause until the coordinator comes back. Typical outage is under
10s if `Restart=always` is configured.

Recovery rebuilds engines from:

1. Checkpoints (carry `shard_key` directly, filtered by ownership).
2. Live executions (filtered by recovered shard_key; paper rows
   are EXCLUDED under N>1 because `ExecutionRow` has no
   `strategy_tag`).
3. Active orders (same treatment as executions).

Under N>1 paper mode, state that never produced a checkpoint is
not recovered by the restarted coordinator. Paper strategies that
need to survive restart MUST persist checkpoints.

## REST orders with `wallet_public_id` under N>1

REST manual orders are supported under N>1. `POST /api/orders`
uses the same `compute_shard_key(...)` helper as the signal engine,
executor, and MCP manual-order path. When `wallet_public_id` is
non-empty, the shard key includes the `.w{wallet_short}` segment;
REST manual orders pass `strategy_tag=None`, so they do not add the
paper strategy segment.

The route writes that shard key to both `execution_plans` and
`trade_commands`, inserts the command with `ownership=None` so any
API worker may accept the request, and lets the trade coordinator
outbox ownership filter decide which instance dispatches it. During
outbox publish, the coordinator registers
`client_order_id -> shard_key`, so the N>1 CID guard accepts the
venue ACK/fill/cancel events only on the owning coordinator.

For manual orders, provide a wallet or omit the field to use the existing
single-accessible-wallet resolution for the requested mode. Explicit blank
values are rejected. Fresh command writes require a nonblank resolved wallet;
the shard helper's legacy empty-wallet support does not permit unscoped new
commands. Signal-driven paper orders may still add a `strategy_tag`; REST
manual orders intentionally do not.

## Non-systemd deployments (contract)

The operational recipes are anchored on the systemd template above.
For non-systemd platforms, the deployment author writes a recipe
that satisfies the same contract:

1. Two distinct supervised processes on the same host (or across
   hosts) with `SNAPPER_COORDINATOR_INSTANCE_ID=0` and `=1`
   respectively, and `SNAPPER_COORDINATOR_INSTANCE_COUNT=2` in both.
2. Per-instance lifecycle (stop / start / restart / logs) via the
   orchestrator's native mechanism — never by process pattern match
   (`pkill -f` is too broad for multi-instance).
3. Scale-up and scale-down follow the same stop-all → verify-empty
   → start-new cutover flow as the systemd recipes above.
4. At least one concrete recipe per chosen orchestrator, verified
   empirically, with a provenance line:

   ```
   Verified on YYYY-MM-DD against <platform-version>
   ```

   The regex enforced by `tests/meta/test_operations_runbook.py` is
   `^Verified on \d{4}-\d{2}-\d{2} against \S+$` — exact casing,
   anchored to the start and end of the line. The meta-test searches
   the entire file body for at least one matching line; there is no
   required section placement.

Acceptable orchestrators for the non-systemd recipe are
`docker compose`, `kubectl` (Kubernetes), or `nomad` — the
meta-test rejects bare `docker` because `docker run` alone does
not provide the per-instance lifecycle the contract requires.

## Compose restart matrix (broker split)

With the dedicated `snapper-broker` service, restart domains are
independent and each restart has explicit loss semantics on the
at-most-once ZMQ bus:

- `docker compose restart snapper` (backend only) — the bus stays up;
  feed publishers and any strategies container keep publishing, BUT
  signals consumed by the trade runtime during the backend window are
  lost (the coordinator was down; signals have no replay). Feed frames
  keep flowing to other subscribers.
- `docker compose restart snapper-feed` — market-data gap for the
  restart window (publishers reconnect and resume; candle corpus heals
  per feed-resilience recovery below).
- `docker compose restart snapper-broker` — a brief bus blip for EVERY
  participant: PUB frames sent during the window drop, all sockets
  auto-reconnect. Treat as a rare, deliberate operation.
- `docker compose restart snapper-strategies` — strategies-only restart:
  in-memory strategy state is lost (candle buffers rebuild via DB
  warmup; open-position beliefs and one-decision floors do NOT — the
  signal-safety re-assertion gate governs arming anything beyond paper), the
  backend and feed keep running, and consults in flight time out and
  retry on the next window.
- `docker compose restart snapper-egress` — tunnel-routed public feeds
  fail closed to quarantine semantics (see egress runbook) until the
  sidecar is healthy again.
- `docker compose restart snapper-notify` — alert evaluation and APNs
  delivery pause for the restart window; the outbox drain on the next
  start recovers queued deliveries, and rule dedup windows persist in
  the database. ZMQ events published while the sidecar is down are not replayed
  in general, so an event that never reached rule evaluation can be missed.
  Portfolio-drift pages additionally have a durable database recovery scan.

### Flat-restart discipline (MANDATORY until venue-truth reconciliation lands)

A strategies-container restart orphans every standing target: the target
re-assertion layer is in-session repair only (`_target` starts empty), so
after ANY restart that touches `snapper-strategies` — including a full
stack down/up — the operator MUST verify positions are flat and flatten
anything standing (`GET /api/positions`, then close) BEFORE trusting
strategy output again. Start every soak or arming window from flat.
This discipline is the compensating control for the documented pre-LIVE
gap (venue-truth position reconciliation + strategy target-state
recovery are NOT built yet).

### Paper-soak checklist and live go/no-go

The 2026-07-03 drill run verified on the split topology: all six
restart domains heal (API ~25s, strategies ~30s incl. DB warmup,
feed ~20s, broker ~30s with bus auto-reconnect, egress ~13s, full cold
start), the signal-safety conventions live-fire (entry re-assert repriced each
bar, `long_only` flat exits at `strength=0.0`, no same-bar duplicates),
one poisoned bus frame is skipped without killing the listen loop, and
the full MCP wake loop works container-to-host (consult admission →
`ai_reviews` frame → host watch → delegate decision inside the deadline
→ resume → attributed target-flat consumed by the trade runtime).

Soak gating (all must hold over the soak window, ≥24h before LIVE
go/no-go):

- No unexplained container restarts; every strategy heartbeat stays
  fresh (no zombie: a dead listen loop must surface as a process
  restart, not a healthy heartbeat).
- Zero signal-safety violations: no `side=sell strength>0` rows from
  `long_only` strategies in `signals` scoped to the soak window.
- Consult admission succeeds each heartbeat window while the delegate
  watch is connected; missed windows are gating.
- The live-trading kill switch (`live_trading_mode` DB setting) stays
  `halted` for the entire soak; LIVE arming is the explicit, separate
  act of setting it `enabled` — only that value admits non-paper
  submits (`reduce_only` and any unreadable/unknown value also block).
- Transient SQLite `database is locked` noise is acceptable ONLY if all
  drills, heartbeats and signal writes heal in-window (a SQLite soak
  does not validate Postgres contention).
- Still OPEN by design before LIVE: venue-truth reconciliation,
  strategy target-state recovery after restart, proprietary parlay/spy
  exit conversion, Postgres for prod, migration head applied in prod
  (`0034` as of 2026-07-20; `0015` was current at the 2026-07-03 drill).

## Verified environments

Verified on 2026-04-18 against docker-compose-v2.29.7

# Kraken Equities (TradFi) market data

TradFi index futures (Kraken FCM: MNQ/ES/YM/RTY/NKD/M2K) are shipped
in market-data-only mode. The symbol updater remains disabled by
default at code level, while the publisher is registered as an enabled
market-data process. Persisted DB `Setting` rows still control runtime
enable/autostart state, so rollout + rollback happen without code
deploys.

Contracts used by this flow:

- REST: `iapi.kraken.com/api/internal/markets/all/futures-contracts`
  for instrument metadata; `.../{ws_symbol}/ticker/history` for
  historical candles. Requires `Origin: https://pro.kraken.com` +
  `Referer: https://pro.kraken.com/` headers.
- WS: `wss://ws-equities.kraken.com` for live delayed (~10 min)
  ticks + trades. The outer envelope's `delayed` flag propagates
  into every `TickData` message as `is_delayed`.
- Optional realtime WS: `wss://ws-equities-auth.kraken.com/?f`, gated by
  `kraken_equities_realtime_ws_enabled=false` by default. The client mints
  a short-lived token from the configured `exchange='kraken'` Spot wallet
  credential and injects it only into outbound subscribe params. If the
  wallet id is empty, the credential row is missing, or Kraken rejects the
  token mint, the publisher logs the fallback and stays on the public
  delayed feed. When `feed_egress_enabled=true`, the token mint is routed
  through the same `kraken_equities` public egress pool selection as the
  Equities WebSocket so the mint and WS handshake originate from the same
  configured tunnel.
- No order API — every submit route (`POST /api/orders`,
  `POST /api/execution-plans`, `POST /api/trailing-stops`) calls
  `snapper.server._capability_guard.require_tradable` and rejects
  TradFi instruments with HTTP 422
  `error_code=instrument_market_data_only`.

## Realtime authenticated feed

The realtime feed is market-data only. It does not enable orders,
execution subscriptions, or account channels in Snapper. To enable it:

1. Seed or select a wallet credential row with `exchange='kraken'`,
    `credential_type='api_key_secret'`, and Kraken's WebSocket interface
    permission.
2. Optionally set
    `kraken_equities_realtime_wallet_public_id` to pin a specific wallet —
    either `<wallet_public_id>` used verbatim, or `label:<wallet-label>`
    resolved at runtime to the single matching live wallet (fails closed
    to the delayed feed on zero or multiple matches; label form is
    seed-file friendly because public ids are minted per database). When
    this setting is empty, Snapper auto-selects the first active Kraken
    Spot `api_key_secret` wallet credential in deterministic repository
    order.
3. Set `kraken_equities_realtime_ws_enabled=true`.
4. Restart `snapper-feed`; these settings are read when the
    `kraken_equities_feed_publisher` constructs its exchange client.
5. Verify incoming ticker frames have `is_delayed=false`. If token minting
    fails, or no matching Kraken Spot credential exists, the same publisher
    keeps running against `wss://ws-equities.kraken.com` and frames remain
    `is_delayed=true`.

Tokens are never persisted in `SubscriptionRequest.parameters_json` or any
settings row. Reconnect recovery rebuilds the SDK client and replays
Snapper's stable subscription cache with a freshly minted token. Although
Kraken exposes `GetWebSocketsToken` under `/0/private/`, Snapper treats this
read-only market-data session credential as public Equities feed traffic for
egress routing so it shares the auth WebSocket source IP. If feed egress is
off or no pool is configured, the mint remains direct.

## Market diagnostics

Use these REST endpoints during feed rollout and incident response:

- `GET /api/market/feed-health` — current per-symbol publisher health.
  Filter with `exchange=kraken_equities` and optionally
  `fresh_within_seconds=<n>` to hide stale rows from stopped publishers.
- `GET /api/market/coverage` — per-exchange counts for active
  instruments, fresh ticks, fresh candles, gated-off instruments, and
  dark instruments. Tune `tick_window_seconds` and
  `candle_window_seconds` to match the venue's expected cadence.
- `GET /api/market/cache/health` — cache and persist-policy snapshot:
  number of cached instruments, cached pair-stat entries, and persisted
  instrument universe size.
- `GET /api/market/cache/stats/configured` and
  `/api/market/cache/stats/{exchange_a}/{symbol_a}/{exchange_b}/{symbol_b}`
  — cached Pearson/cointegration diagnostics for configured pairs.

The feed-health table is current-state rather than SCD2 history. Its
natural key is `(coordinator, exchange, channel, symbol)`, where
`channel` encodes the stream kind and timeframe together (e.g.
`ohlc:1m`) and `symbol` is the wire-format product id, so split-feed
deployments can show which coordinator is publishing a stale or
missing stream.

## Feed resilience and outage recovery

Market-data publishers self-recover from multi-minute network outages on
both the public (egress) and private (direct) paths without operator
action:

- **Persistent recovery.** When a feed goes silent past its liveness
  threshold, recovery runs as a single-flight loop with capped
  exponential backoff (1→60 s + jitter) that retries until messages
  resume or the publisher stops — it never gives up on a timeout.
- **Per-venue liveness thresholds.** Kraken Spot and Futures recover
  after 60 s of message silence (their wildcard/continuous universes
  always tick, so 60 s reliably means a dark feed); Kraken Equities uses
  120 s and is fully suppressed during scheduled CME closure windows.
- **Kraken Equities live-trade age guard.** Each live WebSocket trade is
  parsed against one reception timestamp shared by its frame. Trades more
  than 24 hours old are rejected before they can refresh feed health, enter
  the raw-trade queue, or update the calculated-candle builder; a trade
  exactly 24 hours old is accepted. This leaves ample room for the public
  feed's normal ~10-minute delay while blocking historical replay after a
  reconnect. Rejections are emitted as rate-limited warning summaries.
  Historical recovery remains the responsibility of the REST backfill path.
- **Native-candle liveness.** The message watchdog above shares one
  watermark across ticks, trades, and candles, so a venue whose 1m bars
  arrive on a dedicated native channel (Kraken Spot's `ohlc:1m`) can have
  that channel stall silently while ticks/trades keep the watermark fresh
  (the 2026-06-18 incident: trades alive ~21 h, candles dark). Spot
  therefore also runs a venue-wide candle watchdog: if no native 1m candle
  arrives for 300 s (well above the ~60 s venue-wide cadence) while a
  candle subscription is active, it spawns the same WS-restart recovery
  (which re-subscribes the dead channel). This recovery is candle-aware —
  it is only declared successful once a fresh native candle resumes, not on
  trades alone — and it never escalates to the dark-feed process exit, so a
  candle-only stall cannot kill the still-live trade feed.
- **Transport keepalive.** Every Kraken WebSocket handshake is opened
  with an explicit `ping_timeout`/`close_timeout` so a silently dead
  socket (a yanked link with no close frame) surfaces in tens of seconds
  rather than waiting out the app-level silence threshold.
- **Bounded connect.** The WebSocket connect itself (`SpotWSClient`/
  `FuturesWSClient` `start()`) is wrapped in a 20 s timeout
  (`_WS_CONNECT_TIMEOUT_S`). The vendored SDK's `start()` polls for the
  socket with a connect timeout that never fires, so on a prolonged
  blackout — where the connector exhausts its reconnect ceiling and never
  establishes a socket — an unbounded `start()` would hang forever and
  wedge in-process recovery (only a process restart would clear it).
  Bounding it makes a stalled handshake fail fast so the recovery loop
  tears the partial client down and retries with a fresh one, reconnecting
  as soon as the network returns.
- **Bounded teardown.** Tearing a client down is bounded too: every venue
  WS close runs under a 10 s timeout, and a close that times out or raises
  escalates to a last-resort force close that cancels the SDK's connector
  run tasks, drains them (5 s bound), and closes the SDK's internal aiohttp
  session directly. Previously a close landing during the SDK's reconnect
  backoff sleep (up to ~3 min; the SDK's stop flag did not interrupt it)
  abandoned the SDK's own session cleanup, so each rebuild cycle during a
  prolonged blackout leaked one HTTP session and the abandoned reconnect
  later spawned orphaned background tasks. The reconnect backoff is now
  hardened to re-check the stop flag every 0.5 s and to reap its child
  tasks on every exit path, so blackout-driven rebuild cycles no longer
  accumulate leaked sessions. This covers every process that tears down a
  Kraken WS client — the executors' private clients included, not just the
  publishers. A `close timed out - forcing cleanup` warning followed by
  `kraken WS force-close: leaked aiohttp session closed` is the cleanup
  working, not a fault to act on.
- **Bounded subscribe sends + serialized reconnects.** Every Kraken
  Futures SDK subscribe/unsubscribe send is wrapped in a 5 s timeout
  (`_SDK_SEND_TIMEOUT_S`): the Futures SDK's `send_message` spins forever
  on a client whose socket never came up, and one such send once wedged
  the shared subscribe throttle lock — starving the post-reconnect
  subscription replay and leaving the feed connected-but-dark until a
  process restart. Spot and Equities market-data subscribe sends are not
  wrapped — the spinning `send_message` is a Futures-SDK behavior, so
  those venues rely on the bounded connect, the ping/close timeouts, and
  the 180 s per-attempt recovery bound instead; the Spot *private*
  executions subscribe send IS bounded by the same 5 s timeout (see
  "Private fill-stream supervision"). Client (re)connects are serialized
  per venue (`_ws_connect_lock`), slot writes are compare-and-clear, and
  a connect raced by a disconnect closes its own client instead of
  leaking it.
  Each recovery attempt is additionally bounded to 180 s
  (`_RECOVERY_ATTEMPT_TIMEOUT_S`) so even an unforeseen hang becomes a
  logged, retried failure rather than a silent permanent wedge. Futures
  dark auto-recovery covers the `trade` channel too (its candles are
  synthesized from trades).
- **Dark-feed watchdog.** If a feed stays dark for ~25 min despite
  recovery (a wedged SDK or recovery bug), the publisher exits non-zero
  and `ProcessLauncherService` respawns it. The exit ceiling is kept
  above the launcher's lifetime-restart reset window, so a prolonged
  outage produces an unbounded slow restart-and-retry cadence that
  self-heals the instant connectivity returns — it never permanently
  abandons the feed.

Watch recovery via `GET /api/market/feed-health` /
`GET /api/market/coverage` and the `pub:<exchange>` log lines
(`liveness recovery triggered`, `feed recovered after N attempt(s)`,
`feed dark for …s … exiting for launcher restart`).

### Order-path retry semantics (Kraken Spot REST)

Order **creation** is deliberately excluded from the REST network-retry
wrapper: a network-class failure such as a request timeout is ambiguous
(the order may have reached Kraken and executed even though the response
was lost), Kraken deduplicates `cl_ord_id` only among *open* orders, and
strategy orders are market orders that fill instantly — so a blind
re-send could double a real position. During a network blip an order
submit therefore fails fast (one venue call, logged as
`Network error (no retry, non-idempotent call)`) instead of silently
retrying up to three times; the failure still feeds the REST circuit
breaker so a sustained outage trips fail-fast mode. Idempotent calls
(cancel, fetch, balance) keep the 3-attempt network retry. Expect more
transient submit failures in logs during connectivity incidents — that
is the guard working as intended, not a regression.

### Ambiguous submits — the UNKNOWN order state

On every venue client, an otherwise-successful submit response whose
snapshot or venue order id is missing or empty is placement-unproven
and enters this ambiguity path; it never directly fabricates REJECTED.
The same applies to a submit that fails *after the request may have left
the process* (request timeout, connection reset, gateway 5xx, or an
unparseable success body) where the adapter can establish that timing.
On the Kraken Spot
ccxt path the same treatment covers a submit whose placement is simply
*unproven*: a venue error ccxt could not classify — the bare
`ccxt.ExchangeError` type — parks and verifies rather than rejecting,
because "not proven safe to reject" is not the same as "rejected". The executor
first verifies against venue truth — up to three lookups by client id
(`cl_ord_id` on Kraken Spot via open+closed orders, `cliOrdId` on
Futures via order status; 15s bound each). The spacing before each
lookup depends on which kind of ambiguity produced it. A *lost
response* backs off 2s/5s/10s, because the venue may still be
materializing an order the request just created and its listings lag
the write. An *unclassified venue error* takes its first lookup
immediately and then 2s/5s, because the venue answered synchronously
and is healthy: nothing is in flight, so a collision can only be
against an order already resting from an earlier delivery. That
matters operationally — verification runs inline on the executor's
single order-command loop, so a delay there also delays a queued
cancel for an unrelated live order. The two-consecutive-absence rule
is identical on both schedules. A found
order finalizes as accepted (an already-terminal one additionally
projects its fills through the disappeared-order reconciler); two
consecutive authoritative not-found answers make the rejection
venue-truth-based and safe. Only when verification cannot resolve —
venue unreachable or lookup unsupported
— does the executor park the order, record a non-terminal
`order_submit_unknown` venue event, and publish a single
`orders.events.*.unknown` message:

- the engine **holds its in-flight guard indefinitely** (the 60 s
  timeout valve is disabled while UNKNOWN) so no replacement order can
  double exposure while the original may be live;
- a safety-critical `order_unknown` operator alert fires ("do not
  assume flat", 5-minute dedup window);
- failures that provably happened *before* any send (circuit breaker
  open, credentials, symbol/order-type validation) and authoritative
  venue answers (4xx, `success=false`, exhausted 429) still reject
  immediately on every venue — only genuine ambiguity parks. On the
  Kraken Spot ccxt path "authoritative" means ccxt *classified* the
  error into a typed `ExchangeError` subclass (`InvalidOrder`,
  `InsufficientFunds`, `BadSymbol`, `PermissionDenied`, `BadRequest`,
  …); those still reject at once. An error ccxt left unclassified is
  matched on runtime TYPE, never on the venue's error text — Kraken
  documents no duplicate-`cl_ord_id` error and ccxt maps none, so the
  wire text for the motivating case is unknown and a string match would
  be a guess. A breaker-open refusal additionally lands a durable terminal
  disposition before its REJECTED — a venue event recording the
  refusal plus the command row moved to FAILED — and publishes the
  rejection with reason `circuit_breaker_open`, so consumers can tell
  an infra refusal from a venue rejection and a redispatched frame can
  never resubmit the command after the breaker closes; if any step
  fails, the entry parks and the 60s reconciliation loop reruns the
  disposition until engine intent is released. Connection
  refused rejects immediately only on Walutomat, where httpx surfaces
  connection-setup errors distinctly; on Kraken Spot and Futures
  connection-setup failures park as UNKNOWN by design, because the ccxt
  and `requests` transports cannot reliably distinguish sent from
  not-sent (a connection error can fire mid-body), and a false park
  merely alerts while a false rejection could double a position.

Walutomat verification checks active orders first. When an order filled
during the ambiguity window and is no longer active, the adapter also
checks the newest account-history page for the submitted `submitId`.
It adopts a non-terminal OPEN existence witness only when history
positively supplies both opposite-signed `MARKET_FX` currency legs,
each carrying the requested submit id, and one unambiguous venue order
id. The witness carries the observed fill-so-far but never claims the
requested amount or completion, which account history cannot prove.
History absence, an incomplete leg pair, multiple matching order ids,
or a zero-fill cancellation does not prove refusal and therefore
continues to park UNKNOWN.

Kraken Futures uses `python-kraken-sdk` for the private send-order call.
The SDK's `check_send_status` raises only for status strings present in
its exception assignment and otherwise returns the payload unchanged;
it also returns payloads with no `sendStatus`. The Snapper Futures
mutation call site therefore validates the returned `order_id` and
raises a venue-answered ambiguity when `sendStatus` or its identity is
malformed. It accepts only explicitly classified placement statuses,
keeps the SDK's known definitive-refusal behavior, and parks any
unmapped status even when an order id is present. This closes the
unmapped-error path without patching the shared SDK boundary used by
live market-data and read endpoints.

Resolution: a late `accepted` event clears the UNKNOWN flag (the order
resumes its normal lifecycle with the guard still held until fills); a
COMPLETE fill or a rejection releases the guard entirely. A partial
fill alone neither clears the flag nor the guard — the order stays
guarded until its terminal event.
A PARKED UNKNOWN order is retried by the 60s reconciliation loop
(fresh venue-verification round each cycle); if it stays unresolved
across cycles, check the venue's open and closed orders for the client
id from the alert before taking any manual action.
The same 60s loop also heals fill gaps: when the venue reports more
filled quantity than the executor has recorded, it emits a corrective
fill, pricing market orders that lack a snapshot price from the VWAP
in the venue's own per-order fills summary (`get_order_fill_summary`).
Only when no venue price resolves at all does it log
`Recon: fill gap ... no price on market order, skipping corrective fill`
— a fill mismatch needs manual reconciliation against the venue's fill
history only if that ERROR persists across cycles.
Correctives also carry the venue's real cumulative fee — the order
snapshot's running commission, or the fills-summary total — instead of
fabricating a zero fee. When the snapshot carries no fee and the fills
summary cannot answer (failed lookup, partial page, an implemented
source with no rows), the corrective is deferred with a WARNING for at
most 5 cycles, then emitted fee-less under a single CRITICAL log so
the order can still reach its terminal state. After that CRITICAL the
quantity is healed but the fee is not — reconcile the fee manually
against the venue's fill history.

On Walutomat, where fills come from polling, an order that disappears
from the active list is never guessed terminal: a failed final-state
query keeps the order tracked and retries every poll cycle (a wrong
FILLED would create phantom position; a wrong CANCELED would free
engine intent while the order may have filled). Each failed attempt
logs a WARNING; the 10th consecutive failure escalates to a single
CRITICAL and the retries continue. On that CRITICAL, check the order
on the venue — no manual terminal injection is needed, and the 60s
reconciliation loop converges the pending entry independently once
the venue answers.

### Stop orders — venue support and pre-send rejects

Order commands speak the core order-type vocabulary end-to-end —
`market`, `limit`, `stop`, `stop_limit` — across REST, MCP, the
durable `trade_commands` rows (a stop's trigger is the
`trade_commands.stop_price` column), and the outbox frames. Venue wire
names (`stop-loss`, `stop-loss-limit`) appear only at the executor's
venue boundary, where the request is translated immediately before the
exchange call — never on the bus or in `trade_commands`. Older
execution plans may still carry the legacy `venue_order_type` wire
value in their params; readers normalize it back to the core
vocabulary. Venue support:

- **Kraken Spot** and **Kraken Futures** accept `stop` and
  `stop_limit`: the trigger comes from `stop_price`; once triggered,
  `stop` executes at market and `stop_limit` places a limit order at
  the command's `price`.
- **Walutomat** (its FX market API has no stop orders) and **paper
  trading** (no trigger simulation — a stop would fill immediately,
  misrepresenting the protective semantics) reject every stop-typed
  submit.

A stop-typed submit that cannot be sent whole is rejected PRE-SEND:
the failure is provably-not-placed, so it takes the definitive-reject
disposition (REJECTED published, durable `order_rejected` venue event,
engine in-flight intent released) — never an UNKNOWN park, never a
venue round-trip. The pre-send gates are:

- an order type with no venue wire mapping (vocabulary error);
- `stop` / `stop_limit` with no `stop_price`;
- `stop_limit` with no `price` (the limit leg) on the Kraken venues;
- any stop on Walutomat or paper.

`POST /api/orders` and the MCP `submit_manual_order` tool refuse the
same malformed shapes up front — unknown `order_type`,
`limit`/`stop_limit` without `price`, `stop`/`stop_limit` without
`stop_price` — before any command row is persisted (HTTP 422 on REST;
the same evaluator rule surfaces as a tool error on MCP). An
executor-side pre-send stop rejection in the logs
(`Error processing order ...` followed by the REJECTED publish) is
therefore safe to act on: nothing reached the venue, engine intent is
released, and the command can be corrected and resubmitted.

### Duplicate-dispatch guard and dispatch TTL

Two executor/outbox gates close the remaining order-flow loss windows:

- **Duplicate-submit guard.** A coordinator crash between the ZMQ
  publish and the CREATED→DISPATCHED commit (or a failed bulk write)
  re-publishes the same `client_order_id`. The executor now drops such
  replays silently: an entry already pending, an accepted order
  awaiting its durable-event heal, or durable venue-event evidence
  (accepted/fill/terminal/unknown/breaker-open/interlock-blocked —
  rejections excluded so legitimate retries still flow) all block the
  re-submit before the venue order-placement call. Two evidence kinds
  are exceptions to the silent drop: a replay carrying breaker-open OR
  `order_interlock_blocked` evidence reruns its own terminal disposition
  (above) so a crash mid-disposition cannot leave engine intent held.
  A failed evidence check drops FAIL-CLOSED.
- **Dispatch max-age TTL** (`TRADE_COMMAND_DISPATCH_TTL_S`, default
  30s, `<= 0` disables). Stale CREATED create/submit commands expire
  at the outbox to the terminal EXPIRED status (never published; engine
  intent released through the regular expired pipeline), and stale
  frames buffered through an outage (the order-flow socket has no
  high-water mark) are resolved at the executor against venue truth: a
  frame whose order EXISTS under the client id is adopted as accepted
  (a crash-window replay that actually placed), an unverifiable frame
  drops silently (the row stays DISPATCHED for the verification sweep
  below and the engine valve), and only a venue-verified-absent frame
  rejects. A stale row that DOES
  carry submit evidence publishes anyway and is absorbed by the
  duplicate guard — expiring it would fabricate a terminal state for a
  possibly-live order. Cancels and replaces are exempt: expiring a
  stale cancel would strand a live order, and replace outcomes are
  reported on the lightweight cancel/replace event path. Keep the TTL
  below the engine's 60s in-flight valve.

Behind the two gates, the durable command plane reconciles itself. The
coordinator's reconciliation loop folds the append-only `venue_events`
rows into `trade_commands` status advances (ack, partial/full fill,
terminal), which rescopes its stale-command logging: a
`stale command ... no venue evidence` WARN fires only for an over-age
create/submit command with ZERO venue evidence (a real anomaly);
unknown-only evidence logs at INFO (executor verification is already
working it); a command with real evidence is silent — an old open
limit order is healthy, not stale. Cancel commands keep the legacy
WARN. The executor's 60s cycle works the zero-evidence queue from the
venue side: each unresolved DISPATCHED command gets a client-id lookup
— a found order is adopted under its original request (this also
recovers parked-UNKNOWN entries lost to an executor restart; the
DISPATCHED row is the durable record of the send intent), and only two
consecutive authoritative absences on a command younger than an hour
auto-REJECT to release engine intent. Absence never auto-rejects when
the dispatch TTL is disabled (nothing expires frames, so one may
legitimately still be in flight at any age) or past the hour bound
(venue closed-order lookback makes absence non-authoritative) — those
cases WARN for operator attention instead. Consecutive executor venue
reconciliation failures are exposed on executor heartbeats; after
three failures, the coordinator halts known real shard keys for that
exchange/wallet scope and drops new cold-path signals for the same
scope until a healthy heartbeat clears the scope gate. Existing shard
halts remain explicit operator/release decisions.

The same cycle adopts ghost
orders — open at the venue under our client id with no in-memory entry
— by rebuilding the request from the command row; an open order with
NO command row is foreign/manual, warned about once and never touched.
A durably REJECTED command later found open (or with live venue
evidence postdating the rejection) is healed loudly: the order is
adopted and the command resurrected to ACCEPTED, so a false rejection
cannot silently strand a live order.

Adoption also re-arms the engine. Every adoption of a venue-LIVE
order — ghost adoption, ambiguous-submit verification, the
false-reject heal, startup recovery of a venue-verified-open row —
publishes its ACCEPTED with `reason="adopted"`; terminal-snapshot
adoptions stay reason-less (re-arming for a dead order would block
honest emission). On that marker the coordinator re-arms a running
engine's RELEASED in-flight guard (it consumed the false REJECTED, or
the 60s timeout valve had already cleared it), so the next signal
cannot emit a second order while the adopted one still works the
book. A `RE-ARMED in-flight intent for adopted order ...` WARN in the
coordinator log is that guard working, not a fault. An engine already
in flight for a DIFFERENT order is never clobbered (WARN — both
orders stay live and fills of the adopted one still book position),
and a client id retired by an honest terminal (FILLED fill or
cancelled/expired confirm; rejections never retire) refuses late
duplicate adopted frames. A failed adopted ACCEPTED publish parks on
the executor and the 60s reconciliation loop retries it until it
lands.

### Private fill-stream supervision

The private execution (fills) WebSocket in each executor is supervised,
not spawned once. Previously any termination of that stream — typically
SDK reconnect-budget exhaustion after ~2 min (Futures) / ~5 min (Spot)
of network outage — left fills permanently dark while the executor kept
reporting RUNNING until a manual restart: Futures surfaced a single
error line, Spot exited silently and kept treating the dead client as
connected, blocking any rebuild. Now:

- **Death is detectable.** A Spot stream whose connection is terminally
  lost raises instead of ending cleanly and closes the poisoned client,
  and on both venues ensure-connected rebuilds a slot holding a
  terminally-failed client instead of returning it as connected. The
  Spot private subscribe send is bounded by a 5 s timeout
  (`_SDK_SEND_TIMEOUT_S`) so a wedged socket fails the attempt fast
  instead of hanging the resubscribe.
- **A supervisor respawns dead streams.** Any termination — a clean
  generator return included — triggers a respawn with capped jittered
  exponential backoff (1→60 s); it never abandons the stream. A stream
  that ran healthy for 5+ minutes resets the backoff so an old
  incident's ceiling is not inherited by the next blip.
- **The dark window is healed before re-entry.** Every post-death
  respawn first runs a best-effort reconciliation pass (serialized with
  the periodic 60 s cycle so two concurrent cycles cannot double-emit
  the same corrective fill), then resubscribes.
- **Resubscribe is idempotent.** Spot subscribes without order/trade
  snapshots and Futures drops `fills_snapshot` frames at the source, so
  neither a supervised respawn nor an SDK in-budget reconnect replays
  already-booked fills.
- **Per-order fill booking is atomic.** A per-order lock plus exec-id
  dedupe serializes booking across the live stream, recon corrective
  fills, and cancel handling, so concurrent paths cannot double-book a
  fill, and a publish failure no longer silently corrupts fill tracking
  (the gap is re-absorbed by later frames or the recon corrective).

During a private WS outage expect recurring executor-log warnings —
`Execution stream died (...) - respawn #N after Ns backoff` or
`Execution stream returned cleanly - respawn #N ...` — that is the
supervisor retrying; no manual restart is needed, and fills plus the
dark-window gap reconcile automatically once connectivity returns. The
supervisor exits permanently only on shutdown and on venues without
WebSocket execution streaming, where the 60 s reconciliation loop is
the only fill source.

### Recovery-time corrective fills

Executor startup recovery no longer re-baselines fill tracking to the
venue's current cumulative (which silently swallowed every fill that
landed while the stack was down). Instead, recovery reads both truth
planes per order — the durable `venue_events` fill rows and the
`executions` log (rows exist only for successfully published fills) —
seeds the committed and durable watermarks from what each plane proves,
republishes recorded-but-unpublished fills through the normal pipeline
under their original exec ids (every consumer dedupes by exec id, so
this is idempotent), and emits the remaining venue-ahead gap as a recon
corrective with a deterministic id (`recon-{order}-c{cum}`), so repeated
restarts and publish retries converge instead of double-applying.
Orders that went terminal during the downtime get their fill gap healed
BEFORE the terminal event is projected. Orders the venue cannot verify
at startup are parked in tracking with DB-derived seeds — the 60s recon
loop retries them every cycle instead of dropping them. If the durable
plane is unreadable for an order, recovery degrades to the legacy
venue-truth seeding for that order only and logs an ERROR (the downtime
gap for it stays unhealed — fix the DB and restart).

### Executor task supervision and service restarts

Every executor core loop (order handler, reconciliation, heartbeat —
plus the fill stream covered above) runs under an in-process
supervisor: a loop that dies (exception or unexpected clean return)
respawns with capped jittered backoff (1s doubling to 60s; a 5-minute
healthy run resets it), preserving all in-memory order state. The order
handler's supervisor rebuilds its SUB socket before re-entry; poison
frames (already consumed by ZMQ) are logged and skipped, never
respawn-looped. Reconciliation cycles are bounded (300s) so a hung
venue call cannot hold the recon lock forever, and parked-UNKNOWN
verification is fairness-capped (3 per cycle, index-rotated) so a large
parked set cannot starve its tail. On the core loops (order handler,
reconciliation, heartbeat — the fill stream's supervisor only backs
off, it never escalates), a death streak older than 25 minutes
escalates: the whole service crashes out of `start()` (siblings
cancelled, sockets closed) and the process launcher — whose
task-completion handler now drives the same restart watchdog as native
subprocesses — rebuilds a FRESH instance, made safe by recovery-time
corrective fills. The escalation ceiling deliberately exceeds the
launcher's healthy-uptime reset, so escalations retry forever at a slow
cadence instead of exhausting the restart budget; only fast startup
crash-loops (bad credentials, DB down at boot) park the executor after
the budget.

Executor heartbeats report derived health, not a hardcoded HEALTHY:
each beat is computed from the supervised-loop seams, so degradation
pages operators through the existing system-degradation alert
(`critical_system_error`: 3 consecutive non-HEALTHY beats, deduped to
about one page per hour per instance) instead of staying green while a
loop is dead. An active death streak reports WARNING from its second
death and ERROR once the streak is 10 minutes old (well before the
25-minute escalation); a reconciliation loop with no completed pass
for 5 / 15 minutes reports WARNING / ERROR (recon swallows per-cycle
faults by design, so only its progress clock can expose a recon that
fails forever without dying); an order command stuck in flight for
2 / 10 minutes reports WARNING / ERROR (a wedged handler neither dies
nor progresses); an unhealed accept-event backlog or parked-UNKNOWN
submits report WARNING. The executor heartbeat's `lag_ms` is the age
of the last successful reconciliation pass. Per-wallet executor
instances publish on per-wallet heartbeat topics
(`system.heartbeats.executor.{exchange}.{wallet_short}`), and the
alert rule parses those — previously per-wallet executor heartbeats
were silently discarded, so a degraded executor could never page.

A parked executor is paged, not just logged: on giving up, the
launcher speaks for the dead instance on its own per-wallet heartbeat
topic with a synthetic 3-frame ERROR burst, re-bursting hourly while
the instance stays parked — the page is level-triggered (a notify
restart that loses one burst is healed by the next) while the alert
dedup still caps it at about one page per hour. A successful (re)start
or a successful deliberate stop unparks the instance and cancels the
burst. `GET /health` additionally reports ERROR while any process is
parked — checked before every other branch, including API-only mode —
as the backstop for a lost burst.

### Fault-injection testing

`scripts/resilience_fault_injection.py` simulates a real exchange outage and
asserts recovery. It drops outbound HTTPS (:443) **inside the feed container's
network namespace** with a throwaway privileged helper
(`docker run --net=container:<feed> --cap-add=NET_ADMIN`), holds the outage,
restores it, and measures candle-freshness recovery against the SLA. Only :443
is dropped, so the ZMQ bus and DB writes keep working — the outage is
market-data only (order execution, a separate container, is unaffected).

It targets the feed's own netns because the runtime image has no `iptables`
(`docker exec <feed> iptables` fails). This matches the default direct-feed
path (`feed_egress_enabled=false`). If feed egress is enabled, publishers
connect to `snapper-egress` SOCKS ports instead, so drop the SOCKS/egress path
rather than feed-container `:443`.
Restore is safety-critical and proven, not best-effort: every step is
exit-code-checked, the helper runs a prebuilt `snapper-fault-helper` image (no
package install while the link is down), removal is proven with `iptables -C`,
and if removal cannot be proven the feed container is recreated (fresh netns)
and re-verified — the harness raises loudly rather than reporting an uncertain
restore. It builds the helper image on first run.

```bash
# Default: drop the feed's :443 for 180 s, expect spot+futures back within 180 s.
DB_URL=... python scripts/resilience_fault_injection.py --hold-s 180

# Longer outage + a wider recovery SLA (e.g. validating the dark-feed watchdog).
DB_URL=... python scripts/resilience_fault_injection.py --hold-s 900 --sla-s 600

# Freshness bar vs SLA: --fresh-s is HOW FRESH the newest candle must be to
# count as recovered; --sla-s is the wall-clock budget for getting there.
DB_URL=... python scripts/resilience_fault_injection.py --hold-s 240 --sla-s 360 --fresh-s 120

# Override the target container, port, or verified exchanges.
DB_URL=... python scripts/resilience_fault_injection.py \
    --container snapper-feed --port 443 --exchanges kraken,kraken_futures
```

Run it in a low-impact window — it darkens live market data for the hold
duration.

### Egress routing for feeds (`feed_egress_enabled`)

By default the feed publishers connect **directly** to the exchanges and do
not initialize an egress pool in their own processes. Set the
`feed_egress_enabled` setting to enable per-publisher egress routing: each
feed subprocess then initializes its own process-local egress pool at
startup, so the Kraken connect shim and Walutomat's pooled HTTP transport
route through the configured tunnels (per-exchange pins apply; direct stays
the fallback).

This is gated off by default because routing live market data through shared
VPN IPs changes latency and can make Kraken/Cloudflare throttle the feed
differently than the host IP. Roll it out cautiously:

1. Set `feed_egress_enabled=true` and restart the feed (the setting is read at
    publisher startup, not live).
2. In `snapper-feed`, confirm `ss -tnp` shows connections to
    `snapper-egress:1081-1085` (SOCKS) rather than direct exchange `:443`.
3. Compare candle freshness, tick/trade lag, and dark-instrument counts to the
    direct baseline; watch the reconnect logs for throttling/close codes.
4. Revert instantly by setting `feed_egress_enabled=false` and restarting the
    feed.

With routing enabled, a TCP/SOCKS connect-level failure through a proxy
route — timeout, connection refused, or a tunnel that went dark —
quarantines that route for 30 s (`_WS_CONNECT_ERROR_QUARANTINE_S`), so
the next reconnect fails over to another tunnel or the direct fallback
instead of re-dialing the dead route. Public direct routes are not
quarantined: a connect error with no proxy means the exchange or the
local uplink is down, not the route. Private executor WebSocket direct
connect errors are the exception: direct is quarantined briefly
(~15 s) so the next reconnect can use `private_fallback_route_id`.

### Spot candle source (`spot_candle_source`)

By default the Kraken Spot feed reads live 1m candles from the venue-native
`ohlc:1m` WebSocket channel (`spot_candle_source=native`). Set the
`spot_candle_source` setting to `trade_built` to switch the live Spot 1m
source to candles synthesized locally from the already-consumed spot trade
stream: the publisher then consumes
`KrakenExchangeClient.subscribe_trade_built_candles` and **drops** the
`ohlc:1m` subscription entirely (the native-candle subscription set goes
empty — a bandwidth saving), tagging the persisted rows `source='calculated'`
instead of `source='native'`.

The setting value is read at feed startup, where the Spot 1m subscription
(native `ohlc:1m` vs the trade-built stream) is established once — so **a flip
only takes effect after a `snapper-feed` restart**. A live settings broadcast
over ZMQ (`_handle_settings_update`) refreshes the publisher's cached value but
does **not** re-establish the subscription, so it cannot switch the live source
on its own; always restart the feed. Roll it out cautiously:

1. Set `spot_candle_source=trade_built`.
2. Restart `snapper-feed` (required — the subscription source is fixed at
    startup; the live ZMQ broadcast updates the cached value only).
3. Verify live Spot 1m rows are landing with `source='calculated'`, that no
    new native `ohlc:1m` subscription is opened (the native-candle
    subscription set is empty), and that the feed stays healthy.
4. Note the illiquid-pair tradeoff: a minute with zero trades emits **no**
    base 1m bar, and `candle_forward_fill` fills only higher timeframes — not
    the base 1m — so thinly-traded pairs will show 1m gaps that the native
    `ohlc:1m` channel would have carried.
5. Revert instantly by setting `spot_candle_source=native` and restarting the
    feed. The default is `native`.

## Per-wallet executors

Executor process configs are templates named `executor_<exchange>`.
At runtime, `ProcessLauncherService` spawns one instance per active
`wallet_credentials` row, named `executor_<exchange>_w<wallet_short>`.
Those instances load credentials through `CredentialResolver` and ignore
commands for other wallets. Operators should start/stop the generated
wallet instances, not the bare templates.

## Persistent process control across containers

The read-side process catalog routes (`GET /api/processes/available`,
`/configured`, `/schema/<name>`, and `/runs`) require effective
`READ_PROCESSES`; `GET /api/processes/summary` requires effective
`READ_SYSTEM_STATUS`. The current `viewer` named set contains both read
permissions, so a viewer can inspect the same process surface as an operator
but has none of the lifecycle permissions described below.

`POST /api/processes/<name>/start` and `/stop` are LOCAL runtime
operations on the node that receives them, so in the split topology
(feed in `snapper-feed`, strategies in `snapper-strategies`, executors
under their coordinator) they cannot control a process owned by a
different container. For STRATEGY targets, local start requires effective
`START_STRATEGIES` and local stop requires effective `STOP_STRATEGIES`; both
operations require effective `MANAGE_PROCESSES` for non-strategy targets. The
persistent, cross-container-safe control is
`PATCH /api/processes/<name>/desired-state`. Non-strategy targets require
effective `MANAGE_PROCESSES`; a STRATEGY `enable` requires effective
`START_STRATEGIES`, `disable` requires effective `STOP_STRATEGIES`, and
`restart` requires both. These mutation routes also require CSRF on the
cookie-authenticated path; `Authorization: Bearer` requests bypass CSRF per the
project-wide auth contract. The desired-state endpoint mutates only the DB state
(`enabled` / `restart_nonce`) of the `process_<name>` config — the source of
truth — and never starts or stops anything locally; the owning coordinator's
reconcile loop converges the running state. A `restart` requires the process
to be enabled (409 otherwise) and a client-minted `restart_nonce`
(`^[A-Za-z0-9_-]+$`, 8-64 chars, 422 if missing; idempotent — resending the
same nonce does not double-bounce). A per-wallet executor instance (404) and a
bare executor template (422) have no desired-state row; enabling a STRATEGY
re-runs the operator/wallet/grant scope check fail-closed before the write.

`POST /api/processes` uses the same target-aware model when it creates a
configuration. A STRATEGY target requires effective `CONFIGURE_STRATEGIES` and,
when created enabled, effective `START_STRATEGIES`; a non-strategy target
requires effective `MANAGE_PROCESSES`.

### Retargeting a strategy's scope (restart required)

`PATCH /api/processes/<name>/config` retargets an existing STRATEGY
config's scope (operator / wallet / AI-reviewer reference-identity
params) without hand-editing the config JSON. It requires effective
`CONFIGURE_STRATEGIES`; cookie-authenticated requests also require CSRF. The
route fully enforces the caller's authorization at edit time via
`_enforce_strategy_scope` (operator membership, wallet grant, output coverage
→ 403/400). A non-strategy target → 400, an
executor instance / unknown name → 404, a bare executor template → 422,
and an undeclared reference-identity key → 400. `operator_public_id` /
`wallet_public_id` may be a concrete public_id or a `label:<name>`
reference: a label is resolved at edit time scope-qualified to the
caller's own operator memberships (so it can never resolve outside the
caller's authority), and an in-scope unambiguous label is accepted while
a blank/unresolved/ambiguous label → 400 and a concrete foreign operator
→ 403. The value is persisted verbatim — a `label:` reference is stored
as-is and resolved to a concrete UUID only at the next (re)start (the
authorization check runs on a throwaway resolved copy). The edit does NOT
restart the process: the
response carries `restart_required: true` (surfaced in the UI as a
"restart required" banner) and the new scope takes effect only on the
next (re)start of the owning process (via the desired-state `restart`
action above, or a `snapper-strategies` restart subject to the
flat-restart discipline above).

## Enable the feed

```
# 1. Populate Symbol + SymbolExchangeCapability rows from iapi.
#    (kraken_equities entry must be present for is_tradeable
#    cache hits; default deny on missing row.)
snapper update-kraken-equities-symbols --force

# 2. Verify the rows landed. SCD2 active rows have known_to set to the
#    canonical sentinel (9999-12-31 23:59:59.000000 on SQLite — see
#    _KNOWN_TO_ACTIVE_SQLITE in src/snapper/data/models.py).
sqlite3 data/snapper.db \
  "SELECT COUNT(*) FROM symbol_exchange_capabilities
   WHERE exchange='kraken_equities'
         AND known_to='9999-12-31 23:59:59.000000';"
# expect >= 38 (indices only; full catalog up to ~180 contracts)

# 3. Trigger an immediate runtime launch of the symbol-updater +
#    feed-publisher processes. `POST /api/processes/<name>/start`
#    invokes `ProcessLauncherService.start_process_by_name`, which
#    reads the existing persisted Setting row and applies the
#    request payload as RUNTIME-ONLY overrides — it does NOT
#    persist a new config row, and it does NOT go through
#    `ProcessRegistrySyncer`. (Persistent enable/autostart state
#    lives on the create / configure path: `POST /api/processes` or
#    the Settings page; the launcher reads that persisted state at
#    startup, while the registry syncer only seeds and updates the
#    DB rows from the code registry.) With
#    SNAPPER_COORDINATOR_INSTANCE_COUNT >= 2 only coordinator
#    instance 0 runs the startup registry sync; other instances log
#    "Skipping process registry sync on coordinator instance
#    <id>/<count>; instance 0 owns it" so replicas never race the
#    same temporal Setting rows.
#    The body is the full request envelope: `json_body` calls
#    `ProcessStartRequest.model_validate_json` on the raw body, and the
#    envelope (StrictDataSchema, extra="forbid") REQUIRES session_id,
#    sequence_id, public_id, and timestamp. A bare `{"payload":{}}` is
#    rejected with HTTP 422 before the handler runs. `payload` may carry
#    optional `mode` / `parameters` runtime overrides.
curl -X POST http://localhost:8000/api/processes/kraken_equities_symbol_updater/start \
     -H "Authorization: Bearer <token>" \
     -H "X-CSRF-Token: <csrf>" \
     -H "Content-Type: application/json" \
     -d '{"type":"process_start_request","session_id":"<sid>","sequence_id":1,"public_id":"<uuid7>","timestamp":"2026-06-07T00:00:00Z","payload":{}}'

curl -X POST http://localhost:8000/api/processes/kraken_equities_feed_publisher/start \
     -H "Authorization: Bearer <token>" \
     -H "X-CSRF-Token: <csrf>" \
     -H "Content-Type: application/json" \
     -d '{"type":"process_start_request","session_id":"<sid>","sequence_id":1,"public_id":"<uuid7>","timestamp":"2026-06-07T00:00:00Z","payload":{}}'

# To stop a running process later:
# curl -X POST http://localhost:8000/api/processes/<name>/stop ...

# 4. `/start` returns once the process is launched in the runtime;
#    there is no "wait one syncer interval" step on this path.
#    If you need the new state persisted across restarts, do that
#    via `POST /api/processes` (create / configure) rather than
#    via `/start`.
```

## Backfill historical candles

```
# 30-day 1-hour backfill of the configured KRAKEN_EQUITIES instruments.
# The default setting is the wildcard ["*"], which expands at runtime to
# the full mapped venue universe (all ~180 contracts).
snapper kraken-equities-backfill-candles -t 1h -d 30

# Or the Makefile wrapper (equivalent)
make backfill-kraken-equities-candles

# Single symbol, daily candles, 90 days
snapper kraken-equities-backfill-candles -s MNQU6-CME -t 1d -d 90 --no-resume
```

The endpoint is undocumented/internal — upstream application-layer
failures (HTTP 200 with `result=null` or non-empty `errors`) raise
`RuntimeError` so they are distinguishable from legitimately-empty
windows. See `src/snapper/infrastructure/exchanges/implementations/
kraken_equities.py:get_ohlcv` for the error contract.

To seed daily history for the single-source read cutover and the DB-first
warmup, load cached Polygon grouped-daily rows as 1d candles. This loader is
cache-only — it reads the on-disk grouped-daily cache and never contacts the
Polygon API:

```
# Load every cached day strictly before the cut date as
# source='native', complete=True 1d candles under the live-read venue.
snapper polygon-load-grouped-candles --exchange kraken --cut-date 2026-01-01

# Or the Makefile wrapper (equivalent)
make run-polygon-grouped-candles EXCHANGE=kraken CUT_DATE=2026-01-01

# Single symbol; or --all for every Polygon-mapped native symbol.
snapper polygon-load-grouped-candles -e kraken --cut-date 2026-01-01 -s BTC-USD
```

`--exchange` is the **live-read venue** the persisted bars live under (e.g.
`kraken`) — never `polygon`; the loader writes the 1d history under the same
venue the read cutover and warmup resolve. `--cut-date` must sit at or before
the first UTC day live synthesis owns: the loader writes only days **strictly
before** it, keeping native backfill and synthesized live bars on disjoint day
ranges. There is **no** Docker variant of this loader (`make
docker-polygon-grouped` wraps the separate grouped-daily CSV download, not the
candle load). See [docs/cli.md](cli.md) for the full option reference
(`--symbol`/`-s`, `--all`, `--lookback-days`).

## Quarterly rotation

The default `KRAKEN_EQUITIES` instruments setting is the wildcard
`["*"]`, which expands at runtime to the full mapped venue universe,
so the default scope tracks every available contract automatically.
Rotation discipline applies when an operator narrows the setting to
an explicit allowlist: the chosen TradFi symbols are quarterly expiry
contracts (`MNQU6-CME` = Sep 26, etc.) and must be rotated before the
`InstrumentSpec.expiry_at` timestamp on any listed symbol drops below
14 days. Rotation cadence:

1. Pull the current expiry list. `native_symbol` lives on the
   `symbols` table (joined to `instruments` via
   `symbol_public_id`); expiry data lives on `instrument_specs`
   keyed by `instrument_public_id`:

   ```
   sqlite3 data/snapper.db \
     "SELECT sym.native_symbol, datetime(spec.expiry_at)
        FROM instruments AS i
        JOIN symbols AS sym
          ON sym.public_id = i.symbol_public_id
         AND sym.known_to = '9999-12-31 23:59:59.000000'
        JOIN instrument_specs AS spec
          ON spec.instrument_public_id = i.public_id
         AND spec.known_to = '9999-12-31 23:59:59.000000'
        WHERE i.exchange = 'kraken_equities'
              AND i.known_to = '9999-12-31 23:59:59.000000';"
   ```
2. Identify the next quarterly (e.g. `MNQZ6-CME` Dec 26 when
   `MNQU6-CME` Sep 26 drops below 14 days).
3. Add the new nearest contract by either changing the default
   `KRAKEN_EQUITIES` entry inside the `AppSettings.instruments`
   property default literal in `src/snapper/config/app.py` (code
   path — requires a deploy; roll back via git revert), OR overriding
   the `instruments` DB Setting row via the Settings page or
   `POST /api/settings/instruments/set` (immediate, no deploy;
   requires `CONFIGURE_SYSTEM` + CSRF). `instruments` is a
   read-only `@property`, so the DB Setting row is the only runtime
   override.

## Troubleshooting

- **Zero instruments after `update-kraken-equities-symbols --force`**:
  iapi reachability / Origin header mismatch. Verify
  `curl -H 'Origin: https://pro.kraken.com' -H 'Referer: https://pro.kraken.com/' \
   'https://iapi.kraken.com/api/internal/markets/all/futures-contracts?delayed=true' | head -c 200`
  returns JSON with `result.data[...]`, not `result:null,errors:[...]`.
- **WS disconnect loops on `ws-equities.kraken.com`**: inspect Kraken
  status page; no operator action required — the WS client backs
  off on reconnect. If the symbol_updater keeps returning zero
  rows across two consecutive runs, fire a monitoring alert.
- **Submit returns `422 instrument_market_data_only`**: expected.
  TradFi is observation-only. Point the strategy at a
  `can_trade=True` instrument (crypto/xStocks) for execution.
- **Fragmented Kraken Equities 1m candles** (a settled minute holding
  more than one historical candle version, written from partial trade
  sets): the live-synthesis builder fix (`065463de`, live since
  2026-06-10) stops new fragmentation, and the historical damage window
  (2026-05-24 → 2026-06-10) was repaired in full on 2026-06-11
  (158 444 candles rebuilt from raw trades, rerun-idempotent). The
  one-off repair tooling was removed after execution; if fragmentation
  ever reappears, recover the `repair-equity-candle-fragmentation` CLI
  and its service/repository code from commit `50c98684`
  (`ix_trades_executed_at` from migration 0007 is still in place).
