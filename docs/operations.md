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
ZMQ_BROKER_XSUB=tcp://broker.internal:7500
ZMQ_BROKER_XPUB=tcp://broker.internal:7501
MASTER_PASSWORD=...
SNAPPER_COORDINATOR_INSTANCE_COUNT=2
```

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

Use `wallet_public_id=""` only for backward-compatible or explicitly
single-wallet workflows where the wallet segment should be omitted.
Signal-driven paper orders may still add a `strategy_tag`; REST
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
- No order API — every submit route (`POST /api/orders`,
  `POST /api/execution-plans`, `POST /api/trailing-stops`) calls
  `snapper.server._capability_guard.require_tradable` and rejects
  TradFi instruments with HTTP 422
  `error_code=instrument_market_data_only`.

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

On every venue (Kraken Spot ccxt + native, Kraken Futures, Walutomat)
an order submit that fails *after the request may have left the
process* (request timeout, connection reset, gateway 5xx, unparseable
success body) no longer fabricates a REJECTED event. The executor
first verifies against venue truth — up to three lookups by client id
(`cl_ord_id` on Kraken Spot via open+closed orders, `cliOrdId` on
Futures via order status; 2s/5s/10s backoff, 15s bound each). A found
order finalizes as accepted (an already-terminal one additionally
projects its fills through the disappeared-order reconciler); two
consecutive authoritative not-found answers make the rejection
venue-truth-based and safe. Only when verification cannot resolve —
venue unreachable, lookup unsupported (Walutomat, Spot native-only
symbols) — does the executor park the order, record a non-terminal
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
  immediately on every venue — only genuine ambiguity parks. Connection
  refused rejects immediately only on Walutomat, where httpx surfaces
  connection-setup errors distinctly; on Kraken Spot and Futures
  connection-setup failures park as UNKNOWN by design, because the ccxt
  and `requests` transports cannot reliably distinguish sent from
  not-sent (a connection error can fire mid-body), and a false park
  merely alerts while a false rejection could double a position.

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
fill, pricing market orders that lack a snapshot price from the
venue's own per-order fills VWAP (`get_order_fill_vwap`). Only when no
venue price resolves at all does it log
`Recon: fill gap ... no price on market order, skipping corrective fill`
— a fill mismatch needs manual reconciliation against the venue's fill
history only if that ERROR persists across cycles.

### Duplicate-dispatch guard and dispatch TTL

Two executor/outbox gates close the remaining order-flow loss windows:

- **Duplicate-submit guard.** A coordinator crash between the ZMQ
  publish and the CREATED→DISPATCHED commit (or a failed bulk write)
  re-publishes the same `client_order_id`. The executor now drops such
  replays silently: an entry already pending, an accepted order
  awaiting its durable-event heal, or durable venue-event evidence
  (accepted/fill/terminal/unknown — rejections excluded so legitimate
  retries still flow) all block the re-submit before any venue call.
  A failed evidence check drops FAIL-CLOSED.
- **Dispatch max-age TTL** (`TRADE_COMMAND_DISPATCH_TTL_S`, default
  30s, `<= 0` disables). Stale CREATED create/submit commands expire
  at the outbox to the terminal EXPIRED status (never published; engine
  intent released through the regular expired pipeline), and stale
  frames buffered through an outage (the order-flow socket has no
  high-water mark) are resolved at the executor against venue truth: a
  frame whose order EXISTS under the client id is adopted as accepted
  (a crash-window replay that actually placed), an unverifiable frame
  drops silently (the reconciler WARN and engine valve cover it), and
  only a venue-verified-absent frame rejects. A stale row that DOES
  carry submit evidence publishes anyway and is absorbed by the
  duplicate guard — expiring it would fabricate a terminal state for a
  possibly-live order. Cancels and replaces are exempt: expiring a
  stale cancel would strand a live order, and replace outcomes are
  reported on the lightweight cancel/replace event path. Keep the TTL
  below the engine's 60s in-flight valve.

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
instead of re-dialing the dead route. Direct routes are never
quarantined: a connect error with no proxy means the exchange or the
local uplink is down, not the route.

## Per-wallet executors

Executor process configs are templates named `executor_<exchange>`.
At runtime, `ProcessLauncherService` spawns one instance per active
`wallet_credentials` row, named `executor_<exchange>_w<wallet_short>`.
Those instances load credentials through `CredentialResolver` and ignore
commands for other wallets. Operators should start/stop the generated
wallet instances, not the bare templates.

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
#    the Settings page; the registry syncer reconciles that
#    persisted state onto the launcher.)
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
snapper kraken-equities-backfill-candles -s MNQM6-CME -t 1d -d 90 --no-resume
```

The endpoint is undocumented/internal — upstream application-layer
failures (HTTP 200 with `result=null` or non-empty `errors`) raise
`RuntimeError` so they are distinguishable from legitimately-empty
windows. See `src/snapper/infrastructure/exchanges/implementations/
kraken_equities.py:get_ohlcv` for the error contract.

## Quarterly rotation

The default `KRAKEN_EQUITIES` instruments setting is the wildcard
`["*"]`, which expands at runtime to the full mapped venue universe,
so the default scope tracks every available contract automatically.
Rotation discipline applies when an operator narrows the setting to
an explicit allowlist: the chosen TradFi symbols are quarterly expiry
contracts (`MNQM6-CME` = Jun 26, etc.) and must be rotated before the
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
2. Identify the next quarterly (e.g. `MNQU6-CME` Sep 26 when
   `MNQM6-CME` Jun 26 drops below 14 days).
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
  sets): rebuild from persisted raw trades with
  `snapper repair-equity-candle-fragmentation` — dry-run by default,
  with `--end` capped to one hour before now (the settled horizon for
  this delayed feed). The full runbook, including the
  `ix_trades_executed_at` migration prerequisite, is in the
  Maintenance section of `docs/cli.md`. Never run it in parallel with
  an equities candle backfill.
