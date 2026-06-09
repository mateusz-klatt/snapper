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

## Known limitation — REST orders with `wallet_public_id` under N>1

The REST order endpoints at `src/snapper/server/order_routes.py`
build the ``TradeCommand.shard_key`` without the wallet segment,
while the signal-driven engine's shard_key appends
``.w{wallet_short}`` via ``_compute_shard_key`` when
``wallet_public_id`` is non-empty.

At N=1 this is dormant because every shard is owned by the single
coordinator, the outbox ownership filter is a no-op at N=1 (the
trader always passes ``ShardOwnership(0, 1)``, whose ``owns()``
returns True for every shard when ``instance_count == 1``), and the
CID guard is gated on ``instance_count > 1``.

Under N>1, a REST order with a non-empty ``wallet_public_id``
writes a TradeCommand whose shard_key hashes to a different
instance than the engine for the same (exchange, instrument,
wallet). The owning coordinator dispatches the command to the
venue correctly, but the venue ACK is dropped by the CID guard
on every coordinator because the CID was never registered
in ``_order_shard_keys`` (the REST path does not populate it).

Consequence at N>1: REST orders with ``wallet_public_id`` reach
the venue, but the coordinator's ``TradeService`` projection does
not reflect fills/cancellations. Monitoring via the ``orders``
and ``executions`` tables still works — only the in-memory
coordinator state drifts.

Mitigation until a forward fix lands: for N>1 deployments that
need REST orders, either (a) set ``wallet_public_id=""`` on the
REST request, or (b) stay on N=1 for workflows that mix signal-
driven + REST-driven orders. Signal-only workflows are unaffected.

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

### Fault-injection testing

`scripts/resilience_fault_injection.py` simulates a real exchange outage and
asserts recovery. It drops outbound HTTPS (:443) **inside the feed container's
network namespace** with a throwaway privileged helper
(`docker run --net=container:<feed> --cap-add=NET_ADMIN`), holds the outage,
restores it, and measures candle-freshness recovery against the SLA. Only :443
is dropped, so the ZMQ bus and DB writes keep working — the outage is
market-data only (order execution, a separate container, is unaffected).

It targets the feed's own netns because the runtime image has no `iptables`
(`docker exec <feed> iptables` fails) and the feed connects **directly** to the
exchanges rather than through egress (so pausing/firewalling egress is a no-op).
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

# Override the target container, port, or verified exchanges.
DB_URL=... python scripts/resilience_fault_injection.py \
    --container snapper-feed --port 443 --exchanges kraken,kraken_futures
```

Run it in a low-impact window — it darkens live market data for the hold
duration.

### Egress routing for feeds (`feed_egress_enabled`)

By default the feed publishers connect **directly** to the exchanges — they do
NOT route through the egress WireGuard/SOCKS multiplexer (the pool is only
initialized in the API/coordinator process). Set the `feed_egress_enabled`
setting to enable per-publisher egress routing: each feed subprocess then
initializes the egress pool at startup so the Kraken connect shim and
Walutomat's pooled HTTP transport route through the configured tunnels
(per-exchange pins apply; direct stays the fallback).

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
   `POST /api/settings` (immediate, no deploy). `instruments` is a
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
