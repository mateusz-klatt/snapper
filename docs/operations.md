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
coordinator, the outbox has no ownership filter, and the CID guard
is gated on ``instance_count > 1``.

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
   anchored to the start and end of the line, placed inside the
   non-systemd recipe section (not anywhere in the file).

Acceptable orchestrators for the non-systemd recipe are
`docker compose`, `kubectl` (Kubernetes), or `nomad` — the
meta-test rejects bare `docker` because `docker run` alone does
not provide the per-instance lifecycle the contract requires.

## Verified environments

Verified on 2026-04-18 against docker-compose-v2.29.7

# Kraken Equities (TradFi) market data

TradFi index futures (Kraken FCM: MNQ/ES/YM/RTY/NKD/M2K) are shipped
in market-data-only mode. The symbol updater + publisher processes
are disabled at code level; runtime enable lives in the DB
`Setting` rows for the relevant processes so rollout + rollback
happen without code deploys.

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

## Enable the feed

```
# 1. Populate Symbol + SymbolExchangeCapability rows from iapi.
#    (kraken_equities entry must be present for is_tradeable
#    cache hits; default deny on missing row.)
snapper update-kraken-equities-symbols --force

# 2. Verify the rows landed.
sqlite3 snapper.db \
  "SELECT COUNT(*) FROM symbol_exchange_capabilities
   WHERE exchange='kraken_equities' AND valid_to='9999-12-31T23:59:59';"
# expect >= 38 (indices only; full catalog up to ~180 contracts)

# 3. Enable the symbol-updater + feed-publisher processes via DB
#    Setting rows (NOT env vars — see feedback_no_asking_continue.md
#    for the rollout preference). ``category='process'`` matches the
#    existing ``registry_syncer`` writes; the column is NOT NULL on
#    the settings table (see ``src/snapper/data/models.py::Setting``).
sqlite3 snapper.db <<SQL
INSERT OR REPLACE INTO settings
    (key, value, category, timestamp, session_id, sequence_id)
VALUES
  ('process_kraken_equities_symbol_updater',
   '{"enabled":true}', 'process', datetime('now'), 'ops', 0),
  ('process_kraken_equities_feed_publisher',
   '{"enabled":true}', 'process', datetime('now'), 'ops', 0);
SQL

# 4. Restart the runtime OR trigger a live registry sync
#    (registry_syncer picks up the Setting rows automatically
#    on the next interval).
```

## Backfill historical candles

```
# 30-day 1-hour backfill of the four default TradFi symbols
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

Default TradFi symbols are quarterly expiry contracts
(`MNQM6-CME` = Jun 26, etc.) and must be rotated before the
`SymbolExchangeCapability.maturity` timestamp on any default
symbol drops below 14 days. Rotation cadence:

1. Pull the current maturity list:
   ```
   sqlite3 snapper.db \
     "SELECT native_symbol, datetime(maturity,'unixepoch')
        FROM symbols JOIN symbol_exchange_capabilities
             ON symbols.public_id = symbol_exchange_capabilities.symbol_public_id
        WHERE exchange='kraken_equities'
              AND valid_to='9999-12-31T23:59:59';"
   ```
2. Identify the next quarterly (e.g. `MNQU6-CME` Sep 26 when
   `MNQM6-CME` Jun 26 drops below 14 days).
3. Update `AppSettings.instruments[KRAKEN_EQUITIES]` in code
   AND/OR the `instruments` DB Setting row to include the new
   nearest contract. Deploy + roll back via git revert on the
   code path; DB Setting path is immediate.

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
