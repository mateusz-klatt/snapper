# Operations runbook — multi-instance trade coordinator (Phase 4)

Phase 4 introduces static-hash shard partitioning for the trade
runtime. At `SNAPPER_COORDINATOR_INSTANCE_COUNT=1` (the default)
everything behaves identically to pre-Phase-4 — a single
`TraderCoordinator` owns every shard. Scaling to `N >= 2` deploys
multiple coordinators against the same DB and ZMQ broker, each
owning `~1/N` of the shards deterministically via SHA-256 of the
shard key.

This document covers operating the N-instance trade runtime:
scale-up, scale-down, and crash recovery. It is intentionally
opinionated about systemd because systemd template units are the
stable recipe across 28 rounds of plan review. A contract for
alternative orchestrators (Docker Compose, Kubernetes, Nomad) is
also specified so operators can adopt them with empirical
verification.

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
window is the explicit no-HA trade-off of Phase 4. HA was excluded
from scope by design (plan §D2).

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
   `strategy_tag` — see plan §3.5).
3. Active orders (same treatment as executions).

Under N>1 paper mode, state that never produced a checkpoint is
not recovered by the restarted coordinator. Paper strategies that
need to survive restart MUST persist checkpoints.

## Known limitation — REST orders with `wallet_public_id` under N>1

Pre-existing behavior (predates Phase 4): the REST order endpoints
at `src/snapper/server/order_routes.py` build the
``TradeCommand.shard_key`` without the wallet segment, while the
signal-driven engine's shard_key appends ``.w{wallet_short}`` via
``_compute_shard_key`` when ``wallet_public_id`` is non-empty.

At N=1 this is dormant because every shard is owned by the single
coordinator, the outbox has no ownership filter, and the §3.2 CID
guard is gated on ``instance_count > 1``.

Under N>1, a REST order with a non-empty ``wallet_public_id``
writes a TradeCommand whose shard_key hashes to a different
instance than the engine for the same (exchange, instrument,
wallet). The owning coordinator dispatches the command to the
venue correctly, but the venue ACK is dropped by the §3.2 CID
guard on every coordinator because the CID was never registered
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

This plan's operational recipes are anchored on the systemd
template above because it has been stable across 28 plan-review
rounds. For non-systemd platforms, the deployment author writes a
recipe that satisfies the same contract:

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
