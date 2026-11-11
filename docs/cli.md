# CLI

Snapper provides a command-line interface built on the Typer framework.
All commands are available through the `snapper` command in an activated
virtual environment, or as `.venv/bin/python -m snapper` from this checkout.

## Usage

```bash
snapper [COMMAND] [OPTIONS]
```

Help for a specific command:

```bash
snapper [COMMAND] --help
```

## Server and Infrastructure

### `server`

Starts the FastAPI server with the web dashboard.

```bash
snapper server [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--host` | string | from env | Listen address |
| `--port` | int | from env | Server port |
| `--reload` / `--no-reload` | bool | from env (`SERVER_RELOAD`) | Auto-reload for development; explicit flag overrides the env-var setting |

**Environment:**

Set `SERVER_API_ONLY=true` to skip process autostart while keeping
the ZMQ-WebSocket bridge alive (useful when the engine runs as a
separate process). Multi-worker uvicorn (multiple FastAPI processes
sharing a broker) is supported — see the AI-review fanout dedup
contract in `docs/architecture.md` (Deployment Modes); set
`SNAPPER_COORDINATOR_INSTANCE_ID` + `SNAPPER_COORDINATOR_INSTANCE_COUNT`
per worker (env vars only — `snapper server` does not expose
per-worker CLI flags; the `--instance-id` / `--instance-count` flags
in the table below are on `snapper trade-zmq`) to enable the shared
partitioning that prevents duplicate caps-violation WS frames under
N>1.

**Examples:**

```bash
# Start with default settings (all-in-one dev mode)
snapper server

# Start on all interfaces
snapper server --host 0.0.0.0 --port 8000

# Development mode with auto-reload
snapper server --reload

# API-only mode (engine runs separately)
SERVER_API_ONLY=true snapper server
```

### `broker`

Starts the ZeroMQ XPUB/XSUB broker for message routing.

```bash
snapper broker [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--xsub` | string | from env | XSUB endpoint (publishers) |
| `--xpub` | string | from env | XPUB endpoint (subscribers) |

**Example:**

```bash
snapper broker --xsub tcp://127.0.0.1:7500 --xpub tcp://127.0.0.1:7501
```

### `trade-zmq`

Starts the central trade runtime coordinator.

The process still runs as the `trade-zmq` command, but after the trade
runtime redesign it also hosts `TradeService`, the
outbox dispatcher, and the reconciliation loops. It writes
`TradeCommand` rows, consumes `orders.events.*` to keep projections in
sync, and persists checkpoints for recovery.

```bash
snapper trade-zmq [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--signal-topics` | string | `signals.` | Signal topics to subscribe |
| `--instance-id` | int | `0` (or `$SNAPPER_COORDINATOR_INSTANCE_ID`) | Multi-instance partitioning: zero-based id for this coordinator in a multi-instance deployment. CLI flag > env var > default. |
| `--instance-count` | int | `1` (or `$SNAPPER_COORDINATOR_INSTANCE_COUNT`) | Multi-instance partitioning: total coordinator instance count across the deployment. Must match across all coordinators. |

**Example:**

```bash
snapper trade-zmq --signal-topics "signals.paper.BTC-USD.rsi_btc_1h,signals.kraken.BTC-USD.live"
```

**Multi-instance deployment:**

```bash
# Coordinator 0 of 2
snapper trade-zmq --instance-id 0 --instance-count 2

# Coordinator 1 of 2 (separate process, same DB + broker)
snapper trade-zmq --instance-id 1 --instance-count 2
```

Each instance owns `~1/N` of the shard_keys via deterministic
SHA-256 hashing. Default `--instance-count 1` is byte-identical to
single-coordinator behavior (every shard owned by the single
coordinator).

Multi-instance deployment requires PostgreSQL. With
`--instance-count` greater than 1 on a SQLite backend the coordinator
fails fast at startup with a `ValueError`: SQLite compiles
`SELECT ... FOR UPDATE` to a plain SELECT, so cross-instance row
claims cannot serialize. A single-instance SQLite coordinator is
allowed but logs a startup warning about the same locking caveat.

See [docs/operations.md](operations.md) for systemd template unit +
scale-up/down/crash-recovery runbooks.

### `executor`

Starts the standalone order execution service for a selected exchange.
Process-managed deployments use per-wallet executor instances instead:
bare `executor_<exchange>` process configs are templates, and the
launcher expands active `wallet_credentials` rows into runnable
`executor_<exchange>_w<wallet_short>` instances. Start/stop the generated
per-wallet instance names in the process UI/API; starting a bare executor
template is rejected.

```bash
snapper executor [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-e, --exchange` | string | `kraken` | Exchange (`kraken`, `walutomat`) |

`wallet_short` is the last 12 lowercase hex characters of the wallet
UUID7. Each per-wallet instance subscribes to the same exchange command
prefix as the template, then filters by `wallet_public_id` before loading
credentials or placing orders.

**Examples:**

```bash
# Kraken executor
snapper executor -e kraken

# Walutomat executor
snapper executor --exchange walutomat
```

### `feed`

Starts the direct Kraken market data publisher helper via WebSocket.
For the dedicated feed-container topology, use `snapper feed-engine`
instead; it starts every enabled market-data publisher from the process
registry as a supervised subprocess.

```bash
snapper feed [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--symbols` | string | `BTC-USD` | Comma-separated native symbols passed to the Kraken publisher. Symbols must be in native dash format (e.g. `BTC-USD`); slash-format WebSocket symbols are skipped as unknown. |

**Example:**

```bash
snapper feed --symbols "BTC-USD,ETH-USD"
```

### `feed-engine`

Runs the dedicated feed-container entrypoint. It syncs the process
registry, starts enabled market-data publishers as OS subprocesses,
and exits non-zero if any supervised publisher crashes so the
orchestrator restarts the feed container. It does not start a broker;
publisher subprocesses connect to the backend's configured
`ZMQ_BROKER_*` endpoints. Per-process RSS/CPU summary events are emitted
best-effort when the backend broker is reachable, but metrics-publisher
setup failure does not abort feed startup. Set
`PROCESS_AUTOSTART_PROFILE=feed` to enable desired-state reconciliation and
the remote process-command listener; the command still starts publishers
without that profile, but disables those control loops.

```bash
snapper feed-engine
```

### `strategies-engine`

Runs the dedicated strategies-container entrypoint. It syncs the process
registry (including `STRATEGY_EXTRA_PACKAGES` mounts), then starts every
role-`STRATEGY` process THREAD-mode inside this container exactly as they
ran embedded in the backend (boot wallet/grant scope enforcement,
watchdog, parking). Set `STRATEGIES_EMBEDDED=false` on the backend so it
stops owning strategies, and run this container with
`PROCESS_AUTOSTART_PROFILE=strategy` (required for the reconcile loop that
converges running processes to the DB desired-state). It does not start a
broker; it connects to the backend's configured `ZMQ_BROKER_*` endpoints.
Exits non-zero only when a CORE process fails to start — strategy crashes
are watchdog-managed and, after the restart budget, parked (visible via
the coordinator summary frames), never container-fatal.

```bash
snapper strategies-engine
```

### `delegate-engine`

Runs registered generic delegate workloads as managed subprocesses. Requires
`PROCESS_AUTOSTART_PROFILE=delegate` and a loadable extra package that registers
the runner. This command provides process lifecycle management; it does not
implement model requests, consult handling, or egress itself. The optional
Compose `delegate` profile instead starts the separate runner-only PID1 and
does not invoke this coordinator.

```bash
snapper delegate-engine
```

### `zmq-logger`

Monitors and logs ZMQ message traffic.

```bash
snapper zmq-logger [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--payload/--no-payload` | bool | `false` | Log full payload |
| `--file/--no-file` | bool | `true` | Write to audit file |
| `--audit-file` | string | `data/zmq_audit.jsonl` | Audit file path |
| `--max-length` | int | `200` | Max payload preview length |

**Example:**

```bash
snapper zmq-logger --payload --audit-file logs/zmq.jsonl
```

### `egress`

Runs the snapper-egress sidecar from the unified Snapper image.
Additional arguments after `snapper egress` are forwarded verbatim to
the egress entrypoint's argparse parser. This is the compose dispatch
path for `ENTRYPOINT ["snapper"]` plus `command: ["egress"]`; the module
entrypoint `python -m snapper.egress` remains supported for bare-shell
invocations. The Typer wrapper propagates the egress entrypoint's
integer return code as the process exit code.

```bash
snapper egress [--instance-id snapper-egress-prod]
```

See [snapper-egress.md](snapper-egress.md) for tunnel settings and
verification steps.

## Database

### `retire-trade-projection-checkpoints`

Preview and SCD2-close stale trade-projection checkpoints so the next
trader boot rebuilds them from replay. The command never deletes a row or
changes checkpoint values; it only closes the current knowledge interval by
setting `known_to`.

Selection and confirmation are deliberately separate. Choose exactly one of
`--shard-key KEY` or `--all`. Every invocation prints the credential-free
database host and database name plus each candidate's shard key, position
quantity, realized P&L, turnover, and checkpoint time. Without `--confirm`,
the command is a read-only dry run.

```bash
snapper retire-trade-projection-checkpoints --all
snapper retire-trade-projection-checkpoints --all --confirm
```

Use `--all` when checkpoint age cannot distinguish clean state from state
accumulated under older projection semantics. A confirmed invocation still
prints the full preview before writing, revalidates that exact set in the
repository transaction, and refuses if it changed.

### `retire-accruals`

Preview and SCD2-close schedule-based rollover accruals that a live Kraken spot
shard still carries. Actual live Kraken spot financing costs come only from
native postings; the coordinator no longer writes schedule estimates for that
route, but rows written earlier stay current, and any current accrual row keeps
the shard uncertified at recovery. The command closes the current knowledge
interval of the asserted rows by setting `known_to`. It never deletes a row,
never writes a balancing row, and does not touch fills, checkpoints or balances.

```bash
snapper retire-accruals --wallet <wallet-public-id> --instrument <instrument-public-id> \
    --accrual <accrual-public-id> --accrual <accrual-public-id>
```

| Option | Meaning |
| ------ | ------- |
| `--wallet` | Wallet public id of the shard. Required. |
| `--instrument` | Instrument public id of the shard. Required. |
| `--accrual` | Public id of one current accrual to close. Required; repeat it for every row. |
| `--expect-total` | Optional asserted sum and asset in one value, for example `"-0.0031 EUR"`. |
| `--confirm` | Close the previewed rows. Without it the command is a read-only preview. |

The mode is always `live`. Every invocation prints the credential-free database
identity, every current accrual of the wallet and instrument, and their exact
sum, spelled so that it can be pasted back as `--expect-total`. The checks run
in this order and the first failure exits `1` without writing:

1. Every identity is a public id and no `--accrual` is repeated; `--expect-total`
    is one finite amount and one asset code.
2. The instrument's funding type is `spot_margin_rollover`, and every current
    row is a `kraken` rollover accrual.
3. The asserted ids are exactly the current rows. The refusal lists rows that
    are current but not asserted and ids that are asserted but not current.
4. The asserted total and asset, when given, equal those of the rows.

With `--confirm` the repository reselects the rows under lock, refuses if any
version differs from the preview, closes them at one instant and prints a JSON
receipt with the scope, the retired ids, that instant and the count. A second
run with the same ids refuses at check 3, because those ids are no longer
current.

Closed accruals stop counting for recovery certification and for P&L read at or
after the closure; a P&L read as known before the closure still returns them.
The coordinator certifies the shard at its next start, so recreate the API
container after a retirement. Durable evidence that the wallet used margin is
kept on purpose: retirement does not change the reconciliation method.

### `repair-candles`

Repair one-minute candles that are wrong in exactly two ways: a minute with
trades and no candle row of any version (`insert`), and a current flat
synthesized candle with volume 0 and no trades for a minute that had trades
(`correct`). The correct bars come from a saved Kraken REST OHLC snapshot or
from stored trades. Rows are written only through the guarded repository writers
`insert_missing_native_candles`, `insert_missing_calculated_candles` and
`correct_false_zero_candles`, which never close, update or replace a native or
calculated row. Higher timeframes are not repaired.

```bash
snapper repair-candles --source snapshot --snapshot-dir <snapshot-root> --all-mapped \
    --start 2026-11-10T09:00:00Z --end 2026-11-10T22:00:00Z --output-dir <new-dir>
snapper repair-candles --source trades --exchange kraken --symbol XRP-EUR \
    --start 2026-10-29T00:00:00Z --end 2026-11-10T00:00:00Z --output-dir <new-dir>
```

To write, repeat the same options with a new `--output-dir` and add
`--confirm --voted-preview <dry-run-dir> --expect-digest <digest>`.

| Option | Meaning |
| ------ | ------- |
| `--source` | `snapshot` or `trades`. Required. |
| `--snapshot-dir` | Snapshot root holding `ohlc/` and `ohlc_tokenized/`, one `<pair key>.json.gz` per pair. Snapshot source only. |
| `--exchange` | Exchange of the stored trades. Trades source only. |
| `--symbol` | Native symbol; repeat it for every symbol. |
| `--all-mapped` | Every snapshot file whose pair key maps to an instrument, instead of `--symbol`. Snapshot source only. |
| `--start`, `--end` | UTC minutes with an explicit offset. The window is `[start, end)` and must end at least 180 minutes before the run. Required. |
| `--output-dir` | New or empty directory for this run's files. Required. |
| `--confirm` | Write the voted rows. Needs `--voted-preview` and `--expect-digest`. |
| `--voted-preview` | Output directory of the voted dry run. |
| `--expect-digest` | sha256 of the voted `approval.json`. |
| `--max-rows` | Most rows this write applies; a later write with the same vote applies the rest. |
| `--chunk-pause-seconds` | Pause between write chunks. Default `1.0`. |
| `--max-consecutive-lock-timeouts` | Consecutive lock timeouts that stop a write. Default `3`. |

Every invocation prints the credential-free database identity. A usage error or
a refusal prints one `refused:` line and exits `1`; any other failure prints the
error's class and SQLSTATE only, never its text.

**Sources.** A snapshot file is valid when `error` is empty, `fetched_at` is a
finite epoch time no later than the run and no earlier than the file's newest
row, and `result` holds exactly the pair key and `last`. The pair key maps to
one instrument through an active `kraken` `rest` alias of exactly that name;
invalid, unmapped and ambiguous files are reported in the summary and skipped.
The file's window, `[max(oldest row, --start), min(fetched_at to the minute,
--end))`, is parsed in windows of at most 719 minutes. The last row, still open
at `fetched_at`, and rows with a trade count of 0 are dropped; a minute missing
from a file is unknown and never treated as untraded. Two files with equal bars
for a minute make one candidate known at the later `fetched_at`; different bars
are skipped as `conflicting_snapshot_files`. Stored trades are folded per
instrument and UTC day through the live trade candle builder, with the same row
mapping as `backfill-candles-from-trades`; a trade at a window's exclusive end
belongs to the next window.

**Plan.** Each minute is classified against every stored version of its candle
as `insert`, `correct`, `equal`, `differs` (another native or calculated row) or
`skip` with a reason. The guarded writers then confirm the planned writes in a
dry run, 100 rows at a time; a row they drop becomes `refused_by_guard`. The run
writes these files with mode `0600`, never over an existing file:

- `preview.csv`: one line per minute, sorted by instrument and minute, with the
    current version and the new bar; floats are written with `repr`.
- `approval.json`: canonical JSON holding the sha256 of `preview.csv`, the
    database endpoint (dialect, host, port and database name, or the SQLite
    file's absolute path), the request, the resolved instruments, every
    contributing snapshot file with its sha256 and `fetched_at`, and the sha256
    of the installed source files that decide what is written.
- `summary.json`: counts per action, reason, symbol and UTC day.

The digest to vote on is the sha256 of `approval.json`.

**Write.** Before any database read, a write refuses unless the voted
`approval.json` hashes to `--expect-digest` and binds the voted `preview.csv`.
It then refuses if that approval names another database, request, instrument
set, snapshot file or source revision. It rebuilds the plan and applies only the
rows the vote holds with the same action and values and, for a correction, the
same zero version, which the writer checks again under its lock. Other planned
writes are reported as `not_voted`; voted rows that no longer need a write are
reported as `already_applied` or `changed`. Inserts come first, then
corrections, in chunks of at most 100 rows. A chunk is retried, with at most
four attempts and backoffs of 0.25, 0.5 and 1.0 seconds, only after a lock
timeout, a serialization failure, a deadlock or a conflict on the candle
natural-key index. Any other error makes the chunk uncertain: one read without a
table lock reports its rows as `applied`, `applied_superseded` or `uncertain`,
and the write stops. `receipt.jsonl` gets one line per chunk as soon as the
chunk ends, and `receipt.json` is written even when the write fails or is
cancelled. A write that stops before its end exits `1`.

Each correction chunk takes `SHARE ROW EXCLUSIVE` on `candles`, so live candle
writers wait up to one second per chunk: run writes in quiet hours.

### `annulment`

Host-side execution-ledger correction commands use the configured `DB_URL`.
Start with the read-only inspection:

```bash
snapper annulment inspect --wallet <wallet-public-id> --mode paper
```

| Subcommand | Required options | Behavior |
| ---------- | ---------------- | -------- |
| `inspect` | `--wallet`, `--mode` | Read unwitnessed executions, canonical row digests, existing annulments, and missing visibility observations. Optional `--exchange` and `--limit` bound discovery. |
| `annul` | `--request-file`, `--confirm` | Append one guarded annulment from a JSON request. There is no bulk mode. |
| `complete-visibility` | `--wallet`, `--mode` | Idempotently complete pending durability observations; optional `--limit` bounds one pass. This writes observations. |
| `retire-derived` | `--wallet`, `--mode`, `--confirm` | Retire affected active trade-projection checkpoints and positions. Repeat `--checkpoint` and `--position` to assert the expected active public IDs; execution-plan checkpoints are not retired. |

Modes are `live` and `paper`. An annulment request must contain exactly
`target_execution_public_id`, `expected_execution_digest`, `wallet_public_id`,
`exchange`, `mode`, `scope_sequence`, `annulled_by_user_public_id`,
`correction_time`, `reason`, and `evidence`. The writer validates the target,
scope, digest, reason, actor, and absence of a durable fill witness. The acting
user is an operator assertion checked against an active database user, not an
authenticated session identity.

Exit `0` means the operation completed, `1` means refusal, and `3` means a
correction is durable but its knowledge/visibility proof remains incomplete.
Treat exit `3` as an incomplete operation requiring follow-up, not success.

### `db-init`

Initializes the database schema.

```bash
snapper db-init
```

### `db-upgrade`

Runs Alembic migrations to a specified revision.

```bash
snapper db-upgrade [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--revision` | string | `head` | Target revision |

**Examples:**

```bash
# Migrate to latest version
snapper db-upgrade

# Migrate to specific revision
snapper db-upgrade --revision abc123
```

`db-upgrade` applies schema migrations only. `make migrate-prod` additionally
reseeds the `prod` profile, so use `db-upgrade` when a deployment needs a
migration and nothing else.

Migration `0067` adds the `(plan_public_id, known_to)` lookup index
`ix_epc_plan_known_to` on `execution_plan_checkpoints`. PostgreSQL builds it
with `CREATE INDEX CONCURRENTLY`: checkpoint writes continue, and on a table
with millions of versions the build takes minutes. An interrupted build can
leave the index behind in an invalid state. Remove it with
`DROP INDEX CONCURRENTLY IF EXISTS ix_epc_plan_known_to` and run the upgrade
again.

### `db-downgrade`

Rolls back Alembic migrations.

```bash
snapper db-downgrade [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--revision` | string | `-1` | Steps to roll back |

**Examples:**

```bash
# Roll back one migration
snapper db-downgrade

# Roll back to specific revision
snapper db-downgrade --revision base
```

### `daily-partitions`

Inspects and operates PostgreSQL daily partitions for the static `ticks`,
`candles`, and `trades` allowlist. Mutating subcommands are dry-run by
default and require `--apply`. This lifecycle is separate from
`RETENTION_POLICIES`; it never materializes rows for deletion.

```bash
snapper daily-partitions inspect trades
snapper daily-partitions adopt trades --anchor 2026-07-30T00:00:00+00:00
snapper daily-partitions ensure trades
snapper daily-partitions detach trades --day 2026-06-01
```

`adopt` is the explicit manual path for populated tables. It requires a
UTC-midnight anchor and refuses an unsupported or drifted catalog,
target-name collisions, or a host load above 1.5 per CPU. Add `--apply` only
after reviewing its printed DDL plan. `ensure` maintains fourteen future
leaves and refuses while the retained DEFAULT anomaly partition contains
rows. `detach` enforces the seven-day ticks or thirty-day candles/trades
horizon, uses plain bounded-lock `DETACH`, and leaves the detached table
intact; dropping or archiving it is a separate operator action.

### `fx-proofs backfill`

Reports the exact execution-minute FX conversion requirements sealed by the
persisted watermark maps of every active P&L anchor and sample. Report mode is
the default and performs no writes. Each election manifest is the consumer
union's complete exact-minute set for one currency pair, never a collection of
singleton manifests. This is the identity F4 must request: changing the minute
electorate can change both the manifest digest and the coverage-ranked plane.
The report groups post-run states by wallet, currency pair, and election scope
and lists every unprovable instrument with its lost durable events.

```bash
snapper fx-proofs backfill
snapper fx-proofs backfill --apply
snapper fx-proofs backfill --apply --checkpoint data/fx-proof-backfill-checkpoint.json
snapper fx-proofs backfill --apply --reset-checkpoint
```

`--apply` pins proofs and canonical refusal audits in fixed-size batches. The
anchored checkpoint advances after each completed consumer union and is only a
performance hint: every rerun asks the database whether each requirement is
already pinned, so a restored empty database is repopulated even when an old
cursor survives. The checkpoint is removed only after post-apply verification
proves every requirement has a proof or canonical refusal audit. Use
`--reset-checkpoint` with `--apply` to discard the hint under the same exclusive
host lock used by the writer.

The printed fully-resolved flag means every requirement has either a proof or
a canonical refusal audit; F5's coverage gate must not interpret it as proof
coverage. A partially proven requirement exits successfully when its remaining
gap is carried by the embedded canonical refusal, which automation must treat
as resolved but not fully proven.

All execution, symbol, candle, accrual, and proof reads use the active durable
consumer union's maximum knowledge timestamp. The calculation-version label is
also consumer-owned: anchors and samples can produce separate otherwise-equal
election identities when their stored versions differ. The scope deliberately
excludes spot-reconciliation anchors and never extends persisted cuts with a
newly captured execution prefix.

Each backdated election `timestamp` asserts knowledge at the durable consumer
horizon by design, so the historical `as_of` lookup can see exactly the proof
or refusal that certifies that consumer.
### `pnl-minimum-gate`

Builds the read-only evidence required before the F6 below-venue-minimum
rule may change published equity. The command evaluates the existing temporal
minimum proof against authoritative production balance observations, but it
does not write samples, change the snapshotter, or alter an API response.

The venue denominator is not operator input. Every command loads the complete
active credential catalogue, applies the same executor-topology policy and
`resolve_spot_venues` semantics as the live snapshotter, and fails closed when
the wallet has no executor-backed live spot venue. The artifact includes only
non-secret scope provenance: credential public ids, exchange and credential
type, executor disposition, and inclusion flags. It never serializes encrypted
payloads, labels, or the raw pin declaration.

The one-time activation artifact is always exactly 24 hours, exactly 1440
finalized minute points, and bound to the operator's threshold:

```bash
snapper pnl-minimum-gate report \
  --wallet <wallet-public-id> \
  --max-bound-share <accepted-equity-fraction> \
  > pnl-minimum-gate.json
```

`report` ends its minute grid two finalized minutes behind the current clock.
All commands are intentionally current-only: the credential catalogue, executor
topology, fresh mint-wallet pin, market evidence, and P&L samples are read for one
captured current horizon. There is no backdated `--as-of` surface that could mix
historical topology with a current setting. Each would-be excluded leg is bounded
independently of the snapshotter's sparse minute price plane. For every certified
direct USD plane, the report takes its latest causal close no more than 24 hours
old, then uses the highest of those closes. The leg bound is
`min_order_size * selected_close`.

When exclusions exist, their per-minute aggregate is divided by a conservative
equity denominator. For each impacted minute, the denominator is the lowest
positive complete current-epoch equity observed causally through that minute,
provided the latest causal sample is no more than five minutes old. A complete
sample with missing, non-finite, or non-positive equity, a missing causal sample,
or a stale sample fails closed. The latest complete sample must also be within
five minutes of the report end. The JSON names the exact minimum proof, candle
version, close age, per-impact causal equity floor, Rule-W refusals, and every
minute that lacked an authoritative basket. A finite positive bound overflow is
recorded as a definite threshold breach rather than ambiguous evidence.

The current-time evaluator for a future standing alarm accepts a window of at
least 24 hours; shorter checks are refused:

```bash
snapper pnl-minimum-gate check \
  --wallet <wallet-public-id> \
  --hours 24 \
  --max-bound-share <accepted-equity-fraction>
```

For diagnostics only, `inspect` permits a shorter bounded window. Its JSON
always says `"activation_eligible": false`, even when every available point is
complete:

```bash
snapper pnl-minimum-gate inspect \
  --wallet <wallet-public-id> \
  --hours 1
```

The check emits the same canonical JSON with a `gate` verdict. Exit codes are
stable for cron, systemd, or another alert runner:

| Exit | Meaning |
| ---- | ------- |
| `0` | Complete evidence and the observed bound does not exceed the stated share |
| `1` | Invalid input or the database read failed |
| `2` | Typer rejected the command-line syntax before the report ran |
| `3` | Evidence is incomplete, including no authoritative minutes, no exercised minimum proof, a missing or non-finite USD bound, or no positive complete equity denominator |
| `4` | At least one fully known bound exceeds the stated share |

The threshold comparison is strict: equality passes. A known breach takes
priority over incomplete evidence, so a partial authoritative window, a Rule-W
refusal, or an unpriced leg cannot hide an already-proven alarm. Any of those
conditions still remains visible and prevents a passing activation artifact.
Redirect standard output to retain the review artifact; refusals go to standard
error.

No standing alarm or scheduler is deployed by these commands. They provide the
read-only evaluator only. As of this documentation update the F6 activation
gate remains unsatisfied: no passing 24-hour threshold-bound production
artifact has been accepted, no recurring alarm has been deployed, and the
legacy sample-basis plus coordinated frontend/server prerequisites remain open.
Nothing in this section claims S3/S4 activation is complete.

### `db-seed`

Seeds the database with environment-specific data from TOML profiles.
Automatically run by `make migrate-dev` and `make migrate-prod`.

```bash
snapper db-seed [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--profile` | string | `dev` | Seed profile name (e.g. dev, prod) |

## Users

### `init-admin`

Creates the initial administrator user.

```bash
snapper init-admin [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--username` | string | `admin` | Username |
| `--password` | string | _(prompted, hidden, confirmed)_ | Password — no default; the command prompts with hidden input and confirmation when the flag is omitted, so the value never lands in shell history. |

**Example:**

```bash
# Prompts for password with hidden input + confirmation; no inline value
# means nothing lands in shell history
snapper init-admin --username admin
```

### `list-users`

Displays a list of all system users.

```bash
snapper list-users
```

### `reset-password`

Resets a user's password.

```bash
snapper reset-password USERNAME [OPTIONS]
```

**Arguments:**

- `USERNAME` — Username (required)

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--new-password` | string | (prompt) | New password |

**Example:**

```bash
# Prompts for the new password with hidden input + confirmation; the secret
# never lands in shell history when --new-password is omitted
snapper reset-password john
```

### `token preflight`

Read-only verification of an existing access credential against the deployment's
signing configuration and token inventory:

```bash
snapper token preflight --token-file data/dev-pat.json
```

The required file can contain a raw JWT or the MCP bridge JSON envelope with
`SNAPPER_ACCESS_TOKEN`. Run against the issuing deployment's `DB_URL` and
`MASTER_PASSWORD`. The verifier checks the signature, lifecycle, active owner,
access-token purpose, and signed `jti` agreement using the production token
verification path. It prints correlation IDs and lifecycle diagnostics, but
neither the JWT nor its hash. Exit `0` means accepted; `1` means rejected or
unreadable input. This does not prove permissions for a particular endpoint.

## Symbol Synchronization

### `update-kraken-symbols`

Synchronizes symbol mappings from the Kraken API.

Tokenized assets are classified as equity. Other pairs are classified as forex
when both currency codes supplied by their mapping or discovery path are in
`USD`, `EUR`, `GBP`, `JPY`, `CAD`, `AUD`, and `CHF`; remaining pairs are classified
as crypto. This rule applies to REST, WebSocket-only, and BTNL records. A changed
classification creates a new symbol version while preserving its public
identity and history.

```bash
snapper update-kraken-symbols [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-f, --force` | bool | `false` | Force update |

### `update-kraken-futures-symbols`

Syncs Kraken Futures crypto symbol mappings from the exchange API.

```bash
snapper update-kraken-futures-symbols [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-f, --force` | bool | `false` | Force update |

### `update-kraken-equities-symbols`

Syncs Kraken Equities (FCM Futures) symbol mappings from the exchange
API.

```bash
snapper update-kraken-equities-symbols [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-f, --force` | bool | `false` | Force update |

### `update-walutomat-symbols`

Synchronizes symbol mappings from the Walutomat API.

```bash
snapper update-walutomat-symbols [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-f, --force` | bool | `false` | Force update |

### `update-polygon-symbols`

Synchronizes symbol mappings from the Polygon.io API. By default it
updates existing rows only; pass `--insert-new` to insert symbols that
are not yet in the database.

```bash
snapper update-polygon-symbols [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-f, --force` | bool | `false` | Force update |
| `--insert-new` | bool | `false` | Insert new symbols |

**Note:** This operation may take 10-15 minutes (44k+ symbols).

### `update-underlyings`

Syncs underlying asset definitions from YAML to the database. It matches
active instruments to underlyings via pattern rules, upserts mappings,
and applies YAML-fallback `instrument_type` / `expiry_override` values to
`InstrumentSpec` rows when API-sourced values are NULL.

```bash
snapper update-underlyings [OPTIONS]
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `-f, --force` | bool | `false` | Bypass safety guard for stale cleanup |

### `reconcile-symbol-aliases`

Closes active `symbol_aliases` rows whose owning capability row is no
longer tradeable and no longer market-data-enabled. The command is
idempotent and can be scoped to one exchange.

```bash
snapper reconcile-symbol-aliases [OPTIONS]
```

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-e, --exchange` | string | `all` | Exchange to reconcile (`kraken`, `kraken_futures`, `kraken_equities`, `walutomat`, `polygon`, or `all`) |

## Market Snapshots

### `update-kraken-market-snapshot`

Updates Kraken market snapshots with current prices.

```bash
snapper update-kraken-market-snapshot
```

### `update-kraken-futures-market-snapshot`

Updates Kraken Futures market snapshots with current prices.

```bash
snapper update-kraken-futures-market-snapshot
```

### `update-kraken-equities-market-snapshot`

Updates Kraken Equities market snapshots with current prices.

```bash
snapper update-kraken-equities-market-snapshot
```

### `update-kraken-futures-funding-rates`

Backfills historical funding rates for Kraken Futures perpetuals
into the `funding_rates` table.  Duplicates are silently skipped via
the partial unique index.

```bash
snapper update-kraken-futures-funding-rates [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--symbol` / `-s` | list[str] | None | One or more native perpetual symbols (`-s BTC-USD-PERP -s ETH-USD-PERP`) |
| `--all` | bool | `false` | Backfill every mapped Kraken Futures perpetual |

### `update-walutomat-market-snapshot`

Updates Walutomat market snapshots with current prices.

```bash
snapper update-walutomat-market-snapshot
```

## Data Backfill

### `polygon-backfill-aggregates`

Downloads historical aggregated data from the Polygon.io API to the
on-disk CSV cache. This command is download-only: it writes CSV files
and never touches the database. Populating the `candles` table is the
separate second step, handled by [`polygon-load-csv`](#polygon-load-csv).

The full workflow is two steps:

1. `polygon-backfill-aggregates` downloads candles into the CSV cache.
2. `polygon-load-csv` reads that cache and upserts the candles into the
    database (no Polygon API calls).

```bash
snapper polygon-backfill-aggregates [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-s, --symbol` | string[] | from settings | Symbols to download |
| `--all` | bool | `false` | All mapped symbols |
| `-m, --multiplier` | int | `1` | Timeframe multiplier |
| `-t, --timespan` | string | `minute` | Timespan (`minute`, `hour`, `day`) |
| `-d, --days` | int | `None` (effective default `30`) | Days back |
| `--resume/--no-resume` | bool | `true` | Skip days already cached on disk |
| `--csv/--no-csv` | bool | `true` | Save to CSV files |

**Examples:**

```bash
# Download specific symbols to the CSV cache
snapper polygon-backfill-aggregates -s AAPL -s MSFT --days 365

# Download all mapped symbols
snapper polygon-backfill-aggregates --all --timespan day

# Hourly download, skipping already-cached days
snapper polygon-backfill-aggregates -s SPY -t hour -m 1 --resume

# Then load the downloaded cache into the database
snapper polygon-load-csv --all --timespan day
```

### `polygon-load-csv`

Loads candles from the on-disk Polygon CSV cache into the database. This
is the cache-only counterpart to `polygon-backfill-aggregates`: it reads
the CSV files produced by that command and upserts the candles into the
`candles` table. It never contacts the Polygon API.

Run this after `polygon-backfill-aggregates` has populated the cache.
Archive-symbol cache directories with no current `Symbol` identity are
skipped with a warning, so a stale cache directory never aborts the load.

When neither `--symbol` nor `--all` is given, the symbols default to the
settings-configured Polygon instruments (the `instruments` setting). The
wildcard sentinel `["*"]` in that setting is treated like `--all` and
loads every cached archive symbol; an explicit settings list loads only
those symbols.

```bash
snapper polygon-load-csv [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-s, --symbol` | string[] | from settings | Symbols to load (native or Polygon format) |
| `--all` | bool | `false` | Load every archive symbol present in the cache |
| `-t, --timespan` | string | `day` | Timespan subtree to read (`minute`, `hour`, `day`) |
| `--since` | string | `None` | Earliest day to load, inclusive (`YYYY-MM-DD`) |
| `--until` | string | `None` | Latest day to load, inclusive (`YYYY-MM-DD`) |

**Examples:**

```bash
# Load every cached minute symbol into the database
snapper polygon-load-csv --all --timespan minute

# Load a single symbol within a date window
snapper polygon-load-csv -s X:BTCUSD --since 2024-01-01 --until 2024-01-31
```

### `polygon-repair-splits`

Detects and repairs stale split-price bases in polygon equities. The
incremental CSV cache freezes each day's file at the price-adjustment
basis of its fetch time, so a stock split executed while the cache is
being collected leaves an unadjusted discontinuity in both the cache
and the database. This command automates the repair chain proven in
the 2026-07-02 audit:

1. Pulls split events from the Polygon reference endpoint for the
    trailing `--lookback-days` window and maps them onto active
    polygon instruments (crypto/FX never split and are skipped by
    construction).
2. Confirms via the current `1d` close history which symbols actually
    carry a consecutive-close break matching the split's ratio — the
    break sits at the fetch boundary between cache waves, not
    necessarily at the execution date. Symbols whose whole history was
    fetched after the split are already uniform and are skipped, so
    the command is idempotent and safe to run on every fetch cycle.
3. For confirmed symbols only: re-fetches the full `--window-days`
    range (adjusted as of today), prunes cache files older than the
    window (the plan's minute-data lookback limit means they can never
    be refreshed and would reintroduce the stale basis on a future
    full-range load), SCD2-supersedes every current candle row,
    reloads the refreshed cache, re-synthesizes the higher timeframes,
    and re-runs the detector.

Two safety behaviors to know about:

- Micro-splits whose ratio is closer to 1.0 than 1.4 (e.g. `20:21`,
    `1000:1061`) are reported as `undetectable` and skipped — an
    ordinary flat close would match them forever, so a price detector
    cannot repair them without endless churn; verify those manually.
- An EMPTY current `1d` history for a candidate confirms the repair
    (it is the signature of a run that crashed between supersede and
    reload), so simply rerunning the command self-heals a partial
    repair.

Exits non-zero when a repaired symbol still shows the matching break
afterwards. MUST run on the host, not in a container — the fetch and
prune steps write the `data/polygon` CSV cache, which containers mount
read-only (for the same reason this service is deliberately not
registered with the process manager).

```bash
snapper polygon-repair-splits [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-s, --symbol` | string[] | all equities | Restrict the check to these native symbols |
| `--lookback-days` | int | `45` | Trailing window of split executions to inspect |
| `--window-days` | int | `730` | Re-fetch window in days (plan minute-data lookback limit) |
| `--dry-run` | bool | `false` | Detect and report stale split bases without repairing |

**Examples:**

```bash
# After each fetch cycle: check recent splits, repair what is broken
snapper polygon-repair-splits

# Preview without mutating anything
snapper polygon-repair-splits --dry-run

# Targeted re-check of one symbol
snapper polygon-repair-splits -s NFLX
```

### `polygon-load-grouped-candles`

Loads the on-disk Polygon **grouped-daily** cache into the database as
`1d` candles tagged `source='native', complete=True`. This is the
cache-only counterpart to `polygon-backfill-grouped` (which only writes
CSV): it reads the grouped-daily cache — the same corpus the strategy
warmup reads — and upserts each leg's days into the `candles` table so the
persisted plane holds the daily history the single-source candle read path
and the DB-first warmup depend on. It never contacts the Polygon API.

`--exchange` is **required**: it is the venue the persisted `1d` bars live
under, which **must** be the same venue the leg's live read and warmup
resolve (e.g. `kraken` for FET/RENDER) — *not* `polygon`. Instrument
identity is per `(symbol, exchange)`; live synthesized higher-TF bars
persist under the live publisher's venue, so a Polygon-keyed backfill would
be an orphaned plane that no live read or warmup ever sees. `polygon` here
is only the CSV cache segment that supplies the OHLCV.

`--cut-date` is **required**: it is the first UTC day that synthesized live
persistence may own. The backfill writes only days strictly *before* it, so
under one venue native backfilled history and synthesized live bars never
share a bar key (the candle unique key excludes `source`). Before writing,
an ownership preflight — reading back under the same venue — fails closed if
the persisted plane already holds a synthesized `1d` row before the cut date
or a native `1d` row at/after it, so the operator resolves any overlap
rather than silently version-thrashing.

When neither `--symbol` nor `--all` is given, the symbols default to the
settings-configured Polygon instruments; the wildcard sentinel `["*"]` is
treated like `--all` and enumerates every Polygon-mapped native symbol.

```bash
snapper polygon-load-grouped-candles --exchange VENUE --cut-date YYYY-MM-DD [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-e, --exchange` | string | *required* | Venue the persisted bars live under (e.g. `kraken`); the leg's live-read/warmup venue, never `polygon` |
| `--cut-date` | string | *required* | First UTC day synthesis may own (`YYYY-MM-DD`); writes only days before it |
| `-s, --symbol` | string[] | from settings | Native symbols to load |
| `--all` | bool | `false` | Load every Polygon-mapped native symbol |
| `--lookback-days` | int | `800` | Calendar-day cap on the backward cache walk per symbol |

**Examples:**

```bash
# Load FET/RENDER daily history (under the kraken venue) before the cutover boundary
snapper polygon-load-grouped-candles --exchange kraken --cut-date 2026-06-16 -s FET-USD -s RENDER-USD

# Or via Make (EXCHANGE and CUT_DATE are required; unset fails fast)
make run-polygon-grouped-candles EXCHANGE=kraken CUT_DATE=2026-06-16
```

### `audit-candles`

Audits one explicit persisted candle window without modifying the database.
Both `--window` values are inclusive expected candle opens and must include a
UTC offset. The command freezes one `as_of` instant, verifies the native symbol
is active on the selected exchange in the same database statement that reads
the bounded range, and checks empty or missing window edges,
incomplete rows, OHLC/price/volume invariants, timestamp alignment, duplicates,
ordering, interior gaps, and split suspects.

```bash
snapper audit-candles \
  --target polygon AAPL \
  --timeframe 1m \
  --window 2026-07-01T13:30:00+00:00 2026-07-01T19:59:00+00:00
```

Use `--json` for one stable machine-readable document. Exit status is `0` for
a complete clean audit, `1` for a complete audit with anomalies, `2` for
invalid command usage, and `3` when the audit cannot be completed. Windows are
bounded to 100,000 expected slots and must contain only candles already closed
at the captured `as_of`.

Intraday grids default to a zero-second UNIX-epoch anchor. Pass
`--anchor-offset-seconds N` when a venue's opens use a different explicit
anchor; the canonical range is `0 <= N < timeframe_seconds`. Daily audits
require offset zero and are limited to one slot until a calendar-aware expected
open contract exists.

The gap policy is deliberately `unsuppressed`: every missing slot is an
anomaly. Polygon carries equities, crypto, and FX, so the command never guesses
a session calendar from the exchange name. For an intraday session market,
request one expected trading session at a time. Calendar-aware multi-session
audits, scheduling, automatic repair, and cache-directory continuity remain
outside this command.

### `verify-candle-coverage`

Read-only gate that verifies the persisted `candles` plane is ready to be the
single source for `>1m` reads before the read-path cutover. For each
`(symbol, timeframe)` under `--exchange` it RANGE-reads an explicit canonical
window and checks the FULL expected slot grid (not just the newest rows) is
fresh, gap-free, complete and correctly-tagged — `1d` is `native` strictly
before `--cut-date` and `synthesized` at/after, every other higher timeframe is
`synthesized`. Exits `1` (and prints the failing `(symbol, timeframe)` reasons)
when any pair is incomplete, so it can gate the cutover and the DB-first warmup;
exits `0` when every pair passes.

It reads under the live venue — the same `--exchange` the read path and warmup
resolve; passing `-e polygon` is rejected (that plane is orphaned). For `1d` the
window is extended back across the `--cut-date` seam, so the §4e invariant is
enforced directly: a `[cut_date, publisher_start)` gap (synthesis not yet
persisting from the cut) surfaces as a missing-bar failure rather than silently
staling the warmup. `--min-bars` is the window DEPTH verified — set it to the
consumer's required lookback (e.g. the warmup depth) to verify that far back.

The `derive_snaps` OHLCV-parity rung (the "no derive regression" check) needs a
genuinely live, SUB-socket-warm cache as an independent witness, so the
standalone CLI runs **structural checks only**; parity is deferred to an
in-process slice-4 cutover check.

```bash
snapper verify-candle-coverage --exchange VENUE --cut-date YYYY-MM-DD [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-e, --exchange` | string | *required* | Live venue the persisted bars + read path resolve under |
| `--cut-date` | string | *required* | Synthesis ownership boundary (`YYYY-MM-DD`); decides `1d` provenance |
| `-s, --symbol` | string[] | from settings | Native symbols to verify |
| `-t, --timeframe` | string[] | `5m 15m 30m 1h 4h 1d` | Timeframes to verify |
| `--min-bars` | int | `30` | Window depth (canonical slots) verified per pair |
| `--writer-lag-seconds` | int | `120` | Grace seconds for the writer to flush the just-closed bar |

**Examples:**

```bash
# Gate the FET/RENDER plane before the single-source cutover
snapper verify-candle-coverage --exchange kraken --cut-date 2026-06-16 -s FET-USD -s RENDER-USD

# Verify only the daily plane
snapper verify-candle-coverage -e kraken --cut-date 2026-06-16 -s FET-USD -t 1d
```

### `kraken-futures-backfill-candles`

Backfills historical OHLCV candles for Kraken Futures perpetuals.

```bash
snapper kraken-futures-backfill-candles [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-s, --symbol` | str[] | None | Native symbols to backfill (e.g. `BTC-USD-PERP`) |
| `--all` | bool | `false` | Backfill all mapped Kraken Futures symbols |
| `-t, --timeframe` | str | `1h` | Candle interval (`1m`, `1h`, `4h`, `1d`) |
| `-d, --days` | int | `90` | Days back to fetch |
| `--resume` / `--no-resume` | bool | `--resume` | Resume from latest stored candle |

### Bounded insert-only Kraken minute repair

Use `scripts/repair_kraken_candles.py` for holes inside an explicit historical
window. The existing `kraken-futures-backfill-candles` command retains its original
resume and upsert behavior; its newest-candle resume cannot repair an older hole.
The repair script has no `--resume`: rerun the same bounds to resume safely.

Run from the checkout on the host with a host-reachable `DB_URL` environment
variable, for example a PostgreSQL URL using `postgresql+asyncpg` and `127.0.0.1`.
The script uses configured settings when `DB_URL` is unset. It does not create
instruments or database partitions. Existing daily candle partitions must cover
the requested range.

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--source` | `archive` | `archive` for spot OHLCVT; `futures` for Futures trade charts; `equities` for FCM venue bars |
| `--archive` | Required for archive | Official quarterly ZIP; not applicable to other sources |
| `--timeframe` | `1m` | Bar size; `equities` requires `15m`, `30m`, `1h` or `1d` and other sources stay on `1m` |
| `--start` | Required | Inclusive ISO timestamp with explicit UTC timezone |
| `--end` | Required | Exclusive ISO timestamp with explicit UTC timezone |
| `--symbol` | All active instruments | Exact native symbol, for example `BTC-USD` or `BTC-USD-PERP` |
| `--batch-size` | `100` | Rows per transaction, from `1` to `100` |
| `--dry-run` | `false` | Read and count absent keys without writes or writer locks |
| `--allow-unmapped` | `false` | Acknowledge skipped in-window rows of unmapped archive pairs |
| `--allow-invalid` | `false` | Acknowledge skipped invalid records |

Both bounds must be minute-aligned. The end must be at or before the current
minute's opening. Rows at the end, before the start, or in the current open minute
are never inserted. Counts include rows in the requested window, absent rows,
already-present rows, invalid records, written rows, matched one-minute members,
in-window unmapped row counts and exact unmapped pairs. The summary also names
skipped Futures symbols without CCXT aliases and symbols whose fetch failed.

The Equities source serves a rolling venue window per bar size: about 4 days of
`1m`, 19 days of `5m`, 60 days of `15m`, 120 days of `30m` and 8 months of `1h`,
measured on 2026-11-06, so `1m` and `5m` cannot repair an older outage. It reads
only contracts the venue still lists, reports delisted ones as skipped and stores
native bars with unknown VWAP and trade count.

Unmapped in-window archive rows and invalid records produce a nonzero exit
unless explicitly acknowledged with `--allow-unmapped` and `--allow-invalid`,
respectively. Names and counts remain in the summary even when acknowledged.
An archive with zero matched one-minute members or zero in-window records always
fails, including with both acknowledgements. A fatal scan or database error also
always fails, reporting the batches whose commits were acknowledged.
Dry-run absence counts are a read-time observation; concurrent inserts may reduce
the subsequent write count.

The [official quarterly OHLCVT archive](https://support.kraken.com/articles/360047124832-downloadable-historical-ohlcvt-open-high-low-close-volume-trades-data)
is sparse: no-trade minutes are absent and remain absent. ZIP members named
`PAIR_1.csv` at the root or under one leading directory are streamed; nothing is extracted. Single-line records are
limited to 4096 characters. The seven headerless columns are Unix seconds, open,
high, low, close, volume and integer trade count. Invalid records are logged and
skipped. Identity comes exclusively from active `symbol_aliases` rows with
`exchange='kraken'` and `channel='rest'`, joined to active symbols and instruments.
Direct equality with an archive pair takes priority, for example `XDGUSD` maps to
`DOGE-USD`. Only when a REST name is absent from the archive pair set, one legacy
normalization removes the X/Z prefixes from two four-character asset codes:
`XXBTZUSD` becomes `XBTUSD`, `XETHXXBT` becomes `ETHXBT`, and `XXDGXXBT` becomes
`XDGXBT`. Normalized collisions fail rather than choosing an instrument. Bitnomial
`-BTNL` relay instruments without a Kraken REST alias cannot receive archive rows.
Native VWAP remains null because the archive does not supply it.

The Futures mode pages `fetch_ohlcv` through the existing Kraken Futures client,
using the official [trade charts endpoint](https://docs.kraken.com/api-reference/candles/market-candles).
Requests cover at most 500 minutes, including empty slices, and stop at the
exclusive end. Charts provide neither VWAP nor trade counts; both remain null.
A short page resumes after its last returned minute. A symbol without an exact
CCXT alias is skipped and named without failing the run; dated contracts are
never attempted through a guessed alias. A per-symbol fetch failure is named and
counted, scanning continues, and the final exit is nonzero. Database failures
follow the bounded batch retry policy below. Reruns can pass over symbols that
still have no CCXT alias. No foreign or synthesized prices are tagged as native.

Archive preview and insert:

```bash
PYTHONPATH=src:proprietary/src .venv/bin/python scripts/repair_kraken_candles.py \
    --archive /mnt/d/projects/snapper-market-repair-20261105/kraken/Kraken_OHLCVT_2026Q3.zip \
    --start 2026-09-14T13:57:00Z --end 2026-09-30T19:28:00Z --batch-size 100 --dry-run
PYTHONPATH=src:proprietary/src .venv/bin/python scripts/repair_kraken_candles.py \
    --archive /mnt/d/projects/snapper-market-repair-20261105/kraken/Kraken_OHLCVT_2026Q3.zip \
    --start 2026-09-14T13:57:00Z --end 2026-09-30T19:28:00Z --batch-size 100
```

Futures preview and insert:

```bash
PYTHONPATH=src:proprietary/src .venv/bin/python scripts/repair_kraken_candles.py \
    --source futures --start 2026-09-14T13:57:00Z --end 2026-09-30T19:28:00Z --batch-size 100 --dry-run
PYTHONPATH=src:proprietary/src .venv/bin/python scripts/repair_kraken_candles.py \
    --source futures --start 2026-09-14T13:57:00Z --end 2026-09-30T19:28:00Z --batch-size 100
```

The native repository repair checks absence across **all knowledge versions**,
including closed older rows, and never updates or closes a row. Inserts are
complete native `1m` rows stamped with actual load time, a run session and a
positive sequence. OHLC geometry, finite numbers, alignment and closure are
validated before writing.

After reviewing the skipped pair names and invalid counts, an operator may
explicitly acknowledge both categories. This still fails on an empty selection
or a scan/database error:

```bash
PYTHONPATH=src:proprietary/src .venv/bin/python scripts/repair_kraken_candles.py \
    --archive /mnt/d/projects/snapper-market-repair-20261105/kraken/Kraken_OHLCVT_2026Q3.zip \
    --start 2026-09-14T13:57:00Z --end 2026-09-30T19:28:00Z --batch-size 100 \
    --allow-unmapped --allow-invalid --dry-run
```

Run **one repair at a time**, including native loading and insert-only synthesis.
Both closed-window repair writers request PostgreSQL `ROW EXCLUSIVE` through the
existing reconciliation lock helper. Live writers only touch current windows;
the all-version absence statement protects prior history, and the active partial
unique index arbitrates an unexpected concurrent insert. This mode participates
in normal table/partition maintenance locking without excluding live row writers.
It does not serialize two repair operators. The existing `insert_missing_candles`
and `upsert_historical_synthesized_candles` keep their original stronger lock mode.

The default batch is 100 rows, which is also the hard repair batch limit. Each
batch owns its transaction, commits and releases its locks before the next batch.
The existing lock acquisition limit remains at most one second. Lock timeouts,
unique conflicts and transient database errors retry the same idempotent batch
at most three times after the initial attempt, with 0.25, 0.5 and 0.75 seconds of
backoff outside the transaction. Permanent errors or exhausted retries abort
with a nonzero exit and acknowledged committed progress in the summary. An
uncertain commit acknowledgement may undercount a batch; an idempotent rerun
checks durable absence again. No conflict-exhausted row is silently treated as a
successful repair.

### `kraken-equities-backfill-candles`

Backfills historical OHLCV candles for Kraken Equities (TradFi FCM)
contracts via the internal `iapi.kraken.com` ticker-history endpoint.
Response is ~10-minute delayed per FCM policy. This REST backfill path is
independent of `kraken_equities_realtime_ws_enabled`: enabling the
authenticated realtime WS makes the *live* tick/candle feed realtime but
does not change what this endpoint returns. Per-chunk upstream
failures raise `RuntimeError` so outages are distinguishable from
legitimately-empty candle windows.

```bash
snapper kraken-equities-backfill-candles [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-s, --symbol` | str[] | None | Native symbols to backfill (e.g. `MNQM6-CME`) |
| `--all` | bool | `false` | Backfill all mapped Kraken Equities symbols |
| `-t, --timeframe` | str | `1h` | Candle interval (`1m`, `5m`, `15m`, `30m`, `1h`, `1d`) |
| `-d, --days` | int | `30` | Days back to fetch |
| `--resume` / `--no-resume` | bool | `--resume` | Resume from latest stored candle |

### `backfill-candles-from-trades`

Backfills calculated 1-minute candles from persisted trades.

```bash
snapper backfill-candles-from-trades --exchange EXCHANGE --start START --end END [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--exchange` | str | *required* | Exchange whose trades should be aggregated |
| `--start` | str | *required* | Inclusive UTC start date or datetime |
| `--end` | str | *required* | Inclusive UTC end date or datetime |
| `-s, --symbol` | str[] | None | Native symbols to backfill |
| `--all` | bool | `false` | Backfill all active symbols on the exchange |

### `backfill-synthesized-candles`

Backfills synthesized higher-timeframe candles from persisted 1m candles.

```bash
snapper backfill-synthesized-candles --exchange EXCHANGE --start START --end END [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--exchange` | str | *required* | Exchange whose 1m candles should be aggregated |
| `--start` | str | *required* | Inclusive UTC 1m candle lower bound (date or datetime) |
| `--end` | str | *required* | UTC upper bound used to seal higher-timeframe windows |
| `-s, --symbol` | str[] | None | Native symbols to backfill |
| `--all` | bool | `false` | Backfill all active symbols on the exchange |
| `--timeframes` | str | `5m,15m,30m,1h,4h,1d` | Comma-separated higher timeframes to synthesize |
| `--cut-date` | str | None | UTC date (`YYYY-MM-DD`) where synthesized `1d` ownership begins |

For operator repairs, `scripts/backfill_synth_candles.py` is the maintained
bounded synthesis path. Paired `--start` and `--end` require `--insert-only`.
It also accepts `--dry-run`, `--exchanges`, `--symbols`, `--timeframes`,
`--batch-size` (1–100, default 100) and
`--settle-minutes` (default 120). Without explicit bounds it retains its lookback
and progress-log workflow. Explicit bounds ignore the legacy progress log so a
wider rerun recomputes each instrument.

The script reads current complete 1m candles as known at acquisition time. It
emits only whole UTC-aligned higher-timeframe windows contained in `[start, end)`
and at or before the settle cutoff and observed-minute closure watermark. Sparse
windows require at least one observed minute; wholly empty windows are omitted.
The bounded insert-only mode uses `insert_missing_synthesized_candles` and the
same all-version absence statement as native repair. It cannot reach
`_upsert_historical_synthesized_row`. No existing row of any source is changed,
including synthesized rows and closed older versions. **A rerun after correcting
1m data does not improve existing higher-timeframe bars.** Such corrections
require a separate, explicitly scoped revision workflow. Legacy lookback runs
without explicit bounds retain the guarded revision path.

Unknown input VWAP and trade counts propagate as null for new bars. When input
VWAP is known, a zero-volume window retains the fold's original close-price VWAP
convention. If any input VWAP is null, the result stays null even at zero volume.
This repair path does not require the live finalizer's non-null VWAP gate and
does not change live feed behavior.

After loading native minutes, preview and rebuild a wider bounded range:

```bash
REPAIR_END=$(date -u +%Y-%m-%dT%H:%M:00Z)
PYTHONPATH=src:proprietary/src .venv/bin/python scripts/backfill_synth_candles.py \
    --exchanges kraken,kraken_futures --start 2026-09-01T00:00:00Z --end "$REPAIR_END" --insert-only \
    --timeframes 5m,15m,30m,1h,4h,1d --batch-size 100 --concurrency 1 --dry-run
PYTHONPATH=src:proprietary/src .venv/bin/python scripts/backfill_synth_candles.py \
    --exchanges kraken,kraken_futures --start 2026-09-01T00:00:00Z --end "$REPAIR_END" --insert-only \
    --timeframes 5m,15m,30m,1h,4h,1d --batch-size 100 --concurrency 1
```

The default settle delay leaves recent windows to the live writer. Use
`--settle-minutes 0` to include all observed closed windows up to the fixed end.
These commands do not reconstruct missing Walutomat or Kraken Equities minutes.

### `build-continuous`

Builds and displays a continuous-contract series for an underlying.

```bash
snapper build-continuous TICKER EXCHANGE CONTRACT_FAMILY [OPTIONS]
```

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `TICKER` | Underlying asset ticker (e.g. `SPX`, `GOLD`) |
| `EXCHANGE` | Exchange to source contracts from |
| `CONTRACT_FAMILY` | Product root (e.g. `ES`, `GC`) |

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `-t, --timeframe` | str | `1d` | Candle timeframe |
| `-m, --method` | str | `panama` | Adjustment method (`unadjusted`, `ratio`, `panama`) |
| `--start` | str | 365 days ago | Series start time (ISO) |
| `--end` | str | now | Series end time (ISO) |
| `--rollover-days` | int | `0` | Days before expiry to roll (0..365) |

### `polygon-backfill-grouped`

Downloads grouped daily data from the Polygon.io API to the CSV cache.
Like `polygon-backfill-aggregates`, this is download-only and writes CSV
files without touching the database.

```bash
snapper polygon-backfill-grouped [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--market` / `-m` | str | `crypto` | Market type (`crypto`, `stocks`, `fx`) |
| `--days` / `-d` | int | `3` | Number of recent days to fetch |
| `--locale` / `-l` | str | `global` | Market locale (`global`, `us`) |
| `--csv` / `--no-csv` | flag | `--csv` | Save data to CSV files |
| `--adjusted` / `--unadjusted` | flag | `--adjusted` | Use adjusted prices |

## Data Archive

### `archive`

Export data to CSV archive files.  Supports candle cache projection,
append-only event tables (`ticks`, `trades`, `signals`, `executions`,
`telemetry`, `control`), and SCD2 state tables (`orders`, `positions`,
`instruments`, `instrument_specs`, `settings`, `symbols`,
`symbol_aliases`, `symbol_exchange_capabilities`, `users`,
`user_login_events`, `market_snapshots`, `process_runs`,
`underlying_assets`, `instrument_underlying_mappings`,
`continuous_contract_configs`).  Merges with existing files and
deduplicates rows.

```bash
snapper archive [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--table` | TEXT | `candles` | Table to archive. Accepts `candles`, `candles-audit`, any event table (`ticks`, `trades`, `signals`, `executions`, `telemetry`, `control`), or any state table (see the section intro for the full list). |
| `--exchange` | TEXT | None | Exchange filter (e.g. `polygon`, `kraken`) |
| `--symbol` | TEXT | None | Native symbol filter (e.g. `BTC-USD`), resolved to stable archive_symbol |
| `--timeframe` | TEXT | `1m` | Candle timeframe (`1m`, `5m`, `1h`, `1d`) — `candles` and `candles-audit` only |
| `--day` | str | None | Single day to archive (`YYYY-MM-DD`; parsed in the handler) |
| `--from` | str | None | Start of date range (`YYYY-MM-DD`; parsed in the handler) |
| `--to` | str | None | End of date range (`YYYY-MM-DD`; parsed in the handler) |
| `--dry-run` | FLAG | False | Report counts without writing files |
| `--purge` | FLAG | False | Delete exported rows from DB after writing (event tables **except `executions`**, plus `candles-audit`/state tables when combined with `--closed-only`). Refused for `executions` — see the execution exception note below. |
| `--closed-only` | FLAG | False | Only closed SCD2 versions (`candles-audit` and state tables) |
| `--output-dir` | str | `data` | Base output directory |

**Output structure:**

```text
Candles:  {output_dir}/{exchange}/cache/{timespan}/{archive_symbol}/{year}/{date}.csv
Events:   {output_dir}/archive/{table}/{exchange}/{archive_symbol}/{year}/{date}.csv
Flat:     {output_dir}/archive/{table}/{year}/{date}.csv  (telemetry, control)
```

**Examples:**

```bash
snapper archive --day 2024-03-15 --exchange polygon --symbol BTC-USD
snapper archive --from 2024-01-01 --to 2024-03-31 --dry-run
snapper archive --day 2024-03-15 --timeframe 1d
snapper archive --table ticks --day 2024-03-15 --exchange polygon
snapper archive --table trades --from 2024-01-01 --to 2024-03-31 --purge
snapper archive --table control --day 2024-03-15
snapper archive --table candles-audit --day 2024-03-15 --timeframe 1m
snapper archive --table candles-audit --day 2024-03-15 --closed-only --purge
snapper archive --table orders --day 2024-03-15
snapper archive --table symbols --day 2024-03-15 --closed-only --purge
snapper archive --table instruments --day 2024-03-15
```

**Notes:**

- `--symbol` resolves the current native_symbol to the stable archive_symbol
  via the Symbol anchor row.  After a symbol rename, the CLI argument and
  the output directory may differ.
- `--day` or `--from`/`--to` is required.
- Daily timespan (`1d`) produces monthly CSV files to match Polygon layout.
- `--purge` is supported for event tables, plus `candles-audit` and
  state tables when combined with `--closed-only` (which protects
  active rows); it is **not** supported for the candle cache export.
- **Execution exception (`--purge` refused):** `snapper archive --table
  executions --purge` fails closed with `ExecutionPurgeUnsupportedError`
  before any file is written or any row is deleted. Executions are the
  replayable evidence behind spot reconciliation and precision
  certification; deciding which execution rows are safe to delete
  requires reading the active per-wallet anchors and unresolved
  spot/margin signals, which the archiver does not consult. Executions
  still export to CSV — only the DB delete is refused. As a direct
  consequence, execution storage is currently **UNBOUNDED**: no
  automatic retention prunes it (only telemetry has a configured
  retention policy), and no manual purge path exists. Bounding execution
  storage is deferred pending an anchor-aware retention design.
- Event archive CSV files include full temporal metadata (`public_id`,
  `timestamp`, `known_to`, `session_id`, `sequence_id`).
- Merge/dedup for events uses `(public_id, timestamp, known_to)` as key.
- Executions are partitioned by exchange/archive_symbol via order -> instrument.
- ``candles-audit`` exports all SCD2 versions (closed + active) grouped by
  ``open_at`` date.  ``--purge`` requires ``--closed-only`` to protect active rows.

### `restore`

Restore plain or zstd-compressed CSV data back into the database.
The restorer streams ``.csv`` and ``.csv.zst`` files in bounded batches,
validates each complete header against the table's positional archive
contract, and deduplicates by the exact typed
``(public_id, timestamp, known_to)`` identity.

```bash
snapper restore [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `--table` | TEXT | (required) | Table name to restore into |
| `--file` | str | None | Single CSV file to restore |
| `--dir` | str | None | Directory to scan recursively for `.csv` and `.csv.zst` files |
| `--source` | TEXT | `audit` | Restore mode (`audit` for full history) |
| `--batch-size` | INTEGER | `10000` | Maximum rows parsed and inserted per batch |

**Examples:**

```bash
snapper restore --table ticks --file data/archive/ticks/polygon/BTC-USD/2024/2024-01-01.csv
snapper restore --table trades --file /mnt/archive/trades-2024-01-01.csv.zst
snapper restore --table settings --dir data/archive/settings/
snapper restore --table candles --dir data/archive/candles/polygon/BTC-USD/
```

**Notes:**

- At least one of `--file` or `--dir` is required.
- Directory scans include both ``.csv`` and ``.csv.zst`` files.
- Headerless files, transposed columns, and other header mismatches are refused
  before any database access.
- The default 10,000-row batch size can be lowered to reduce peak memory or
  raised to favor throughput.
- Rows already present in DB (matching `public_id + timestamp + known_to`) are skipped.
- Audit restore inserts all temporal columns exactly as exported.
- **Execution exception (restore refused):** `snapper restore --table
  executions` fails closed with `ExecutionRestoreUnsupportedError`. The
  audit restore's direct bulk insert would bypass the per-wallet
  execution fence and the scope-counter protocol that guarantee a
  contiguous, gap-free execution range proof, so restoring executions is
  refused. An anchor-aware offline replay is required before executions
  can be restored; that path does not yet exist.

## Encryption Management

### `settings-rotate-encryption`

Rotates encryption keys for settings in the database.

```bash
snapper settings-rotate-encryption [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--new-password` | string | _(prompted, hidden, confirmed)_ | New master password — no default; the command prompts with hidden input and confirmation when the flag is omitted, so the value never lands in shell history. Inline `--new-password X` is still accepted for non-interactive automation but the same shell-history caveats apply. |
| `--old-password` | string | from env | Current master password (optional; reads from env/bootstrap when omitted). Avoid inline use — passing a secret on the command line leaks it to shell history. |
| `--dry-run` | bool | `false` | Show changes without applying |

**Example:**

```bash
# Simulate rotation — prompts hidden for the new master password
snapper settings-rotate-encryption --dry-run

# Actual rotation — same prompt flow, then re-encrypts every encrypted setting
snapper settings-rotate-encryption
```

The CLI prompts for `--new-password` with hidden input and confirmation when
the flag is omitted (preferred), so the master password never lands in shell
history, scrollback, or CI logs. After a successful rotation the command
prints only a reminder to update `MASTER_PASSWORD` in `.env` — it does NOT
echo the value you entered. Inline `--new-password X` is still accepted for
non-interactive automation but the same shell-history caveats apply.

## Example Workflows

### New Installation Initialization

```bash
# 1. Initialize database
snapper db-init

# 2. Create administrator (prompts for password — never put it on the cmdline)
snapper init-admin --username admin

# 3. Synchronize symbols
snapper update-kraken-symbols
snapper update-polygon-symbols

# 4. Start server
snapper server
```

### Full Trading System Startup

```bash
# Terminal 1: ZMQ Broker
snapper broker

# Terminal 2: Data feed
snapper feed --symbols "BTC-USD,ETH-USD"

# Terminal 3: Executor
snapper executor -e kraken

# Terminal 4: Trade runtime
snapper trade-zmq

# Terminal 5: Server with dashboard
SERVER_API_ONLY=true snapper server
```

Starting all services does NOT enable live trading. The `live_trading_mode`
DB setting is seeded `halted` and gates every non-paper submit; live orders
require the explicit operator action of setting it to `enabled` (see
[configuration.md](configuration.md#live-trading-interlock-kill-switch)).

### Historical Data Backfill

Backfilling Polygon candles is a two-step workflow: download to the CSV
cache, then load the cache into the database.

```bash
# Step 1 - download last 30 days for SPY to the CSV cache (no DB write)
snapper polygon-backfill-aggregates -s SPY --days 30 --timespan minute

# Step 1 - download daily data for all symbols to the CSV cache
snapper polygon-backfill-aggregates --all --timespan day --days 365

# Step 2 - load the downloaded CSV cache into the database (no API calls)
snapper polygon-load-csv --all --timespan day
```

## Notifications

### `notify`

Long-running notification sidecar (domain events → alert history, web fanout,
APNs HTTP/2 and optional Android FCM HTTP v1 FID delivery). It evaluates the registered notify rules,
fans each resulting alert out through the outbox-backed `NotifySidecar`,
and retries server/throttled failures on its own 30-second scheduler.
It also eagerly scans current open portfolio-drift episodes and repeats
that recovery every `PORTFOLIO_DRIFT_RECOVERY_INTERVAL_SECONDS` (default
60), restoring any `drift.<episode_public_id>` page whose real-time bus
event was lost. APNs configuration comes from the `apns_*` settings provisioned
through the seed profile. Android is disabled unless an explicit
`SNAPPER_ANDROID_PUSH_CONFIG_FILE` configures its sender; `apns_enabled:false`
permits FCM-only startup without APNs credentials. See [Android push](android-push.md).

```bash
snapper notify
```

Run under systemd / K8s with `Restart=always` — the sidecar exits
only on unhandled errors or SIGINT; the outbox drain on the next
start recovers any queued deliveries left behind. Drift recovery is
notify-only: it reads episode/ownership/dedup evidence and writes only the
normal alert/delivery outbox rows; it never trades, rebases, or mutates
portfolio truth.

### `android-push-state`

Read the configured database's current Android configuration head without loading
sender credentials or changing state:

```sh
snapper android-push-state
```

The JSON contains `revision` and `active_configuration_id`. Copy the opaque revision
into runtime `expected_configuration_revision` before a new activation or explicit
disable. Both fields are null only before any decision; a disabled head retains its
revision. This command neither enables delivery nor contacts Google. See
[configuration decisions](android-push.md#runtime-configuration).

## AI Integration

### `mcp-oauth provision-client`

Pre-registers one confidential MCP OAuth client in the configured database.
Run migration `0052` before using the command. It generates a 256-bit client
identifier and a 384-bit client secret, stores only the secret's bcrypt hash,
and prints the raw secret exactly once after the database commit succeeds.

```bash
snapper mcp-oauth provision-client \
    --name "Private ChatGPT" \
    --redirect-uri "https://chatgpt.com/connector/oauth/callback"
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--name NAME` | string | required | Exact client name for the future consent screen |
| `--redirect-uri URI` | string, repeatable | required | Exact registered callback URI |
| `--auth-method METHOD` | string | `client_secret_basic` | `client_secret_basic` or `client_secret_post` |
| `--scope SCOPE` | string, repeatable | `snapper.read`, `offline_access` | Initial-release ceiling: only `snapper.read` or `offline_access` |

Redirect URIs are stored without normalization because the authorization flow
must compare them exactly. The command accepts HTTPS URIs and HTTP only for the
exact loopback hosts `localhost`, `127.0.0.1`, and `[::1]`; it rejects fragments,
userinfo, whitespace, noncanonical spellings, and duplicates. Scope and redirect
URI duplicates are refusals rather than silent deduplication.
The dormant first connector is strictly read-only: provisioning rejects
`snapper.account.read` and every write scope even though those names already
exist in the future OAuth scope catalog.

Successful stdout is a one-line JSON object containing `client_id`,
`client_secret`, and the registered public metadata. Save it immediately in the
client's secret store: Snapper cannot recover or print the raw secret again. A
validation failure or generated-ID collision exits `1` and prints no secret.

This command only creates the dormant client row. It does not enable
`MCP_OAUTH_ENABLED`, publish OAuth metadata, or add authorize/token/revoke
routes. Existing delegate PAT clients are unchanged.

### `dev-mint-pat`

Mints a long-lived AI delegate JWT into `data/dev-pat.json`
(mode `0600`) for the local Snapper MCP bridge. See
[ai-integration.md](ai-integration.md) for the wider Claude Code /
Cursor / Windsurf wire-up.

```bash
snapper dev-mint-pat [OPTIONS]
```

**Options:**

| Option | Env var | Default | Description |
| ------ | ------- | ------- | ----------- |
| `--base-url URL` | `SNAPPER_DEV_BASE_URL` | `http://localhost:8000` | Base URL of the running backend |
| `--admin-username USER` | `SNAPPER_DEV_ADMIN_USERNAME` | `None` (resolved from seed profiles, `mcp` then `dev`) | Admin login |
| `--admin-password PASS` | `SNAPPER_DEV_ADMIN_PASSWORD` | `None` (resolved from seed profiles, `mcp` then `dev`) | Admin password |
| `--output PATH` | `SNAPPER_DEV_PAT_OUTPUT` | `data/dev-pat.json` | Output file path |
| `--label LABEL` | — | `Local Dev MCP` | Delegate label |

The script drives the production REST endpoints (`POST /api/auth/login` +
`POST /api/ai-delegates`) so the seeded delegate exercises the same
code path the AI Integration UI uses. Tokens never appear in
stderr — HTTP error bodies are passed through a JWT-shape redaction
helper before printing.

## Backtesting

### `backtest-run`

Run a backtest synchronously. Creates a DB record, executes the engine
in-process, and persists results (signals, trades, equity, metrics).

```bash
snapper backtest-run \
    --strategy MACDCrossover \
    --instrument BTC-USD \
    --exchange kraken \
    --start 2026-01-01 \
    --end 2026-06-01 \
    --timeframe 1h \
    --initial-cash 10000 \
    --params '{"fast": 12, "slow": 26, "signal_period": 9}'
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--strategy` | str | required | Registered strategy class name |
| `--instrument` | str | required | Native instrument symbol |
| `--exchange` | str | required | Exchange name |
| `--start` | str | required | Start date (ISO format) |
| `--end` | str | required | End date (ISO format) |
| `--timeframe` | str | `1h` | Candle timeframe |
| `--initial-cash` | float | `10000` | Starting cash balance |
| `--wallet` | str | `cli` | Wallet public ID |
| `--params` | str | `{}` | Strategy params as JSON |
| `--execution-mode` | str | `direct_db` | Engine type (`direct_db` or `zmq_replay`) |
| `--fill-model` | str | `market` | Fill simulation model |
| `--slippage-bps` | float | `0.0` | Per-fill slippage in basis points |
| `--commission-bps` | float | `0.0` | Per-fill commission in basis points |

`--execution-mode` is persisted on the run, but the CLI computes the
pairing config hash with execution mode excluded and currently
instantiates `DirectDbEngine` directly. Use `POST /api/backtests` for
a run that actually executes through `zmq_replay`.

### `backtest-list`

List backtest runs with optional filters.

```bash
snapper backtest-list --status completed --limit 10
```

**Options:**

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `--strategy` | str | None | Filter by strategy name |
| `--status` | str | None | Filter by status (`pending`, `running`, `completed`, `failed`, `cancel_requested`, `cancelled`) |
| `--wallet` | str | None | Filter by wallet public ID |
| `--limit` | int | `20` | Maximum number of rows to return |

### `backtest-cancel`

Cancel a pending or running backtest run.

```bash
snapper backtest-cancel <run-public-id>
```

### `backtest-rerun`

Re-run a CLI-compatible backtest with the fields exposed by
`backtest-run`, including strategy params. The CLI rerun path does not
preserve `target_execution_exchange`; use the API/process runner for
cross-asset reruns.

```bash
snapper backtest-rerun <run-public-id>
```
