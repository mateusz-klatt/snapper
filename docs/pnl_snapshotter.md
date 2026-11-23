# Portfolio P&L snapshotter catch-up

The snapshotter remains disabled unless `PNL_SNAPSHOTTER_ENABLED` is enabled
explicitly. It writes calculation version `5C.6` for scopes with an existing
activation anchor.

A replay generation fixes the finalized target minute and knowledge horizon.
The two-minute finalization lag still applies. Each tick commits at most one
inclusive chunk of at most 1,440 minutes per eligible USD live scope. When
valuation exceeds the minute-instrument work budget, it retries a smaller
chunk without advancing progress. The next tick begins at the explicit
persisted checkpoint, even when the last committed chunk wrote no samples.

The scheduler resumes in-progress work before selecting another generation.
Late fills, newly admitted execution annulments, retryable incomplete
minutes, and outstanding withheld repairs can move the next generation's
start earlier. Its detector watermark advances only when all work through the
target is reconciled. Each sample watermark is clipped to the execution
prefix effective at that minute. A partially completed correction cannot
advertise that it processed a new execution or completed a missing minute.

Each chunk loads account observations as an ordered stream, with one
latest-at-start row per eligible exchange. It uses the same temporal sample
selection as a per-minute read, including later failures. The synchronous 5A
valuation runs on a worker thread, with repository operations on the caller's
event loop. The scheduler requests cancellation when a tick exceeds the
configured interval. After a cancelled or timed-out chunk, its scope retries
with a smaller window and scheduling resumes at the next wallet. A worker
already in flight must exit before its valuation context can close, so the
interval is a scheduling budget rather than a hard wall-clock guarantee.

The crypto-to-USD candle read first selects a distinct set of candidate
instrument identities from historical metadata known at the tick's `as_of`.
PostgreSQL bounds candle keys with one typed UUID array; SQLite uses an `IN`
subquery. This candidate set does not replace the candle-author-time ownership
joins, denomination and spot-kind unanimity, or venue certification. Corrected
or delisted metadata and ambiguous price planes retain their existing meaning.

That PostgreSQL read owns an explicit transaction with JIT disabled and the
shared P&L budget: the statement timeout is capped at 20 seconds and the lock
timeout at 1 second, and a shorter limit already in effect is kept. The settings
end with that transaction and do not change other repository reads. A statement
timeout (SQLSTATE `57014`) or lock timeout (`55P03`) aborts the affected scope's
remaining work for the tick, logs its wallet and chunk range once per failure
streak, and contributes to the existing per-tick failed-scope count. Other
database errors abort the scope the same way without that timeout diagnostic.
Catch-up samples and the detector baseline cannot advance past the failed
evidence load. Earlier committed correction chunks remain durable. The next
tick can retry the failed work; this query limit does not provide a resumable
correction cursor or a wall-clock bound for the complete tick.

Release acceptance remains separate from local tests: the exact source needs
the full quality gate and independent review, and the runtime owner controls
deployment and the explicit snapshotter enablement.

## Durable correction continuation

Migration `0069` adds `portfolio_pnl_correction_frontiers`, a control table
keyed by wallet, mode, valuation currency, anchor epoch and calculation
version. Migration `0070` adds the certified raw execution watermark cut,
the annulment manifest digest, the fill-gap result sampled with that cut,
the spot venues used to value the basket, and the venue event bounds and
candidate digest that certify the fill witnesses.
The raw cut is separate from the detector and financial sample watermarks.
Neither migration rewrites financial samples. A legacy checkpoint without
the frozen evidence fields is rebuilt from the first post-anchor minute.

A generation takes the raw cut during its initial evidence probe and reopens
that exact cut at its frozen knowledge horizon for every chunk. It uses the
same fill-gap result and venue set, and verifies the annulment manifest and
physical witness membership before planning rows. A later event above the
saved bound belongs to a future generation. A lower event ID that becomes
visible inside the bound changes the digest and requires recertification.
The admission probe repeats the raw cut, manifest and witness capture after
reading fill-gap evidence. If a saved certificate can no longer be proved,
the writer attempts to start a new generation from the anchor. It leaves the
checkpoint and samples intact until the new prefix can be certified.
A backdated execution with valid witnesses is considered on the next
evidence probe.

All effective-execution proof reads use one database snapshot. PostgreSQL
uses a repeatable-read, read-only transaction. SQLite explicitly begins a
deferred transaction before its first query. This also applies to ordinary
request and activation prefix bundles, so a concurrent append cannot give
their two cuts different views of the ledger.

The witness digest covers venue events eligible to certify the captured
execution lineage. An unrelated unmatched fill can appear after the
independent scope-level gap read without changing that digest. Current
equity reads recheck the global fill gap before exposing persisted samples.

Each correction checkpoint carries a generation, a revision, an explicit next
minute, its target and knowledge horizon, the detector's durable and pending
execution watermarks, and the causal peak. A new invalidation changes the
generation and revision; successful chunk advancement changes the revision.
Completed checkpoints remain present so an invalidation racing completion
cannot be mistaken for first creation. A restart reads the checkpoint instead
of inferring progress from the last sample or its calculation version.

Before planning a chunk, the bounded evidence reader returns its complete
active sample set and scalar prior peak. The set includes rows from other
epochs or calculation versions occupying the same wallet/mode/currency
minutes. Each observation retains the original internal row ID and payload:
SCD2 replacements reuse public IDs, so neither a public ID nor identical
contents is a sufficient physical version token. The reader accepts at most
1,440 exact minutes and refuses excess or off-grid rows rather than silently
truncating the evidence.

Chunk commit acquires the existing sample writer fence and verifies the active
anchor, checkpoint generation/revision, complete presence/absence set and
planned prior peak against both the checkpoint and the current fenced prefix.
It then applies the validated inserts, replacements and
retractions and checks the resulting causal peak. Those edits and the updated
continuation, peak and baseline commit in one transaction. A conflict or an
exception rolls back the whole chunk, including edits preceding the failure.
There is no per-minute peak query: scalar prefix checks run at chunk
boundaries, under the transaction's database limits.

An empty edit list can advance explicit scan progress. Withheld minutes are
different from fully reconciled unchanged minutes: their earliest boundary is
retained as `repair_from`, including after scan completion or a new generation, and the old
execution-detection baseline remains unconsumed. A new generation must revisit
that pending suffix. Its chunks must explicitly acknowledge successful repair;
starting a generation or silently scanning it again does not clear the obligation.
The caller certifies that retained rows were semantically recomputed. When a
chunk acknowledges an outstanding repair inside its admitted range, the
repository also checks its post-edit occupancy: every non-withheld minute from
that repair boundary through the chunk end must have a sample of the current
epoch and calculation version. A missing result refuses the entire transaction
without advancing its checkpoint or consuming its baseline. This is an
occupancy check, not proof that the caller recomputed a retained value correctly.
Initial sparse chunks and chunks which do not acknowledge outstanding repair
retain their existing zero-edit progress semantics.

A newly withheld minute may retract its exact observed prior row and retain its
repair obligation in the same transaction. It cannot simultaneously insert or
replace a row. The caller applies the planner's policy for whether an existing
row must be retracted or preserved; the repository does not invent that decision.
An old epoch/version left unchanged must be explicitly
retained for repair, rather than acknowledged as reconciled. No missing point
is converted to a zero or used as an implicit progress marker.

A chunk may also classify a subset of its `withheld_minutes` as
`unrepairable_withheld_minutes`, which drops their future `repair_from` obligation
without changing how any minute is valued. The planner sets this only when a
withheld minute's `reason_codes` are non-empty and all belong to
`REPAIR_EXEMPT_WITHHOLD_REASONS` (`instrument_reconciliation_failed`,
`attribution_reconciliation_failed`, `instrument_sum_unrepresentable`, and
`attribution_sum_unrepresentable`) AND no active row survives the chunk's edit
(either no row existed or the chunk explicitly retracts that exact version).
If `*_sum_unrepresentable` preserves an existing row (`preserves_persisted_row`),
the minute keeps its `repair_from` obligation (guard R1), because determinism of
the new failed calculation proves nothing about an older retained row. Both the
pure state transition and the repository fence enforce R1 against
`chunk.evidence.versions` and `chunk.edits`, refusing any list entry that is not
in `withheld_minutes`, lies outside the chunk, repeats, or leaves a current,
legacy, or foreign active row unretracted. Mixed-reason and empty-reason-code
withholds (`missing_series_minute`, `foreign_epoch_sample`) and market-data
withholds (`mark_unavailable`, `fx_conversion_unproven`) likewise retain their
repair obligation. On frozen snapshotter inputs, `*_sum_unrepresentable` is
deterministic across chunk widths because `recovery_budget_per_point` gives each
point a fresh count-based `FaithfulRecoveryBudget`, `MAX_RECOVERY_KEYS` depends
solely on the point's own inputs, and exhausting the chunk-shared
`_ReplayRecoveryWork` reserve raises `PnlTimelineWorkBudgetError` (splitting the
chunk) rather than producing a withheld minute.

A foreign-epoch row is kept as an explicit unresolved minute. A cursor can
continue scanning, while the older evidence stays intact and the detector
baseline remains pinned. This requires investigation before that scope can
be considered fully reconciled.

Current-truth equity reads suppress the suffix from the earliest next or
repair minute in the same SQL statement that reads complete USD samples.
That boundary also limits the USD samples eligible for EUR and PLN conversion.
A current-truth fill evidence gap or a late pre-activation execution
suppresses the whole equity overlay.
A prefix retained before the boundary carries the peak computed from the
same stored cash and position values the database uses. A slice with missing
or withheld data therefore cannot expose an older dependent drawdown.

PostgreSQL correction transactions cap statement timeouts at 20 seconds and
lock waits at one second, preserving any shorter existing limits. The settings
are transaction-local. SQLite uses the same `BEGIN IMMEDIATE` sample fence.
These limits do not constitute a hard in-flight deadline for a complete chunk
or authorize applying the migration or enabling the snapshotter in production.

## Sample calculation version

Timeline version `5A.16` and sample version `5C.4` share bounded faithful
multi-bucket recovery after every historical total/absorber pass fails. The
activation anchor stays at `5A.13`. Both publication dimensions must choose
the same faithful total; exact and zero buckets never move. The request admits
at most 64 ordered keys, 12 binary choices per dimension and 262144 actual
candidate-key visits across all raw minutes and the three flow families.
Completed positive and negative projections reuse only the last immutable
exact-unit/key-order input per family. Budget interruption retains the legacy
reason without treating an unfinished search as an impossible representation.

Sample version `5C.5` retains faithful flow recovery and selects exact FX
evidence independently for each mark, execution, accrual and basket minute.
Evidence is loaded in batches. Other minutes in the batch cannot choose which
venue prices a given minute. An instrument-owned canonical FX plane keeps its
precedence. A missing required exact rate withholds the affected valuation.
The public timeline is versioned separately (`5A.18`), keeps its FX policy and
enables neither `5C.6` behaviour.

Sample version `5C.6` adds two snapshotter-only behaviours: (i) the
compensated-total unrealized fallback (lazy builtin `sum` T1 then `math.fsum`
T2, with naive then compensated attribution buckets using compensated
(Neumaier) summation in allocation order, used only where `5C.5` withheld
`instrument_reconciliation_failed`, relying on CPython 3.12+ compensated `sum`
pinned by a test) and (ii) width-independent FX conversion minutes, requesting
every nonzero finite fee, positive-price and nonzero finite accrual conversion
minute from the conversion population and ignoring the invalid-event mark latch
so persisted values and reasons do not depend on the replay chunk's window.

The snapshotter checks price references and display identity through the
generation's fixed target. A symbol or venue revision therefore produces the
same trust decision at every chunk width. Pointwise FX shadow proofs use the
`pnl-fiat-pointwise-v1` policy, with at most 64 evaluations per tick and
64 rows per pair and minute. FX proof backfill accepts only known versions
that used the range-wide policy. That excludes pointwise `5C.5` and `5C.6`
consumers.

Each snapshotter minute has a separate faithful flow-recovery allowance.
Its financial result is independent of the recovery work performed for
other minutes in the same chunk. A separate limit on recovery work for an
entire chunk triggers subdivision before any rows from that attempt are
committed.

Only a recomputed row receives `5C.6`. During the `5C.6` catch-up, the equity
overlay serves only the reconciled `5C.6` prefix: `require_reconciled` hides
rows at or after the same-version frontier's `next_minute` or `repair_from`, so
coverage restarts from the anchor and grows forward as the `5C.6` frontier
advances. `5C.5` rows are never served and there is no mixed-version fallback.
Older rows remain available as historical evidence until the writer recomputes
them.

## Known limits

A newly matched fill above the frozen witness bound can leave published
samples below the cursor stale until the next generation reads the fill.

A backdated accrual committed between chunks can reach the remaining chunks
without an invalidation of previously committed minutes.

Price reference and display identity decisions use revisions known through
the generation tip. A revision later in the generation can therefore
withhold an earlier sample consistently across chunk sizes.

When a settled generation spans from the first post-anchor minute with
`has_fill_gap` enabled and `repair_from` at that first minute, a subsequent
metadata-only admission probe finds identical frozen execution watermarks,
execution manifest digest, spot venues, and fill-witness cut, no late fill
reaches back through the frozen frontier tip, and no active sample rows occupy
`[first .. tip]` across any epoch or calculation version, the snapshotter skips
re-replay and pauses the frontier. While paused, the frontier's `tip`, `as_of`,
`generation`, `revision`, `pending_watermarks`, `repair_from`, and
`baseline_watermarks` stay at the settled generation's values, persisted rows
and `audit_json` remain untouched, and `require_reconciled` reads continue to
withhold every minute from the first post-anchor minute onward.

Monitoring note: each pause streak emits one `persistent_fill_gap_skip` log
event per scope (`scope_skip_reason="persistent_fill_gap_skip"`) carrying
`wallet_public_id`, `mode`, `valuation_ccy`, `epoch_public_id`,
`frontier_tip`, `frontier_as_of`, `as_of`, and `gap_age`. The streak ends only
when replay actually resumes for that scope, and the frontier resumes a full
replay from the anchor's first post-anchor minute through the current finalized
tip as soon as evidence changes or the fill gap clears.

Repair-lag monitoring note: whenever a frontier read or chunk update in
`_sample_scope` or `_advance_replay` observes `repair_from` more than 24 hours
behind `frontier.tip`, the snapshotter emits one rate-limited structured warning
per scope and distinct `repair_from` value carrying `repair_from_lag`,
`wallet_public_id`, `mode`, `valuation_ccy`, `epoch_public_id`, `repair_from`,
`frontier_tip`, and `as_of`. Rate-limit state clears when `repair_from`
resolves or drops within 24 hours and when absent scopes are pruned.

Disclosed residual difference for repair-exempt withholds: value-neutrality
holds on frozen evidence. If a later market-data correction (such as a revised
candle version) lands on the exact minute of an exempt unrepairable withhold and
changes its arithmetic outcome, the snapshotter leaves that minute withheld
until another generation covers it via a late-fill rewind, an evidence change at
or before it, or an explicit rewind — matching the policy already applied to
`complete` minutes, which are likewise not revisited on candle corrections
alone. The 24-hour `repair_from_lag` warning does not fire for an exempt
unrepairable minute because no `repair_from` obligation remains.

## Replay acceptance

Exercise the exact candidate on an isolated local PostgreSQL database on WSL.
Seed one executor-backed synthetic spot wallet with observations and a ledger
prefix across multiple chunks. Compare an uninterrupted replay with an
interrupted one that stops before a commit and with one that loses the
acknowledgement after a commit. Reopen with a fresh writer and database
session. Check that point values, causal peak and drawdown match the
uninterrupted run and that checkpoints advance exactly once per commit.
Verify that a withheld interior minute keeps the old detector baseline across
restart and that the equity read excludes dependent samples during repair.
Repeat with late ID-less partial fills, including a committed witness within a
saved bound, and compare a full chunk with a restart using smaller chunks and
competing FX venues. Compare both basket equity and accounting flows.

Run the complete quality gate on the same source. Production enablement
requires a separate decision after this verification.

A standalone writer-disabled release may publish reconstructed live P&L while
all sample writers remain off. With zero qualifying `5C.6` rows, persisted
equity/cash/position/drawdown overlay fields stay null and `sampled` stays false;
these values are unavailable, not reconstructed equity. Runtime preflight must
verify both writer settings and the absence of qualifying rows. Enabling a
writer or repairing history requires a separate bounded-replay admission; the
version bump alone does not authorize either operation.

## Opening checkpoint mode

`PNL_SNAPSHOTTER_CHECKPOINT_MODE` selects the snapshotter's durable
opening-checkpoint behavior (`off` by default, or `shadow`, or `active`):

- Unset, empty, or whitespace-only values resolve to `off`.
- Surrounding whitespace is stripped and tokens match case-insensitively.
- Any unrecognised value raises `ValueError` at startup naming
    `PNL_SNAPSHOTTER_CHECKPOINT_MODE`.

Migration `0071` adds the `portfolio_pnl_opening_checkpoints` sidecar table
keyed by `(wallet_public_id, mode, valuation_ccy, epoch_public_id, calc_version)`
without changing existing financial sample rows or altering `candles`. Both
`CHECKPOINT_MODES_ENABLED = frozenset({"off", "shadow", "active"})` in
`pnl_snapshotter_config.py` and `capture_permitted=True` for `5C.6` in
`pnl_engine_options.py` are now enabled in code; startup info logs both
`checkpoint_mode` and `configured_checkpoint_mode`.

Mode semantics:

- `off` (default when unset): preserves pure cold anchor replay with zero
    checkpoint reads, captures, resumes, or shadow comparisons, and plans once
    per chunk.
- `shadow`: replays cold from anchor to persist authoritative chunks and
    captures checkpoints at settling tips (on the pointwise, no-carry `5C.6`
    read profile), while also replaying verified checkpoints on later suffix
    ticks to compare outputs against anchor replay without altering persisted
    samples.
- `active`: captures opening checkpoints at clean settling tips and resumes
    subsequent suffix chunks directly from verified checkpoints without
    prefix execution reads, falling back to anchor replay (and bounded
    recapture when needed) on any closed verifier refusal.

Shadow verification and resumed replay are limited to two wallets per tick,
with budget-consuming wallets rotating the next tick's starting position.
A wallet consumes a slot as soon as live checkpoint verification runs, even
when the verifier then refuses, so the cap bounds verification cost as well as
comparisons. Wallets beyond that limit use the exact `off` cold-replay path for
the tick.
Shadow errors increment `shadow_skipped` with reason `shadow_error:<Type>`,
log one warning per scope failure episode, and never prevent the authoritative
anchor samples and frontier from committing.
In shadow mode, live checkpoint verification still runs before the chunk is
replayed, but the cold anchor plan then commits before any resumed replay or
comparison starts. No resumed-side outcome can therefore skip or delay the
authoritative write, including a valuation worker thread that cannot be
cancelled and outlives every deadline. The resumed side then runs under its
own nested deadline that ends a quarter of the loop interval
(`SHADOW_TICK_RESERVE_FRACTION`) before the tick deadline, which normally
leaves time for the tick's later wallets. A resumed side that overruns it is
counted as `shadow_error:TimeoutError`; when no shadow budget remains, the
comparison is skipped as `tick_budget`. External task cancellation is never
converted into a shadow failure. If an uncancellable worker outlives the whole
tick, the tick deadline wins: no `shadow_error:TimeoutError` is recorded, the
tick ends after that wallet's commit, its replay chunk limit is kept with a
warning, and the remaining wallets run on the next tick.
A resume fallback found by the shadow branch deletes the stored checkpoint but
neither recaptures inline nor counts a recapture, because the chunk has already
committed. A fresh checkpoint is captured by the next eligible clean-tip replay
on the cold path; at a stationary tip that waits until new evidence opens a
replay chunk.
The cold anchor and the resumed side are planned from one shared read of the
chunk's observation, valuation and position evidence, so a write committed
between two reads cannot appear as a mismatch that deletes a valid checkpoint.

Rollout and rollback:

1. **Pre-flight probe**: the deployed environment must **not** set
    `PNL_SNAPSHOTTER_CHECKPOINT_MODE` (leave it unset so default `off`
    applies) until the operator's shadow call. First run the read-only probe
    script
    `PYTHONPATH=src python scripts/pnl_checkpoint_resume_probe.py --wallet <ID> --mode live --valuation-ccy USD`
    against a replica or production read path to compare cold anchor replay vs.
    in-memory capture and suffix resume bit-for-bit before enabling writes.
2. **Shadow soak**: set `PNL_SNAPSHOTTER_CHECKPOINT_MODE=shadow` and restart
    the service; monitor `checkpoint_metrics_summary` logs to confirm
    `captures_committed > 0`, `resumes_succeeded > 0`, and
    `shadow_mismatches=0` before promoting.
3. **Active rollout**: set `PNL_SNAPSHOTTER_CHECKPOINT_MODE=active` and restart
    to switch catch-up and incremental ticks to suffix-only resume.
4. **Immediate rollback**: unset `PNL_SNAPSHOTTER_CHECKPOINT_MODE` (or set it
    to `off`) and restart the service. No schema rollback is needed;
    correction-invalidation deletes during frontier writes continue to retire any
    previously persisted opening checkpoint invalidated by a rewrite.

## Running the probe in production

Run the probe as a standalone process. It installs class-global SQLAlchemy
session listeners for the duration of the probe.

Create a dedicated least-privilege PostgreSQL role (`snapper_ro`) with read-only
transaction defaults (replace every `<...>` placeholder before running):

```sql
CREATE ROLE snapper_ro LOGIN PASSWORD '<set-me>';
GRANT CONNECT ON DATABASE <database> TO snapper_ro;
GRANT USAGE ON SCHEMA public TO snapper_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO snapper_ro;
ALTER ROLE snapper_ro SET default_transaction_read_only = on;
```

Pass the `snapper_ro` connection string via `--db-url` (or `PNL_PROBE_DB_URL`
as the environment fallback). This overrides only the probe's isolated engine
without modifying the application's `DB_URL`, passes the default writable-role
check without `--allow-writable-role-with-readonly-session`, and redacts
credentials across all outputs. If neither `--db-url` nor `PNL_PROBE_DB_URL` is
set, the probe prints one `WARNING` line recommending the dedicated read-only
role and continues with its existing checks:

Keep the password out of the URL, argv and the environment: asyncpg reads it
from `~/.pgpass` (or the file named by `PGPASSFILE`, mode `0600`) when the
DSN carries none, and the `.pgpass` host field must match the DSN host.

`snapper_ro` may connect from the database host's loopback (`127.0.0.1`) or
the LAN (`192.168.5.0/24`) but never from a container on the Docker bridge,
which is reserved for the application role. Run the probe either from a LAN
host's virtualenv or from a one-off container on the host network, where
`<db-host>` defaults to `127.0.0.1`. Run the container command from the
repository root with an image built from the same revision as the probe.

```bash
docker run --rm --memory=3g --network host --user "$(id -u)" --entrypoint timeout \
    -v ~/.pgpass:/run/secrets/pgpass:ro \
    -v "$PWD/scripts/pnl_checkpoint_resume_probe.py:/app/scripts/pnl_checkpoint_resume_probe.py:ro" \
    -e PGPASSFILE=/run/secrets/pgpass \
    -e PNL_PROBE_DB_URL='postgresql+asyncpg://snapper_ro@<db-host>:5432/<database>' \
    <image> --kill-after=5s 150s python /app/scripts/pnl_checkpoint_resume_probe.py \
    --wallet <WALLET_PUBLIC_ID> --mode live --valuation-ccy USD --window-minutes 60
```

The same run from a LAN host's virtualenv uses `--db-url` instead:

```bash
PYTHONPATH=src python scripts/pnl_checkpoint_resume_probe.py \
    --wallet <WALLET_PUBLIC_ID> --mode live --valuation-ccy USD --window-minutes 60 \
    --db-url 'postgresql+asyncpg://snapper_ro@<db-host>:5432/<database>'
```

The `--max-rss-mb` guard (default `2048` MiB) checks RSS growth over the probe's
startup baseline. On Linux it samples `/proc/self/statm`, so temporary spikes
between samples can escape detection. On macOS it uses `ru_maxrss`. On Linux
the CLI can lower soft `RLIMIT_AS` to the startup virtual-memory size
(`VmSize` from `/proc/self/status`) plus four times `--max-rss-mb` in MiB.
This leaves room for shared libraries and the allocator and preserves any
tighter inherited soft limit.

The probe checks `--max-runtime-seconds` at asynchronous cancellation points
and before reporting the comparison verdict. CPU-bound work can continue past
the deadline until the next check. For hard enforcement, run the probe with a
container memory limit (`docker run --memory`) and an outer timeout that can
terminate the probe process, as in the container example. Size the container
limit for the entire process, including startup memory.

`--window-minutes` bounds the output of each replay. Capture ends at the
checkpoint minute, and the cold and resumed comparisons start one minute
later. Capture and cold replay still process the history needed to reconstruct
opening state. Row caps count the actual execution, fill, accrual, and candle
queries at their read horizons, including FX candles for fees. The count and
load share a read snapshot pinned at transaction start. Each budgeted read
adds up to one bounded scan for the count. Allow for about twice the candle
scan work when setting `--max-runtime-seconds` and the outer timeout.

Exit codes:

- `0`: `RESULT: PASS` — cold replay and checkpoint-resumed replay matched
    bit-for-bit.
- `1`: `RESULT: FAIL <mismatch_detail>` — cold vs. resumed points or rate
    sources diverged.
- `2`: `RESULT: FAIL <reason>` — missing anchor or `no_frontier`, safety or
    writable-role refusal, `timeout`, `interrupted`, or configuration/DB error.
- `3`: `RESULT: REFUSED <reason>` — closed checkpoint or frontier refusal (for
    example `frontier_has_fill_gap`, `capture_fill_gap`, or
    `checkpoint_verify_fallback:...`), `caps_exceeded:<kind>`, or `cut_moved`.
    The probe refuses with `cut_moved` before buffering an over-cap execution
    or fill query for the fresh capture cut.
