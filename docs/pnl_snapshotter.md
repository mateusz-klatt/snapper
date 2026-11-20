# Portfolio P&L snapshotter catch-up

The snapshotter remains disabled unless `PNL_SNAPSHOTTER_ENABLED` is enabled
explicitly. This change does not activate it, create an anchor, alter the sample
calculation version, or change the financial valuation rules.

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
target is reconciled. Sample watermarks remain clipped to their valuation
window. A partially completed correction therefore cannot advertise that it
already processed a new execution or completed a missing minute.

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
end with that transaction and do not change other repository reads. A timeout aborts the affected scope's remaining
work for the tick, logs its wallet and chunk range once per failure streak,
and contributes to the existing per-tick failed-scope count. Catch-up samples
and the detector baseline cannot advance past the failed evidence load.
Earlier committed correction chunks remain durable. The next tick can retry
the failed work; this query limit does not provide a resumable correction
cursor or a wall-clock bound for the complete tick.

Release acceptance remains separate from local tests: the exact source needs
the full quality gate and independent review, and the runtime owner controls
deployment and the explicit snapshotter enablement.

## Durable correction continuation

Migration `0069` adds `portfolio_pnl_correction_frontiers`, a control table
keyed by wallet, mode, valuation currency, anchor epoch and calculation
version. Migration `0070` adds the certified raw execution watermark cut,
the annulment manifest digest, the fill-gap result sampled with that cut, and
the spot venues used to value the basket.
The raw cut is separate from the detector and financial sample watermarks.
Neither migration rewrites financial samples. A legacy checkpoint without
the frozen evidence fields is rebuilt from the first post-anchor minute.

A generation takes the raw cut during its initial evidence probe and reopens
that exact cut at its frozen knowledge horizon for every chunk. It uses the
same fill-gap result and venue set, and verifies the manifest digest before
planning rows. The probe recertifies the raw cut and manifest after reading
fill-gap evidence, before it creates a checkpoint. A changed certified
manifest starts a new generation from the anchor.
A newly committed execution with an older event time is considered on the
next evidence probe. The valuation continues to use the input history
effective at each minute.

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

## Faithful flow recovery and the 5C.4 transition

Timeline version `5A.16` and sample version `5C.4` share bounded faithful
multi-bucket recovery after every historical total/absorber pass fails. The
activation anchor stays at `5A.13`. Both publication dimensions must choose
the same faithful total; exact and zero buckets never move. The request admits
at most 64 ordered keys, 12 binary choices per dimension and 262144 actual
candidate-key visits across all raw minutes and the three flow families.
Completed positive and negative projections reuse only the last immutable
exact-unit/key-order input per family. Budget interruption retains the legacy
reason without treating an unfinished search as an impossible representation.

The sample-version change makes stale-epoch recomputation revisit previously
withheld interior minutes that have no row. It does not relabel existing
history: only a recomputed replacement receives `5C.4`. A disabled snapshotter
cannot heal old samples. The current overlay reads only the selected sample
version, so old `5C.3` rows may be retained as evidence while remaining absent
from a `5C.4` overlay.

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

Run the complete quality gate on the same source. Production enablement
requires a separate decision after this verification.

A standalone writer-disabled release may publish reconstructed live P&L while
all sample writers remain off. With zero qualifying `5C.4` rows, persisted
equity/cash/position/drawdown overlay fields stay null and `sampled` stays false;
these values are unavailable, not reconstructed equity. Runtime preflight must
verify both writer settings and the absence of qualifying rows. Enabling a
writer or repairing history requires a separate bounded-replay admission; the
version bump alone does not authorize either operation.
