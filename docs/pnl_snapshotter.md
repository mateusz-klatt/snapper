# Portfolio P&L snapshotter catch-up

The snapshotter remains disabled unless `PNL_SNAPSHOTTER_ENABLED` is enabled
explicitly. It writes calculation version `5C.5` for scopes with an existing
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
The public timeline keeps its existing `5A.16` calculation and FX policy.

The snapshotter checks price references and display identity through the
generation's fixed target. A symbol or venue revision therefore produces the
same trust decision at every chunk width. Pointwise FX shadow proofs use the
`pnl-fiat-pointwise-v1` policy, with at most 64 evaluations per tick and
64 rows per pair and minute. FX proof backfill accepts only known versions
that used the range-wide policy. That excludes pointwise 5C.5 consumers.

Each snapshotter minute has a separate faithful flow-recovery allowance.
Its financial result is independent of the recovery work performed for
other minutes in the same chunk. A separate limit on recovery work for an
entire chunk triggers subdivision before any rows from that attempt are
committed.

Only a recomputed row receives `5C.5`. The current overlay reads the selected
sample version. Older rows remain available as historical evidence until the
writer recomputes them.

## Known limits

A newly matched fill above the frozen witness bound can leave published
samples below the cursor stale until the next generation reads the fill.

A backdated accrual committed between chunks can reach the remaining chunks
without an invalidation of previously committed minutes.

Price reference and display identity decisions use revisions known through
the generation tip. A revision later in the generation can therefore
withhold an earlier sample consistently across chunk sizes.

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
all sample writers remain off. With zero qualifying `5C.5` rows, persisted
equity/cash/position/drawdown overlay fields stay null and `sampled` stays false;
these values are unavailable, not reconstructed equity. Runtime preflight must
verify both writer settings and the absence of qualifying rows. Enabling a
writer or repairing history requires a separate bounded-replay admission; the
version bump alone does not authorize either operation.
