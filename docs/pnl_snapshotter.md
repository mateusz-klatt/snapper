# Portfolio P&L snapshotter catch-up

The snapshotter remains disabled unless `PNL_SNAPSHOTTER_ENABLED` is enabled
explicitly. This change does not activate it, create an anchor, alter the sample
calculation version, or change the financial valuation rules.

Each tick selects at most 1440 finalized minutes for each supported wallet
scope. The interval is inclusive: its end is at most 1439 minutes after its
start. A shorter finalized tail keeps its actual end, and the two-minute
finalization lag still applies. Work-budget subdivision stays inside this
selected window. The service commits that window before selecting the next
day on a later tick, rather than computing and retaining the entire backlog.

The latest durable sample remains the progress boundary. A restart resumes
after that boundary; an unsuccessful insert cannot move it forward. A
100-day gap therefore needs at least 100 successful ticks per scope. The tick
interval is a scheduling interval, not a promised completion time.

Late-fill and retryable-minute corrections retain their existing precedence.
The snapshotter computes the selected catch-up evidence, reconciles any
affected persisted suffix, and only then inserts new samples. A correction
conflict prevents catch-up writes. The detector baseline must not advance past
withheld minutes, and execution watermarks remain clamped to the selected
window's effective end.

The synchronous valuation of already-loaded evidence runs on a worker thread
for snapshotter requests. Repository reads and writes stay on the caller's
event loop. The normal API path keeps its existing execution mode. Both paths
use the same valuation code, including FX conversion, diagnostic context and
point assembly. Cancelling the snapshotter waits for an already-running
valuation before propagating cancellation, so its request-local contexts are
not closed while its worker still uses them.

The new limit bounds catch-up minutes and retained catch-up results per scope.
It does not truncate late-fill or self-heal suffix reconciliation: that work
must reach the existing persisted tip before catch-up can publish. Such a
correction may still span multiple chunks and take longer than one scheduling
interval. Evidence loading and asynchronous correction writes also retain
their existing scope; this is not a hard wall-clock deadline for an entire
tick or a fixed bound on all historical inputs.

Release acceptance remains separate from local tests: the exact source needs
the full quality gate and independent review, and the runtime owner controls
deployment and the explicit snapshotter enablement.

## Correction continuation repository foundation

Migration `0069` adds `portfolio_pnl_correction_frontiers`, a mutable control
table keyed by wallet, mode, valuation currency, anchor epoch and calculation
version. It does not rewrite existing financial samples. The current
snapshotter is not wired to this foundation: its correction loop remains as
described above. A later integration must impose a per-tick scheduling budget;
this repository contract alone is not a wall-clock limit for a tick.

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
An old epoch/version left unchanged must be explicitly
retained for repair, rather than acknowledged as reconciled. No missing point
is converted to a zero or used as an implicit progress marker.

PostgreSQL correction transactions cap statement timeouts at 20 seconds and
lock waits at one second, preserving any shorter existing limits. The settings
are transaction-local. SQLite uses the same `BEGIN IMMEDIATE` sample fence.
These limits do not constitute a hard in-flight deadline for a complete chunk
or authorize applying the migration or enabling the snapshotter in production.
