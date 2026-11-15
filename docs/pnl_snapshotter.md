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
