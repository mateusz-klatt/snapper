# Operations runbook — paired-execution guard

The paired-execution guard provides atomicity-by-bounded-compensation for
multi-leg strategies (e.g. `CointegrationPairs`): either every leg of a
group fills, or the guard cancels the live remainder, flattens the filled
exposure with reduce-only orders, and halts the pair scope until the books
are square. The whole guard is gated by `PAIRED_EXECUTION_GUARD_ENABLED`
(default `false`, fail-closed: live multi-leg signals are refused while the
flag is off). This runbook covers the operator's share of the lifecycle:
reading incidents, resolving `manual_intervention` groups at the venue, and
attesting the resolution so the halt clears.

## State model in sixty seconds

A multi-leg signal creates one **group**
(`assembling → armed → broken → compensating → completed`, with
`manual_intervention` as the operator hand-off state) and one **leg** per
instrument. Each leg tracks signed accounting:

- `filled_signed_qty` — cumulative original-order fill (buy `+`, sell `-`),
- `compensated_signed_qty` — cumulative reduce-only flatten fills, negated
    so it moves toward `filled_signed_qty`,
- `open_qty = filled_signed_qty - compensated_signed_qty` — the residual
    exposure the guard still owes a flatten. `open_qty > 0` means a net-long
    residual (flatten by SELLING `|open_qty|`); `open_qty < 0` means net-short
    (flatten by BUYING).

A failing group's scope — `(wallet, strategy, group_key)` where `group_key`
is the canonical sorted `exchange:instrument:mode` token set — carries one
durable halt row in `paired_execution_halts`. The halt blocks NEW grouped
signals on the same pair and is mirrored into each trade coordinator's
in-memory shard halts. The guard scanner interval is
`max(1.0, PAIRED_EXECUTION_ASSEMBLY_TIMEOUT_S / 2)`, which is 2.5
seconds with the default 5-second assembly timeout.

## The automatic lifecycle (no operator action)

Breaks (assembly timeout, fill timeout, a leg rejected/cancelled/expired
before filling) are handled end-to-end by the scanner: held commands are
cancelled, live originals are cancelled at the venue, filled exposure is
flattened with reduce-only market orders (re-flattening on late fills with a
fresh `compensation_seq` each round), and once every leg is settled at zero
open exposure the group COMPLETES, the scope's halt clears, and the pair may
trade again — typically within one scanner cycle of the last fill settling.
If you see a `broken` or `compensating` incident with automation still in
flight, the correct action is usually to wait one cycle.

A submit the executor refuses locally because its venue circuit breaker is
open counts as a leg rejection too: the order is rejected with reason
`circuit_breaker_open` and its command is durably FAILED, so it can never
fire late once the breaker closes. A flatten refused the same way counts as
that flatten's terminal — the leg reopens to `filled` and the sweep
re-flattens it next cycle, retrying until the breaker closes. Breakage does
not depend on live messages alone: the coordinator's reconciliation loop
folds durable venue events into command statuses, and the scanner projects
a durably-terminal original command onto its stuck leg, so a lost
reject/cancel message still breaks the group within a cycle.

## When the guard hands off to you: `manual_intervention`

The guard escalates a leg (and its group) to `manual_intervention` instead
of flattening when an automatic flatten would be unsafe:

- the venue/instrument has no reduce-only capability,
- the instrument spec is missing, or its lot size is missing/non-positive,
- the residual rounds below one lot or the venue minimum order size (dust),
- the instrument cannot be resolved (e.g. delisted mid-compensation).

A `manual_intervention` group stays HALTED every cycle by design until you
resolve and attest it. Clearing the halt row by hand does NOT help — the
scanner re-creates it on the next cycle while the group remains exposed.

## Triage: read the incident

```bash
curl -s -H "Authorization: Bearer $SNAPPER_PAT" \
    "$SNAPPER_BASE_URL/api/paired-execution/incidents" | jq
```

One incident per halted/exposed scope, carrying the durable halt (reason,
owning group), every exposed group with its status and `failure_reason`, and
per-leg exposure: `status`, `filled_signed_qty`, `compensated_signed_qty`,
`open_qty`, `compensation_seq`. Notes:

- `halt_missing: true` flags an exposed scope whose halt row is transiently
    absent (a crash/clear window). The scanner re-halts it within a cycle; it
    is surfaced so you never lose sight of exposure.
- A `broken` group with zero-fill legs (e.g. an assembly timeout) is NOT an
    incident — the guard deliberately leaves it free to re-assemble.
- Requires `READ_POSITIONS`; non-admin callers see only their wallets.

## Resolve at the venue

For each leg with `status = manual_intervention` and `open_qty != 0`:

1. Confirm the actual venue position for that instrument/wallet.
2. Close the residual at the venue (UI or API): SELL `|open_qty|` when
    `open_qty > 0`, BUY `|open_qty|` when `open_qty < 0`. Use reduce-only if
    the venue supports it.
3. Verify the venue position is flat (or back at the strategy's intended
    level).

Orders you place outside Snapper's order flow do NOT update
`compensated_signed_qty` — the leg's books will still show the residual.
That is expected: the leg keeps the true record of what THE GUARD did, and
your attestation (below) is the durable record that the remainder was
resolved manually.

## Attest: terminalize the group

```bash
curl -s -X POST -H "Authorization: Bearer $SNAPPER_PAT" \
    -H "X-CSRF-Token: $CSRF" \
    "$SNAPPER_BASE_URL/api/paired-execution/groups/$GROUP_ID/terminalize" | jq
```

Requires `MANAGE_PAIRED_EXECUTION` (OPERATOR or ADMIN). On success the group
becomes `completed` with `terminalized_by=<you>` stamped into its
`failure_reason`, legs keep their true accounting, and the scanner clears
the scope's durable halt and every coordinator's in-memory mirror within one
cycle — the pair can trade again. Responses:

- `404` — no such group in your accessible wallets.
- `409` — not attestable right now. The detail's `status` is freshly read.
    Causes: the group is not `manual_intervention` (nor a `compensating`
    group re-opened onto a manual leg); a sibling leg still has automation
    in flight (a working original or an unfinished flatten — wait a cycle
    and retry); the group has no legs; or a held original command still
    exists.

If a LATE original fill lands after your attestation, the guard re-opens
the group to `compensating`; automation re-flattens what it can, and if the
manual leg is the residual holder the group will refuse auto-completion
again — re-resolve at the venue if needed and POST terminalize again (the
re-attestation path accepts a `compensating` group with a manual leg).

## Known windows and boundaries

- **Reopen right after a clear**: a late fill landing immediately after a
    halt cleared leaves the scope exposed and un-halted for at most one
    scanner cycle; the next cycle re-halts. Self-healing, no action.
- **Missed not-tradeable reject**: a flatten rejected locally as
    not-tradeable (a delisted instrument) writes no durable venue event; if
    its live message is also lost, the leg stays `compensating` + halted,
    and the group is NOT attestable while it does (terminalize requires
    every non-manual leg settled, so POST terminalize returns 409). The
    executor's dispatched-command verification sweep normally heals this:
    the flatten's command row is still `dispatched` with no venue evidence,
    so on a venue that can look an order up by its client order id the
    sweep verifies it absent twice (no earlier than
    `max(120 s, 2 × dispatch TTL)` after creation), publishes the REJECTED
    status, and records the durable `order_rejected` event — the
    compensating sweep then settles the leg, and the residual re-flattens
    or escalates to `manual_intervention` under the normal sweep rules.
    The sweep does NOT auto-reject when the dispatch TTL is disabled (a
    frame may then legally be in flight at any age), when the command has
    aged past one hour (venue closed-order lookback makes absence
    non-authoritative — WARN-only escalation), or when the venue has no
    client-order-id lookup; in those cases the scope stays halted
    (fail-safe) until a durable venue event for the flatten appears or the
    rows are reconciled by hand.
- **Dispatch max-age TTL applies to paired commands too**: a paired
    `CREATED` command older than `TRADE_COMMAND_DISPATCH_TTL_S`
    (default 30 s) is CAS-expired by the outbox instead of dispatched —
    a group wedged pre-dispatch longer than the TTL breaks via the
    assembly/fill deadlines rather than firing stale legs.
- **Late fills older than 7 days**: startup recovery replays fills into
    groups completed within the last 7 days. Older late fills are
    reconciliation territory.
- **Reconciliation halts are separate**: a shard halted by the recon
    circuit breaker is NOT released by paired-execution machinery (and vice
    versa — the paired release is reason-scoped). A wedged recon halt
    currently requires a coordinator restart.
- **Do not hand-edit guard tables.** Clearing halts re-halts within a
    cycle while exposure remains; editing leg statuses falsifies the books
    the accounting relies on. The only supported operator mutation is the
    terminalize endpoint.

## Enablement

The guard ships dark. Enabling real-money multi-leg execution
(`PAIRED_EXECUTION_GUARD_ENABLED=true`) should follow the staged enablement
plan (staging trial, outbox-gate query plan check on the production
database, rollback criteria) maintained alongside the deployment docs.
