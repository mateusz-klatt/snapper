# Operations

## Durable-command cutover runbook (dual-write → durable)

Snapper ships with two command-dispatch modes:

- **Dual-write** (default) — `TradingEngineService._send_order`
  publishes directly to ZMQ and writes audit rows to DB in the same
  flow.
- **Durable** (outbox-driven) — the engine writes the `TradeCommand`
  row only; `OutboxDispatcher` picks up the row and publishes from the
  DB. Executor `VenueEvent` writes become fail-closed on accepted /
  fill paths.

Flipping from dual-write to durable is an operational cutover.
**It requires a coordinator restart** — the dispatch-mode wiring in
`TraderCoordinator._setup_trade_services()` reads
`use_durable_commands` only at startup, and
`TraderCoordinator._setup_divergence_detector()` reads
`enable_divergence_detector` at the same boundary.

Run both DB settings through the existing `SettingsService` API (or
the `snapper settings set` CLI); restart the coordinator after each
change.

### Pre-flight (T-0)

1. **Enable the observability detector.**

    ```text
    SET enable_divergence_detector = true
    ```

2. **Restart the coordinator.** The detector is wired at `start()`;
    without a restart the flag has no effect. Verify after restart:

    ```text
    grep "DivergenceDetector snapshot" logs/engine.log | tail -3
    ```

3. **Confirm counters populate in dual-write mode.** Durable counters
    stay at 0 — this is expected; it proves the wiring is live before
    you flip the durable flag.
    - `commands_created_total` increments on each engine order.
    - `commands_published_dual_write_total` increments on each
      successful ZMQ send.
    - `commands_published_durable_notified_total` = 0.
    - `commands_published_durable_dispatched_total` = 0.
    - `venue_events_observed_total` increments on each fill.

### T-0 flip to durable

1. `SET use_durable_commands = true`.
2. **Restart the coordinator.** Engines created after restart carry
    the outbox dispatcher reference.
3. Confirm on the next snapshot:
    - `commands_published_dual_write_total` stops incrementing (flat).
    - `commands_published_durable_notified_total` begins incrementing.
    - `commands_published_durable_dispatched_total` follows the
      notified counter with small lag (dispatcher-side actual ZMQ send).

### Observation window (T+0 to T+24h)

Concrete threshold formulas, evaluated over the full 24 h window.
Snapshot the counters at T+0 and T+24h and apply:

- **Created vs dispatched alignment:** at T+24h,
  `abs(commands_created_total - commands_published_durable_dispatched_total)
  / commands_created_total < 0.01` — fewer than 1 % of created
  commands unaccounted-for.
- **Notify vs dispatch alignment:** at T+24h,
  `abs(commands_published_durable_notified_total -
  commands_published_durable_dispatched_total)
  / commands_published_durable_notified_total < 0.005` — fewer than
  0.5 % of engine notifications fail to dispatch. This is the
  silent-ZMQ-loss detector.
- **Dual-write residual:** at T+24h,
  `commands_published_dual_write_total -
  commands_published_dual_write_total_at_T+1h < 100` — fewer than 100
  dual-write publishes after the flip. A spike indicates an engine
  that did not get restarted with the new mode.
- **Venue-event healthy ratio:** at T+24h,
  `venue_events_observed_total /
  commands_published_durable_dispatched_total >= 0.5` — at least half
  of dispatched commands produce a venue event. A lower ratio
  suggests silent delivery failure OR a normal high-cancel-rate
  strategy; operator interprets via the existing Order/Execution
  audit tables.
- **Reconciliation verdict ratio:** at T+24h,
  `reconciliation_failure_total /
  (reconciliation_ok_total + reconciliation_failure_total) < 0.02` —
  circuit-breaker trips below 2 % of cycle checks.

### GO/NO-GO at T+24h

**GO** — all five thresholds satisfied.
**NO-GO** — any threshold failing.

### Manual rollback (NO-GO path)

1. `SET use_durable_commands = false`.
2. **Restart the coordinator.** Engines created after restart revert
    to direct-publish dual-write behaviour.
3. Verify on the next snapshot: `commands_published_dual_write_total`
    resumes incrementing; durable counters go flat.

### Post-cutover hygiene

Once the flip is permanent (N days of clean durable operation) and
the observation data no longer carries diagnostic value, disable the
detector to remove periodic-log noise:

```text
SET enable_divergence_detector = false
```

Restart the coordinator for the change to take effect.

A follow-up plan will introduce automated rollback triggers based on
the thresholds above, once operational experience confirms their
validity.
