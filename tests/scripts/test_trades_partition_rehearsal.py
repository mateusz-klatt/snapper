"""Mutation-sensitive tests for the trades partition rehearsal proof harness."""

import inspect
from copy import deepcopy
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest

from scripts import trades_partition_rehearsal as rehearsal


def _horizon() -> datetime:
    """Return one stable UTC partition boundary for SQL and routing tests."""
    return datetime(2030, 1, 2, tzinfo=UTC)


def _attach_evidence() -> dict[str, object]:
    """Return the smallest exact successful ATTACH evidence object."""
    oids = [10, 20, 30, 40, 50, 60, 70]
    return {
        "attach_seconds": 0.05,
        "child_index_oids_before": oids,
        "child_index_oids_after": list(oids),
        "debug_implication_observed": True,
        "ddl_session_initial_timezone": "Europe/Warsaw",
        "ddl_transaction_timezone": "UTC",
        "h_boundary_routes": {
            "h_minus_1us": "trades_legacy",
            "h_exact": "trades_d20300102",
            "h_plus_1d": "trades_d20300103",
        },
        "index_inheritance_pairs": [list(pair) for pair in sorted(rehearsal._EXPECTED_INDEX_PAIRS)],
        "parent_unique_constraint_negative_control": {
            "child_index_oids_after_rollback": oids,
            "child_index_oids_before": list(oids),
            "child_index_oids_during_attach": [*oids, 80],
            "fixture_rows": 10_000_000,
            "new_child_index_count": 1,
            "proved_at_fixture_scale": True,
        },
        "implication_negative_controls": {
            "bare_date_attach": {
                "session_timezone": "Europe/Warsaw",
                "sqlstate": "23514",
            },
            "bare_date_parse": {
                "same_instant": False,
                "session_timezone": "Europe/Warsaw",
            },
            "missing_is_not_null": {
                "attach_seconds": 0.5,
                "debug_implication_observed": False,
                "scan_verification_observed": True,
            },
            "timezone_instant_mismatch": {
                "attach_seconds": 0.5,
                "check_instant": "2030-01-02T01:00:00+00:00",
                "check_literal": "TIMESTAMPTZ '2030-01-02 00:00:00-01:00'",
                "debug_implication_observed": False,
                "partition_bound_literal": "TIMESTAMPTZ '2030-01-02 00:00:00+00:00'",
                "scan_verification_observed": True,
            },
        },
    }


def _identity(scenario: str, expected: int) -> rehearsal.IdentityEvidence:
    """Return one successful bidirectional identity-multiset result."""
    return rehearsal.IdentityEvidence(
        scenario=scenario,
        expected=expected,
        missing=0,
        extra_or_changed=0,
        missing_samples=(),
        extra_samples=(),
    )


def _catalog_state(mode: str) -> rehearsal.CatalogState:
    """Return one coherent terminal catalog state for kill-result tests."""
    if mode == "PRE":
        return rehearsal.CatalogState(
            mode="PRE",
            trades_kind="r",
            staged_parent_kind="p",
            legacy_kind=None,
            topology_epoch="ORDINARY",
            legacy_attached=False,
            sequence_owned=True,
            index_pairs=(),
            legacy_bound=None,
        )
    return rehearsal.CatalogState(
        mode="POST",
        trades_kind="p",
        staged_parent_kind=None,
        legacy_kind="r",
        topology_epoch="PARTITIONED",
        legacy_attached=True,
        sequence_owned=True,
        index_pairs=tuple(sorted(rehearsal._EXPECTED_INDEX_PAIRS)),
        legacy_bound="FOR VALUES FROM (MINVALUE) TO ('2030-01-02 00:00:00+00')",
    )


def _scenario_result(name: str) -> rehearsal.ScenarioResult:
    """Return scenario-bound evidence for one successful generic kill row."""
    mode = "POST" if name == "after_commit" else "PRE"
    terminal = _catalog_state(mode)
    resolution = "POST_COMMITTED" if mode == "POST" else "PRE_ABORTED"
    queries = {
        "after_begin": "SELECT to_regclass('trades')",
        "after_commit": "SELECT to_regclass('trades')",
        "during_commit": "COMMIT",
        "orphan_timeout": "SELECT pg_sleep(30)",
    }
    events: dict[str, dict[str, object]] = {
        "after_begin": {"scenario": "after_begin"},
        "after_commit": {
            "attach_seconds": 0.05,
            "debug_messages": [rehearsal._DEBUG_IMPLICATION],
        },
        "during_commit": {"backend_pid": 4321},
        "orphan_timeout": {
            "application_name": "rehearsal-controller-g1-orphan_timeout",
            "backend_pid": 4321,
            "holder_pid": 5321,
            "holder_start_ticks": 123456,
            "socket_fd_held": 12,
        },
    }
    fault: dict[str, object] = {
        "backend_gone_at": "2030-01-02T00:00:01+00:00",
        "event": events[name],
        "early_resumes_before_timeout": 0,
        "holder_alive_after_controller_kill": name == "orphan_timeout",
        "holder_terminated_after_timeout": name == "orphan_timeout",
        "lease_expires_at": "2030-01-02T00:00:02+00:00",
        "observed_backend_pid": 4321,
        "observed_query": queries[name],
        "observed_state_before_kill": mode,
        "post_kill_backend_active": name == "orphan_timeout",
        "resolution": resolution,
        "resolver": "watchdog-a",
        "resume_count": 2,
        "stale_authority_denied": True,
        "stale_authority": {
            "actual_authorizer_attempted": True,
            "catalog_unchanged": True,
            "denied": True,
            "rejection": "controller generation is already terminal",
        },
        "terminal_catalog": mode,
        "terminal_fingerprint": terminal.fingerprint(),
        "timeout_log_observed": name == "orphan_timeout",
        "timeout_log_application": (
            "rehearsal-controller-g1-orphan_timeout" if name == "orphan_timeout" else ""
        ),
        "timeout_log_backend_pid": 4321 if name == "orphan_timeout" else 0,
        "winner_count": 1,
    }
    return rehearsal.ScenarioResult(
        name=name,
        terminal_state=terminal,
        controller_returncode=-rehearsal.signal.SIGKILL,
        resolution=resolution,
        resolver="watchdog-a",
        resume_count=2,
        winner_count=1,
        stable_after_resume=True,
        stale_authority_denied=True,
        identity=_identity(name, 2),
        surfaced_unique_violations=0,
        hidden_conflicts=0,
        fault_evidence=fault,
    )


def _metrics() -> rehearsal.ValidationMetrics:
    """Return one positive VALIDATE measurement."""
    return rehearsal.ValidationMetrics(
        fixture_rows=10_000_000,
        heap_bytes=2_000_000_000,
        seconds=10.0,
        raw_seconds=10.05,
        rows_per_second=1_000_000.0,
        bytes_per_second=200_000_000.0,
        production_rows_eta_seconds=377.0,
        production_heap_eta_seconds=(59 * 1024**3) / 200_000_000.0,
        conservative_eta_seconds=377.0,
        writer_commits_during=4,
        writer_overlap_backend_pid=4321,
        controlled_pause_seconds=0.05,
        active_recheck=True,
        cache_condition="warm post-load filesystem cache",
        set_not_null_seconds=0.01,
        set_not_null_scan_control_seconds=1.0,
        set_not_null_scan_skipped=True,
        implication_negative_controls={
            "bare_date_attach": {
                "session_timezone": "Europe/Warsaw",
                "sqlstate": "23514",
            },
            "bare_date_parse": {
                "same_instant": False,
                "session_timezone": "Europe/Warsaw",
            },
            "missing_is_not_null": {
                "attach_seconds": 0.5,
                "debug_implication_observed": False,
                "scan_verification_observed": True,
            },
            "timezone_instant_mismatch": {
                "attach_seconds": 0.5,
                "check_instant": "2030-01-02T01:00:00+00:00",
                "check_literal": "TIMESTAMPTZ '2030-01-02 00:00:00-01:00'",
                "debug_implication_observed": False,
                "partition_bound_literal": "TIMESTAMPTZ '2030-01-02 00:00:00+00:00'",
                "scan_verification_observed": True,
            },
        },
    )


def _writer_stats() -> rehearsal.WriterStats:
    """Return conflict-bridge evidence spanning both catalog modes."""
    return rehearsal.WriterStats(
        conflict_batches_exercised=3,
        clean_duplicates_classified=3,
        mismatched_twins_quarantined=3,
        conflict_modes={
            "ORDINARY_TARGETLESS",
            "PARTITIONED_TARGETLESS",
            "PARTITIONED_U3",
        },
    )


def _detach_evidence() -> dict[str, object]:
    """Return one successful bounded plain-DETACH result."""
    return {
        "archive_gate": {
            "archive_days": 56,
            "archive_manifest_rows": 10_000_100,
            "full_column_extra": 0,
            "full_column_missing": 0,
            "legacy_rows": 10_000_100,
            "paper_replay_extra": 0,
            "paper_replay_missing": 0,
            "restore_rows": 10_000_100,
        },
        "default_attached_before": True,
        "default_attached_after_plain_detach": True,
        "plain_detach_succeeded": True,
        "concurrent_detach_sqlstate": "55000",
        "failed_attempt_epoch_rolled_back": True,
        "first_plain_attempt_sqlstate": "55P03",
        "plain_detach_attempts": 2,
        "default_rows_before_detach": 0,
        "archive_gate_passed": True,
        "generation_serialization": {
            "latch_wait_event_type": "Lock",
            "new_generation": 1,
            "obligation_worklog_matches": True,
            "obligation_worklog_rows": 2,
            "old_row_physical_table": "trades_legacy",
            "post_generation_row_absent_from_live": True,
            "post_generation_row_staged_pending": True,
            "serialized_before_latch": True,
        },
        "legacy_drop_exercised": True,
        "pending_reconciliation": {
            "resolution": "POST_COMMITTED",
            "resume_count": 2,
            "stale_pending_rejection": "U3_PENDING fence is already terminal",
            "stale_pending_setter_denied": True,
            "terminal_catalog": "POST",
            "winner_count": 1,
        },
        "pending_state_before_detach": "PENDING",
        "retention_authorization": {
            "backend_pid": 1234,
            "cleanup_margin_seconds": 1.0,
            "initial_timezone": "Europe/Warsaw",
            "pinned_timezone": "UTC",
            "remaining_lease_seconds": 5.0,
            "transaction_timeout_seconds": 2.0,
        },
        "retention_first_authorization": {
            "backend_pid": 1234,
            "cleanup_margin_seconds": 1.0,
            "initial_timezone": "Europe/Warsaw",
            "pinned_timezone": "UTC",
            "remaining_lease_seconds": 5.0,
            "transaction_timeout_seconds": 2.0,
        },
        "retention_acknowledgements": [
            {
                "data_sessions_observed": 0,
                "inflight": 0,
                "retention_generation": 1,
                "session_closed": True,
            },
            {
                "data_sessions_observed": 0,
                "inflight": 0,
                "retention_generation": 1,
                "session_closed": True,
            },
        ],
        "retention_barrier": {
            "dispositioned_quarantine_rows": 2,
            "final_blockers": [],
            "final_snapshot": {
                "actual_pre_generation_pending": 0,
                "backscan_complete": True,
                "blocked_batches": 0,
                "expected_fence_acknowledgements": 2,
                "fence_acknowledgements": 2,
                "fixed_safe_cutoff": "2030-01-02T00:00:00+00:00",
                "horizon": "2030-01-02T00:00:00+00:00",
                "m1_cursor": "2030-01-02T00:00:00+00:00",
                "m2_cursor": "2030-01-02T00:00:00+00:00",
                "monitor_lag_breach": True,
                "pre_generation_obligations": 0,
                "remaining_lease_seconds": 5.0,
                "unresolved_quarantine": 0,
                "worklog_pending": 0,
                "wrong_generation_acknowledgements": 0,
            },
            "initial_blockers": [
                "backscan_complete",
                "blocked_batches",
                "fixed_safe_cutoff",
                "m1_cursor",
                "m2_cursor",
                "pre_generation_obligations",
                "unresolved_quarantine",
                "worklog_pending",
            ],
            "monitor_scans": {
                "m1_backscan_rows": 10,
                "m1_rows_seen": 100,
                "m2_backscan_rows": 10,
                "m2_rows_seen": 100,
            },
            "negative_rejection": "retention barrier blocked DETACH",
            "obligation_drain": {
                "blocked_batch_physical_table": "trades_legacy",
                "drained_blocked_batches": 1,
                "drained_monitor_worklog": 1,
                "settled_live_obligations": 1,
            },
            "passed": True,
        },
        "retention_epoch_after_commit": "U3",
        "retention_resumes": 2,
        "terminal_catalog_after_drop": "PURGED",
        "staged_replay": {
            "abort_left_pending_without_row_or_worklog": True,
            "first_applications": [True],
            "full_row_extra": 0,
            "full_row_missing": 0,
            "idempotent_reruns": [False],
            "row_and_worklog_same_transaction": True,
            "trade_rows": 1,
            "worklog_rows": 1,
        },
    }


def _acceptance_inputs() -> rehearsal.AcceptanceInputs:
    """Return inputs that satisfy all six mechanical acceptance criteria."""
    return rehearsal.AcceptanceInputs(
        attach=_attach_evidence(),
        identities=[
            _identity("fixture", 10_000_000),
            _identity("all_writers", 10),
        ],
        writer_stats=[_writer_stats()],
        scenario_results=[],
        metrics=_metrics(),
        detach=_detach_evidence(),
        horizon=_horizon(),
        configured_rows=10_000_000,
    )


def _mutate_detach(path: tuple[str, ...], value: object) -> dict[str, object]:
    """Return successful DETACH evidence with one nested fact replaced."""
    result = deepcopy(_detach_evidence())
    target = result
    for key in path[:-1]:
        target = cast(dict[str, object], target[key])
    target[path[-1]] = value
    return result


def test_horizon_literal_requires_an_explicit_utc_offset() -> None:
    """The cutover horizon is never rendered as an ambiguous timestamp.

    Given: One UTC-aware horizon and the same wall time without timezone data,
    When: The harness builds the SQL literals used by CHECK and partition DDL,
    Then: The UTC value carries +00:00 and the naive value is rejected.
    """
    literal = rehearsal._horizon_literal(_horizon())

    assert literal == "TIMESTAMPTZ '2030-01-02 00:00:00+00:00'"
    with pytest.raises(rehearsal.RehearsalError, match="timezone-naive"):
        rehearsal._horizon_literal(datetime(2030, 1, 2))


def test_bound_comparison_uses_the_instant_not_rendered_timezone() -> None:
    """Catalog bounds are compared as instants instead of rendered wall times.

    Given: Equivalent and different partition bounds rendered with varied offsets,
    When: Recovery compares each catalog expression with the approved UTC horizon,
    Then: Only expressions representing the identical instant are accepted.
    """
    cases: tuple[tuple[str, bool], ...] = (
        ("FOR VALUES FROM (MINVALUE) TO ('2030-01-02 01:00:00+01')", True),
        (
            (
                "FOR VALUES FROM (MINVALUE) TO "
                "('2030-01-02 01:00:00+01'::timestamp with time zone)"
            ),
            True,
        ),
        ("FOR VALUES FROM (MINVALUE) TO ('2030-01-02 00:00:01+00')", False),
        ("DEFAULT", False),
    )

    for bound, expected in cases:
        assert rehearsal._bound_matches_horizon(bound, _horizon()) is expected


def test_real_schema_and_index_manifest_are_pinned() -> None:
    """The scaled fixture retains the production trades catalog shape.

    Given: The table and index builders used to create every scenario fixture,
    When: Their generated DDL is compared with the approved schema manifest,
    Then: All twelve columns, the sequence check, and exactly seven indexes remain.
    """
    table_sql = rehearsal._base_table_sql()
    index_sql = "\n".join(rehearsal._base_index_sql())

    for column in (
        "id bigint",
        "public_id uuid",
        "instrument_public_id uuid",
        "trade_id character varying(64)",
        "price double precision",
        "size double precision",
        "side character varying(4)",
        "executed_at timestamp with time zone",
        "session_id uuid",
        "sequence_id integer",
        '"timestamp" timestamp with time zone',
        "known_to timestamp with time zone",
    ):
        assert column in table_sql
    assert "ck_trades_sequence_id CHECK (sequence_id > 0)" in table_sql
    assert "uq_trade_instr_tid_exec" in index_sql
    assert len(rehearsal._EXPECTED_LEGACY_INDEXES) == 7


def test_parent_arbiter_is_standalone_and_parent_is_minimal() -> None:
    """The staged parent uses only the approved attach-compatible indexes.

    Given: The partitioned parent table and index DDL produced by the harness,
    When: The arbiter and forbidden parent constraints are inspected,
    Then: U3 is standalone and neither a primary key nor the legacy U2 exists.
    """
    parent_sql = rehearsal._parent_table_sql()
    index_sql = "\n".join(rehearsal._parent_index_sql())

    assert "PARTITION BY RANGE (executed_at)" in parent_sql
    assert "PRIMARY KEY" not in parent_sql
    assert "uq_trade_instrument_trade_id" not in parent_sql
    assert "CREATE UNIQUE INDEX trades_p_uq_instr_tid_exec" in index_sql
    assert len(rehearsal._EXPECTED_PARENT_INDEXES) == 4


def test_range_constraint_contains_both_implication_conjuncts() -> None:
    """The legacy range proof contains every fact needed to skip ATTACH scanning.

    Given: An aware UTC horizon used for the separately committed CHECK constraint,
    When: The harness renders the NOT VALID range constraint,
    Then: It includes IS NOT NULL, the explicit-offset upper bound, and NOT VALID.
    """
    sql = rehearsal._range_constraint_sql(_horizon())

    assert "executed_at IS NOT NULL" in sql
    assert "executed_at < TIMESTAMPTZ '2030-01-02 00:00:00+00:00'" in sql
    assert "NOT VALID" in sql


def test_writer_insert_is_targetless_and_observable() -> None:
    """Concurrent writes preserve the production targetless conflict contract.

    Given: A synthetic publisher batch containing two stable row identities,
    When: The harness constructs the SQL executed through the cutover,
    Then: Conflict handling has no target and every accepted identity is returned.
    """
    sql = rehearsal._insert_sql(2)

    assert "ON CONFLICT DO NOTHING" in sql
    assert "ON CONFLICT (" not in sql
    assert "RETURNING public_id::text" in sql
    assert "$22" in sql


def test_conflict_lookup_uses_the_real_partial_public_id_index() -> None:
    """Conflict verification preserves the predicate required by the real index.

    Given: The production-shaped partial unique index on current public identities,
    When: Both conflict-classification lookups are inspected,
    Then: Each lookup supplies the matching known_to predicate and bound value.
    """
    source = inspect.getsource(rehearsal._exercise_conflict_batch)

    assert source.count("AND known_to = $2") == 2
    assert source.count("_KNOWN_TO") == 2


@pytest.mark.asyncio
async def test_fence_readiness_waits_for_the_initial_conflict_proof() -> None:
    """A committed batch alone cannot open the publisher fence.

    Given: A publisher with enough committed rows but no completed conflict proof,
    When: Fence readiness is checked before and after that proof completes,
    Then: The first check fails and the second check becomes ready.
    """
    ready = rehearsal.WriterStats(committed=4, conflict_batches_exercised=1)
    not_ready = rehearsal.WriterStats(committed=4)

    with pytest.raises(rehearsal.RehearsalError, match="finish the conflict proof"):
        await rehearsal._wait_writer_fence_ready([ready, not_ready], 4, timeout=0.01)

    not_ready.conflict_batches_exercised = 1
    await rehearsal._wait_writer_fence_ready([ready, not_ready], 4, timeout=0.01)


def test_ack_barrier_rejects_terminal_and_short_lease_windows() -> None:
    """Acknowledgements cannot authorize an expired or under-sized lease.

    Given: The required transaction timeout and backend cleanup interval,
    When: Live, terminal, and too-short database-clock windows are checked,
    Then: Only the live window with strict positive margin is accepted.
    """
    rehearsal._require_live_fence_window(None, 4.001, 4.0)

    with pytest.raises(rehearsal.RehearsalError, match="already terminal"):
        rehearsal._require_live_fence_window("PRE_ABORTED", 5.0, 4.0)
    with pytest.raises(rehearsal.RehearsalError, match="not more than the required"):
        rehearsal._require_live_fence_window(None, 4.0, 4.0)


def test_every_ack_barrier_receives_the_full_required_lease_window() -> None:
    """Every fence caller preserves both timeout and cleanup components.

    Given: All five controller and retention flows that wait for publisher acknowledgements,
    When: Their call sites and the acknowledgement loop are inspected,
    Then: Each supplies timeout plus cleanup and the loop enforces the live-window predicate.
    """
    callers = "\n".join(
        inspect.getsource(function)
        for function in (
            rehearsal._run_normal_cutover,
            rehearsal._run_kill_scenario,
            rehearsal._ensure_post,
            rehearsal._prove_pending_reconciliation,
            rehearsal._prepare_retention_detach,
        )
    )
    barrier = inspect.getsource(rehearsal._wait_fence_acks)

    assert callers.count("config.transaction_timeout_seconds + config.cleanup_margin_seconds") == 5
    assert "_require_live_fence_window(" in barrier


@pytest.mark.asyncio
async def test_event_waiter_preserves_an_early_child_failure(tmp_path: Path) -> None:
    """A controller crash is reported directly instead of becoming an event timeout.

    Given: A child that writes a diagnostic sentinel and exits before its event,
    When: The process-aware durable-event waiter observes the missing event,
    Then: It fails promptly with the event, return code, and retained stderr.
    """
    process = await rehearsal.asyncio.create_subprocess_exec(
        rehearsal.sys.executable,
        "-c",
        "import sys; sys.stderr.write('controller-sentinel\\n'); raise SystemExit(7)",
        stdout=rehearsal.asyncio.subprocess.PIPE,
        stderr=rehearsal.asyncio.subprocess.PIPE,
    )
    try:
        await rehearsal.asyncio.wait_for(process.wait(), timeout=0.5)
        with pytest.raises(rehearsal.RehearsalError) as raised:
            await rehearsal.asyncio.wait_for(
                rehearsal._wait_event(
                    tmp_path / "missing-event.json",
                    timeout=10.0,
                    process=process,
                ),
                timeout=0.5,
            )
    finally:
        if process.returncode is None:
            process.kill()
            await rehearsal.asyncio.wait_for(process.wait(), timeout=0.5)

    message = str(raised.value)
    assert "missing-event.json" in message
    assert "returncode=7" in message
    assert "controller-sentinel" in message
    assert "controller event timeout" not in message


@pytest.mark.asyncio
async def test_durable_event_precedes_an_already_exited_child(tmp_path: Path) -> None:
    """A durably published kill point wins the race against later child exit.

    Given: An atomic event file and a child that has already exited nonzero,
    When: The process-aware event waiter observes both terminal facts,
    Then: It returns the durable event so independent backend checks decide the proof.
    """
    event_path = tmp_path / "published-event.json"
    rehearsal._write_event(event_path, {"backend_pid": 4321, "phase": "published"})
    process = await rehearsal.asyncio.create_subprocess_exec(
        rehearsal.sys.executable,
        "-c",
        "raise SystemExit(7)",
        stdout=rehearsal.asyncio.subprocess.PIPE,
        stderr=rehearsal.asyncio.subprocess.PIPE,
    )
    try:
        await rehearsal.asyncio.wait_for(process.wait(), timeout=0.5)
        event = await rehearsal._wait_event(event_path, timeout=0.0, process=process)
    finally:
        if process.returncode is None:
            process.kill()
            await rehearsal.asyncio.wait_for(process.wait(), timeout=0.5)

    assert event == {"backend_pid": 4321, "phase": "published"}


@pytest.mark.asyncio
async def test_event_waiter_rechecks_child_exit_after_deadline(tmp_path: Path) -> None:
    """A deadline-edge child failure retains its output before timeout reporting.

    Given: An exited diagnostic child and a zero-duration missing-event deadline,
    When: The final event and process checks run after the polling loop,
    Then: The child failure is retained instead of being mislabeled as a timeout.
    """
    process = await rehearsal.asyncio.create_subprocess_exec(
        rehearsal.sys.executable,
        "-c",
        "import sys; sys.stderr.write('deadline-sentinel\\n'); raise SystemExit(8)",
        stdout=rehearsal.asyncio.subprocess.PIPE,
        stderr=rehearsal.asyncio.subprocess.PIPE,
    )
    try:
        await rehearsal.asyncio.wait_for(process.wait(), timeout=0.5)
        with pytest.raises(rehearsal.RehearsalError) as raised:
            await rehearsal._wait_event(
                tmp_path / "deadline-event.json",
                timeout=0.0,
                process=process,
            )
    finally:
        if process.returncode is None:
            process.kill()
            await rehearsal.asyncio.wait_for(process.wait(), timeout=0.5)

    message = str(raised.value)
    assert "returncode=8" in message
    assert "deadline-sentinel" in message
    assert "controller event timeout" not in message


def test_writer_rows_cross_h_only_after_partitioned_mode(tmp_path: Path) -> None:
    """Writer timestamps exercise both sides of H without invalid legacy inserts.

    Given: One publisher generating the same stable identities in both catalog modes,
    When: Ordinary and partitioned batches are constructed around the horizon,
    Then: Ordinary timestamps precede H and partitioned timestamps are at or after H.
    """
    stats = rehearsal.WriterStats()
    context = rehearsal.WriterContext(
        instance=rehearsal.InstanceRef(
            pgdata=tmp_path / "reports" / "pgdata",
            socket_dir=Path("/tmp/snapper-trades-rehearsal-sock-test"),
            port=55439,
            user="tester",
            postmaster_pid=999,
            source_snapshot=rehearsal._capture_source_snapshot(),
        ),
        publisher_id="publisher-1",
        scenario="routing",
        horizon=_horizon(),
        writer_count=2,
        writer_rate=80.0,
        ledger_path=tmp_path / "identities.jsonl",
        stop_event=rehearsal.asyncio.Event(),
        stats=stats,
    )

    ordinary = rehearsal._writer_rows(context, "ORDINARY_TARGETLESS", 1, 4)
    partitioned = rehearsal._writer_rows(context, "PARTITIONED_TARGETLESS", 1, 4)

    assert all(row.identity.executed_at < context.horizon for row in ordinary)
    assert all(row.identity.executed_at >= context.horizon for row in partitioned)
    assert {row.identity.public_id for row in ordinary} == {
        row.identity.public_id for row in partitioned
    }


def test_catalog_fingerprint_changes_for_every_deciding_fact() -> None:
    """Recovery fingerprints cover every catalog fact used to authorize resumption.

    Given: One coherent PRE state and mutations of each deciding catalog field,
    When: A fingerprint is computed for the original and every mutation,
    Then: No altered topology can retain the original recovery fingerprint.
    """
    state = rehearsal.CatalogState(
        mode="PRE",
        trades_kind="r",
        staged_parent_kind="p",
        legacy_kind=None,
        topology_epoch="ORDINARY",
        legacy_attached=False,
        sequence_owned=True,
        index_pairs=(),
        legacy_bound=None,
    )
    mutations = (
        replace(state, mode="POST"),
        replace(state, trades_kind="p"),
        replace(state, staged_parent_kind=None),
        replace(state, legacy_kind="r"),
        replace(state, topology_epoch="PARTITIONED"),
        replace(state, legacy_attached=True),
        replace(state, sequence_owned=False),
        replace(state, index_pairs=(("child", "parent"),)),
        replace(state, legacy_bound="bound"),
    )

    assert all(mutation.fingerprint() != state.fingerprint() for mutation in mutations)


def test_each_acceptance_criterion_has_a_failing_mutation() -> None:
    """Every advertised acceptance criterion has a mechanically failing branch.

    Given: A complete green evidence set for all six approved criteria,
    When: One decisive fact for each criterion is independently made false,
    Then: The matching criterion fails instead of preserving an unconditional PASS.
    """
    inputs = _acceptance_inputs()
    assert [result.passed for result in rehearsal._criteria(inputs)] == [True] * 6

    slow_attach = dict(inputs.attach)
    slow_attach["attach_seconds"] = 1.0
    lost_identity = replace(inputs.identities[0], missing=1)
    lost_identities = [lost_identity, *inputs.identities[1:]]
    unique_stats = [
        replace(inputs.writer_stats[0], surfaced_unique_violations=1),
        *inputs.writer_stats[1:],
    ]
    zero_metrics = replace(inputs.metrics, seconds=0.0)
    no_retry = dict(inputs.detach)
    no_retry["first_plain_attempt_sqlstate"] = ""
    wrong_timezone = dict(inputs.attach)
    wrong_timezone["ddl_transaction_timezone"] = "Europe/Warsaw"

    mutations = (
        replace(inputs, attach=slow_attach),
        replace(inputs, identities=lost_identities),
        replace(inputs, writer_stats=unique_stats),
        replace(inputs, metrics=zero_metrics),
        replace(inputs, detach=no_retry),
        replace(inputs, attach=wrong_timezone),
    )
    for number, mutation in enumerate(mutations):
        results = rehearsal._criteria(mutation)
        assert results[number].passed is False


def test_identity_oracles_use_bidirectional_multiset_differences() -> None:
    """The loss proof cannot collapse identities into a scalar row total.

    Given: Both terminal identity oracle implementations used by criterion two,
    When: Their executable source and advertised operator are inspected,
    Then: Each direction uses EXCEPT ALL and the criterion names that multiset proof.
    """
    writer_source = inspect.getsource(rehearsal._identity_multiset)
    fixture_source = inspect.getsource(rehearsal._fixture_identity_multiset)
    criterion = rehearsal._criteria(_acceptance_inputs())[1]

    assert writer_source.count("EXCEPT ALL") >= 4
    assert fixture_source.count("EXCEPT ALL") >= 4
    assert criterion.evidence["comparison_operator"] == "EXCEPT ALL in both directions"


@pytest.mark.parametrize(
    "scenario",
    ("after_begin", "during_commit", "after_commit", "orphan_timeout"),
)
def test_generic_kill_rows_are_bound_to_observed_faults(scenario: str) -> None:
    """A kill row fails when its scenario-specific observation is corrupted.

    Given: One green controller SIGKILL result tied to its backend query and event,
    When: The observed query, terminal fingerprint, and scenario event are mutated,
    Then: Every corrupted result fails the same mechanical kill-result predicate.
    """
    result = _scenario_result(scenario)
    assert rehearsal._scenario_kill_result(result).passed is True

    empty_query = dict(result.fault_evidence)
    empty_query["observed_query"] = ""
    wrong_fingerprint = dict(result.fault_evidence)
    wrong_fingerprint["terminal_fingerprint"] = "wrong"
    wrong_authorizer = deepcopy(result.fault_evidence)
    stale = cast(dict[str, object], wrong_authorizer["stale_authority"])
    stale["actual_authorizer_attempted"] = False
    wrong_scenario_fact = dict(result.fault_evidence)
    if scenario == "orphan_timeout":
        wrong_scenario_fact["timeout_log_observed"] = False
    else:
        wrong_scenario_fact["event"] = {}

    for fault in (
        empty_query,
        wrong_fingerprint,
        wrong_authorizer,
        wrong_scenario_fact,
    ):
        assert (
            rehearsal._scenario_kill_result(replace(result, fault_evidence=fault)).passed is False
        )


def test_validate_kill_row_requires_the_same_frozen_active_backend() -> None:
    """The VALIDATE kill cannot pass from a stale pg_stat_activity sample.

    Given: A green kill record bound to one stopped VALIDATE PID and its retry,
    When: The frozen state, final PID, overlap commit, or constraint state changes,
    Then: The specialized kill predicate rejects every corrupted observation.
    """
    evidence: dict[str, object] = {
        "constraint_after_kill": "NOT VALID",
        "constraint_after_retry": "VALID",
        "controller_killed_while_backend_stopped": True,
        "controller_returncode": -rehearsal.signal.SIGKILL,
        "final_active_backend_pid": 4321,
        "final_active_recheck": True,
        "observed_backend_pid": 4321,
        "observed_backend_process_state": "T",
        "observed_query": ("ALTER TABLE trades VALIDATE CONSTRAINT ck_trades_legacy_range"),
        "writer_commits_during_active_validate": 1,
    }
    identity = _identity("validate_kill", 2)
    assert rehearsal._validate_kill_passed(evidence, identity) is True

    mutations = (
        ("observed_backend_process_state", "R"),
        ("final_active_backend_pid", 4322),
        ("writer_commits_during_active_validate", 0),
        ("constraint_after_kill", "VALID"),
        ("controller_killed_while_backend_stopped", False),
    )
    for field_name, value in mutations:
        corrupted = dict(evidence)
        corrupted[field_name] = value
        assert rehearsal._validate_kill_passed(corrupted, identity) is False


def test_writer_drop_row_requires_page_resume_and_real_stale_authorizer() -> None:
    """The ACTIVE-fence writer kill is bound to every contemporaneous action.

    Given: A green writer SIGKILL record with page, resume, and real authorizer denial,
    When: Each decisive page, timing, marker, or authorizer fact is corrupted,
    Then: The specialized kill predicate reports failure.
    """
    evidence: dict[str, object] = {
        "active_event": {"generation": 7, "held_obligations": 1},
        "backend_gone_at": "2030-01-02T00:00:01+00:00",
        "controller_returncode": -rehearsal.signal.SIGKILL,
        "durable_page_at": "2030-01-02T00:00:01.1+00:00",
        "durable_page_reason": "publisher died with held obligations",
        "fence_aborted": True,
        "fence_generation": 7,
        "ingestion_resumed_at": "2030-01-02T00:00:01.2+00:00",
        "lease_expires_at": "2030-01-02T00:00:02+00:00",
        "marker_prearmed": True,
        "observed_backend_pid": 4321,
        "stale_authority_denied": True,
        "stale_authority": {
            "actual_authorizer_attempted": True,
            "catalog_unchanged": True,
            "denied": True,
            "rejection": "controller generation is already terminal",
        },
    }
    identity = _identity("writer_drop_resume", 1)
    stats = _writer_stats()
    assert rehearsal._writer_drop_kill_passed(evidence, identity, stats) is True

    mutations = (
        ("marker_prearmed", False),
        ("durable_page_reason", "missing"),
        ("ingestion_resumed_at", "2030-01-02T00:00:03+00:00"),
        ("stale_authority_denied", False),
    )
    for field_name, value in mutations:
        corrupted = deepcopy(evidence)
        corrupted[field_name] = value
        assert rehearsal._writer_drop_kill_passed(corrupted, identity, stats) is False

    stale_corrupted = deepcopy(evidence)
    stale = cast(dict[str, object], stale_corrupted["stale_authority"])
    stale["actual_authorizer_attempted"] = False
    assert rehearsal._writer_drop_kill_passed(stale_corrupted, identity, stats) is False


def test_acceptance_supporting_controls_are_also_falsifiable() -> None:
    """Supporting controls cannot remain green when their evidence is corrupted.

    Given: Complete successful evidence for scale, controls, archive, and timezone,
    When: Each supporting proof fact is independently replaced with a false value,
    Then: Its owning acceptance criterion reports FAIL.
    """
    inputs = _acceptance_inputs()
    undersized = replace(inputs, configured_rows=5_600)
    assert rehearsal._criteria(undersized)[3].passed is False

    no_global_writer_scope = replace(inputs, identities=[inputs.identities[0]])
    assert rehearsal._criteria(no_global_writer_scope)[1].passed is False

    no_archive = dict(inputs.detach)
    no_archive["archive_gate_passed"] = False
    assert rehearsal._criteria(replace(inputs, detach=no_archive))[4].passed is False

    no_negative_scan = dict(inputs.attach)
    controls = dict(
        cast(
            dict[str, object],
            no_negative_scan["implication_negative_controls"],
        )
    )
    missing_control = dict(cast(dict[str, object], controls["missing_is_not_null"]))
    missing_control["debug_implication_observed"] = True
    controls["missing_is_not_null"] = missing_control
    no_negative_scan["implication_negative_controls"] = controls
    assert rehearsal._criteria(replace(inputs, attach=no_negative_scan))[0].passed is False

    no_parent_index = dict(inputs.attach)
    parent_control = dict(
        cast(
            dict[str, object],
            no_parent_index["parent_unique_constraint_negative_control"],
        )
    )
    parent_control["new_child_index_count"] = 0
    no_parent_index["parent_unique_constraint_negative_control"] = parent_control
    assert rehearsal._criteria(replace(inputs, attach=no_parent_index))[0].passed is False

    no_conflict_bridge = [replace(inputs.writer_stats[0], conflict_batches_exercised=0)]
    assert rehearsal._criteria(replace(inputs, writer_stats=no_conflict_bridge))[2].passed is False

    no_validate_writes = replace(inputs.metrics, writer_commits_during=0)
    assert rehearsal._criteria(replace(inputs, metrics=no_validate_writes))[3].passed is False

    false_eta = replace(inputs.metrics, production_rows_eta_seconds=1.0)
    assert rehearsal._criteria(replace(inputs, metrics=false_eta))[3].passed is False

    nonempty_default = dict(inputs.detach)
    nonempty_default["default_rows_before_detach"] = 1
    assert rehearsal._criteria(replace(inputs, detach=nonempty_default))[4].passed is False

    wrong_bare_attach = dict(inputs.attach)
    wrong_controls = dict(
        cast(
            dict[str, object],
            wrong_bare_attach["implication_negative_controls"],
        )
    )
    wrong_bare = dict(cast(dict[str, object], wrong_controls["bare_date_attach"]))
    wrong_bare["sqlstate"] = "00000"
    wrong_controls["bare_date_attach"] = wrong_bare
    wrong_bare_attach["implication_negative_controls"] = wrong_controls
    assert rehearsal._criteria(replace(inputs, attach=wrong_bare_attach))[5].passed is False


def test_binding_v16_retention_closures_cannot_be_omitted() -> None:
    """The pass gate requires every binding day-30 and staged-replay closure.

    Given: Green retention evidence covering epoch atomicity, generation, and replay,
    When: Each v16 closure is independently removed from that evidence,
    Then: The plain-DETACH acceptance criterion fails.
    """
    inputs = _acceptance_inputs()
    mutations: tuple[tuple[tuple[str, ...], object], ...] = (
        (("default_attached_after_plain_detach",), False),
        (("failed_attempt_epoch_rolled_back",), False),
        (("concurrent_detach_sqlstate",), "00000"),
        (("retention_epoch_after_commit",), "PARTITIONED"),
        (("terminal_catalog_after_drop",), "INCOHERENT"),
        (("retention_resumes",), 1),
        (("retention_acknowledgements",), []),
        (("pending_state_before_detach",), "POST"),
        (("pending_reconciliation", "winner_count"), 0),
        (("pending_reconciliation", "terminal_catalog"), "PENDING"),
        (("pending_reconciliation", "stale_pending_setter_denied"), False),
        (("retention_authorization", "remaining_lease_seconds"), 2.5),
        (("retention_authorization", "initial_timezone"), "UTC"),
        (("retention_first_authorization", "remaining_lease_seconds"), 2.5),
        (("retention_barrier", "passed"), False),
        (("retention_barrier", "dispositioned_quarantine_rows"), 0),
        (("retention_barrier", "final_blockers"), ["quarantine"]),
        (("retention_barrier", "negative_rejection"), ""),
        (("retention_barrier", "obligation_drain", "drained_blocked_batches"), 0),
        (("retention_barrier", "monitor_scans", "m1_backscan_rows"), 0),
        (("retention_barrier", "final_snapshot", "unresolved_quarantine"), 1),
        (("retention_barrier", "final_snapshot", "monitor_lag_breach"), False),
        (
            ("retention_barrier", "final_snapshot", "wrong_generation_acknowledgements"),
            1,
        ),
        (("generation_serialization", "serialized_before_latch"), False),
        (("generation_serialization", "new_generation"), 2),
        (("generation_serialization", "old_row_physical_table"), "trades_default"),
        (("generation_serialization", "post_generation_row_absent_from_live"), False),
        (("generation_serialization", "post_generation_row_staged_pending"), False),
        (("generation_serialization", "obligation_worklog_matches"), False),
        (("generation_serialization", "obligation_worklog_rows"), 1),
        (("generation_serialization", "latch_wait_event_type"), "Client"),
        (("archive_gate", "archive_days"), 55),
        (("archive_gate", "archive_manifest_rows"), 1),
        (("archive_gate", "full_column_missing"), 1),
        (("archive_gate", "full_column_extra"), 1),
        (("archive_gate", "paper_replay_missing"), 1),
        (("archive_gate", "paper_replay_extra"), 1),
        (("staged_replay", "abort_left_pending_without_row_or_worklog"), False),
        (("staged_replay", "row_and_worklog_same_transaction"), False),
        (("staged_replay", "first_applications"), [True, False]),
        (("staged_replay", "idempotent_reruns"), [False, True]),
        (("staged_replay", "worklog_rows"), 2),
        (("staged_replay", "trade_rows"), 2),
        (("staged_replay", "full_row_missing"), 1),
        (("staged_replay", "full_row_extra"), 1),
    )

    for path, value in mutations:
        mutated = replace(inputs, detach=_mutate_detach(path, value))
        assert rehearsal._criteria(mutated)[4].passed is False


def test_retention_barrier_has_a_real_rejecting_state() -> None:
    """The day-30 barrier fails before disposition and passes only after closure.

    Given: A snapshot with unresolved quarantine, blocked work, and missing cutoff proof,
    When: The same rejecting function evaluates it before and after every field is closed,
    Then: The first evaluation raises and the complete positive snapshot returns normally.
    """
    initial = cast(
        dict[str, object],
        cast(dict[str, object], _detach_evidence()["retention_barrier"])["final_snapshot"],
    )
    blocked = dict(initial)
    blocked["blocked_batches"] = 1
    blocked["unresolved_quarantine"] = 2
    blocked["fixed_safe_cutoff"] = None
    blocked["backscan_complete"] = False
    blocked["monitor_lag_breach"] = True

    with pytest.raises(rehearsal.RehearsalError, match="retention barrier blocked DETACH"):
        rehearsal._require_retention_barrier(blocked)

    initial["monitor_lag_breach"] = True
    rehearsal._require_retention_barrier(initial)


def test_u3_epoch_and_writer_drop_protocol_are_present_in_harness_schema() -> None:
    """The generated control and writer SQL includes the binding v16 states.

    Given: The harness control schema and both post-cutover writer modes,
    When: Their DDL and conflict clauses are inspected,
    Then: U3, staged worklog, durable pages, targetless, and explicit U3 are present.
    """
    schema = rehearsal._control_schema_sql()
    targetless = rehearsal._insert_sql(1, "PARTITIONED_TARGETLESS")
    explicit_u3 = rehearsal._insert_sql(1, "PARTITIONED_U3")

    assert "'U3'" in schema
    assert "'U3_PENDING'" in schema
    assert "rehearsal.retention_blocked_batches" in schema
    assert "rehearsal.retention_monitor_worklog" in schema
    assert "rehearsal.retention_monitor_scans" in schema
    assert "rehearsal.retention_resolution_winners" in schema
    assert "rehearsal.staged_replay" in schema
    assert "rehearsal.obligation_worklog" in schema
    assert "rehearsal.replay_worklog" in schema
    assert "rehearsal.publisher_runtime" in schema
    assert "rehearsal.fence_pages" in schema
    assert "ON CONFLICT DO NOTHING" in targetless
    assert "ON CONFLICT (instrument_public_id, trade_id, executed_at) DO NOTHING" in explicit_u3
    assert "writer during ACTIVE fence abort/page" in rehearsal._REQUIRED_KILLS


def test_rehearsal_evidence_refuses_missing_results() -> None:
    """The overall result cannot pass on absent or failed proof records.

    Given: Empty evidence, then six green criteria and six green kill results,
    When: Completeness is checked and one kill result is subsequently failed,
    Then: Only the complete all-green intermediate state reports PASS.
    """
    evidence = rehearsal.RehearsalEvidence()
    assert evidence.passed() is False

    evidence.criteria = [
        rehearsal.CriterionResult(number, f"criterion-{number}", True, {}) for number in range(1, 7)
    ]
    evidence.kills = [
        rehearsal.KillResult(name, True, {})
        for name in (
            "during VALIDATE",
            "after DDL BEGIN before COMMIT",
            "during COMMIT (ambiguous)",
            "after COMMIT before resume",
            "orphan transaction_timeout before lease expiry",
            "writer during ACTIVE fence abort/page",
        )
    ]
    assert evidence.passed() is True

    evidence.kills[2] = rehearsal.KillResult("during COMMIT (ambiguous)", False, {})
    assert evidence.passed() is False

    evidence.kills[2] = evidence.kills[1]
    assert evidence.passed() is False


def test_cleanup_guard_rejects_broad_or_misnamed_targets(tmp_path: Path) -> None:
    """Instance cleanup is restricted to the allocated throwaway directory.

    Given: An exact instance child alongside broader and incorrectly rooted paths,
    When: Each candidate is checked before lifecycle cleanup,
    Then: Only the named child of the allocated run directory is accepted.
    """
    parent = tmp_path / "run"
    parent.mkdir()
    exact = parent / "instance"
    exact.mkdir()

    rehearsal._validate_allocated_path(exact, parent, "instance")
    with pytest.raises(rehearsal.RehearsalError, match="unexpected cleanup"):
        rehearsal._validate_allocated_path(parent, tmp_path, "instance")
    with pytest.raises(rehearsal.RehearsalError, match="broad cleanup"):
        rehearsal._validate_allocated_path(exact, tmp_path, "instance")


def test_controller_command_is_niced_private_and_has_no_dsn(tmp_path: Path) -> None:
    """Crash controllers can address only the guarded private rehearsal instance.

    Given: A controller specification with private coordinates and captured source hashes,
    When: Exact and hash-tampered child commands are assembled and parsed,
    Then: The exact command is niced and private while either drift boundary rejects tampering.
    """
    instance = rehearsal.InstanceRef(
        pgdata=tmp_path / "reports" / "pgdata",
        socket_dir=Path("/tmp/snapper-trades-rehearsal-sock-test"),
        port=55439,
        user="tester",
        postmaster_pid=999,
        source_snapshot=rehearsal._capture_source_snapshot(),
    )
    spec = rehearsal.ControllerSpec(
        instance=instance,
        scenario="after_begin",
        generation=3,
        token=UUID("00000000-0000-7000-8000-00000000c799"),
        horizon=_horizon(),
        writers=2,
        transaction_timeout_seconds=3.0,
        cleanup_margin_seconds=1.0,
        events_dir=tmp_path / "events",
    )

    command = rehearsal._controller_command(spec)

    assert command[:10] == [
        "setpriv",
        "--pdeathsig",
        "KILL",
        "nice",
        "-n",
        "19",
        "ionice",
        "-c",
        "3",
        rehearsal.sys.executable,
    ]
    assert "--socket-dir" in command
    assert "55439" in command
    assert "5432" not in command
    assert command[command.index("--script-sha256") + 1] == instance.source_snapshot.script_sha256
    assert command[command.index("--test-sha256") + 1] == instance.source_snapshot.test_sha256
    assert all("DB_URL" not in value and "postgresql://" not in value for value in command)
    drifted_snapshot = replace(instance.source_snapshot, script_sha256="0" * 64)
    drifted_spec = replace(spec, instance=replace(instance, source_snapshot=drifted_snapshot))
    with pytest.raises(rehearsal.RehearsalError, match="rehearsal source drift detected"):
        rehearsal._controller_command(drifted_spec)
    private_arguments = command[command.index("--role") :]
    private_arguments[private_arguments.index("--test-sha256") + 1] = "0" * 64
    with pytest.raises(rehearsal.RehearsalError, match="rehearsal source drift detected"):
        rehearsal._parse_private_child(private_arguments)


def test_repository_verification_manifest_is_exact_and_warning_clean() -> None:
    """The retained quality gate cannot silently omit or weaken a required check.

    Given: The harness exposes its ordered repository verification manifest,
    When: The complete command vectors are compared with the approved targeted gate,
    Then: All eleven commands are exact and pytest promotes every warning to an error.
    """
    source_files = (
        "scripts/trades_partition_rehearsal.py",
        "tests/scripts/test_trades_partition_rehearsal.py",
    )
    python = str(rehearsal._VENV_BIN / "python")
    expected = (
        (
            "targeted pytest",
            (
                str(rehearsal._VENV_BIN / "pytest"),
                "-q",
                "-W",
                "error",
                "tests/scripts/test_trades_partition_rehearsal.py",
            ),
        ),
        ("ruff", (str(rehearsal._VENV_BIN / "ruff"), "check", *source_files)),
        ("black", (str(rehearsal._VENV_BIN / "black"), "--check", *source_files)),
        ("isort", (str(rehearsal._VENV_BIN / "isort"), "--check-only", *source_files)),
        ("targeted mypy", (str(rehearsal._VENV_BIN / "mypy"), *source_files)),
        ("complexity", (python, "scripts/check_complexity_ratchet.py")),
        ("no comments", (python, "scripts/check_no_comments.py", "--strict")),
        (
            "docstrings",
            (
                python,
                "scripts/check_docstrings.py",
                "--strict",
                "--verbose",
                "--enforce-bdd",
                "--enforce-google-sections",
            ),
        ),
        (
            "coverage exclusions",
            (python, "scripts/check_coverage_exclusions.py", "--strict"),
        ),
        ("main guard", (python, "scripts/check_main_guard.py", "--strict")),
        ("init files", (python, "scripts/check_init_files.py", "--strict")),
    )

    commands = rehearsal._repository_verification_commands()

    assert commands == expected
    assert len(commands) == 11
    assert tuple(name for name, argv in commands if argv) == (
        "targeted pytest",
        "ruff",
        "black",
        "isort",
        "targeted mypy",
        "complexity",
        "no comments",
        "docstrings",
        "coverage exclusions",
        "main guard",
        "init files",
    )


def test_report_lists_only_files_that_exist_or_will_be_written(tmp_path: Path) -> None:
    """An early failure report does not claim logs or archives never produced.

    Given: A partially initialized run directory containing only the initdb log,
    When: The machine and Markdown reports enumerate created evidence files,
    Then: Both report targets and the real log appear while absent artifacts do not.
    """
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    initdb_log = run_dir / "initdb.log"
    initdb_log.write_text("initdb failed\n", encoding="utf-8")
    paths = rehearsal.RuntimePaths(
        run_id="test-run",
        run_dir=run_dir,
        instance_dir=run_dir / "instance",
        pgdata=run_dir / "instance" / "pgdata",
        socket_dir=tmp_path / "socket",
        postgres_log=run_dir / "postgres.log",
        initdb_log=initdb_log,
        report_json=run_dir / "report.json",
        report_markdown=run_dir / "report.md",
        events_dir=run_dir / "events",
        identities_dir=run_dir / "identities",
        archive_dir=run_dir / "archive",
        quality_log=run_dir / "quality.log",
    )
    report = rehearsal._report_json(
        rehearsal.RehearsalEvidence(),
        rehearsal.RunConfig(
            rows=10_000_000,
            days=56,
            writers=2,
            writer_rate=80.0,
            lease_seconds=10.0,
            transaction_timeout_seconds=3.0,
            cleanup_margin_seconds=1.0,
            guard_seconds=0.5,
            load_limit_per_cpu=1.5,
        ),
        paths,
        "FAIL",
    )
    files = cast(list[str], report["files_created"])
    markdown = rehearsal._markdown_report(report)

    assert str(initdb_log.resolve()) in files
    assert str(paths.report_json.resolve()) in files
    assert str(paths.report_markdown.resolve()) in files
    assert str(paths.postgres_log.resolve()) not in files
    assert str(paths.postgres_log.resolve()) not in markdown


def test_source_snapshot_rejects_script_and_test_drift(tmp_path: Path) -> None:
    """Every child and final verifier remain bound to the initially hashed sources.

    Given: Temporary harness and targeted-test files captured as one source snapshot,
    When: Either file changes after that capture,
    Then: The integrity assertion fails with both expected and observed SHA-256 evidence.
    """
    script_path = tmp_path / "trades_partition_rehearsal.py"
    test_path = tmp_path / "test_trades_partition_rehearsal.py"
    script_path.write_text("script version one\n", encoding="utf-8")
    test_path.write_text("test version one\n", encoding="utf-8")
    initial = rehearsal._capture_source_snapshot(script_path, test_path)

    rehearsal._assert_source_snapshot(initial, script_path, test_path)
    script_path.write_text("script version two\n", encoding="utf-8")
    with pytest.raises(rehearsal.RehearsalError, match="rehearsal source drift detected"):
        rehearsal._assert_source_snapshot(initial, script_path, test_path)

    script_path.write_text("script version one\n", encoding="utf-8")
    test_path.write_text("test version two\n", encoding="utf-8")
    with pytest.raises(rehearsal.RehearsalError, match="rehearsal source drift detected"):
        rehearsal._assert_source_snapshot(initial, script_path, test_path)


def test_validation_eta_uses_the_slower_of_row_and_heap_models() -> None:
    """The reported production ETA never selects the more optimistic model.

    Given: Valid measured throughput with a slower heap projection than row projection,
    When: The VALIDATE acceptance evidence chooses its production estimate,
    Then: The conservative ETA equals the slower heap-based duration.
    """
    metrics = _metrics()
    heap_bytes = round((59 * 1024**3) * metrics.seconds / 900.0)
    bytes_per_second = heap_bytes / metrics.seconds
    heap_eta = (59 * 1024**3) / bytes_per_second
    inputs = replace(
        _acceptance_inputs(),
        metrics=replace(
            metrics,
            heap_bytes=heap_bytes,
            bytes_per_second=bytes_per_second,
            production_heap_eta_seconds=heap_eta,
            conservative_eta_seconds=heap_eta,
        ),
    )

    result = rehearsal._criteria(inputs)[3]

    assert result.passed is True
    assert result.evidence["production_conservative_eta"] == "0h 15m 0s"


def test_explicit_h_boundary_names_are_date_stable() -> None:
    """Daily partition names remain stable across the explicit UTC horizon window.

    Given: Fourteen consecutive UTC day boundaries beginning exactly at H,
    When: The harness derives the daily leaf relation name for each boundary,
    Then: The names are unique and cover both the first and final expected dates.
    """
    horizon = _horizon()
    names = {
        f"trades_d{(horizon + timedelta(days=offset)).strftime('%Y%m%d')}" for offset in range(14)
    }

    assert len(names) == 14
    assert "trades_d20300102" in names
    assert "trades_d20300115" in names
