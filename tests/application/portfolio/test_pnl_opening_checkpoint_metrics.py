"""Process-local metric and summary formatting tests for durable opening checkpoints."""

import logging
from datetime import UTC
from datetime import datetime

import pytest

from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    CHECKPOINT_SUMMARY_EVERY_TICKS,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import PnlCheckpointMetrics
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    advance_checkpoint_summary_tick,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_metrics
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_metrics_by_scope
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_scope_key
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_wallet_alias
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    emit_checkpoint_metrics_summary,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    format_checkpoint_metrics_summary,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import prune_checkpoint_metrics
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_capture_committed,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_capture_refusal,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import record_checkpoint_fallback
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_invalidation_deleted,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import record_checkpoint_recapture
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_resume_attempted,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_resume_succeeded,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_shadow_comparison,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_shadow_mismatch,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_shadow_skipped,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import reset_checkpoint_metrics
from snapper.data.repository import PortfolioPnlSampleScope


def test_checkpoint_metrics_are_detached_and_grouped_by_reason() -> None:
    """Count every transition and isolate snapshots from later mutations.

    Given: Reset process-local counters.
    When: Capture, resume, fallback, deletion, and shadow events are recorded.
    Then: Scalar counters and reason maps agree and earlier snapshots stay detached.
    """
    reset_checkpoint_metrics()
    record_checkpoint_capture_committed()
    record_checkpoint_capture_refusal("capture_value_error")
    record_checkpoint_capture_refusal("capture_value_error")
    record_checkpoint_resume_attempted()
    record_checkpoint_resume_succeeded()
    record_checkpoint_fallback("checkpoint_absent")
    record_checkpoint_invalidation_deleted("generation")
    record_checkpoint_shadow_comparison()
    record_checkpoint_shadow_mismatch()
    record_checkpoint_recapture("age")
    record_checkpoint_recapture("age")

    snapshot = checkpoint_metrics()
    assert snapshot.captures_committed == 1
    assert snapshot.capture_refusals == {"capture_value_error": 2}
    assert snapshot.resumes_attempted == 1
    assert snapshot.resumes_succeeded == 1
    assert snapshot.fallbacks == {"checkpoint_absent": 1}
    assert snapshot.invalidations_deleted == {"generation": 1}
    assert snapshot.shadow_comparisons == 1
    assert snapshot.shadow_mismatches == 1
    assert snapshot.recaptures == {"age": 2}
    assert checkpoint_metrics_by_scope() == {}

    record_checkpoint_fallback("checkpoint_absent")
    assert snapshot.fallbacks == {"checkpoint_absent": 1}
    assert checkpoint_metrics().fallbacks == {"checkpoint_absent": 2}
    reset_checkpoint_metrics()
    assert checkpoint_metrics().captures_committed == 0
    assert checkpoint_metrics().capture_refusals == {}


def test_per_scope_metrics_track_scopes_independently_and_sum_globally() -> None:
    """Track per-scope counters independently while preserving global totals.

    Given: Two distinct scopes passed as a ``PortfolioPnlSampleScope`` and a tuple key.
    When: Checkpoint events are recorded against each scope separately.
    Then: Each scope keeps independent detached counts, global totals sum both, and reset clears all.
    """
    reset_checkpoint_metrics()
    scope_a = PortfolioPnlSampleScope(
        wallet_public_id="wallet-a",
        mode="live",
        valuation_ccy="USD",
        epoch_public_id="epoch-1",
        anchor_point_time=datetime(2026, 7, 20, 12, 0, tzinfo=UTC),
    )
    scope_b = ("wallet-b", "live", "EUR")

    record_checkpoint_capture_committed(scope_a)
    record_checkpoint_capture_refusal("not_settled", scope_a)
    record_checkpoint_resume_attempted(scope_a)
    record_checkpoint_resume_succeeded(scope_a)
    record_checkpoint_recapture("absent", scope_a)

    record_checkpoint_fallback("checkpoint_late_execution", scope_b)
    record_checkpoint_invalidation_deleted("generation", scope_b)
    record_checkpoint_shadow_comparison(scope_b)
    record_checkpoint_shadow_mismatch(scope_b)
    record_checkpoint_shadow_skipped("window_mismatch", scope_b)
    record_checkpoint_recapture("age", scope_b)

    by_scope = checkpoint_metrics_by_scope()
    alias_a = checkpoint_scope_key(scope_a)
    alias_b = checkpoint_scope_key(scope_b)
    assert set(by_scope) == {alias_a, alias_b}
    assert "wallet-a" not in str(list(by_scope))
    assert "wallet-b" not in str(list(by_scope))
    assert by_scope[alias_a] == PnlCheckpointMetrics(
        captures_committed=1,
        capture_refusals={"not_settled": 1},
        resumes_attempted=1,
        resumes_succeeded=1,
        recaptures={"absent": 1},
    )
    assert by_scope[alias_b] == PnlCheckpointMetrics(
        fallbacks={"checkpoint_late_execution": 1},
        invalidations_deleted={"generation": 1},
        shadow_comparisons=1,
        shadow_mismatches=1,
        shadow_skipped={"window_mismatch": 1},
        recaptures={"age": 1},
    )

    totals = checkpoint_metrics()
    assert totals.captures_committed == 1
    assert totals.capture_refusals == {"not_settled": 1}
    assert totals.resumes_attempted == 1
    assert totals.resumes_succeeded == 1
    assert totals.fallbacks == {"checkpoint_late_execution": 1}
    assert totals.invalidations_deleted == {"generation": 1}
    assert totals.shadow_comparisons == 1
    assert totals.shadow_mismatches == 1
    assert totals.shadow_skipped == {"window_mismatch": 1}
    assert totals.recaptures == {"absent": 1, "age": 1}

    pruned = prune_checkpoint_metrics({"wallet-a"})
    assert pruned == 1
    assert list(checkpoint_metrics_by_scope()) == [alias_a]

    reset_checkpoint_metrics()
    assert checkpoint_metrics_by_scope() == {}


def test_format_checkpoint_metrics_summary_is_deterministic_and_sorted() -> None:
    """Format reason maps compactly and deterministically in ascending key order.

    Given: A scope with unsorted insertion order across all reason maps.
    When: ``format_checkpoint_metrics_summary`` renders the summary line repeatedly.
    Then: The output starts with ``checkpoint_metrics_summary``, uses a keyed wallet alias,
        and sorts every map by key.
    """
    metrics = PnlCheckpointMetrics(
        captures_committed=3,
        capture_refusals={"not_settled": 2, "capture_value_error": 1},
        resumes_attempted=5,
        resumes_succeeded=4,
        fallbacks={"checkpoint_late_execution": 2, "checkpoint_absent": 1},
        invalidations_deleted={"generation": 1},
        shadow_comparisons=4,
        shadow_mismatches=1,
        shadow_skipped={"window_mismatch": 2},
        recaptures={"suffix_size": 3, "absent": 1, "age": 2},
    )
    alias = checkpoint_wallet_alias("wallet-1")
    scope = (alias, "live", "USD")
    expected = (
        "checkpoint_metrics_summary "
        f"wallet={alias} mode=live ccy=USD "
        "captures_committed=3 "
        "capture_refusals={capture_value_error:1,not_settled:2} "
        "resumes_attempted=5 resumes_succeeded=4 "
        "fallbacks={checkpoint_absent:1,checkpoint_late_execution:2} "
        "invalidations_deleted={generation:1} "
        "shadow_comparisons=4 shadow_mismatches=1 "
        "shadow_skipped={window_mismatch:2} "
        "recaptures={absent:1,age:2,suffix_size:3}"
    )

    first = format_checkpoint_metrics_summary(scope, metrics)
    second = format_checkpoint_metrics_summary(scope, metrics)
    assert first == expected
    assert second == expected
    assert "wallet-1" not in first


def test_summary_tick_gate_emits_at_constant_period_and_skips_off(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Emit cumulative per-scope summaries every N active ticks and never when off.

    Given: The default period constant and recorded scopes.
    When: Off ticks run, empty scopes tick to N, and populated scopes tick through 2*N.
    Then: Off ticks never emit or advance, empty ticks emit nothing, and ticks N and 2*N emit one line per scope.
    """
    reset_checkpoint_metrics()
    assert CHECKPOINT_SUMMARY_EVERY_TICKS == 10
    test_logger = logging.getLogger("test.checkpoint_metrics.summary")

    with caplog.at_level(logging.INFO, logger=test_logger.name):
        assert advance_checkpoint_summary_tick(0, "off", test_logger) == 0
        emit_checkpoint_metrics_summary(test_logger)
        assert caplog.records == []

        tick = 0
        for _ in range(CHECKPOINT_SUMMARY_EVERY_TICKS - 1):
            tick = advance_checkpoint_summary_tick(tick, "shadow", test_logger)
        assert caplog.records == []

        record_checkpoint_capture_committed(("wallet-b", "live", "USD"))
        record_checkpoint_fallback("checkpoint_absent", ("wallet-a", "live", "USD"))
        tick = advance_checkpoint_summary_tick(tick, "shadow", test_logger)
        assert tick == CHECKPOINT_SUMMARY_EVERY_TICKS
        assert len(caplog.records) == 2
        assert all(record.levelname == "INFO" for record in caplog.records)
        aliases = sorted([checkpoint_wallet_alias("wallet-a"), checkpoint_wallet_alias("wallet-b")])
        assert (
            caplog.records[0]
            .getMessage()
            .startswith(f"checkpoint_metrics_summary wallet={aliases[0]} ")
        )
        assert (
            caplog.records[1]
            .getMessage()
            .startswith(f"checkpoint_metrics_summary wallet={aliases[1]} ")
        )
        for record in caplog.records:
            assert "wallet-a" not in record.getMessage()
            assert "wallet-b" not in record.getMessage()
