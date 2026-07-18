"""Tests for the fail-closed portfolio reconciliation read projection."""

from copy import deepcopy
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest

from snapper.api.schemas.portfolio import PortfolioReconciliationView
from snapper.application.portfolio.reconciliation_view import (
    PORTFOLIO_RECONCILIATION_FUTURE_TOLERANCE,
)
from snapper.application.portfolio.reconciliation_view import PORTFOLIO_RECONCILIATION_STALE_AFTER
from snapper.application.portfolio.reconciliation_view import build_portfolio_reconciliation_view
from snapper.data.repository_types import PortfolioDriftEpisodeRow
from snapper.data.repository_types import PortfolioReconciliationLineageObservationRow
from snapper.data.repository_types import PortfolioReconciliationMethodConfigRow
from snapper.data.repository_types import PortfolioReconciliationReadContextRow
from snapper.data.repository_types import PortfolioReconciliationStateRow
from snapper.data.repository_types import SpotReconciliationAnchorRow
from snapper.data.repository_types import VenueAccountStateRow

_NOW = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
_WALLET_ID = "01980f9b-d000-7000-8000-000000000001"
_ACCOUNT_STATE_ID = "01980f9b-d000-7000-8000-000000000002"
_STATE_ID = "01980f9b-d000-7000-8000-000000000003"
_EPISODE_ID = "01980f9b-d000-7000-8000-000000000004"
_ANCHOR_ID = "01980f9b-d000-7000-8000-000000000006"
_EXPECTED_JSON = '{"BTC-PERP":{"size":1}}'
_ACTUAL_JSON = '{"BTC-PERP":{"size":1.0}}'
_DIFFERENCE_JSON = '{"BTC-PERP":{"size":0}}'
_TOLERANCE_JSON = '{"absolute":0.0001}'


def _account_state() -> VenueAccountStateRow:
    """Build the rendered account identity for a reconciliation context."""
    return {
        "wallet_public_id": _WALLET_ID,
        "exchange": "binance",
        "mode": "live",
        "sync_status": "observed",
        "balance_status": "observed",
        "position_status": "observed",
        "valuation_status": "native_only",
        "balances_json": "[]",
        "open_positions_json": "[]",
        "balance_observed_at": _NOW,
        "position_observed_at": _NOW,
        "current_attempt_observation_id": 41,
        "balance_payload_source_observation_id": 41,
        "position_payload_source_observation_id": 41,
        "authoritative_until": _NOW + timedelta(seconds=30),
        "error": None,
        "public_id": _ACCOUNT_STATE_ID,
        "timestamp": _NOW,
        "session_id": "account-session",
        "sequence_id": 41,
    }


def _matched_state() -> PortfolioReconciliationStateRow:
    """Build one coherent current full matched state."""
    return {
        "wallet_public_id": _WALLET_ID,
        "exchange": "binance",
        "mode": "live",
        "method": "futures_position",
        "current_evaluation_status": "matched",
        "current_observation_id": 7,
        "last_full_observation_id": 7,
        "last_full_outcome": "matched",
        "detail_source_observation_id": 7,
        "consecutive_full_mismatches": 0,
        "open_drift_episode_public_id": None,
        "anchor_public_id": None,
        "venue_account_state_public_id": _ACCOUNT_STATE_ID,
        "venue_account_observation_id": 41,
        "source_watermark_kind": "scope_sequence",
        "source_watermark": 9001,
        "expected_json": _EXPECTED_JSON,
        "actual_json": _ACTUAL_JSON,
        "difference_json": _DIFFERENCE_JSON,
        "tolerance_json": _TOLERANCE_JSON,
        "reconciled_at": _NOW,
        "authoritative_until": _NOW + timedelta(seconds=30),
        "error": None,
        "public_id": _STATE_ID,
        "timestamp": _NOW,
        "session_id": "reconciliation-session",
        "sequence_id": 7,
    }


def _matched_observation() -> PortfolioReconciliationLineageObservationRow:
    """Build the full observation referenced by the matched state."""
    return {
        "id": 7,
        "wallet_public_id": _WALLET_ID,
        "exchange": "binance",
        "mode": "live",
        "method": "futures_position",
        "evaluation_status": "matched",
        "venue_account_state_public_id": _ACCOUNT_STATE_ID,
        "venue_account_observation_id": 41,
        "account_authoritative_until": _NOW + timedelta(seconds=30),
        "source_watermark_kind": "scope_sequence",
        "source_watermark": 9001,
        "anchor_public_id": None,
        "expected_json": _EXPECTED_JSON,
        "actual_json": _ACTUAL_JSON,
        "difference_json": _DIFFERENCE_JSON,
        "tolerance_json": _TOLERANCE_JSON,
        "resulting_full_mismatch_count": 0,
        "drift_episode_public_id": None,
        "error": None,
        "timestamp": _NOW,
        "session_id": "reconciliation-session",
        "sequence_id": 7,
    }


def _method_config(
    method: str = "futures_position",
    classified_after_observation_id: int | None = None,
) -> PortfolioReconciliationMethodConfigRow:
    """Build one active durable reconciliation method configuration."""
    return {
        "wallet_public_id": _WALLET_ID,
        "exchange": "binance",
        "mode": "live",
        "method": method,
        "classified_after_observation_id": classified_after_observation_id,
        "public_id": "01980f9b-d000-7000-8000-000000000005",
        "timestamp": _NOW - timedelta(days=1),
        "session_id": "config-session",
        "sequence_id": 1,
    }


def _matched_context() -> PortfolioReconciliationReadContextRow:
    """Build a complete batched read context for one matched account."""
    return {
        "account_state": _account_state(),
        "duplicate_active_rows": False,
        "state": _matched_state(),
        "observations": [_matched_observation()],
        "config": _method_config(),
        "latest_ordered_observation_id": 7,
        "latest_appended_observation_id": 7,
        "open_drift_episode": None,
        "spot_anchor": None,
    }


def _spot_anchor() -> SpotReconciliationAnchorRow:
    """Build the certified anchor referenced by a full spot result."""
    return {
        "public_id": _ANCHOR_ID,
        "wallet_public_id": _WALLET_ID,
        "exchange": "binance",
        "mode": "live",
        "venue_account_state_public_id": _ACCOUNT_STATE_ID,
        "balance_observation_id": 41,
        "source_watermark_kind": "scope_sequence",
        "source_watermark": 9000,
        "balances_json": '{"BTC":"1"}',
        "first_request_started_at": _NOW - timedelta(seconds=4),
        "first_request_completed_at": _NOW - timedelta(seconds=3),
        "second_request_started_at": _NOW - timedelta(seconds=2),
        "second_request_completed_at": _NOW - timedelta(seconds=1),
        "boundary_status": "cursor_certified",
        "inventory_status": "venue_reported_full",
        "margin_status": "cash",
        "provenance": "test",
        "session_id": "anchor-session",
        "sequence_id": 1,
        "timestamp": _NOW - timedelta(seconds=1),
        "source_chain_tip": "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4",
        "venue_cursor_kind": "account_history_item_id",
        "venue_cursor_scheme": "binance:ccxt:account/history:v1",
        "venue_cursor_value": "9000",
        "venue_cursor_requested_at": _NOW - timedelta(seconds=8),
        "venue_cursor_observed_at": _NOW - timedelta(seconds=7),
        "venue_cursor_confirmed_at": _NOW - timedelta(seconds=1),
        "source_watermark_requested_at": _NOW - timedelta(seconds=6),
        "source_watermark_captured_at": _NOW - timedelta(seconds=5),
    }


def _spot_matched_context() -> PortfolioReconciliationReadContextRow:
    """Build a coherent full spot result with a certified anchor boundary."""
    context = _matched_context()
    state = cast(PortfolioReconciliationStateRow, context["state"])
    observation = context["observations"][0]
    config = context["config"]
    assert config is not None
    state["method"] = "spot_execution_replay"
    state["anchor_public_id"] = _ANCHOR_ID
    observation["method"] = "spot_execution_replay"
    observation["anchor_public_id"] = _ANCHOR_ID
    config["method"] = "spot_execution_replay"
    context["spot_anchor"] = _spot_anchor()
    return context


def _spot_incomplete_context() -> PortfolioReconciliationReadContextRow:
    """Build a spot non-full successor retaining certified full evidence."""
    context = _incomplete_context()
    state = cast(PortfolioReconciliationStateRow, context["state"])
    config = context["config"]
    assert config is not None
    state["method"] = "spot_execution_replay"
    state["anchor_public_id"] = _ANCHOR_ID
    for observation in context["observations"]:
        observation["method"] = "spot_execution_replay"
    context["observations"][0]["anchor_public_id"] = _ANCHOR_ID
    config["method"] = "spot_execution_replay"
    context["spot_anchor"] = _spot_anchor()
    return context


def _required_state(
    context: PortfolioReconciliationReadContextRow,
) -> PortfolioReconciliationStateRow:
    """Return the non-null state from a test context."""
    return cast(PortfolioReconciliationStateRow, context["state"])


def _current_observation(
    context: PortfolioReconciliationReadContextRow,
) -> PortfolioReconciliationLineageObservationRow:
    """Return the current observation from a test context."""
    state = _required_state(context)
    return next(
        observation
        for observation in context["observations"]
        if observation["id"] == state["current_observation_id"]
    )


def _incomplete_context(status: str = "incomplete") -> PortfolioReconciliationReadContextRow:
    """Build a current non-full successor retaining validated full evidence."""
    context = _matched_context()
    state = _required_state(context)
    state["current_evaluation_status"] = status
    state["current_observation_id"] = 8
    state["session_id"] = "reconciliation-session-2"
    state["sequence_id"] = 8
    current = deepcopy(_matched_observation())
    current["id"] = 8
    current["evaluation_status"] = status
    current["venue_account_state_public_id"] = None
    current["venue_account_observation_id"] = None
    current["account_authoritative_until"] = None
    current["source_watermark_kind"] = None
    current["source_watermark"] = None
    current["expected_json"] = None
    current["actual_json"] = None
    current["difference_json"] = None
    current["tolerance_json"] = None
    current["session_id"] = "reconciliation-session-2"
    current["sequence_id"] = 8
    context["observations"].append(current)
    context["latest_ordered_observation_id"] = 8
    context["latest_appended_observation_id"] = 8
    return context


def _open_episode_context() -> PortfolioReconciliationReadContextRow:
    """Build a coherent third consecutive mismatch with an open episode."""
    context = _matched_context()
    state = _required_state(context)
    observation = _current_observation(context)
    state["current_evaluation_status"] = "mismatched"
    state["last_full_outcome"] = "mismatched"
    state["consecutive_full_mismatches"] = 3
    state["open_drift_episode_public_id"] = _EPISODE_ID
    observation["evaluation_status"] = "mismatched"
    observation["resulting_full_mismatch_count"] = 3
    observation["drift_episode_public_id"] = _EPISODE_ID
    context["open_drift_episode"] = {
        "wallet_public_id": _WALLET_ID,
        "exchange": "binance",
        "mode": "live",
        "public_id": _EPISODE_ID,
        "status": "open",
        "opened_at": _NOW - timedelta(minutes=1),
        "trigger_observation_id": 7,
        "last_observation_id": 7,
        "details_source_observation_id": 7,
        "latest_full_mismatch_count": 3,
    }
    return context


def _unclassified_context() -> PortfolioReconciliationReadContextRow:
    """Build a coherent initial incomplete state without retained evidence."""
    context = _matched_context()
    state = _required_state(context)
    observation = _current_observation(context)
    state["method"] = "unclassified"
    state["current_evaluation_status"] = "incomplete"
    state["last_full_observation_id"] = None
    state["last_full_outcome"] = None
    state["detail_source_observation_id"] = None
    state["venue_account_state_public_id"] = None
    state["venue_account_observation_id"] = None
    state["source_watermark_kind"] = None
    state["source_watermark"] = None
    state["expected_json"] = None
    state["actual_json"] = None
    state["difference_json"] = None
    state["tolerance_json"] = None
    state["reconciled_at"] = None
    state["authoritative_until"] = None
    observation["method"] = "unclassified"
    observation["evaluation_status"] = "incomplete"
    observation["venue_account_state_public_id"] = None
    observation["venue_account_observation_id"] = None
    observation["account_authoritative_until"] = None
    observation["source_watermark_kind"] = None
    observation["source_watermark"] = None
    observation["expected_json"] = None
    observation["actual_json"] = None
    observation["difference_json"] = None
    observation["tolerance_json"] = None
    context["config"] = None
    return context


def _pending_reclassification_context(
    method: str = "futures_position",
) -> PortfolioReconciliationReadContextRow:
    """Build a causally proven classification awaiting its next evaluation."""
    context = _unclassified_context()
    state = _required_state(context)
    context["config"] = _method_config(method, state["current_observation_id"])
    return context


def _assert_corrupt(view: PortfolioReconciliationView) -> None:
    """Assert the evidence-free corruption boundary."""
    assert view.effective_status == "corrupt"
    assert view.is_authoritative is False
    assert view.method is None
    assert view.evaluation_status is None
    assert view.evaluated_at is None
    assert view.current_observation_id is None
    assert view.last_full_observation_id is None
    assert view.detail_source_observation_id is None
    assert view.last_full_outcome is None
    assert view.consecutive_full_mismatches == 0
    assert view.anchor_public_id is None
    assert view.venue_account_state_public_id is None
    assert view.venue_account_observation_id is None
    assert view.source_watermark_kind is None
    assert view.source_watermark is None
    assert view.expected is None
    assert view.actual is None
    assert view.difference is None
    assert view.tolerance is None
    assert view.reconciled_at is None
    assert view.authoritative_until is None
    assert view.open_drift_episode is None


def test_fresh_current_full_match_is_authoritative() -> None:
    """A fully revalidated fresh current match is authoritative.

    Given: A coherent matched state and complete persisted lineage.
    When: The pure reconciliation view is built at its verdict time.
    Then: It exposes structured evidence with authority enabled.
    """
    view = build_portfolio_reconciliation_view(_matched_context(), _NOW)

    assert view.effective_status == "matched"
    assert view.is_authoritative is True
    assert view.method == "futures_position"
    assert view.evaluation_status == "matched"
    assert view.evaluated_at == _NOW
    assert view.expected == {"BTC-PERP": {"size": 1}}
    assert view.actual == {"BTC-PERP": {"size": 1.0}}
    assert view.difference == {"BTC-PERP": {"size": 0}}
    assert view.tolerance == {"absolute": 0.0001}


def test_fresh_full_spot_match_with_certified_anchor_is_authoritative() -> None:
    """A full spot match is authoritative only across its certified anchor.

    Given: A coherent spot state whose loaded anchor certifies its boundary.
    When: The shared anchor invariant validates the read context.
    Then: The fresh matched verdict remains authoritative.
    """
    view = build_portfolio_reconciliation_view(_spot_matched_context(), _NOW)

    assert view.effective_status == "matched"
    assert view.is_authoritative is True
    assert view.method == "spot_execution_replay"
    assert view.anchor_public_id == _ANCHOR_ID


def test_fresh_current_full_mismatch_is_authoritative() -> None:
    """A fresh current mismatch is authoritative without an early episode.

    Given: A coherent first full mismatch below the episode threshold.
    When: The pure reconciliation view revalidates the context.
    Then: The mismatch is authoritative and carries no open episode.
    """
    context = _matched_context()
    state = _required_state(context)
    observation = _current_observation(context)
    state["current_evaluation_status"] = "mismatched"
    state["last_full_outcome"] = "mismatched"
    state["consecutive_full_mismatches"] = 1
    observation["evaluation_status"] = "mismatched"
    observation["resulting_full_mismatch_count"] = 1

    view = build_portfolio_reconciliation_view(context, _NOW)

    assert view.effective_status == "mismatched"
    assert view.is_authoritative is True
    assert view.consecutive_full_mismatches == 1


def test_stale_view_retains_evidence_and_open_episode() -> None:
    """Staleness denies authority without suppressing validated evidence.

    Given: A coherent open drift episode whose verdict is over 900 seconds old.
    When: Read-time freshness is derived.
    Then: Evidence and the episode remain visible without authority.
    """
    context = _open_episode_context()
    state = _required_state(context)
    observation = _current_observation(context)
    stale_at = _NOW - PORTFOLIO_RECONCILIATION_STALE_AFTER - timedelta(microseconds=1)
    state["timestamp"] = stale_at
    state["reconciled_at"] = stale_at
    observation["timestamp"] = stale_at

    view = build_portfolio_reconciliation_view(context, _NOW)

    assert view.effective_status == "stale"
    assert view.is_authoritative is False
    assert view.expected == {"BTC-PERP": {"size": 1}}
    assert view.open_drift_episode is not None
    assert view.open_drift_episode.public_id == _EPISODE_ID
    assert view.open_drift_episode.status == "open"


def test_clock_error_retains_validated_evidence() -> None:
    """A verdict beyond future tolerance is non-authoritative clock error.

    Given: A coherent matched verdict stamped beyond the clock tolerance.
    When: Read-time clock status is derived.
    Then: The view reports clock_error while retaining evidence.
    """
    context = _matched_context()
    future = _NOW + PORTFOLIO_RECONCILIATION_FUTURE_TOLERANCE + timedelta(microseconds=1)
    _required_state(context)["timestamp"] = future

    view = build_portfolio_reconciliation_view(context, _NOW)

    assert view.effective_status == "clock_error"
    assert view.is_authoritative is False
    assert view.actual == {"BTC-PERP": {"size": 1.0}}


def test_freshness_and_future_boundaries_are_inclusive() -> None:
    """Exactly 900 seconds old and exactly five seconds ahead remain current.

    Given: Coherent verdicts exactly on both demotion boundaries.
    When: Their effective statuses are derived.
    Then: Neither boundary value is demoted.
    """
    stale_boundary = _matched_context()
    _required_state(stale_boundary)["timestamp"] = _NOW - PORTFOLIO_RECONCILIATION_STALE_AFTER
    future_boundary = _matched_context()
    _required_state(future_boundary)["timestamp"] = _NOW + PORTFOLIO_RECONCILIATION_FUTURE_TOLERANCE

    assert build_portfolio_reconciliation_view(stale_boundary, _NOW).effective_status == "matched"
    assert build_portfolio_reconciliation_view(future_boundary, _NOW).effective_status == "matched"


@pytest.mark.parametrize("status", ["incomplete", "unsupported"])
def test_nonfull_current_status_retains_last_full_evidence(status: str) -> None:
    """A non-full successor stays non-authoritative with last-known evidence.

    Given: A valid non-full successor after a full matched result.
    When: The reconciliation view is built.
    Then: The raw non-full status is exposed without authority or evidence loss.

    Args:
        status: Valid current non-full evaluation status.
    """
    view = build_portfolio_reconciliation_view(_incomplete_context(status), _NOW)

    assert view.effective_status == status
    assert view.evaluation_status == status
    assert view.is_authoritative is False
    assert view.last_full_outcome == "matched"
    assert view.expected == {"BTC-PERP": {"size": 1}}


def test_error_current_status_requires_reason_and_retains_evidence() -> None:
    """A valid error successor remains non-authoritative with retained evidence.

    Given: An error successor with a non-empty reason after a full result.
    When: The persisted context is revalidated.
    Then: The error and retained evidence remain visible without authority.
    """
    context = _incomplete_context("error")
    state = _required_state(context)
    observation = _current_observation(context)
    state["error"] = "venue timeout"
    observation["error"] = "venue timeout"

    view = build_portfolio_reconciliation_view(context, _NOW)

    assert view.effective_status == "error"
    assert view.error == "venue timeout"
    assert view.is_authoritative is False


def test_no_state_is_well_defined_incomplete_truth() -> None:
    """An account without reconciliation history receives a stable shape.

    Given: An account context without reconciliation state or history.
    When: Its reconciliation view is built.
    Then: The result is evidence-free incomplete truth without authority.
    """
    context = _matched_context()
    context["state"] = None
    context["observations"] = []
    context["latest_ordered_observation_id"] = None
    context["latest_appended_observation_id"] = None
    context["open_drift_episode"] = None

    view = build_portfolio_reconciliation_view(context, _NOW)

    assert view.effective_status == "incomplete"
    assert view.is_authoritative is False
    assert view.method is None
    assert view.error is None


def test_initial_unclassified_state_has_no_evidence() -> None:
    """A valid initial unclassified state is incomplete and evidence-free.

    Given: A coherent unclassified first evaluation without method config.
    When: Shared state and config validators revalidate it.
    Then: It remains incomplete with no fabricated evidence.
    """
    view = build_portfolio_reconciliation_view(_unclassified_context(), _NOW)

    assert view.method == "unclassified"
    assert view.effective_status == "incomplete"
    assert view.is_authoritative is False
    assert view.expected is None


@pytest.mark.parametrize(
    "method",
    ["futures_position", "spot_execution_replay", "margin_ledger_replay"],
)
def test_causally_proven_pending_reclassification_is_incomplete(method: str) -> None:
    """Every real classification can expose only an evidence-free pending view.

    Given: A real method config naming the exact active unclassified observation.
    When: The state has not yet folded a real-method evaluation.
    Then: The configured method is incomplete without authority or evidence.

    Args:
        method: Real method selected by the operator.
    """
    view = build_portfolio_reconciliation_view(
        _pending_reclassification_context(method),
        _NOW,
    )

    assert view.method == method
    assert view.evaluation_status == "incomplete"
    assert view.effective_status == "incomplete"
    assert view.is_authoritative is False
    assert view.evaluated_at is None
    assert view.current_observation_id is None
    assert view.last_full_observation_id is None
    assert view.detail_source_observation_id is None
    assert view.last_full_outcome is None
    assert view.consecutive_full_mismatches == 0
    assert view.anchor_public_id is None
    assert view.venue_account_state_public_id is None
    assert view.venue_account_observation_id is None
    assert view.source_watermark_kind is None
    assert view.source_watermark is None
    assert view.expected is None
    assert view.actual is None
    assert view.difference is None
    assert view.tolerance is None
    assert view.reconciled_at is None
    assert view.authoritative_until is None
    assert view.error is None
    assert view.open_drift_episode is None


@pytest.mark.parametrize("classified_after_observation_id", [None, 6, 8])
def test_pending_reclassification_requires_exact_causal_identity(
    classified_after_observation_id: int | None,
) -> None:
    """Missing, earlier, and later predecessor identities all fail closed.

    Given: An unclassified state beside a real config without an exact capture.
    When: The pending predicate compares durable causal lineage.
    Then: Shape similarity cannot soften the context from corrupt.

    Args:
        classified_after_observation_id: Missing or unequal captured identity.
    """
    context = _pending_reclassification_context()
    config = context["config"]
    assert config is not None
    config["classified_after_observation_id"] = classified_after_observation_id

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_pending_reclassification_with_retained_state_evidence_is_corrupt() -> None:
    """Causal identity cannot excuse retained evidence on an unclassified state.

    Given: An exact config capture beside an unclassified state retaining detail.
    When: The pending branch runs the strict method-transition validation.
    Then: The contradictory state remains evidence-free corruption.
    """
    context = _pending_reclassification_context()
    _required_state(context)["expected_json"] = "{}"

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_pending_reclassification_clears_prior_unclassified_reason() -> None:
    """The normal unclassified reason does not make exact pending lineage corrupt.

    Given: Exact causal lineage carrying the dispatcher's unclassified reason.
    When: The operator classification is pending its first real evaluation.
    Then: The configured method remains incomplete and the old reason is cleared.
    """
    context = _pending_reclassification_context()
    state = _required_state(context)
    observation = _current_observation(context)
    state["error"] = "reconciliation_method_unclassified"
    observation["error"] = "reconciliation_method_unclassified"

    view = build_portfolio_reconciliation_view(context, _NOW)

    assert view.method == "futures_position"
    assert view.evaluation_status == "incomplete"
    assert view.effective_status == "incomplete"
    assert view.error is None


def test_pending_reclassification_rejects_blob_error_fields() -> None:
    """Pending projection cannot erase malformed persisted error values.

    Given: Exact pending lineage whose state and observation errors are BLOB bytes.
    When: The pending exception validates both persisted error field types.
    Then: The malformed context fails closed as evidence-free corruption.
    """
    context = _pending_reclassification_context()
    state = _required_state(context)
    observation = _current_observation(context)
    blob_error = b"malformed pending error"
    cast(dict[str, object], state)["error"] = blob_error
    cast(dict[str, object], observation)["error"] = blob_error

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


@pytest.mark.parametrize("joined_row", ["episode", "anchor"])
def test_pending_reclassification_rejects_joined_evidence(joined_row: str) -> None:
    """Unexpected active episode and anchor joins both fail pending truth closed.

    Given: A causally exact pending context with an unrelated joined evidence row.
    When: Strict downstream lineage checks run before rendering.
    Then: The context remains corrupt rather than hiding the joined row.

    Args:
        joined_row: Joined durable evidence kind to forge.
    """
    context = _pending_reclassification_context()
    if joined_row == "episode":
        context["open_drift_episode"] = cast(PortfolioDriftEpisodeRow, {})
    else:
        context["spot_anchor"] = cast(SpotReconciliationAnchorRow, {})

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_unclassified_error_is_not_a_pending_reclassification() -> None:
    """A valid unclassified error near-miss remains corrupt beside real config.

    Given: An exact config capture beside an unclassified evaluation with error.
    When: The pending predicate requires incomplete state and evaluation statuses.
    Then: The ordinary strict config conflict fails the context closed.
    """
    context = _pending_reclassification_context()
    state = _required_state(context)
    observation = _current_observation(context)
    state["current_evaluation_status"] = "error"
    state["error"] = "classification unavailable"
    observation["evaluation_status"] = "error"
    observation["error"] = "classification unavailable"

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


@pytest.mark.parametrize(
    ("timestamp", "expected_status"),
    [
        (
            _NOW - PORTFOLIO_RECONCILIATION_STALE_AFTER - timedelta(microseconds=1),
            "stale",
        ),
        (
            _NOW + PORTFOLIO_RECONCILIATION_FUTURE_TOLERANCE + timedelta(microseconds=1),
            "clock_error",
        ),
    ],
)
def test_pending_reclassification_applies_freshness_demotions(
    timestamp: datetime,
    expected_status: str,
) -> None:
    """Old and future pending predecessors remain non-authoritative demotions.

    Given: A causally exact pending predecessor outside a freshness boundary.
    When: The pending result derives effective status from the state timestamp.
    Then: Stale and future-clock demotions still apply without evidence.

    Args:
        timestamp: Stored predecessor timestamp outside one freshness boundary.
        expected_status: Effective status required for that boundary breach.
    """
    context = _pending_reclassification_context()
    _required_state(context)["timestamp"] = timestamp

    view = build_portfolio_reconciliation_view(context, _NOW)

    assert view.method == "futures_position"
    assert view.evaluation_status == "incomplete"
    assert view.effective_status == expected_status
    assert view.is_authoritative is False
    assert view.expected is None


def test_duplicate_active_rows_fail_closed_before_projection() -> None:
    """Loader-detected active multiplicity always produces one corrupt view.

    Given: A context marked as containing duplicate active state or config rows.
    When: The read view begins projection.
    Then: It clears all evidence before inspecting either selected row.
    """
    context = _matched_context()
    context["duplicate_active_rows"] = True

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_real_state_and_different_real_config_remain_corrupt() -> None:
    """The pending exception never softens a real-method config mismatch.

    Given: A futures state beside a different real active configuration.
    When: The state cannot satisfy the unclassified pending predicate.
    Then: Ordinary strict config validation keeps the context corrupt.
    """
    context = _matched_context()
    config = context["config"]
    assert config is not None
    config["method"] = "spot_execution_replay"
    config["classified_after_observation_id"] = 7

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


@pytest.mark.parametrize(
    "orphan_field",
    ["observations", "latest_ordered", "latest_appended", "episode", "anchor"],
)
def test_no_state_with_orphaned_lineage_is_corrupt(orphan_field: str) -> None:
    """Persisted lineage without its active state fails closed.

    Given: One kind of reconciliation history without an active state.
    When: The no-state context is projected.
    Then: The orphaned history produces evidence-free corruption.

    Args:
        orphan_field: Context field carrying orphaned reconciliation history.
    """
    context = _matched_context()
    context["state"] = None
    context["observations"] = []
    context["latest_ordered_observation_id"] = None
    context["latest_appended_observation_id"] = None
    context["open_drift_episode"] = None
    context["spot_anchor"] = None
    if orphan_field == "observations":
        context["observations"] = [_matched_observation()]
    elif orphan_field == "latest_ordered":
        context["latest_ordered_observation_id"] = 7
    elif orphan_field == "latest_appended":
        context["latest_appended_observation_id"] = 7
    elif orphan_field == "episode":
        context["open_drift_episode"] = cast(PortfolioDriftEpisodeRow, {})
    else:
        context["spot_anchor"] = cast(SpotReconciliationAnchorRow, {})

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_invalid_method_claiming_matched_is_corrupt() -> None:
    """A forged invalid method can never mint an authoritative match.

    Given: State and observation rows claiming matched under an invalid method.
    When: The shared config validator is called by the view.
    Then: The view fails closed as corrupt with evidence cleared.
    """
    context = _matched_context()
    _required_state(context)["method"] = "invalid"
    _current_observation(context)["method"] = "invalid"

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_null_required_string_field_is_corrupt() -> None:
    """A persisted NULL required string cannot escape the account boundary.

    Given: A self-consistent forged context whose exchange fields are NULL.
    When: Shared config validation attempts string normalization.
    Then: The account degrades to corrupt instead of raising AttributeError.
    """
    context = _matched_context()
    state = _required_state(context)
    observation = _current_observation(context)
    config = context["config"]
    assert config is not None
    cast(dict[str, object], context["account_state"])["exchange"] = None
    cast(dict[str, object], state)["exchange"] = None
    cast(dict[str, object], observation)["exchange"] = None
    cast(dict[str, object], config)["exchange"] = None

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_stale_ahead_source_watermark_is_corrupt() -> None:
    """A forged state watermark ahead of its detail lineage fails closed.

    Given: A state watermark that disagrees with its referenced detail row.
    When: Shared observation-lineage validation runs.
    Then: The view is corrupt and cannot expose evidence or authority.
    """
    context = _matched_context()
    _required_state(context)["source_watermark"] = 9002

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_cross_account_observation_reference_is_corrupt() -> None:
    """A referenced observation owned by another account fails closed.

    Given: A state whose current observation belongs to another wallet.
    When: Shared observation-lineage validation runs.
    Then: The foreign reference produces evidence-free corruption.
    """
    context = _matched_context()
    _current_observation(context)["wallet_public_id"] = "01980f9b-d000-7000-8000-000000000099"

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


@pytest.mark.parametrize(
    "invalid_json",
    ["not json", "[]", '{"value":NaN}'],
)
def test_invalid_evidence_json_is_corrupt(invalid_json: str) -> None:
    """Malformed, non-object, and non-finite evidence all fail closed.

    Given: Coherent lineage whose retained evidence has an invalid JSON shape.
    When: Strict JsonObject parsing runs at read time.
    Then: The whole reconciliation view fails closed as corrupt.

    Args:
        invalid_json: Invalid persisted JSON representation to revalidate.
    """
    context = _matched_context()
    state = _required_state(context)
    observation = _current_observation(context)
    state["expected_json"] = invalid_json
    observation["expected_json"] = invalid_json

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_foreign_state_or_config_identity_is_corrupt() -> None:
    """A batched context that crosses identity boundaries fails closed.

    Given: State and config contexts that disagree with the rendered account.
    When: The batched context identity is certified.
    Then: Both variants produce evidence-free corruption.
    """
    state_context = _matched_context()
    state = _required_state(state_context)
    state["wallet_public_id"] = "01980f9b-d000-7000-8000-000000000099"
    _current_observation(state_context)["wallet_public_id"] = state["wallet_public_id"]
    config_context = _matched_context()
    config = config_context["config"]
    assert config is not None
    config["exchange"] = "kraken"

    _assert_corrupt(build_portfolio_reconciliation_view(state_context, _NOW))
    _assert_corrupt(build_portfolio_reconciliation_view(config_context, _NOW))


def test_missing_config_or_latest_lineage_is_corrupt() -> None:
    """Config and both latest-observation certifications are mandatory.

    Given: Otherwise coherent states missing config or one latest identity.
    When: The shared config and latest-lineage validators run.
    Then: Every uncertified variant fails closed as corrupt.
    """
    config_context = _matched_context()
    config_context["config"] = None
    ordered_context = _matched_context()
    ordered_context["latest_ordered_observation_id"] = 6
    appended_context = _matched_context()
    appended_context["latest_appended_observation_id"] = 6

    _assert_corrupt(build_portfolio_reconciliation_view(config_context, _NOW))
    _assert_corrupt(build_portfolio_reconciliation_view(ordered_context, _NOW))
    _assert_corrupt(build_portfolio_reconciliation_view(appended_context, _NOW))


def test_missing_referenced_observation_is_corrupt() -> None:
    """An absent current observation cannot be reconstructed or trusted.

    Given: A state whose referenced observation was not loaded from storage.
    When: Shared observation-lineage validation runs.
    Then: The missing evidence yields an evidence-free corrupt view.
    """
    context = _matched_context()
    context["observations"] = []

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_strict_output_validation_failure_is_corrupt() -> None:
    """A forged retained outcome rejected by the strict schema fails closed.

    Given: Self-consistent lineage carrying an invalid retained outcome literal.
    When: The strict reconciliation response object is constructed.
    Then: Schema rejection is converted to evidence-free corruption.
    """
    context = _incomplete_context()
    state = _required_state(context)
    state["last_full_outcome"] = "forged"
    context["observations"][0]["evaluation_status"] = "forged"

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_partial_or_misaligned_retained_evidence_is_corrupt() -> None:
    """Partial evidence and unequal detail/last-full lineage fail closed.

    Given: States with partial evidence or divergent retained detail identity.
    When: Read authority prerequisites are validated.
    Then: Neither state can expose an authoritative verdict.
    """
    partial_context = _matched_context()
    partial_state = _required_state(partial_context)
    partial_observation = _current_observation(partial_context)
    partial_state["tolerance_json"] = None
    partial_observation["tolerance_json"] = None
    misaligned_context = _matched_context()
    misaligned_state = _required_state(misaligned_context)
    misaligned_state["detail_source_observation_id"] = 8
    detail = deepcopy(_matched_observation())
    detail["id"] = 8
    misaligned_context["observations"].append(detail)

    _assert_corrupt(build_portfolio_reconciliation_view(partial_context, _NOW))
    _assert_corrupt(build_portfolio_reconciliation_view(misaligned_context, _NOW))


def test_full_state_without_current_detail_lineage_is_corrupt() -> None:
    """A full verdict must be the state's current retained detail.

    Given: A matched current observation distinct from retained full evidence.
    When: Current-full authority prerequisites are validated.
    Then: The contradictory full verdict fails closed as corrupt.
    """
    context = _matched_context()
    state = _required_state(context)
    state["current_observation_id"] = 8
    state["session_id"] = "reconciliation-session-2"
    state["sequence_id"] = 8
    current = deepcopy(_matched_observation())
    current["id"] = 8
    current["session_id"] = state["session_id"]
    current["sequence_id"] = state["sequence_id"]
    context["observations"].append(current)
    context["latest_ordered_observation_id"] = 8
    context["latest_appended_observation_id"] = 8

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_mismatch_streak_and_episode_threshold_corruption_fails_closed() -> None:
    """Contradictory outcomes, counts, and episode thresholds fail closed.

    Given: Several CHECK-like state-shape contradictions and a missing episode.
    When: The read view revalidates mismatch and episode lineage.
    Then: Every contradiction produces evidence-free corruption.
    """
    matched_count = _matched_context()
    _required_state(matched_count)["consecutive_full_mismatches"] = 1
    _current_observation(matched_count)["resulting_full_mismatch_count"] = 1
    matched_error = _matched_context()
    _required_state(matched_error)["error"] = "forged matched error"
    _current_observation(matched_error)["error"] = "forged matched error"
    mismatch_zero = _matched_context()
    mismatch_state = _required_state(mismatch_zero)
    mismatch_observation = _current_observation(mismatch_zero)
    mismatch_state["current_evaluation_status"] = "mismatched"
    mismatch_state["last_full_outcome"] = "mismatched"
    mismatch_observation["evaluation_status"] = "mismatched"
    episode_missing = _open_episode_context()
    episode_missing["open_drift_episode"] = None
    threshold_context = _open_episode_context()
    threshold_state = _required_state(threshold_context)
    threshold_observation = _current_observation(threshold_context)
    threshold_state["open_drift_episode_public_id"] = None
    threshold_observation["drift_episode_public_id"] = None
    threshold_context["open_drift_episode"] = None

    _assert_corrupt(build_portfolio_reconciliation_view(matched_count, _NOW))
    _assert_corrupt(build_portfolio_reconciliation_view(matched_error, _NOW))
    _assert_corrupt(build_portfolio_reconciliation_view(mismatch_zero, _NOW))
    _assert_corrupt(build_portfolio_reconciliation_view(episode_missing, _NOW))
    _assert_corrupt(build_portfolio_reconciliation_view(threshold_context, _NOW))


def test_state_without_full_result_cannot_retain_mismatch_evidence() -> None:
    """A no-full state with a nonzero retained streak is corrupt.

    Given: An incomplete state with no full result but a nonzero mismatch count.
    When: Retained evidence coherence is validated.
    Then: The impossible streak yields a corrupt non-authoritative view.
    """
    context = _incomplete_context()
    state = _required_state(context)
    state["last_full_observation_id"] = None
    state["last_full_outcome"] = None
    state["detail_source_observation_id"] = None
    state["venue_account_state_public_id"] = None
    state["venue_account_observation_id"] = None
    state["source_watermark_kind"] = None
    state["source_watermark"] = None
    state["expected_json"] = None
    state["actual_json"] = None
    state["difference_json"] = None
    state["tolerance_json"] = None
    state["reconciled_at"] = None
    state["authoritative_until"] = None
    state["consecutive_full_mismatches"] = 1
    current = _current_observation(context)
    current["resulting_full_mismatch_count"] = 1
    context["observations"] = [current]

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_full_spot_state_without_anchor_is_corrupt() -> None:
    """A full spot verdict without its bootstrap anchor fails closed.

    Given: A current full spot match with no retained anchor identity.
    When: Full spot authority prerequisites are validated.
    Then: The verdict is corrupt and all evidence is cleared.
    """
    context = _matched_context()
    state = _required_state(context)
    observation = _current_observation(context)
    state["method"] = "spot_execution_replay"
    observation["method"] = "spot_execution_replay"
    config = context["config"]
    assert config is not None
    config["method"] = "spot_execution_replay"

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_full_spot_state_with_missing_anchor_row_is_corrupt() -> None:
    """A full spot verdict cannot trust an anchor identity that resolves absent.

    Given: Coherent spot lineage naming an anchor with no loaded active row.
    When: Shared anchor lineage validation runs.
    Then: The view clears evidence and denies authority as corrupt.
    """
    context = _spot_matched_context()
    context["spot_anchor"] = None

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_spot_nonfull_successor_revalidates_retained_full_anchor() -> None:
    """Retained spot evidence remains contingent on its certified anchor.

    Given: A non-full spot successor retaining a prior full result.
    When: Its present or missing retained anchor is revalidated.
    Then: The valid view keeps evidence, while the missing anchor is corrupt.
    """
    valid_context = _spot_incomplete_context()
    missing_context = _spot_incomplete_context()
    missing_context["spot_anchor"] = None

    valid_view = build_portfolio_reconciliation_view(valid_context, _NOW)

    assert valid_view.effective_status == "incomplete"
    assert valid_view.is_authoritative is False
    assert valid_view.expected == {"BTC-PERP": {"size": 1}}
    _assert_corrupt(build_portfolio_reconciliation_view(missing_context, _NOW))


def test_full_spot_state_requires_scope_sequence_lineage() -> None:
    """A full spot verdict cannot use a non-scope-sequence boundary.

    Given: Spot state and observation lineage naming another watermark kind.
    When: The shared write-and-read anchor validator runs.
    Then: The view fails closed before granting authority.
    """
    context = _spot_matched_context()
    _required_state(context)["source_watermark_kind"] = "venue_event_id"
    _current_observation(context)["source_watermark_kind"] = "venue_event_id"

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("public_id", "01980f9b-d000-7000-8000-000000000099"),
        ("wallet_public_id", "01980f9b-d000-7000-8000-000000000099"),
        ("exchange", "kraken"),
        ("mode", "paper"),
        ("source_watermark_kind", "venue_event_id"),
        ("source_watermark", 9002),
    ],
)
def test_full_spot_state_rejects_invalid_loaded_anchor(field: str, value: object) -> None:
    """Every loaded anchor identity and boundary mismatch fails closed.

    Given: A full spot state with one forged loaded-anchor field.
    When: Shared anchor lineage validation compares the durable boundary.
    Then: The view is corrupt and cannot expose authority or evidence.

    Args:
        field: Anchor field to forge.
        value: Persisted value that violates the referenced boundary.
    """
    context = _spot_matched_context()
    anchor = context["spot_anchor"]
    assert anchor is not None
    cast(dict[str, object], anchor)[field] = value

    _assert_corrupt(build_portfolio_reconciliation_view(context, _NOW))


def test_unexpected_or_inconsistent_open_episode_is_corrupt() -> None:
    """Only the exact active episode named by the state may be exposed.

    Given: An unexpected episode and an episode with a divergent mismatch count.
    When: Active episode lineage is revalidated.
    Then: Both contexts fail closed without surfacing episode evidence.
    """
    unexpected = _matched_context()
    unexpected["open_drift_episode"] = cast(
        PortfolioDriftEpisodeRow,
        _open_episode_context()["open_drift_episode"],
    )
    inconsistent = _open_episode_context()
    episode = inconsistent["open_drift_episode"]
    assert episode is not None
    episode["latest_full_mismatch_count"] = 4

    _assert_corrupt(build_portfolio_reconciliation_view(unexpected, _NOW))
    _assert_corrupt(build_portfolio_reconciliation_view(inconsistent, _NOW))


def test_naive_verdict_or_read_clock_is_corrupt() -> None:
    """Timezone-naive clocks cannot establish a trustworthy freshness boundary.

    Given: A naive stored verdict clock and a naive read clock.
    When: Freshness derivation attempts timezone-aware comparison.
    Then: Both variants fail closed as corrupt.
    """
    verdict_context = _matched_context()
    _required_state(verdict_context)["timestamp"] = _NOW.replace(tzinfo=None)

    _assert_corrupt(build_portfolio_reconciliation_view(verdict_context, _NOW))
    _assert_corrupt(
        build_portfolio_reconciliation_view(_matched_context(), _NOW.replace(tzinfo=None))
    )
