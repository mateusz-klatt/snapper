"""Tests for strict portfolio reconciliation response schemas."""

from datetime import UTC
from datetime import datetime

import pytest
from pydantic import ValidationError

from snapper.api.schemas.portfolio import PortfolioReconciliationDriftEpisode
from snapper.api.schemas.portfolio import PortfolioReconciliationView

_NOW = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)


def _episode_values() -> dict[str, object]:
    """Return a complete valid open-drift-episode payload."""
    return {
        "public_id": "episode-1",
        "status": "open",
        "opened_at": _NOW,
        "trigger_observation_id": 10,
        "last_observation_id": 12,
        "details_source_observation_id": 12,
        "latest_full_mismatch_count": 3,
    }


def _view_values() -> dict[str, object]:
    """Return a complete valid reconciliation-view payload."""
    return {
        "method": "futures_position",
        "evaluation_status": "mismatched",
        "effective_status": "mismatched",
        "is_authoritative": True,
        "evaluated_at": _NOW,
        "current_observation_id": 12,
        "last_full_observation_id": 12,
        "detail_source_observation_id": 12,
        "last_full_outcome": "mismatched",
        "consecutive_full_mismatches": 3,
        "anchor_public_id": None,
        "venue_account_state_public_id": "account-1",
        "venue_account_observation_id": 41,
        "source_watermark_kind": "venue_event_id",
        "source_watermark": 51,
        "expected": {"PF_XBTUSD": 2},
        "actual": {"PF_XBTUSD": 1},
        "difference": {"PF_XBTUSD": -1},
        "tolerance": {"PF_XBTUSD": 0},
        "reconciled_at": _NOW,
        "authoritative_until": _NOW,
        "error": None,
        "open_drift_episode": _episode_values(),
    }


def test_reconciliation_view_validates_nested_json_and_episode() -> None:
    """A complete typed view retains validated evidence and drift details.

    Given: a fully populated reconciliation payload with an open episode,
    When: the strict response schema validates it,
    Then: evidence remains structured and the episode is strongly typed.
    """
    view = PortfolioReconciliationView.model_validate(_view_values())
    assert view.expected == {"PF_XBTUSD": 2}
    assert isinstance(view.open_drift_episode, PortfolioReconciliationDriftEpisode)
    assert view.open_drift_episode.status == "open"


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        pytest.param("effective_status", "unknown", id="unknown_status"),
        pytest.param("expected", '{"PF_XBTUSD": 2}', id="raw_json_evidence"),
    ],
)
def test_reconciliation_view_rejects_invalid_contract_values(
    field: str,
    invalid_value: object,
) -> None:
    """Unknown status and raw stored JSON strings fail strict validation.

    Given: a complete view carrying one invalid contract value,
    When: the strict response schema validates it,
    Then: validation fails instead of coercing or exposing the value.

    Args:
        field: View field to replace.
        invalid_value: Invalid value supplied for that field.
    """
    values = _view_values()
    values[field] = invalid_value
    with pytest.raises(ValidationError):
        PortfolioReconciliationView.model_validate(values)


def test_reconciliation_view_requires_every_field_and_forbids_extras() -> None:
    """Missing or undeclared fields cannot enter the response contract.

    Given: otherwise valid views with a missing or extra field,
    When: the strict response schema validates them,
    Then: both shapes are rejected.
    """
    missing = _view_values()
    del missing["tolerance"]
    with pytest.raises(ValidationError):
        PortfolioReconciliationView.model_validate(missing)
    extra = _view_values()
    extra["unknown_field"] = True
    with pytest.raises(ValidationError):
        PortfolioReconciliationView.model_validate(extra)


def test_drift_episode_accepts_only_open_status_and_strict_counts() -> None:
    """A surfaced drift episode cannot masquerade as open or coerce counts.

    Given: episode payloads with a resolved status or string count,
    When: the strict nested schema validates them,
    Then: both invalid episode shapes are rejected.
    """
    resolved = _episode_values()
    resolved["status"] = "resolved"
    with pytest.raises(ValidationError):
        PortfolioReconciliationDriftEpisode.model_validate(resolved)
    coerced = _episode_values()
    coerced["latest_full_mismatch_count"] = "3"
    with pytest.raises(ValidationError):
        PortfolioReconciliationDriftEpisode.model_validate(coerced)
