"""Fail-closed read projection for durable portfolio reconciliation truth."""

from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import Literal
from typing import cast

from pydantic import ConfigDict
from pydantic import TypeAdapter
from pydantic import ValidationError

from snapper.api.schemas.portfolio import PortfolioReconciliationDriftEpisode
from snapper.api.schemas.portfolio import PortfolioReconciliationEffectiveStatus
from snapper.api.schemas.portfolio import PortfolioReconciliationEvaluationStatus
from snapper.api.schemas.portfolio import PortfolioReconciliationView
from snapper.application.portfolio.reconciliation_invariants import (
    validate_portfolio_reconciliation_evaluation_config,
)
from snapper.application.portfolio.reconciliation_invariants import (
    validate_portfolio_reconciliation_latest_observation_lineage,
)
from snapper.application.portfolio.reconciliation_invariants import (
    validate_portfolio_reconciliation_method_transition,
)
from snapper.application.portfolio.reconciliation_invariants import (
    validate_portfolio_reconciliation_observation_lineage,
)
from snapper.application.portfolio.reconciliation_invariants import (
    validate_portfolio_reconciliation_spot_anchor_lineage,
)
from snapper.application.portfolio.reconciliation_methods import PortfolioReconciliationMethod
from snapper.core.json_types import JsonObject
from snapper.data.repository_types import PortfolioDriftEpisodeRow
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import PortfolioReconciliationLineageObservationRow
from snapper.data.repository_types import PortfolioReconciliationReadContextRow
from snapper.data.repository_types import PortfolioReconciliationStateRow

PORTFOLIO_RECONCILIATION_STALE_AFTER: Final[timedelta] = timedelta(seconds=900)
PORTFOLIO_RECONCILIATION_FUTURE_TOLERANCE: Final[timedelta] = timedelta(seconds=5)
_JSON_OBJECT_ADAPTER: Final[TypeAdapter[JsonObject]] = TypeAdapter(
    JsonObject,
    config=ConfigDict(strict=True, allow_inf_nan=False),
)
_CORRUPT_ERROR: Final[str] = "persisted reconciliation state failed read-time validation"


def _empty_reconciliation_view(
    effective_status: PortfolioReconciliationEffectiveStatus,
    error: str | None,
) -> PortfolioReconciliationView:
    """Return an evidence-free fail-closed reconciliation view."""
    return PortfolioReconciliationView(
        method=None,
        evaluation_status=None,
        effective_status=effective_status,
        is_authoritative=False,
        evaluated_at=None,
        current_observation_id=None,
        last_full_observation_id=None,
        detail_source_observation_id=None,
        last_full_outcome=None,
        consecutive_full_mismatches=0,
        anchor_public_id=None,
        venue_account_state_public_id=None,
        venue_account_observation_id=None,
        source_watermark_kind=None,
        source_watermark=None,
        expected=None,
        actual=None,
        difference=None,
        tolerance=None,
        reconciled_at=None,
        authoritative_until=None,
        error=error,
        open_drift_episode=None,
    )


def no_portfolio_reconciliation_view() -> PortfolioReconciliationView:
    """Return the well-defined fail-closed view for an account without state.

    Returns:
        Evidence-free incomplete reconciliation truth.
    """
    return _empty_reconciliation_view("incomplete", None)


def _evaluation_for_observation(
    observation_id: int,
    observations: list[PortfolioReconciliationLineageObservationRow],
) -> PortfolioReconciliationEvaluationRow:
    """Project one referenced observation to the shared validator input."""
    observations_by_id = {observation["id"]: observation for observation in observations}
    observation = observations_by_id[observation_id]
    return {
        "wallet_public_id": observation["wallet_public_id"],
        "exchange": observation["exchange"],
        "mode": observation["mode"],
        "method": observation["method"],
        "evaluation_status": observation["evaluation_status"],
        "venue_account_state_public_id": observation["venue_account_state_public_id"],
        "venue_account_observation_id": observation["venue_account_observation_id"],
        "account_authoritative_until": observation["account_authoritative_until"],
        "source_watermark_kind": observation["source_watermark_kind"],
        "source_watermark": observation["source_watermark"],
        "anchor_public_id": observation["anchor_public_id"],
        "expected_json": observation["expected_json"],
        "actual_json": observation["actual_json"],
        "difference_json": observation["difference_json"],
        "tolerance_json": observation["tolerance_json"],
        "error": observation["error"],
        "session_id": observation["session_id"],
        "sequence_id": observation["sequence_id"],
        "bus_time": observation["timestamp"],
    }


def _validate_context_identity(context: PortfolioReconciliationReadContextRow) -> None:
    """Require every loaded durable row to belong to the rendered account."""
    state = cast(PortfolioReconciliationStateRow, context["state"])
    account = context["account_state"]
    identity = (account["wallet_public_id"], account["exchange"], account["mode"])
    if (state["wallet_public_id"], state["exchange"], state["mode"]) != identity:
        raise RuntimeError("reconciliation read context identity is inconsistent")
    config = context["config"]
    if (
        config is not None
        and (
            config["wallet_public_id"],
            config["exchange"],
            config["mode"],
        )
        != identity
    ):
        raise RuntimeError("reconciliation read context config identity is inconsistent")


def _validate_state_authority_shape(state: PortfolioReconciliationStateRow) -> None:
    """Require the structural prerequisites for exposing retained evidence."""
    status = state["current_evaluation_status"]
    detail_fields = (
        state["last_full_observation_id"],
        state["last_full_outcome"],
        state["detail_source_observation_id"],
        state["venue_account_state_public_id"],
        state["venue_account_observation_id"],
        state["source_watermark_kind"],
        state["source_watermark"],
        state["expected_json"],
        state["actual_json"],
        state["difference_json"],
        state["tolerance_json"],
        state["reconciled_at"],
        state["authoritative_until"],
    )
    has_detail = all(value is not None for value in detail_fields)
    if has_detail != any(value is not None for value in detail_fields):
        raise RuntimeError("reconciliation state retained evidence is incomplete")
    if has_detail and state["detail_source_observation_id"] != state["last_full_observation_id"]:
        raise RuntimeError("reconciliation state retained evidence lineage is inconsistent")
    if status in ("matched", "mismatched") and (
        not has_detail
        or state["current_observation_id"] != state["last_full_observation_id"]
        or state["current_observation_id"] != state["detail_source_observation_id"]
        or status != state["last_full_outcome"]
    ):
        raise RuntimeError("current full reconciliation evidence is incomplete")
    if state["last_full_outcome"] == "matched" and (
        state["consecutive_full_mismatches"] != 0
        or state["open_drift_episode_public_id"] is not None
    ):
        raise RuntimeError("matched reconciliation state retains mismatch evidence")
    if status == "matched" and state["error"] is not None:
        raise RuntimeError("matched reconciliation state retains error evidence")
    if state["last_full_outcome"] == "mismatched" and (state["consecutive_full_mismatches"] < 1):
        raise RuntimeError("mismatched reconciliation state has no mismatch evidence")
    if state["last_full_outcome"] is None and (
        state["consecutive_full_mismatches"] != 0
        or state["open_drift_episode_public_id"] is not None
    ):
        raise RuntimeError("reconciliation state without a full result retains evidence")
    open_episode_expected = (
        state["last_full_outcome"] == "mismatched" and state["consecutive_full_mismatches"] >= 3
    )
    if (state["open_drift_episode_public_id"] is not None) != open_episode_expected:
        raise RuntimeError("reconciliation state drift episode threshold is inconsistent")


def _parse_evidence(raw: str | None) -> JsonObject | None:
    """Parse one retained JSON object with strict finite-number validation."""
    if raw is None:
        return None
    return _JSON_OBJECT_ADAPTER.validate_json(raw, strict=True)


def _build_open_episode(
    state: PortfolioReconciliationStateRow,
    episode: PortfolioDriftEpisodeRow | None,
) -> PortfolioReconciliationDriftEpisode | None:
    """Validate and project the active drift episode named by the state."""
    expected_public_id = state["open_drift_episode_public_id"]
    if expected_public_id is None:
        if episode is not None:
            raise RuntimeError("reconciliation state has an unexpected open drift episode")
        return None
    if episode is None:
        raise RuntimeError("reconciliation state has no active drift episode")
    if (
        episode["public_id"] != expected_public_id
        or episode["status"] != "open"
        or episode["wallet_public_id"] != state["wallet_public_id"]
        or episode["exchange"] != state["exchange"]
        or episode["mode"] != state["mode"]
        or episode["latest_full_mismatch_count"] != state["consecutive_full_mismatches"]
        or episode["last_observation_id"] != state["last_full_observation_id"]
        or episode["details_source_observation_id"] != state["detail_source_observation_id"]
    ):
        raise RuntimeError("active drift episode lineage does not match reconciliation state")
    return PortfolioReconciliationDriftEpisode(
        public_id=episode["public_id"],
        status="open",
        opened_at=episode["opened_at"],
        trigger_observation_id=episode["trigger_observation_id"],
        last_observation_id=episode["last_observation_id"],
        details_source_observation_id=episode["details_source_observation_id"],
        latest_full_mismatch_count=episode["latest_full_mismatch_count"],
    )


def _derive_effective_status(
    status: PortfolioReconciliationEvaluationStatus,
    evaluated_at: datetime,
    now: datetime,
) -> PortfolioReconciliationEffectiveStatus:
    """Apply future-clock and 900-second freshness demotions."""
    if evaluated_at.tzinfo is None or now.tzinfo is None:
        raise ValueError("reconciliation view requires timezone-aware timestamps")
    if evaluated_at > now + PORTFOLIO_RECONCILIATION_FUTURE_TOLERANCE:
        return "clock_error"
    if now - evaluated_at > PORTFOLIO_RECONCILIATION_STALE_AFTER:
        return "stale"
    return status


def build_portfolio_reconciliation_view(
    context: PortfolioReconciliationReadContextRow,
    now: datetime,
) -> PortfolioReconciliationView:
    """Build one fully revalidated, fail-closed reconciliation read view.

    Args:
        context: Batched durable state, lineage, config, and episode context.
        now: Read instant used for deterministic freshness derivation.

    Returns:
        Strict reconciliation view. Any validation failure returns ``corrupt``
        with all evidence cleared and authority denied.
    """
    state = context["state"]
    if state is None:
        if (
            context["observations"]
            or context["latest_ordered_observation_id"] is not None
            or context["latest_appended_observation_id"] is not None
            or context["open_drift_episode"] is not None
            or context["spot_anchor"] is not None
        ):
            return _empty_reconciliation_view("corrupt", _CORRUPT_ERROR)
        return no_portfolio_reconciliation_view()
    try:
        _validate_context_identity(context)
        validate_portfolio_reconciliation_observation_lineage(
            state,
            context["observations"],
        )
        validate_portfolio_reconciliation_latest_observation_lineage(
            state,
            context["latest_ordered_observation_id"],
            context["latest_appended_observation_id"],
        )
        evaluation = _evaluation_for_observation(
            state["current_observation_id"],
            context["observations"],
        )
        validate_portfolio_reconciliation_evaluation_config(evaluation, context["config"])
        validate_portfolio_reconciliation_method_transition(state, evaluation, context["config"])
        detail_source_observation_id = state["detail_source_observation_id"]
        anchor_evaluation = (
            evaluation
            if detail_source_observation_id is None
            else _evaluation_for_observation(
                detail_source_observation_id,
                context["observations"],
            )
        )
        validate_portfolio_reconciliation_spot_anchor_lineage(
            anchor_evaluation,
            context["spot_anchor"],
        )
        _validate_state_authority_shape(state)
        expected = _parse_evidence(state["expected_json"])
        actual = _parse_evidence(state["actual_json"])
        difference = _parse_evidence(state["difference_json"])
        tolerance = _parse_evidence(state["tolerance_json"])
        open_episode = _build_open_episode(state, context["open_drift_episode"])
        method = cast(PortfolioReconciliationMethod, state["method"])
        status = cast(
            PortfolioReconciliationEvaluationStatus,
            state["current_evaluation_status"],
        )
        effective_status = _derive_effective_status(status, state["timestamp"], now)
        is_current_full = (
            status in ("matched", "mismatched")
            and state["current_observation_id"] == state["last_full_observation_id"]
            and state["current_observation_id"] == state["detail_source_observation_id"]
        )
        return PortfolioReconciliationView(
            method=method,
            evaluation_status=status,
            effective_status=effective_status,
            is_authoritative=(effective_status in ("matched", "mismatched") and is_current_full),
            evaluated_at=state["timestamp"],
            current_observation_id=state["current_observation_id"],
            last_full_observation_id=state["last_full_observation_id"],
            detail_source_observation_id=state["detail_source_observation_id"],
            last_full_outcome=cast(
                Literal["matched", "mismatched"] | None,
                state["last_full_outcome"],
            ),
            consecutive_full_mismatches=state["consecutive_full_mismatches"],
            anchor_public_id=state["anchor_public_id"],
            venue_account_state_public_id=state["venue_account_state_public_id"],
            venue_account_observation_id=state["venue_account_observation_id"],
            source_watermark_kind=state["source_watermark_kind"],
            source_watermark=state["source_watermark"],
            expected=expected,
            actual=actual,
            difference=difference,
            tolerance=tolerance,
            reconciled_at=state["reconciled_at"],
            authoritative_until=state["authoritative_until"],
            error=state["error"],
            open_drift_episode=open_episode,
        )
    except (AttributeError, KeyError, RuntimeError, TypeError, ValueError, ValidationError):
        return _empty_reconciliation_view("corrupt", _CORRUPT_ERROR)
