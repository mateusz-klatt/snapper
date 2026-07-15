"""Pure invariant validation for durable portfolio reconciliation truth."""

from typing import Final

from snapper.application.portfolio.reconciliation_methods import RealPortfolioReconciliationMethod
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import PortfolioReconciliationLineageObservationRow
from snapper.data.repository_types import PortfolioReconciliationMethodConfigRow
from snapper.data.repository_types import PortfolioReconciliationStateRow
from snapper.data.repository_types import SpotReconciliationAnchorRow

_REAL_PORTFOLIO_RECONCILIATION_METHODS: Final[tuple[RealPortfolioReconciliationMethod, ...]] = (
    "futures_position",
    "spot_execution_replay",
    "margin_ledger_replay",
)


def portfolio_reconciliation_evaluation_has_no_nonfull_evidence(
    evaluation: PortfolioReconciliationEvaluationRow,
) -> bool:
    """Return whether a non-full-only method carries no forbidden evidence.

    Args:
        evaluation: Raw reconciliation evaluation to inspect.

    Returns:
        Whether every full-evidence field is absent.
    """
    return all(
        evaluation[field] is None
        for field in (
            "venue_account_state_public_id",
            "venue_account_observation_id",
            "account_authoritative_until",
            "source_watermark_kind",
            "source_watermark",
            "anchor_public_id",
            "expected_json",
            "actual_json",
            "difference_json",
            "tolerance_json",
        )
    )


def unclassified_state_has_no_retained_evidence(
    state: PortfolioReconciliationStateRow,
) -> bool:
    """Return whether an unclassified state can transition to its first config.

    Args:
        state: Reconciliation state to inspect.

    Returns:
        Whether the state is unclassified, non-full, and evidence-free.
    """
    return (
        state["method"] == "unclassified"
        and state["current_evaluation_status"] in ("incomplete", "error")
        and state["last_full_observation_id"] is None
        and state["last_full_outcome"] is None
        and state["detail_source_observation_id"] is None
        and state["consecutive_full_mismatches"] == 0
        and state["open_drift_episode_public_id"] is None
        and state["anchor_public_id"] is None
        and state["venue_account_state_public_id"] is None
        and state["venue_account_observation_id"] is None
        and state["source_watermark_kind"] is None
        and state["source_watermark"] is None
        and state["expected_json"] is None
        and state["actual_json"] is None
        and state["difference_json"] is None
        and state["tolerance_json"] is None
        and state["reconciled_at"] is None
        and state["authoritative_until"] is None
    )


def validate_portfolio_reconciliation_evaluation_config(
    evaluation: PortfolioReconciliationEvaluationRow,
    config: PortfolioReconciliationMethodConfigRow | None,
) -> None:
    """Fail closed on invalid status, config mismatch, or non-full evidence.

    Args:
        evaluation: Raw reconciliation evaluation to validate.
        config: Active durable method config, if one exists.

    Returns:
        None.

    Raises:
        RuntimeError: If identity, method, status, evidence, error detail, or
            active config is inconsistent.
    """
    method = evaluation["method"]
    status = evaluation["evaluation_status"]
    exchange = evaluation["exchange"]
    if evaluation["mode"] != "live" or not exchange or exchange.strip().lower() != exchange:
        raise RuntimeError("portfolio reconciliation identity is invalid")
    if method in ("futures_position", "spot_execution_replay"):
        if status not in ("matched", "mismatched", "incomplete", "unsupported", "error"):
            raise RuntimeError("portfolio reconciliation status is incompatible with method")
    elif method == "margin_ledger_replay":
        if status != "error":
            raise RuntimeError("margin ledger reconciliation permits only error status")
        if not portfolio_reconciliation_evaluation_has_no_nonfull_evidence(evaluation):
            raise RuntimeError("margin ledger reconciliation cannot carry full evidence")
    elif method == "unclassified":
        if status not in ("incomplete", "error"):
            raise RuntimeError("unclassified reconciliation status is invalid")
        if not portfolio_reconciliation_evaluation_has_no_nonfull_evidence(evaluation):
            raise RuntimeError("unclassified reconciliation cannot carry full evidence")
    else:
        raise RuntimeError("portfolio reconciliation method is invalid")
    if status == "error" and not (evaluation["error"] or "").strip():
        raise RuntimeError("error reconciliation requires a non-empty reason")
    if method == "unclassified":
        if config is not None:
            raise RuntimeError("unclassified reconciliation conflicts with active config")
    elif config is None or config["method"] != method:
        raise RuntimeError("reconciliation evaluation conflicts with active method config")


def validate_portfolio_reconciliation_method_transition(
    existing: PortfolioReconciliationStateRow | None,
    evaluation: PortfolioReconciliationEvaluationRow,
    config: PortfolioReconciliationMethodConfigRow | None,
) -> None:
    """Permit only a safe unclassified-to-first-real-method transition.

    Args:
        existing: Active predecessor state, if one exists.
        evaluation: Incoming raw reconciliation evaluation.
        config: Active durable method config, if one exists.

    Returns:
        None.

    Raises:
        RuntimeError: If retained evidence or the method transition is invalid.
    """
    if existing is None:
        return
    incoming_method = evaluation["method"]
    if existing["method"] == incoming_method:
        if existing["method"] == "unclassified" and not (
            unclassified_state_has_no_retained_evidence(existing)
        ):
            raise RuntimeError("unclassified reconciliation state retains forbidden evidence")
        if existing["method"] == "margin_ledger_replay" and (
            existing["last_full_observation_id"] is not None
            or existing["last_full_outcome"] is not None
            or existing["detail_source_observation_id"] is not None
            or existing["consecutive_full_mismatches"] != 0
            or existing["open_drift_episode_public_id"] is not None
            or existing["anchor_public_id"] is not None
            or existing["venue_account_state_public_id"] is not None
            or existing["venue_account_observation_id"] is not None
            or existing["source_watermark_kind"] is not None
            or existing["source_watermark"] is not None
            or existing["expected_json"] is not None
            or existing["actual_json"] is not None
            or existing["difference_json"] is not None
            or existing["tolerance_json"] is not None
            or existing["reconciled_at"] is not None
            or existing["authoritative_until"] is not None
        ):
            raise RuntimeError("margin ledger reconciliation state retains forbidden evidence")
        return
    if (
        existing["method"] == "unclassified"
        and incoming_method in _REAL_PORTFOLIO_RECONCILIATION_METHODS
        and config is not None
        and config["method"] == incoming_method
        and unclassified_state_has_no_retained_evidence(existing)
    ):
        return
    raise RuntimeError("portfolio reconciliation method transition is invalid")


def validate_portfolio_reconciliation_observation_lineage(
    existing: PortfolioReconciliationStateRow,
    observations: list[PortfolioReconciliationLineageObservationRow],
) -> None:
    """Validate predecessor observation references, identity, and metadata.

    Args:
        existing: Active predecessor reconciliation state.
        observations: Observations loaded for every reference retained by the state.

    Returns:
        None.

    Raises:
        RuntimeError: If any referenced observation is missing, foreign, or
            inconsistent with retained state metadata.
    """
    observation_ids = {
        observation_id
        for observation_id in (
            existing["current_observation_id"],
            existing["last_full_observation_id"],
            existing["detail_source_observation_id"],
        )
        if observation_id is not None
    }
    observations_by_id = {observation["id"]: observation for observation in observations}
    if set(observations_by_id) != observation_ids:
        raise RuntimeError("reconciliation state references a missing observation")
    for observation in observations:
        if (
            observation["wallet_public_id"] != existing["wallet_public_id"]
            or observation["exchange"] != existing["exchange"]
            or observation["mode"] != existing["mode"]
        ):
            raise RuntimeError("reconciliation state references a foreign observation")
    current = observations_by_id[existing["current_observation_id"]]
    if (
        current["method"] != existing["method"]
        or current["evaluation_status"] != existing["current_evaluation_status"]
        or current["resulting_full_mismatch_count"] != existing["consecutive_full_mismatches"]
        or current["drift_episode_public_id"] != existing["open_drift_episode_public_id"]
        or current["error"] != existing["error"]
        or current["session_id"] != existing["session_id"]
        or current["sequence_id"] != existing["sequence_id"]
    ):
        raise RuntimeError("reconciliation state current observation metadata is inconsistent")
    if existing["last_full_observation_id"] is not None:
        last_full = observations_by_id[existing["last_full_observation_id"]]
        if (
            last_full["evaluation_status"] != existing["last_full_outcome"]
            or last_full["resulting_full_mismatch_count"] != existing["consecutive_full_mismatches"]
            or last_full["drift_episode_public_id"] != existing["open_drift_episode_public_id"]
        ):
            raise RuntimeError("reconciliation state last-full observation is inconsistent")
    if existing["detail_source_observation_id"] is not None:
        detail = observations_by_id[existing["detail_source_observation_id"]]
        if (
            detail["resulting_full_mismatch_count"] != existing["consecutive_full_mismatches"]
            or detail["drift_episode_public_id"] != existing["open_drift_episode_public_id"]
            or detail["venue_account_state_public_id"] != existing["venue_account_state_public_id"]
            or detail["venue_account_observation_id"] != existing["venue_account_observation_id"]
            or detail["source_watermark_kind"] != existing["source_watermark_kind"]
            or detail["source_watermark"] != existing["source_watermark"]
            or detail["anchor_public_id"] != existing["anchor_public_id"]
            or detail["expected_json"] != existing["expected_json"]
            or detail["actual_json"] != existing["actual_json"]
            or detail["difference_json"] != existing["difference_json"]
            or detail["tolerance_json"] != existing["tolerance_json"]
            or detail["timestamp"] != existing["reconciled_at"]
            or detail["account_authoritative_until"] != existing["authoritative_until"]
        ):
            raise RuntimeError("reconciliation state detail observation is inconsistent")


def validate_portfolio_reconciliation_latest_observation_lineage(
    existing: PortfolioReconciliationStateRow,
    latest_ordered_observation_id: int | None,
    latest_appended_observation_id: int | None,
) -> None:
    """Validate that predecessor state references both notions of latest evidence.

    Args:
        existing: Active predecessor reconciliation state.
        latest_ordered_observation_id: Latest identity by evaluation ordering.
        latest_appended_observation_id: Latest identity by append ordering.

    Returns:
        None.

    Raises:
        RuntimeError: If the active state does not reference both latest identities.
    """
    if (
        existing["current_observation_id"] != latest_ordered_observation_id
        or existing["current_observation_id"] != latest_appended_observation_id
    ):
        raise RuntimeError("reconciliation state does not reference the latest observation")


def validate_portfolio_reconciliation_spot_anchor_lineage(
    evaluation: PortfolioReconciliationEvaluationRow,
    anchor: SpotReconciliationAnchorRow | None,
) -> None:
    """Validate the certified anchor boundary for one full spot result.

    Args:
        evaluation: Full evaluation whose anchor lineage must be certified.
        anchor: Active durable anchor referenced by the evaluation, if found.

    Returns:
        None.

    Raises:
        RuntimeError: If required anchor lineage is absent, foreign, or ahead
            of the evaluation boundary.
    """
    if evaluation["method"] != "spot_execution_replay" or evaluation["evaluation_status"] not in (
        "matched",
        "mismatched",
    ):
        return
    anchor_public_id = evaluation["anchor_public_id"]
    source_watermark = evaluation["source_watermark"]
    if anchor_public_id is None or source_watermark is None:
        raise RuntimeError("full spot reconciliation requires anchor lineage")
    if evaluation["source_watermark_kind"] != "execution_id":
        raise RuntimeError("full spot reconciliation requires execution-id lineage")
    if (
        anchor is None
        or anchor["public_id"] != anchor_public_id
        or anchor["wallet_public_id"] != evaluation["wallet_public_id"]
        or anchor["exchange"] != evaluation["exchange"]
        or anchor["mode"] != evaluation["mode"]
        or anchor["source_watermark_kind"] != "execution_id"
        or anchor["source_watermark"] > source_watermark
    ):
        raise RuntimeError("spot reconciliation anchor lineage is invalid")
