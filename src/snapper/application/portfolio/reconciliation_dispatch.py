"""Fail-closed dispatch for observer-triggered portfolio reconciliation."""

from datetime import datetime

from snapper.application.portfolio import futures_reconciliation
from snapper.data.repository import Repository
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import PortfolioReconciliationMethodConfigRow
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.messaging.schemas.data import PortfolioAccountState

_REAL_METHODS = frozenset({"futures_position", "spot_execution_replay", "margin_ledger_replay"})


def _bounded_error(error: Exception) -> str:
    """Render one non-empty S1-compatible failure reason."""
    return (str(error).strip() or "portfolio_reconciliation_dispatch_failed")[:512]


def _nonfull_evaluation(
    account: PortfolioAccountState,
    evaluated_at: datetime,
    method: str,
    status: str,
    error: str,
) -> PortfolioReconciliationEvaluationRow:
    """Build one method-scoped evaluation without lineage or retained evidence."""
    return {
        "wallet_public_id": account.wallet_public_id,
        "exchange": str(account.exchange).lower(),
        "mode": str(account.mode),
        "method": method,
        "evaluation_status": status,
        "venue_account_state_public_id": None,
        "venue_account_observation_id": None,
        "account_authoritative_until": None,
        "source_watermark_kind": None,
        "source_watermark": None,
        "anchor_public_id": None,
        "expected_json": None,
        "actual_json": None,
        "difference_json": None,
        "tolerance_json": None,
        "error": error[:512],
        "session_id": account.session_id,
        "sequence_id": account.sequence_id,
        "bus_time": evaluated_at,
    }


def _configured_method(
    account: PortfolioAccountState,
    config: PortfolioReconciliationMethodConfigRow | None,
) -> str:
    """Resolve only an exact, real, identity-matching operator config."""
    if config is None:
        return "unclassified"
    exchange = str(account.exchange).lower()
    mode = str(account.mode)
    if (
        config["wallet_public_id"] != account.wallet_public_id
        or config["exchange"] != exchange
        or config["mode"] != mode
        or config["method"] not in _REAL_METHODS
    ):
        return "unclassified"
    return config["method"]


def _account_has_spot_margin_signal(
    account: PortfolioAccountState,
    position_capability: CapabilityStatus,
) -> bool:
    """Check only authoritative liability and position evidence in the snapshot."""
    negative_balance = bool(
        account.is_authoritative
        and account.balances is not None
        and any(entry.total < 0 for entry in account.balances)
    )
    unexpected_positions = bool(
        account.is_authoritative and account.open_positions is not None and account.open_positions
    )
    return (
        negative_balance
        or unexpected_positions
        or position_capability is not CapabilityStatus.NOT_APPLICABLE
    )


async def dispatch_portfolio_reconciliation(
    repository: Repository,
    account: PortfolioAccountState,
    method_config: PortfolioReconciliationMethodConfigRow | None,
    position_capability: CapabilityStatus,
    evaluated_at: datetime,
) -> PortfolioReconciliationEvaluationRow:
    """Dispatch one account snapshot through its durable configured method.

    Args:
        repository: Durable reads needed by the selected method.
        account: Exact immutable venue-account snapshot to evaluate.
        method_config: Active operator-authored method config, when valid.
        position_capability: Capability captured with the account observation.
        evaluated_at: One shared evaluation and temporal-read instant.

    Returns:
        One S1-compatible method-scoped reconciliation evaluation.
    """
    method = _configured_method(account, method_config)
    if method == "unclassified":
        return _nonfull_evaluation(
            account,
            evaluated_at,
            "unclassified",
            "incomplete",
            "reconciliation_method_unclassified",
        )
    if method == "margin_ledger_replay":
        return _nonfull_evaluation(
            account,
            evaluated_at,
            method,
            "error",
            "margin_ledger_replay_not_implemented",
        )
    if method == "spot_execution_replay":
        durable_signal = await repository.has_spot_margin_reconciliation_signal(
            account.wallet_public_id,
            str(account.exchange).lower(),
            str(account.mode),
            evaluated_at,
        )
        reason = (
            "unsupported_margin"
            if durable_signal or _account_has_spot_margin_signal(account, position_capability)
            else "spot_boundary_unavailable"
        )
        return _nonfull_evaluation(
            account,
            evaluated_at,
            method,
            "incomplete",
            reason,
        )
    try:
        venue_symbols = {position.symbol for position in (account.open_positions or [])}
        bundle = await repository.get_futures_reconciliation_bundle(
            account.wallet_public_id,
            str(account.exchange).lower(),
            str(account.mode),
            evaluated_at,
            venue_symbols,
        )
        if bundle.error is not None:
            return _nonfull_evaluation(
                account,
                evaluated_at,
                method,
                "incomplete",
                bundle.error,
            )
        return futures_reconciliation.evaluate(
            bundle.projection,
            account,
            bundle.instrument_public_ids_by_symbol,
            bundle.specs_by_instrument_public_id,
            position_capability,
            evaluated_at,
        )
    except Exception as error:
        return _nonfull_evaluation(
            account,
            evaluated_at,
            method,
            "error",
            _bounded_error(error),
        )
