"""Fail-closed dispatch for observer-triggered portfolio reconciliation."""

from dataclasses import dataclass
from datetime import datetime

from snapper.application.portfolio import futures_reconciliation
from snapper.application.portfolio import spot_reconciliation
from snapper.application.portfolio.spot_reconciliation import SpotInstrumentIdentity
from snapper.application.portfolio.spot_reconciliation import SpotReplayBoundary
from snapper.application.portfolio.spot_reconciliation import SpotReplayExecutionRow
from snapper.application.portfolio.walutomat_history_certificate import CertificateOutcome
from snapper.application.portfolio.walutomat_history_certificate import SpotHistoryRangeCapture
from snapper.application.portfolio.walutomat_history_certificate import (
    certify_walutomat_history_range,
)
from snapper.data.repository import Repository
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import PortfolioReconciliationMethodConfigRow
from snapper.data.repository_types import SpotReconciliationBundle
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.messaging.schemas.data import PortfolioAccountState

_REAL_METHODS = frozenset({"futures_position", "spot_execution_replay", "margin_ledger_replay"})

_UNCERTIFIED_BOUNDARY_REASON = "uncertified_boundary"
"""The spot evaluator's generic cursor-gate incomplete reason.

The evaluator raises it when a computably matched outcome is blocked only by
an uncertified venue cursor; dispatch substitutes the certificate's first
named refusal for exactly this reason and no other, so a specific certificate
failure is observable without ever rewriting evaluator-owned classifications.
"""


def _require_boundary(condition: bool, reason: str) -> None:
    """Reject one incoherent boundary-capture relationship with its named reason."""
    if not condition:
        raise ValueError(reason)


def _require_aware_boundary_instant(value: datetime, reason: str) -> None:
    """Reject one naive boundary-capture instant with its named reason."""
    _require_boundary(value.utcoffset() is not None, reason)


@dataclass(frozen=True)
class SpotReplayBoundaryCapture:
    """One validated observer-captured pre-balance execution watermark.

    Shaped after ``SpotReplayBoundary`` (the spot evaluator input) but owned
    by the observer/dispatch plumbing: it carries only what the observation
    cycle genuinely measured, and construction itself is the validated
    factory — ``__post_init__`` rejects every incoherent relationship, so an
    unvalidated capture cannot exist. The identity fields bind the capture
    to exactly one account-state version (the same
    ``(wallet, exchange, mode, session, sequence)`` identity the
    reconciliation runner checks); dispatch refuses to honor a boundary
    whose binding does not match the account it is evaluating, so a foreign
    or stale-cycle capture can never certify another snapshot.

    ``source_watermark`` is the scoped committed ``max(scope_sequence)``
    read BEFORE the balance request started, with its
    ``watermark_captured_at`` instant; the enforced ordering ``as_of <=
    watermark_captured_at <= request_started_at <= request_completed_at <=
    watermark_after_captured_at`` is the pre-balance certification — it is
    a validated invariant of the type, not a caller-set flag. The read is
    unlocked and sealed by construction: the counter is allocated as
    committed max + 1 under the ingest-side execution fence, so no row at
    or below the watermark can commit after the capture. ``as_of`` does
    not define scope membership (the stored scope columns do); the S4c-4
    replay bundle must pin its temporal reads (instrument identity, specs,
    asset precisions) to this exact instant. ``watermark_after`` is the
    same scoped read taken after the balance and position reads completed
    (``None`` when that read failed), and ``watermark_unchanged`` is
    venue-quiescence evidence — True exactly when both reads succeeded and
    were equal; it is NOT a sealing check (contiguity seals the prefix).
    This capture type deliberately carries no ``range_complete``: the
    S4c-4 replay bundle owns range certification, and the capture must not
    even be able to claim it.
    """

    wallet_public_id: str
    exchange: str
    mode: str
    session_id: str
    sequence_id: int
    source_watermark: int
    as_of: datetime
    watermark_captured_at: datetime
    request_started_at: datetime
    request_completed_at: datetime
    watermark_after: int | None
    watermark_after_captured_at: datetime | None
    watermark_unchanged: bool

    def __post_init__(self) -> None:
        """Reject any capture whose fields do not form one coherent observation."""
        _require_boundary(bool(self.wallet_public_id), "spot_boundary_wallet_unbound")
        _require_boundary(bool(self.exchange), "spot_boundary_exchange_unbound")
        _require_boundary(bool(self.mode), "spot_boundary_mode_unbound")
        _require_boundary(bool(self.session_id), "spot_boundary_session_unbound")
        _require_boundary(self.source_watermark >= 0, "spot_boundary_negative_watermark")
        _require_aware_boundary_instant(self.as_of, "spot_boundary_naive_as_of")
        _require_aware_boundary_instant(
            self.watermark_captured_at, "spot_boundary_naive_watermark_captured_at"
        )
        _require_aware_boundary_instant(
            self.request_started_at, "spot_boundary_naive_request_started_at"
        )
        _require_aware_boundary_instant(
            self.request_completed_at, "spot_boundary_naive_request_completed_at"
        )
        _require_boundary(
            self.as_of <= self.watermark_captured_at,
            "spot_boundary_as_of_after_capture",
        )
        _require_boundary(
            self.watermark_captured_at <= self.request_started_at,
            "spot_boundary_watermark_not_pre_balance",
        )
        _require_boundary(
            self.request_started_at <= self.request_completed_at,
            "spot_boundary_request_window_inverted",
        )
        _require_boundary(
            (self.watermark_after is None) == (self.watermark_after_captured_at is None),
            "spot_boundary_after_read_incoherent",
        )
        if self.watermark_after is not None and self.watermark_after_captured_at is not None:
            _require_aware_boundary_instant(
                self.watermark_after_captured_at,
                "spot_boundary_naive_watermark_after_captured_at",
            )
            _require_boundary(
                self.request_completed_at <= self.watermark_after_captured_at,
                "spot_boundary_after_capture_not_post_balance",
            )
            _require_boundary(
                self.watermark_after >= self.source_watermark,
                "spot_boundary_after_watermark_regressed",
            )
        _require_boundary(
            self.watermark_unchanged
            == (self.watermark_after is not None and self.watermark_after == self.source_watermark),
            "spot_boundary_unchanged_flag_inconsistent",
        )


@dataclass(frozen=True)
class _DispatchContext:
    """Dependencies and immutable evidence for one method dispatch."""

    repository: Repository
    account: PortfolioAccountState
    method: str
    position_capability: CapabilityStatus
    evaluated_at: datetime
    boundary: SpotReplayBoundaryCapture | None
    history_capture: SpotHistoryRangeCapture | None


def _boundary_bound_to_account(
    boundary: SpotReplayBoundaryCapture,
    account: PortfolioAccountState,
) -> bool:
    """Return whether the boundary binds to this exact account-state version.

    A capture certifies exactly one observation cycle; honoring a boundary
    whose ``(wallet, exchange, mode, session, sequence)`` identity does not
    match the evaluated snapshot would let a foreign or stale-cycle capture
    masquerade as this account's pre-balance evidence.
    """
    return (
        boundary.wallet_public_id == account.wallet_public_id
        and boundary.exchange == str(account.exchange).lower()
        and boundary.mode == str(account.mode)
        and boundary.session_id == account.session_id
        and boundary.sequence_id == account.sequence_id
    )


def _account_balance_read_is_current(account: PortfolioAccountState) -> bool:
    """Return whether the snapshot carries a genuinely current balance read.

    A boundary brackets a venue balance request, so it is meaningless when
    the cycle produced no successful CURRENT read: an unsupported capability
    returns without any venue call, and an errored or timed-out read leaves
    an older RETAINED payload visible. Requiring ``balance_status ==
    "observed"`` AND the payload-source observation to be the current
    attempt closes both holes.
    """
    observation_id = account.current_attempt_observation_id
    return (
        account.balance_status == "observed"
        and observation_id is not None
        and account.balance_payload_source_observation_id == observation_id
    )


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


def _certify_spot_history(
    context: _DispatchContext,
    bundle: SpotReconciliationBundle,
) -> CertificateOutcome | None:
    """Certify matching history evidence when it belongs to the anchor epoch."""
    history_capture = context.history_capture
    boundary = context.boundary
    if (
        history_capture is None
        or bundle.anchor is None
        or boundary is None
        or history_capture.anchor_watermark != bundle.anchor["source_watermark"]
    ):
        return None
    return certify_walutomat_history_range(
        evidence=history_capture.evidence,
        anchor=bundle.anchor,
        venue_balances=history_capture.venue_balances,
        parsed_executions=history_capture.parsed_executions,
        order_totals=history_capture.order_totals,
        watermark_unchanged=boundary.watermark_unchanged,
        boundary_watermark=boundary.source_watermark,
    )


def _spot_replay_boundary(
    context: _DispatchContext,
    bundle: SpotReconciliationBundle,
    outcome: CertificateOutcome | None,
) -> SpotReplayBoundary:
    """Build the evaluator boundary from observer and certificate evidence."""
    boundary = context.boundary
    if boundary is None:
        raise ValueError("spot_boundary_unavailable")
    venue_cursor_certified = outcome is not None and outcome.certified
    venue_cursor = outcome.venue_cursor if venue_cursor_certified and outcome is not None else None
    inventory_complete = venue_cursor_certified or (
        bool(context.account.balances)
        and context.account.is_authoritative
        and bundle.anchor is not None
        and bundle.anchor["inventory_status"] == "venue_reported_full"
    )
    return SpotReplayBoundary(
        source_watermark=boundary.source_watermark,
        range_complete=bundle.range_complete,
        watermark_captured_before_balance=True,
        request_started_at=boundary.request_started_at,
        request_completed_at=boundary.request_completed_at,
        venue_cursor=venue_cursor,
        venue_cursor_certified=venue_cursor_certified,
        inventory_complete=inventory_complete,
        inventory_truncated=False,
    )


def _apply_spot_certificate_result(
    evaluation: PortfolioReconciliationEvaluationRow,
    bundle: SpotReconciliationBundle,
    outcome: CertificateOutcome | None,
) -> PortfolioReconciliationEvaluationRow:
    """Attach the certificate refusal or confirmed chain tip when applicable."""
    if (
        outcome is not None
        and outcome.refusals
        and evaluation["evaluation_status"] == "incomplete"
        and evaluation["error"] == _UNCERTIFIED_BOUNDARY_REASON
    ):
        evaluation["error"] = outcome.refusals[0][:512]
    if bundle.boundary_chain_tip is not None and evaluation["evaluation_status"] in (
        "matched",
        "mismatched",
    ):
        evaluation["source_chain_tip"] = bundle.boundary_chain_tip
    return evaluation


async def _dispatch_spot_reconciliation(
    context: _DispatchContext,
) -> PortfolioReconciliationEvaluationRow:
    """Load, certify, and evaluate one spot account."""
    account = context.account
    boundary = context.boundary
    durable_signal = await context.repository.has_spot_margin_reconciliation_signal(
        account.wallet_public_id,
        str(account.exchange).lower(),
        str(account.mode),
        context.evaluated_at,
    )
    if durable_signal or _account_has_spot_margin_signal(account, context.position_capability):
        return _nonfull_evaluation(
            account,
            context.evaluated_at,
            context.method,
            "incomplete",
            "unsupported_margin",
        )
    if (
        boundary is None
        or not _boundary_bound_to_account(boundary, account)
        or not _account_balance_read_is_current(account)
    ):
        return _nonfull_evaluation(
            account,
            context.evaluated_at,
            context.method,
            "incomplete",
            "spot_boundary_unavailable",
        )
    try:
        venue_assets = frozenset(entry.currency for entry in (account.balances or []))
        bundle = await context.repository.get_spot_reconciliation_bundle(
            account.wallet_public_id,
            str(account.exchange).lower(),
            str(account.mode),
            boundary.as_of,
            boundary.source_watermark,
            venue_assets,
        )
        if bundle.error is not None:
            return _nonfull_evaluation(
                account,
                context.evaluated_at,
                context.method,
                "incomplete",
                bundle.error,
            )
        outcome = _certify_spot_history(context, bundle)
        replay_boundary = _spot_replay_boundary(context, bundle, outcome)
        evaluation = spot_reconciliation.evaluate(
            bundle.anchor,
            [SpotReplayExecutionRow(**row) for row in bundle.replay],
            replay_boundary,
            account,
            {
                public_id: SpotInstrumentIdentity(**identity)
                for public_id, identity in bundle.instruments_by_public_id.items()
            },
            bundle.specs_by_instrument_public_id,
            bundle.asset_precisions,
            bundle.previously_confirmed_assets,
            {},
            (),
            context.position_capability,
            context.evaluated_at,
        )
        return _apply_spot_certificate_result(evaluation, bundle, outcome)
    except Exception as error:
        return _nonfull_evaluation(
            account,
            context.evaluated_at,
            context.method,
            "error",
            _bounded_error(error),
        )


async def _dispatch_futures_reconciliation(
    context: _DispatchContext,
) -> PortfolioReconciliationEvaluationRow:
    """Load and evaluate one futures account."""
    account = context.account
    try:
        venue_symbols = {position.symbol for position in (account.open_positions or [])}
        bundle = await context.repository.get_futures_reconciliation_bundle(
            account.wallet_public_id,
            str(account.exchange).lower(),
            str(account.mode),
            context.evaluated_at,
            venue_symbols,
        )
        if bundle.error is not None:
            return _nonfull_evaluation(
                account,
                context.evaluated_at,
                context.method,
                "incomplete",
                bundle.error,
            )
        return futures_reconciliation.evaluate(
            bundle.projection,
            account,
            bundle.instrument_public_ids_by_symbol,
            bundle.specs_by_instrument_public_id,
            context.position_capability,
            context.evaluated_at,
        )
    except Exception as error:
        return _nonfull_evaluation(
            account,
            context.evaluated_at,
            context.method,
            "error",
            _bounded_error(error),
        )


async def dispatch_portfolio_reconciliation(
    repository: Repository,
    account: PortfolioAccountState,
    method_config: PortfolioReconciliationMethodConfigRow | None,
    position_capability: CapabilityStatus,
    evaluated_at: datetime,
    *,
    boundary: SpotReplayBoundaryCapture | None = None,
    history_capture: SpotHistoryRangeCapture | None = None,
) -> PortfolioReconciliationEvaluationRow:
    """Dispatch one account snapshot through its durable configured method.

    Args:
        repository: Durable reads needed by the selected method.
        account: Exact immutable venue-account snapshot to evaluate.
        method_config: Active operator-authored method config, when valid.
        position_capability: Capability captured with the account observation.
        evaluated_at: One shared evaluation and temporal-read instant.
        boundary: The live observer-captured pre-balance watermark evidence
            for this exact snapshot, or ``None`` on crash-recovery and replay
            re-evaluations where no boundary was captured in the same cycle —
            those spot evaluations honestly stay incomplete. A boundary is
            honored only when its identity binding matches the evaluated
            account AND the snapshot carries a current successful balance
            read; anything else is treated exactly like an absent boundary.
        history_capture: The observer's anchored-path venue history-range
            evidence for the same cycle, or ``None`` when the account is
            unanchored, the venue lacks the account-history contract, or any
            evidence read degraded. When present for the bundle's exact
            anchor epoch, the pure walutomat range certificate runs BEFORE
            the evaluator: a certified range hands the evaluator a certified
            venue cursor with complete inventory; an uncertified one keeps
            today's uncertified boundary, and its first named refusal
            replaces only the evaluator's generic
            ``uncertified_boundary`` incomplete reason. The evaluator's
            mismatch-first gate order structurally guarantees the rewrite
            can never suppress a computable ``mismatched`` (or touch a
            ``matched``): the cursor gate is only reached when every asset
            already compared clean.

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
    context = _DispatchContext(
        repository=repository,
        account=account,
        method=method,
        position_capability=position_capability,
        evaluated_at=evaluated_at,
        boundary=boundary,
        history_capture=history_capture,
    )
    if method == "spot_execution_replay":
        return await _dispatch_spot_reconciliation(context)
    return await _dispatch_futures_reconciliation(context)
