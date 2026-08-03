"""Tests for durable-method portfolio reconciliation dispatch."""

from collections.abc import Iterator
from dataclasses import fields
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from loguru import logger

from snapper.application.portfolio import reconciliation_dispatch
from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.reconciliation_dispatch import LEDGER_INTEGRITY_REFUSAL_PREFIX
from snapper.application.portfolio.reconciliation_dispatch import SpotReplayBoundaryCapture
from snapper.application.portfolio.reconciliation_view import no_portfolio_reconciliation_view
from snapper.application.portfolio.walutomat_history_certificate import CertificateOutcome
from snapper.application.portfolio.walutomat_history_certificate import HistoryRangeEvidence
from snapper.application.portfolio.walutomat_history_certificate import SpotHistoryRangeCapture
from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import FuturesReconciliationBundle
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import PortfolioReconciliationMethodConfigRow
from snapper.data.repository_types import PositionRow
from snapper.data.repository_types import SpotReconciliationAnchorRow
from snapper.data.repository_types import SpotReconciliationBundle
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.messaging.schemas.data import AccountBalanceEntry
from snapper.messaging.schemas.data import AccountPositionEntry
from snapper.messaging.schemas.data import PortfolioAccountState

_NOW = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
_WALLET = "00000000-0000-7000-8000-000000000101"
_ACCOUNT = "00000000-0000-7000-8000-000000000201"
_SESSION = "00000000-0000-7000-8000-000000000301"
_CONFIG = "00000000-0000-7000-8000-000000000401"
_INSTRUMENT = "00000000-0000-7000-8000-000000000501"
_SYMBOL = "PF_XBTUSD"


def _balance(
    *,
    total: float = 10.0,
    used: float | None = 0.0,
    used_decimal: str | None = "0",
) -> AccountBalanceEntry:
    """Build one authoritative native balance entry."""
    return AccountBalanceEntry(
        currency="USD",
        total=total,
        free=10.0,
        used=used,
        total_decimal=str(total),
        free_decimal="10",
        used_decimal=used_decimal,
        numeric_provenance="venue_raw",
    )


def _position_entry() -> AccountPositionEntry:
    """Build one venue-native open position."""
    return AccountPositionEntry(
        symbol=_SYMBOL,
        side="buy",
        size=1.0,
        entry_price=100.0,
        mark_price=101.0,
        unrealized_pnl=1.0,
        unrealized_funding=0.0,
        timestamp=_NOW - timedelta(minutes=1),
    )


def _account(
    *,
    exchange: ExchangeEnum = ExchangeEnum.KRAKEN,
    balances: list[AccountBalanceEntry] | None = None,
    positions: list[AccountPositionEntry] | None = None,
    balance_status: str = "observed",
    current_attempt_observation_id: int | None = 17,
    balance_payload_source_observation_id: int | None = 17,
) -> PortfolioAccountState:
    """Build one authoritative live account snapshot."""
    return PortfolioAccountState(
        session_id=_SESSION,
        sequence_id=7,
        public_id=_ACCOUNT,
        timestamp=_NOW - timedelta(minutes=1),
        wallet_public_id=_WALLET,
        exchange=exchange,
        mode="live",
        sync_status="observed",
        effective_status="observed",
        is_authoritative=True,
        balance_status=balance_status,
        position_status="observed",
        valuation_status="native_only",
        balances=[_balance()] if balances is None else balances,
        open_positions=[] if positions is None else positions,
        balance_observed_at=_NOW - timedelta(minutes=1),
        position_observed_at=_NOW - timedelta(minutes=1),
        authoritative_until=_NOW + timedelta(minutes=4),
        current_attempt_observation_id=current_attempt_observation_id,
        balance_payload_source_observation_id=balance_payload_source_observation_id,
        position_payload_source_observation_id=17,
        error=None,
        reconciliation=no_portfolio_reconciliation_view(),
    )


def _config(
    method: str,
    *,
    wallet_public_id: str = _WALLET,
    exchange: str = "kraken",
) -> PortfolioReconciliationMethodConfigRow:
    """Build one active durable method configuration."""
    return {
        "wallet_public_id": wallet_public_id,
        "exchange": exchange,
        "mode": "live",
        "method": method,
        "classified_after_observation_id": None,
        "public_id": _CONFIG,
        "timestamp": _NOW - timedelta(minutes=2),
        "session_id": _SESSION,
        "sequence_id": 1,
    }


def _evaluation(
    method: str,
    status: str,
    error: str | None,
    *,
    exchange: str = "kraken_futures",
) -> PortfolioReconciliationEvaluationRow:
    """Build one generic non-full evaluation result."""
    return {
        "wallet_public_id": _WALLET,
        "exchange": exchange,
        "mode": "live",
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
        "error": error,
        "session_id": _SESSION,
        "sequence_id": 7,
        "bus_time": _NOW,
    }


def _projection() -> PositionRow:
    """Build one complete internal futures position projection."""
    return {
        "public_id": "00000000-0000-7000-8000-000000000601",
        "timestamp": _NOW - timedelta(minutes=1),
        "session_id": _SESSION,
        "sequence_id": 6,
        "instrument": _SYMBOL,
        "instrument_public_id": _INSTRUMENT,
        "exchange": "kraken_futures",
        "mode": "live",
        "quantity": 1.0,
        "average_price": 100.0,
        "unrealized_pnl": 1.0,
        "realized_pnl": 0.0,
        "mark_price": 101.0,
        "marked_at": _NOW - timedelta(minutes=1),
        "source_venue_event_id": 41,
        "position_cycle_public_id": None,
        "wallet_public_id": _WALLET,
    }


def _boundary(
    *,
    wallet_public_id: str = _WALLET,
    exchange: str = "kraken",
    mode: str = "live",
    session_id: str = _SESSION,
    sequence_id: int = 7,
) -> SpotReplayBoundaryCapture:
    """Build one validated boundary bound to the default account identity."""
    return SpotReplayBoundaryCapture(
        wallet_public_id=wallet_public_id,
        exchange=exchange,
        mode=mode,
        session_id=session_id,
        sequence_id=sequence_id,
        source_watermark=7,
        as_of=_NOW - timedelta(seconds=4),
        watermark_captured_at=_NOW - timedelta(seconds=3),
        request_started_at=_NOW - timedelta(seconds=2),
        request_completed_at=_NOW - timedelta(seconds=1),
        watermark_after=7,
        watermark_after_captured_at=_NOW,
        watermark_unchanged=True,
    )


def _unanchored_spot_bundle() -> SpotReconciliationBundle:
    """Return the empty unanchored spot bundle (the honest first-cycle read)."""
    return SpotReconciliationBundle(
        anchor=None,
        replay=[],
        instruments_by_public_id={},
        specs_by_instrument_public_id={},
        asset_precisions={},
        previously_confirmed_assets=frozenset(),
        range_complete=False,
        boundary_chain_tip=None,
        error=None,
    )


def _repository(
    *,
    durable_signal: bool = False,
    bundle: FuturesReconciliationBundle | None = None,
    spot_bundle: SpotReconciliationBundle | None = None,
    spot_error: Exception | None = None,
) -> tuple[Repository, AsyncMock, AsyncMock]:
    """Build a typed repository double and expose its dispatch reads."""
    raw = MagicMock(spec=Repository)
    signal_read = AsyncMock(return_value=durable_signal)
    bundle_read = AsyncMock(
        return_value=(
            FuturesReconciliationBundle(
                projection=[],
                instrument_public_ids_by_symbol={},
                specs_by_instrument_public_id={},
            )
            if bundle is None
            else bundle
        )
    )
    raw.has_spot_margin_reconciliation_signal = signal_read
    raw.get_futures_reconciliation_bundle = bundle_read
    raw.get_spot_reconciliation_bundle = (
        AsyncMock(side_effect=spot_error)
        if spot_error is not None
        else AsyncMock(
            return_value=_unanchored_spot_bundle() if spot_bundle is None else spot_bundle
        )
    )
    return cast(Repository, raw), signal_read, bundle_read


async def test_missing_config_remains_unclassified_for_exchange_looking_identity() -> None:
    """A venue-looking name never substitutes for operator classification.

    Given: A Kraken Futures-looking account identity without an active config.
    When: The observer dispatches the account snapshot.
    Then: Dispatch returns the exact unclassified result without method reads.
    """
    repository, signal_read, bundle_read = _repository()
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(exchange=ExchangeEnum.KRAKEN_FUTURES),
        None,
        CapabilityStatus.SUPPORTED,
        _NOW,
    )
    assert result == _evaluation(
        "unclassified",
        "incomplete",
        "reconciliation_method_unclassified",
    )
    signal_read.assert_not_awaited()
    bundle_read.assert_not_awaited()


async def test_identity_mismatched_config_fails_closed_as_unclassified() -> None:
    """A foreign active config is not accepted by dispatch.

    Given: A method config belonging to another wallet identity.
    When: Dispatch validates it against the observed account.
    Then: The account remains unclassified and no method-specific read occurs.
    """
    repository, signal_read, bundle_read = _repository()
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(),
        _config("spot_execution_replay", wallet_public_id="foreign-wallet"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
    )
    assert result["method"] == "unclassified"
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "reconciliation_method_unclassified"
    signal_read.assert_not_awaited()
    bundle_read.assert_not_awaited()


async def test_margin_ledger_dispatch_is_error_only_and_has_no_evidence() -> None:
    """The unimplemented margin method emits only its prescribed error row.

    Given: A live account configured for margin ledger replay.
    When: The S4a dispatcher selects that unimplemented method.
    Then: It returns error-only evidence with the prescribed bounded reason.
    """
    repository, signal_read, bundle_read = _repository()
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(),
        _config("margin_ledger_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
    )
    assert result == _evaluation(
        "margin_ledger_replay",
        "error",
        "margin_ledger_replay_not_implemented",
        exchange="kraken",
    )
    signal_read.assert_not_awaited()
    bundle_read.assert_not_awaited()


@pytest.mark.parametrize(
    ("used", "used_decimal"),
    [
        (5.0, "5"),
        (0.0, "0"),
        (-5.0, "-5"),
        (0.25, "0.250000000000000001"),
    ],
)
async def test_reserved_cash_used_fields_never_trip_spot_margin(
    used: float,
    used_decimal: str,
) -> None:
    """Reserved-cash values are ignored regardless of sign or precision.

    Given: A cash account whose used fields contain an arbitrary reserved value.
    When: Spot reconciliation runs its defensive margin tripwire.
    Then: Used values do not signal margin and the spot boundary remains unavailable.
    """
    repository, signal_read, bundle_read = _repository()
    account = _account(balances=[_balance(used=used, used_decimal=used_decimal)])
    with patch("snapper.application.portfolio.spot_reconciliation.evaluate") as spot_evaluate:
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            account,
            _config("spot_execution_replay"),
            CapabilityStatus.NOT_APPLICABLE,
            _NOW,
        )
    assert result["method"] == "spot_execution_replay"
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "spot_boundary_unavailable"
    signal_read.assert_awaited_once_with(_WALLET, "kraken", "live", _NOW)
    bundle_read.assert_not_awaited()
    spot_evaluate.assert_not_called()


@pytest.mark.parametrize(
    ("durable_signal", "balances", "positions", "capability"),
    [
        (True, [_balance()], [], CapabilityStatus.NOT_APPLICABLE),
        (False, [_balance(total=-0.01)], [], CapabilityStatus.NOT_APPLICABLE),
        (False, [_balance()], [_position_entry()], CapabilityStatus.NOT_APPLICABLE),
        (False, [_balance()], [], CapabilityStatus.SUPPORTED),
    ],
)
async def test_each_spot_margin_signal_fails_closed(
    durable_signal: bool,
    balances: list[AccountBalanceEntry],
    positions: list[AccountPositionEntry],
    capability: CapabilityStatus,
) -> None:
    """Durable leverage, liabilities, positions, and capability all trip margin.

    Given: One authoritative or durable signal incompatible with cash-only replay.
    When: The spot dispatch branch evaluates its margin tripwire.
    Then: It returns the unsupported-margin incomplete result without evaluation.
    """
    repository, signal_read, bundle_read = _repository(durable_signal=durable_signal)
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(balances=balances, positions=positions),
        _config("spot_execution_replay"),
        capability,
        _NOW,
    )
    assert result["method"] == "spot_execution_replay"
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "unsupported_margin"
    signal_read.assert_awaited_once()
    bundle_read.assert_not_awaited()


@pytest.mark.parametrize(
    ("positions", "expected_error"),
    [
        ([], "spot_boundary_unavailable"),
        ([_position_entry()], "unsupported_margin"),
    ],
    ids=["empty_book", "observed_margin_position"],
)
async def test_kraken_position_observation_preserves_spot_reconciliation_policy(
    positions: list[AccountPositionEntry],
    expected_error: str,
) -> None:
    """Kraken observation support leaves cash-only reconciliation unchanged.

    Given: A Kraken spot snapshot with either an empty or populated observed
        position book, a supported observation declaration, and the legacy
        NOT_APPLICABLE reconciliation declaration.
    When: Dispatch evaluates the same snapshot against the literal pre-change
        policy baseline and Kraken's post-change policy declaration.
    Then: Both full rows are identical; an empty book stays on the cash path,
        while a populated book retains the deliberate unsupported-margin
        refusal. This catches production mutations that replace the legacy
        policy with observation capability or weaken the fail-closed position
        guard.
    """
    assert KrakenExchangeClient.position_observation_capability is CapabilityStatus.SUPPORTED
    assert KrakenExchangeClient.position_capability is CapabilityStatus.NOT_APPLICABLE
    account = _account(positions=positions)
    baseline_repository, _, _ = _repository()
    baseline = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        baseline_repository,
        account,
        _config("spot_execution_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
    )
    declared_repository, _, _ = _repository()
    declared = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        declared_repository,
        account,
        _config("spot_execution_replay"),
        KrakenExchangeClient.position_capability,
        _NOW,
    )
    expected = _evaluation(
        "spot_execution_replay",
        "incomplete",
        expected_error,
        exchange="kraken",
    )
    assert baseline == expected
    assert declared == baseline


async def test_spot_boundary_present_evaluates_the_unanchored_account() -> None:
    """A genuine pre-balance boundary now reaches the real spot evaluator.

    Given: A cash spot account whose observer captured a pre-balance boundary
        and whose bundle reports the honest unanchored state.
    When: Dispatch evaluates the spot branch with that boundary threaded in.
    Then: The evaluator itself classifies the state — an unanchored bundle
        cannot certify its replay range, so the evaluator's own gate order
        yields incomplete_replay_boundary; the old stub reason is gone.
    """
    repository, signal_read, bundle_read = _repository()
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(),
        _config("spot_execution_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
        boundary=_boundary(),
    )
    assert result["method"] == "spot_execution_replay"
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "incomplete_replay_boundary"
    signal_read.assert_awaited_once_with(_WALLET, "kraken", "live", _NOW)
    bundle_read.assert_not_awaited()


async def test_replay_paths_without_a_boundary_stay_boundary_unavailable() -> None:
    """Crash-recovery re-evaluations without a live boundary stay incomplete.

    Given: A cash spot account re-evaluated with an explicitly absent boundary.
    When: Dispatch evaluates the spot branch.
    Then: The existing spot_boundary_unavailable incomplete reason is kept —
        an evaluation may never claim a boundary it did not capture.
    """
    repository, signal_read, bundle_read = _repository()
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(),
        _config("spot_execution_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
        boundary=None,
    )
    assert result["method"] == "spot_execution_replay"
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "spot_boundary_unavailable"
    signal_read.assert_awaited_once()
    bundle_read.assert_not_awaited()


def test_replay_boundary_builder_rejects_an_absent_capture() -> None:
    """The extracted boundary builder preserves the dispatch fail-closed guard.

    Given: An internal spot dispatch context without a boundary capture.
    When: The replay-boundary builder is called outside the guarded dispatch path.
    Then: It rejects the impossible state with the public boundary-unavailable reason.
    """
    repository, _, _ = _repository()
    context = reconciliation_dispatch._DispatchContext(
        repository=repository,
        account=_account(),
        method="spot_execution_replay",
        position_capability=CapabilityStatus.NOT_APPLICABLE,
        evaluated_at=_NOW,
        boundary=None,
        history_capture=None,
    )
    unanchored_bundle = _unanchored_spot_bundle()

    with pytest.raises(ValueError, match="^spot_boundary_unavailable$"):
        reconciliation_dispatch._spot_replay_boundary(
            context,
            unanchored_bundle,
            None,
        )


@pytest.mark.parametrize(
    "foreign_boundary",
    [
        _boundary(wallet_public_id="00000000-0000-7000-8000-000000000999"),
        _boundary(exchange="walutomat"),
        _boundary(mode="paper"),
        _boundary(session_id="00000000-0000-7000-8000-000000000399"),
        _boundary(sequence_id=8),
    ],
)
async def test_foreign_or_stale_boundary_binding_is_treated_as_absent(
    foreign_boundary: SpotReplayBoundaryCapture,
) -> None:
    """A boundary bound to another account or cycle never counts as present.

    Given: A structurally valid boundary whose identity binding differs from
        the evaluated account on exactly one leg (wallet, exchange, mode,
        session, or observation sequence).
    When: Dispatch evaluates the spot branch with that boundary threaded in.
    Then: The reason stays spot_boundary_unavailable exactly as if no
        boundary had been threaded at all — a foreign or stale-cycle capture
        must never certify this snapshot.
    """
    repository, _, _ = _repository()
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(),
        _config("spot_execution_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
        boundary=foreign_boundary,
    )
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "spot_boundary_unavailable"


@pytest.mark.parametrize(
    ("balance_status", "current_attempt_observation_id", "payload_source_observation_id"),
    [
        ("unsupported", 17, None),
        ("error", 17, 16),
        ("observed", 17, 16),
        ("observed", None, None),
    ],
)
async def test_boundary_without_a_current_balance_read_is_treated_as_absent(
    balance_status: str,
    current_attempt_observation_id: int | None,
    payload_source_observation_id: int | None,
) -> None:
    """No boundary is honored when the cycle produced no current balance read.

    An unsupported capability returns without a venue call, an errored or
    timed-out read leaves an older RETAINED payload visible, and a missing
    observation identity can never prove currency — a boundary brackets a
    balance request, so each of those cycles has nothing for it to certify.

    Given: A bound valid boundary and a snapshot whose balance read is not a
        current successful observation (including the retained-payload trap
        and the both-identities-absent trap).
    When: Dispatch evaluates the spot branch.
    Then: The reason stays spot_boundary_unavailable.
    """
    repository, _, _ = _repository()
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(
            balance_status=balance_status,
            current_attempt_observation_id=current_attempt_observation_id,
            balance_payload_source_observation_id=payload_source_observation_id,
        ),
        _config("spot_execution_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
        boundary=_boundary(),
    )
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "spot_boundary_unavailable"


def test_capture_type_carries_no_self_certification_fields() -> None:
    """The capture cannot claim range completion or a pre-balance flag.

    Range certification is owned by the S4c-4 replay bundle, and the
    pre-balance property is an enforced timestamp-ordering invariant of the
    type — neither may exist as a caller-settable boolean.

    Given: The frozen SpotReplayBoundaryCapture dataclass definition.
    When: Its field names are inspected.
    Then: No range_complete or watermark_captured_before_balance field exists.
    """
    field_names = {field.name for field in fields(SpotReplayBoundaryCapture)}
    assert "range_complete" not in field_names
    assert "watermark_captured_before_balance" not in field_names


def test_valid_boundary_construction_carries_every_bound_field() -> None:
    """A coherent capture constructs and exposes its exact bound identity.

    Given: Coherent identity, watermark, and read-window values.
    When: The validated capture is constructed.
    Then: Every field round-trips exactly, including the join as_of the
        S4c-4 bundle must reuse.
    """
    boundary = _boundary()
    assert boundary.wallet_public_id == _WALLET
    assert boundary.exchange == "kraken"
    assert boundary.mode == "live"
    assert boundary.session_id == _SESSION
    assert boundary.sequence_id == 7
    assert boundary.source_watermark == 7
    assert boundary.as_of == _NOW - timedelta(seconds=4)
    assert boundary.watermark_unchanged is True


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"wallet_public_id": ""}, "spot_boundary_wallet_unbound"),
        ({"exchange": ""}, "spot_boundary_exchange_unbound"),
        ({"mode": ""}, "spot_boundary_mode_unbound"),
        ({"session_id": ""}, "spot_boundary_session_unbound"),
        ({"source_watermark": -1}, "spot_boundary_negative_watermark"),
        ({"as_of": _NOW.replace(tzinfo=None)}, "spot_boundary_naive_as_of"),
        (
            {"watermark_captured_at": (_NOW - timedelta(seconds=3)).replace(tzinfo=None)},
            "spot_boundary_naive_watermark_captured_at",
        ),
        (
            {"request_started_at": (_NOW - timedelta(seconds=2)).replace(tzinfo=None)},
            "spot_boundary_naive_request_started_at",
        ),
        (
            {"request_completed_at": (_NOW - timedelta(seconds=1)).replace(tzinfo=None)},
            "spot_boundary_naive_request_completed_at",
        ),
        ({"as_of": _NOW}, "spot_boundary_as_of_after_capture"),
        ({"watermark_captured_at": _NOW}, "spot_boundary_watermark_not_pre_balance"),
        ({"request_started_at": _NOW}, "spot_boundary_request_window_inverted"),
        (
            {"watermark_after": None, "watermark_unchanged": False},
            "spot_boundary_after_read_incoherent",
        ),
        ({"watermark_after_captured_at": None}, "spot_boundary_after_read_incoherent"),
        (
            {"watermark_after_captured_at": _NOW.replace(tzinfo=None)},
            "spot_boundary_naive_watermark_after_captured_at",
        ),
        (
            {"watermark_after_captured_at": _NOW - timedelta(seconds=2)},
            "spot_boundary_after_capture_not_post_balance",
        ),
        (
            {"watermark_after": 6, "watermark_unchanged": False},
            "spot_boundary_after_watermark_regressed",
        ),
        ({"watermark_unchanged": False}, "spot_boundary_unchanged_flag_inconsistent"),
        (
            {"watermark_after": 9, "watermark_unchanged": True},
            "spot_boundary_unchanged_flag_inconsistent",
        ),
        (
            {
                "watermark_after": None,
                "watermark_after_captured_at": None,
                "watermark_unchanged": True,
            },
            "spot_boundary_unchanged_flag_inconsistent",
        ),
    ],
)
def test_every_incoherent_boundary_relationship_is_rejected_at_construction(
    changes: dict[str, object],
    reason: str,
) -> None:
    """Construction is the validated factory — incoherent captures cannot exist.

    Given: A coherent capture and exactly one incoherent relationship
        (unbound identity, negative watermark, naive or unordered timestamp,
        incoherent after-read pairing, regressed after-watermark, or an
        inconsistent quiescence flag).
    When: The mutated capture is constructed via dataclasses.replace (which
        re-runs the validating constructor).
    Then: ValueError with the exact named reason is raised.
    """
    coherent_boundary = _boundary()

    with pytest.raises(ValueError, match=reason):
        replace(coherent_boundary, **changes)


async def test_margin_gate_takes_precedence_over_a_present_boundary() -> None:
    """A captured boundary never weakens the spot margin tripwire.

    Given: A durable margin signal and a genuine pre-balance boundary.
    When: Dispatch evaluates the spot branch.
    Then: The unsupported-margin incomplete result is returned unchanged.
    """
    repository, signal_read, _ = _repository(durable_signal=True)
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(),
        _config("spot_execution_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
        boundary=_boundary(),
    )
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "unsupported_margin"
    signal_read.assert_awaited_once()


async def test_futures_dispatch_passes_exact_complete_bundle_to_evaluator() -> None:
    """Futures dispatch forwards the captured account and one complete bundle.

    Given: A configured futures account and a complete repeatable-read bundle.
    When: Dispatch invokes the live futures evaluator.
    Then: The exact account, bundle, capability, and instant reach the evaluator.
    """
    projection = [_projection()]
    bundle = FuturesReconciliationBundle(
        projection=projection,
        instrument_public_ids_by_symbol={_SYMBOL: _INSTRUMENT},
        specs_by_instrument_public_id={_INSTRUMENT: None},
    )
    repository, signal_read, bundle_read = _repository(bundle=bundle)
    account = _account(
        exchange=ExchangeEnum.KRAKEN_FUTURES,
        balances=[],
        positions=[_position_entry()],
    )
    expected = _evaluation("futures_position", "incomplete", "test-result")
    with patch.object(
        reconciliation_dispatch.futures_reconciliation,
        "evaluate",
        return_value=expected,
    ) as evaluator:
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            account,
            _config("futures_position", exchange="kraken_futures"),
            CapabilityStatus.SUPPORTED,
            _NOW,
        )
    assert result is expected
    bundle_read.assert_awaited_once_with(
        _WALLET,
        "kraken_futures",
        "live",
        _NOW,
        {_SYMBOL},
    )
    evaluator.assert_called_once_with(
        projection,
        account,
        {_SYMBOL: _INSTRUMENT},
        {_INSTRUMENT: None},
        CapabilityStatus.SUPPORTED,
        _NOW,
    )
    signal_read.assert_not_awaited()


async def test_futures_empty_projection_remains_complete() -> None:
    """A proven zero-position projection reaches the evaluator unchanged.

    Given: A futures bundle proving an empty internal position projection.
    When: Dispatch invokes the futures evaluator.
    Then: The empty projection remains complete rather than becoming unavailable.
    """
    bundle = FuturesReconciliationBundle(
        projection=[],
        instrument_public_ids_by_symbol={},
        specs_by_instrument_public_id={},
    )
    repository, _, bundle_read = _repository(bundle=bundle)
    account = _account(exchange=ExchangeEnum.KRAKEN_FUTURES, balances=[], positions=[])
    expected = _evaluation("futures_position", "incomplete", "empty-projection")
    with patch.object(
        reconciliation_dispatch.futures_reconciliation,
        "evaluate",
        return_value=expected,
    ) as evaluator:
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            account,
            _config("futures_position", exchange="kraken_futures"),
            CapabilityStatus.SUPPORTED,
            _NOW,
        )
    assert result is expected
    bundle_read.assert_awaited_once()
    evaluator.assert_called_once()
    assert evaluator.call_args.args[0] == []


async def test_futures_bundle_ambiguity_becomes_nonfull_without_evaluation() -> None:
    """Bundle identity ambiguity is persisted as a non-full futures result.

    Given: A bundle that fails closed on an ambiguous native-symbol mapping.
    When: Futures dispatch receives the unavailable projection.
    Then: It emits a non-full incomplete result and skips the evaluator.
    """
    bundle = FuturesReconciliationBundle(
        projection=None,
        instrument_public_ids_by_symbol={},
        specs_by_instrument_public_id={},
        error="duplicate_futures_native_symbol_mapping",
    )
    repository, _, bundle_read = _repository(bundle=bundle)
    with patch.object(reconciliation_dispatch.futures_reconciliation, "evaluate") as evaluator:
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            _account(exchange=ExchangeEnum.KRAKEN_FUTURES, balances=[], positions=[]),
            _config("futures_position", exchange="kraken_futures"),
            CapabilityStatus.SUPPORTED,
            _NOW,
        )
    assert result["method"] == "futures_position"
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "duplicate_futures_native_symbol_mapping"
    bundle_read.assert_awaited_once()
    evaluator.assert_not_called()


async def test_futures_dispatch_exception_is_method_scoped_error() -> None:
    """An unexpected evaluator failure cannot escape the observer branch.

    Given: A complete futures bundle whose evaluator raises unexpectedly.
    When: Dispatch contains the evaluator failure.
    Then: It returns a futures-scoped error row with the bounded failure reason.
    """
    repository, _, bundle_read = _repository()
    with patch.object(
        reconciliation_dispatch.futures_reconciliation,
        "evaluate",
        side_effect=RuntimeError("evaluator failed"),
    ):
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            _account(exchange=ExchangeEnum.KRAKEN_FUTURES, balances=[], positions=[]),
            _config("futures_position", exchange="kraken_futures"),
            CapabilityStatus.SUPPORTED,
            _NOW,
        )
    assert result["method"] == "futures_position"
    assert result["evaluation_status"] == "error"
    assert result["error"] == "evaluator failed"
    bundle_read.assert_awaited_once()


def _evaluation_result(status: str) -> PortfolioReconciliationEvaluationRow:
    """Build one minimal evaluator result row for orchestration assertions."""
    return {
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "method": "spot_execution_replay",
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
        "error": None,
        "session_id": "session-1",
        "sequence_id": 1,
        "bus_time": _NOW,
    }


def _error_spot_bundle(error: str) -> SpotReconciliationBundle:
    """Return a fail-closed spot bundle carrying one named error."""
    return SpotReconciliationBundle(
        anchor=None,
        replay=[],
        instruments_by_public_id={},
        specs_by_instrument_public_id={},
        asset_precisions={},
        previously_confirmed_assets=frozenset(),
        range_complete=False,
        boundary_chain_tip=None,
        error=error,
    )


async def test_spot_bundle_error_passes_through_by_name() -> None:
    """A named bundle refusal becomes the incomplete reason verbatim.

    Given: A spot bundle refusing with execution_chain_diverged,
    When: Dispatch evaluates the spot branch with a valid boundary,
    Then: The evaluation is incomplete with exactly that named reason.
    """
    repository, _, _ = _repository(spot_bundle=_error_spot_bundle("execution_chain_diverged"))
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(),
        _config("spot_execution_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
        boundary=_boundary(),
    )
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "execution_chain_diverged"


async def test_spot_full_outcome_is_stamped_with_the_boundary_chain_tip() -> None:
    """A full spot outcome carries the bundle-derived checkpoint tip.

    Given: A bundle with a derived boundary chain tip and a patched evaluator
        returning a mismatched full outcome,
    When: Dispatch evaluates the spot branch,
    Then: The evaluation carries source_chain_tip for the writer, and the
        evaluator received the bundle's range proof with the cursor
        honestly uncertified.
    """
    bundle = SpotReconciliationBundle(
        anchor=None,
        replay=[],
        instruments_by_public_id={},
        specs_by_instrument_public_id={},
        asset_precisions={},
        previously_confirmed_assets=frozenset(),
        range_complete=True,
        boundary_chain_tip="d" * 64,
        error=None,
    )
    repository, _, _ = _repository(spot_bundle=bundle)
    full = _evaluation_result("mismatched")
    with patch.object(
        reconciliation_dispatch.spot_reconciliation, "evaluate", return_value=full
    ) as evaluate:
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            _account(),
            _config("spot_execution_replay"),
            CapabilityStatus.NOT_APPLICABLE,
            _NOW,
            boundary=_boundary(),
        )
    assert result["source_chain_tip"] == "d" * 64
    replay_boundary = evaluate.call_args.args[2]
    assert replay_boundary.range_complete is True
    assert replay_boundary.venue_cursor_certified is False
    assert replay_boundary.venue_cursor is None


async def test_spot_incomplete_outcome_is_never_stamped_with_a_tip() -> None:
    """A non-full spot outcome never claims a chain checkpoint.

    Given: A bundle with a derived tip and a patched evaluator returning an
        incomplete outcome,
    When: Dispatch evaluates the spot branch,
    Then: The evaluation carries no source_chain_tip.
    """
    bundle = SpotReconciliationBundle(
        anchor=None,
        replay=[],
        instruments_by_public_id={},
        specs_by_instrument_public_id={},
        asset_precisions={},
        previously_confirmed_assets=frozenset(),
        range_complete=True,
        boundary_chain_tip="d" * 64,
        error=None,
    )
    repository, _, _ = _repository(spot_bundle=bundle)
    incomplete = _evaluation_result("incomplete")
    with patch.object(
        reconciliation_dispatch.spot_reconciliation, "evaluate", return_value=incomplete
    ):
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            _account(),
            _config("spot_execution_replay"),
            CapabilityStatus.NOT_APPLICABLE,
            _NOW,
            boundary=_boundary(),
        )
    assert "source_chain_tip" not in result


_ANCHOR_SCHEME = "walutomat:api-v2.0.0:account/history:v1"


def _anchor_row(*, source_watermark: int = 1) -> SpotReconciliationAnchorRow:
    """Build one sealed cursor-certified anchor row for the evaluated account."""
    sealed_at = _NOW - timedelta(minutes=10)
    return {
        "public_id": "00000000-0000-7000-8000-000000000701",
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "venue_account_state_public_id": _ACCOUNT,
        "balance_observation_id": 17,
        "source_watermark_kind": "scope_sequence",
        "source_watermark": source_watermark,
        "balances_json": '{"USD":"10"}',
        "first_request_started_at": sealed_at,
        "first_request_completed_at": sealed_at,
        "second_request_started_at": sealed_at,
        "second_request_completed_at": sealed_at,
        "boundary_status": "cursor_certified",
        "inventory_status": "venue_reported_full",
        "margin_status": "cash",
        "provenance": f"spot_anchor_bootstrap:v1:{_ANCHOR_SCHEME}:windowed",
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": sealed_at,
        "source_chain_tip": "a" * 64,
        "venue_cursor_kind": "account_history_item_id",
        "venue_cursor_scheme": _ANCHOR_SCHEME,
        "venue_cursor_value": "100",
        "venue_cursor_requested_at": sealed_at,
        "venue_cursor_observed_at": sealed_at,
        "venue_cursor_confirmed_at": sealed_at,
        "source_watermark_requested_at": sealed_at,
        "source_watermark_captured_at": sealed_at,
    }


def _anchored_spot_bundle(
    *,
    boundary_chain_tip: str | None = None,
) -> SpotReconciliationBundle:
    """Return an anchored complete-range bundle for certificate-path tests."""
    return SpotReconciliationBundle(
        anchor=_anchor_row(),
        replay=[],
        instruments_by_public_id={},
        specs_by_instrument_public_id={},
        asset_precisions={},
        previously_confirmed_assets=frozenset(),
        range_complete=True,
        boundary_chain_tip=boundary_chain_tip,
        error=None,
    )


def _history_capture(*, anchor_watermark: int = 1) -> SpotHistoryRangeCapture:
    """Build one observer-captured empty-range certificate evidence bundle."""
    return SpotHistoryRangeCapture(
        evidence=HistoryRangeEvidence(tip_item_id=100, confirming_tip_item_id=100, rows=()),
        parsed_executions=(),
        order_totals={},
        venue_balances={"USD": Decimal("10")},
        anchor_watermark=anchor_watermark,
    )


async def test_certified_history_range_certifies_the_replay_boundary() -> None:
    """A certified range certificate hands the evaluator a certified cursor.

    Given: An anchored bundle, a same-epoch history capture, a certificate
        outcome that certifies, and a patched evaluator returning matched.
    When: Dispatch evaluates the spot branch.
    Then: The certificate received the exact capture evidence and boundary
        watermarks, the evaluator's boundary carries the certified cursor with
        complete untruncated inventory, the matched outcome's error is
        untouched, and the chain-tip stamping is unchanged.
    """
    bundle = _anchored_spot_bundle(boundary_chain_tip="e" * 64)
    repository, _, _ = _repository(spot_bundle=bundle)
    capture = _history_capture()
    matched = _evaluation_result("matched")
    outcome = CertificateOutcome(
        certified=True,
        venue_cursor=f"{_ANCHOR_SCHEME}:100:{'c' * 64}",
        refusals=(),
    )
    with (
        patch.object(
            reconciliation_dispatch,
            "certify_walutomat_history_range",
            return_value=outcome,
        ) as certify,
        patch.object(
            reconciliation_dispatch.spot_reconciliation, "evaluate", return_value=matched
        ) as evaluate,
    ):
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            _account(),
            _config("spot_execution_replay"),
            CapabilityStatus.NOT_APPLICABLE,
            _NOW,
            boundary=_boundary(),
            history_capture=capture,
        )
    certify.assert_called_once_with(
        evidence=capture.evidence,
        anchor=bundle.anchor,
        venue_balances=capture.venue_balances,
        parsed_executions=capture.parsed_executions,
        order_totals=capture.order_totals,
        watermark_unchanged=True,
        boundary_watermark=7,
    )
    replay_boundary = evaluate.call_args.args[2]
    assert replay_boundary.venue_cursor == f"{_ANCHOR_SCHEME}:100:{'c' * 64}"
    assert replay_boundary.venue_cursor_certified is True
    assert replay_boundary.inventory_complete is True
    assert replay_boundary.inventory_truncated is False
    assert replay_boundary.range_complete is True
    assert replay_boundary.source_watermark == 7
    assert result["evaluation_status"] == "matched"
    assert result["error"] is None
    assert result["source_chain_tip"] == "e" * 64


async def test_uncertified_range_substitutes_only_the_generic_gate_reason() -> None:
    """An uncertified certificate names its first refusal on the cursor gate.

    Given: An anchored bundle, a same-epoch capture, a refusing certificate,
        and a patched evaluator returning the generic uncertified_boundary
        incomplete.
    When: Dispatch evaluates the spot branch.
    Then: The evaluator's boundary stays honestly uncertified and the generic
        gate reason is replaced by the certificate's first named refusal.
    """
    repository, _, _ = _repository(spot_bundle=_anchored_spot_bundle())
    incomplete = _evaluation_result("incomplete")
    incomplete["error"] = "uncertified_boundary"
    outcome = CertificateOutcome(
        certified=False,
        venue_cursor=None,
        refusals=("venue_history_advanced", "watermark_advanced"),
    )
    with (
        patch.object(
            reconciliation_dispatch,
            "certify_walutomat_history_range",
            return_value=outcome,
        ),
        patch.object(
            reconciliation_dispatch.spot_reconciliation, "evaluate", return_value=incomplete
        ) as evaluate,
    ):
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            _account(),
            _config("spot_execution_replay"),
            CapabilityStatus.NOT_APPLICABLE,
            _NOW,
            boundary=_boundary(),
            history_capture=_history_capture(),
        )
    replay_boundary = evaluate.call_args.args[2]
    assert replay_boundary.venue_cursor is None
    assert replay_boundary.venue_cursor_certified is False
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "venue_history_advanced"


async def test_uncertified_range_never_touches_other_incomplete_reasons() -> None:
    """A specific evaluator incomplete reason is never rewritten.

    Given: A refusing certificate and a patched evaluator returning an
        incomplete outcome with its own specific reason.
    When: Dispatch evaluates the spot branch.
    Then: The evaluator's reason is preserved verbatim.
    """
    repository, _, _ = _repository(spot_bundle=_anchored_spot_bundle())
    incomplete = _evaluation_result("incomplete")
    incomplete["error"] = "uncertified_inventory"
    outcome = CertificateOutcome(
        certified=False, venue_cursor=None, refusals=("venue_history_unavailable",)
    )
    with (
        patch.object(
            reconciliation_dispatch,
            "certify_walutomat_history_range",
            return_value=outcome,
        ),
        patch.object(
            reconciliation_dispatch.spot_reconciliation, "evaluate", return_value=incomplete
        ),
    ):
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            _account(),
            _config("spot_execution_replay"),
            CapabilityStatus.NOT_APPLICABLE,
            _NOW,
            boundary=_boundary(),
            history_capture=_history_capture(),
        )
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "uncertified_inventory"


async def test_uncertified_range_never_touches_a_mismatched_outcome() -> None:
    """A computable mismatched outcome is never suppressed by refusals.

    The evaluator's mismatch-first gate order means the cursor gate is only
    reached when every asset compared clean, so a full mismatched verdict must
    pass through the certificate plumbing byte-identical.

    Given: A refusing certificate and a patched evaluator returning a full
        mismatched outcome with a derived chain tip on the bundle.
    When: Dispatch evaluates the spot branch.
    Then: The mismatched outcome is untouched and still chain-tip stamped.
    """
    bundle = _anchored_spot_bundle(boundary_chain_tip="f" * 64)
    repository, _, _ = _repository(spot_bundle=bundle)
    mismatched = _evaluation_result("mismatched")
    outcome = CertificateOutcome(
        certified=False, venue_cursor=None, refusals=("venue_history_advanced",)
    )
    with (
        patch.object(
            reconciliation_dispatch,
            "certify_walutomat_history_range",
            return_value=outcome,
        ),
        patch.object(
            reconciliation_dispatch.spot_reconciliation, "evaluate", return_value=mismatched
        ),
    ):
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            _account(),
            _config("spot_execution_replay"),
            CapabilityStatus.NOT_APPLICABLE,
            _NOW,
            boundary=_boundary(),
            history_capture=_history_capture(),
        )
    assert result["evaluation_status"] == "mismatched"
    assert result["error"] is None
    assert result["source_chain_tip"] == "f" * 64


@pytest.mark.parametrize(
    ("spot_bundle", "capture"),
    [
        (_anchored_spot_bundle(), None),
        (_unanchored_spot_bundle(), _history_capture()),
        (_anchored_spot_bundle(), _history_capture(anchor_watermark=2)),
    ],
    ids=["absent_capture", "unanchored_bundle", "foreign_anchor_epoch"],
)
async def test_certificate_never_runs_without_matching_anchored_evidence(
    spot_bundle: SpotReconciliationBundle,
    capture: SpotHistoryRangeCapture | None,
) -> None:
    """The certificate requires same-epoch evidence for an anchored bundle.

    Given: An absent capture, an unanchored bundle, or a capture whose anchor
        watermark disagrees with the bundle's anchor epoch.
    When: Dispatch evaluates the spot branch with a patched evaluator
        returning the generic uncertified_boundary incomplete.
    Then: The certificate never runs, the boundary stays uncertified, and the
        generic gate reason is NOT substituted (no certificate outcome
        exists to name anything).
    """
    repository, _, _ = _repository(spot_bundle=spot_bundle)
    incomplete = _evaluation_result("incomplete")
    incomplete["error"] = "uncertified_boundary"
    with (
        patch.object(
            reconciliation_dispatch,
            "certify_walutomat_history_range",
        ) as certify,
        patch.object(
            reconciliation_dispatch.spot_reconciliation, "evaluate", return_value=incomplete
        ) as evaluate,
    ):
        result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
            repository,
            _account(),
            _config("spot_execution_replay"),
            CapabilityStatus.NOT_APPLICABLE,
            _NOW,
            boundary=_boundary(),
            history_capture=capture,
        )
    certify.assert_not_called()
    replay_boundary = evaluate.call_args.args[2]
    assert replay_boundary.venue_cursor is None
    assert replay_boundary.venue_cursor_certified is False
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "uncertified_boundary"


async def test_spot_branch_bounds_unexpected_errors() -> None:
    """An unexpected spot-branch failure degrades to a bounded error outcome.

    Given: A spot bundle read raising an unexpected exception,
    When: Dispatch evaluates the spot branch,
    Then: The evaluation is an error with the bounded reason text.
    """
    repository, _, _ = _repository()
    raw = cast(MagicMock, repository)
    raw.get_spot_reconciliation_bundle = AsyncMock(side_effect=RuntimeError("boom"))
    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(),
        _config("spot_execution_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
        boundary=_boundary(),
    )
    assert result["evaluation_status"] == "error"
    assert "boom" in str(result["error"])


@pytest.fixture
def loguru_errors() -> Iterator[list[str]]:
    """Capture ERROR-level loguru records so the refusal log can be asserted."""
    messages: list[str] = []

    def _sink(message: object) -> None:
        """Collect one rendered loguru record."""
        messages.append(str(message))

    handler_id = logger.add(_sink, level="ERROR")
    try:
        yield messages
    finally:
        logger.remove(handler_id)


async def test_a_permanent_ledger_refusal_is_not_an_ordinary_evaluation_error(
    loguru_errors: list[str],
) -> None:
    """A contradicted annulment is reported as a ledger refusal, not a hiccup.

    Given: A spot bundle read that raises the typed ``ExecutionChainError`` the
        annulment fold raises when a durable witness now contradicts a committed
        correction — a permanent condition, because both the manifest and its
        witnesses are append-only and neither can be withdrawn.
    When: Dispatch evaluates the spot branch.
    Then: The stored reason carries the machine-readable ledger-integrity
        prefix ahead of the ledger's own message, so an operator and any future
        automated quarantine can tell "this scope will never reconcile again
        until a human rules on the evidence" from "retry in a minute"; the
        correction and execution identities survive into the reason; and the
        same facts are logged at ERROR with the certification scope.
    """
    refusal = (
        "annulled_execution_witnessed: "
        "annulment_public_id=00000000-0000-7000-8000-0000000009a1 "
        "execution_public_id=00000000-0000-7000-8000-000000000e01 "
        "fill_event_public_id=00000000-0000-7000-8000-000000000f01"
    )
    repository, _, _ = _repository(spot_error=ExecutionChainError(refusal))

    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(),
        _config("spot_execution_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
        boundary=_boundary(),
    )

    assert result["evaluation_status"] == "error"
    assert result["error"] == f"{LEDGER_INTEGRITY_REFUSAL_PREFIX}{refusal}"
    assert "00000000-0000-7000-8000-0000000009a1" in str(result["error"])
    assert len(loguru_errors) == 1
    logged = loguru_errors[0]
    assert f"wallet={_WALLET}" in logged
    assert "exchange=kraken" in logged
    assert "mode=live" in logged
    assert "annulled_execution_witnessed" in logged


async def test_a_transient_spot_failure_never_borrows_the_ledger_refusal_marker(
    loguru_errors: list[str],
) -> None:
    """The two failure classes must not be spelled the same way.

    Given: A spot bundle read failing with an ordinary runtime error.
    When: Dispatch evaluates the spot branch.
    Then: The reason is the bare bounded message with NO ledger-integrity
        prefix and nothing is logged at ERROR — otherwise the prefix would mean
        nothing, and an automated quarantine keyed on it would quarantine
        scopes that merely need a retry.
    """
    repository, _, _ = _repository(spot_error=RuntimeError("bundle read timed out"))

    result = await reconciliation_dispatch.dispatch_portfolio_reconciliation(
        repository,
        _account(),
        _config("spot_execution_replay"),
        CapabilityStatus.NOT_APPLICABLE,
        _NOW,
        boundary=_boundary(),
    )

    assert result["evaluation_status"] == "error"
    assert result["error"] == "bundle read timed out"
    assert not str(result["error"]).startswith(LEDGER_INTEGRITY_REFUSAL_PREFIX)
    assert loguru_errors == []
