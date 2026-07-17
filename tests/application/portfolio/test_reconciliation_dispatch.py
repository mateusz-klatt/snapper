"""Tests for durable-method portfolio reconciliation dispatch."""

from dataclasses import fields
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.portfolio import reconciliation_dispatch
from snapper.application.portfolio.reconciliation_dispatch import SpotReplayBoundaryCapture
from snapper.application.portfolio.reconciliation_view import no_portfolio_reconciliation_view
from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import FuturesReconciliationBundle
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import PortfolioReconciliationMethodConfigRow
from snapper.data.repository_types import PositionRow
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
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


def _repository(
    *,
    durable_signal: bool = False,
    bundle: FuturesReconciliationBundle | None = None,
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


async def test_spot_boundary_present_reports_bundle_as_the_remaining_gap() -> None:
    """A genuine pre-balance boundary shifts the incomplete reason truthfully.

    Given: A cash spot account whose observer captured a pre-balance boundary.
    When: Dispatch evaluates the spot branch with that boundary threaded in.
    Then: The evaluation stays incomplete but reports spot_bundle_unavailable,
        making the S4c-2 capture observable without behavior risk.
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
    assert result["error"] == "spot_bundle_unavailable"
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
    with pytest.raises(ValueError, match=reason):
        replace(_boundary(), **changes)


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
