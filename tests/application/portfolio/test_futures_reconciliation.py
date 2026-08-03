"""Tests for the pure futures signed-quantity reconciler.

The suite pins capability selection, fail-closed account authority, signed
direction, exact Decimal threshold behavior, per-instrument S2a certification,
identity ambiguity, watermark provenance, canonical evidence, and compatibility
with the S1 writer. Numeric assertions are deliberately exact because SQLite's
dynamic type affinity can mask PostgreSQL DOUBLE PRECISION, NUMERIC, BIGINT,
and timezone round-trip defects.
"""

import json
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import cast
from unittest.mock import patch
from uuid import uuid7

import pytest

from snapper.application.portfolio.account_view import build_portfolio_account_state
from snapper.application.portfolio.futures_reconciliation import evaluate
from snapper.application.portfolio.reconciliation_view import no_portfolio_reconciliation_view
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import PortfolioReconciliationMethodConfig
from snapper.data.models import Position
from snapper.data.models import Symbol
from snapper.data.models import VenueAccountState
from snapper.data.models import Wallet
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import PositionRow
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.messaging.schemas.data import AccountPositionEntry
from snapper.messaging.schemas.data import PortfolioAccountState

_NOW = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
_WALLET = "00000000-0000-7000-8000-000000000101"
_ACCOUNT = "00000000-0000-7000-8000-000000000201"
_SESSION = "00000000-0000-7000-8000-000000000301"
_INSTRUMENT = "00000000-0000-7000-8000-000000000401"
_OTHER_INSTRUMENT = "00000000-0000-7000-8000-000000000402"
_SYMBOL = "PF_XBTUSD"
_OTHER_SYMBOL = "PF_ETHUSD"


def _position(
    *,
    instrument_public_id: str = _INSTRUMENT,
    instrument: str = _SYMBOL,
    quantity: float = 2.0,
    source_venue_event_id: int | None = 41,
    wallet_public_id: str = _WALLET,
    exchange: str = "kraken_futures",
    mode: str = "live",
) -> PositionRow:
    """Build one fill-derived projection row.

    Args:
        instrument_public_id: Durable instrument identity.
        instrument: Native symbol.
        quantity: Signed fill-derived quantity.
        source_venue_event_id: Maximum consumed venue-event identity.
        wallet_public_id: Owning wallet.
        exchange: Owning venue.
        mode: Trading mode.

    Returns:
        A complete typed position row.
    """
    return PositionRow(
        public_id="position-1",
        timestamp=_NOW,
        session_id=_SESSION,
        sequence_id=1,
        instrument=instrument,
        instrument_public_id=instrument_public_id,
        exchange=exchange,
        mode=mode,
        quantity=quantity,
        average_price=None,
        unrealized_pnl=None,
        realized_pnl=None,
        mark_price=None,
        marked_at=None,
        source_venue_event_id=source_venue_event_id,
        position_cycle_public_id=None,
        wallet_public_id=wallet_public_id,
    )


def _venue_position(
    *,
    symbol: str = _SYMBOL,
    side: str = "buy",
    size: float = 2.0,
) -> AccountPositionEntry:
    """Build one venue-native position entry.

    Args:
        symbol: Venue-native symbol.
        side: Venue position direction.
        size: Unsigned venue size.

    Returns:
        A strict account position entry.
    """
    return AccountPositionEntry(
        symbol=symbol,
        side=side,
        size=size,
        entry_price=1.0,
        mark_price=1.0,
        unrealized_pnl=0.0,
        unrealized_funding=0.0,
        timestamp=_NOW - timedelta(minutes=1),
    )


def _account(
    *,
    positions: list[AccountPositionEntry] | None = None,
    mode: ExecutionModeEnum = ExecutionModeEnum.LIVE,
    sequence_id: int = 7,
) -> PortfolioAccountState:
    """Build an authoritative venue account view.

    Args:
        positions: Open positions, defaulting to one matching long position.
        mode: Account trading mode.
        sequence_id: Observation sequence identity.

    Returns:
        A provenance-coherent account view.
    """
    resolved_positions = [_venue_position()] if positions is None else positions
    return PortfolioAccountState(
        session_id=_SESSION,
        sequence_id=sequence_id,
        public_id=_ACCOUNT,
        timestamp=_NOW - timedelta(minutes=1),
        wallet_public_id=_WALLET,
        exchange=ExchangeEnum.KRAKEN_FUTURES,
        mode=mode,
        sync_status="observed",
        effective_status="observed",
        is_authoritative=True,
        balance_status="observed",
        position_status="observed",
        valuation_status="native_only",
        balances=[],
        open_positions=resolved_positions,
        balance_observed_at=_NOW - timedelta(minutes=1),
        position_observed_at=_NOW - timedelta(minutes=1),
        authoritative_until=_NOW + timedelta(minutes=4),
        current_attempt_observation_id=17,
        balance_payload_source_observation_id=17,
        position_payload_source_observation_id=17,
        error=None,
        reconciliation=no_portfolio_reconciliation_view(),
    )


def _spec(
    *,
    instrument_public_id: str = _INSTRUMENT,
    lot_size: float | None = 1.0,
    contract_size: Decimal | None = Decimal("1.500000000000000001"),
    quantity_unit: str | None = "contract_count",
    spec_source: str | None = "kraken_futures:rest.get_instruments",
    spec_version: str | None = "s2a-v1:test",
    spec_observed_at: datetime | None = _NOW - timedelta(minutes=1),
    unit_certified: bool = True,
    status: str | None = "active",
    instrument_kind: str | None = "perpetual",
) -> InstrumentSpecRow:
    """Build one S2a instrument specification.

    Args:
        instrument_public_id: Durable instrument identity.
        lot_size: Certified comparison step.
        contract_size: Exact contract multiplier evidence.
        quantity_unit: Effective quantity unit.
        spec_source: Venue metadata source.
        spec_version: Venue metadata version.
        spec_observed_at: Metadata observation instant.
        unit_certified: Stored S2a certification flag.
        status: Instrument status.
        instrument_kind: Instrument product kind.

    Returns:
        A complete typed specification row.
    """
    return InstrumentSpecRow(
        instrument_public_id=instrument_public_id,
        tick_size=0.5,
        lot_size=lot_size,
        min_order_size=lot_size,
        max_order_size=None,
        cost_decimals=None,
        qty_decimals=1,
        margin_initial=0.02,
        position_limit_long=None,
        position_limit_short=None,
        status=status,
        contract_size=contract_size,
        quantity_unit=quantity_unit,
        spec_source=spec_source,
        spec_version=spec_version,
        spec_observed_at=spec_observed_at,
        unit_certified=unit_certified,
        expiry_at=None,
        instrument_kind=instrument_kind,
        funding_type=None,
        funding_frequency_hours=None,
        rollover_rate_long=None,
        rollover_rate_short=None,
        max_funding_rate=None,
    )


def _evaluate(
    *,
    projection: list[PositionRow] | None = None,
    account: PortfolioAccountState | None = None,
    mapping: dict[str, str] | None = None,
    specs: dict[str, InstrumentSpecRow | None] | None = None,
    capability: CapabilityStatus = CapabilityStatus.SUPPORTED,
    now: datetime = _NOW,
) -> PortfolioReconciliationEvaluationRow:
    """Evaluate defaults that describe a matching certified long position.

    Args:
        projection: Complete projection, defaulting to one matching row.
        account: Venue account, defaulting to authoritative matching truth.
        mapping: Native symbol resolution.
        specs: Specification evidence by public identity.
        capability: Typed venue capability.
        now: Evaluation instant.

    Returns:
        The pure evaluator result.
    """
    resolved_projection = [_position()] if projection is None else projection
    resolved_mapping = {_SYMBOL: _INSTRUMENT} if mapping is None else mapping
    resolved_specs = {_INSTRUMENT: _spec()} if specs is None else specs
    return evaluate(
        resolved_projection,
        account or _account(),
        resolved_mapping,
        resolved_specs,
        capability,
        now,
    )


def _payload(raw: str | None) -> dict[str, object]:
    """Parse canonical evaluator JSON for structural assertions.

    Args:
        raw: Required serialized evidence.

    Returns:
        Parsed JSON object.
    """
    assert raw is not None
    parsed = json.loads(raw)
    assert isinstance(parsed, dict)
    return cast(dict[str, object], parsed)


def _instrument_evidence(raw: str | None) -> list[dict[str, object]]:
    """Return typed instrument dictionaries from one evidence payload.

    Args:
        raw: Required serialized evidence.

    Returns:
        Parsed instrument evidence.
    """
    instruments = _payload(raw)["instruments"]
    assert isinstance(instruments, list)
    assert all(isinstance(item, dict) for item in instruments)
    return cast(list[dict[str, object]], instruments)


def test_supported_match_maps_every_s1_field_and_exact_decimal_evidence() -> None:
    """A certified long match yields complete canonical S1 writer evidence.

    Given: Matching internal and authoritative venue positions with a fresh spec.
    When: The supported futures evaluator compares the account.
    Then: Every S1 field and exact Decimal evidence string is populated canonically.

    SQLite can mask PostgreSQL NUMERIC and DOUBLE PRECISION conversion defects,
    so contract size and quantity strings are asserted byte-for-byte here and
    require a real PostgreSQL round trip in coordinator verification.
    """
    result = _evaluate()

    assert result == {
        "wallet_public_id": _WALLET,
        "exchange": "kraken_futures",
        "mode": "live",
        "method": "futures_position",
        "evaluation_status": "matched",
        "venue_account_state_public_id": _ACCOUNT,
        "venue_account_observation_id": 17,
        "account_authoritative_until": _NOW + timedelta(minutes=4),
        "source_watermark_kind": "venue_event_id",
        "source_watermark": 41,
        "anchor_public_id": None,
        "expected_json": '{"instruments":[{"instrument_public_id":"00000000-0000-7000-8000-000000000401","signed_qty":"2","source_venue_event_id":41,"symbol":"PF_XBTUSD"}]}',
        "actual_json": '{"instruments":[{"instrument_public_id":"00000000-0000-7000-8000-000000000401","side":"buy","signed_qty":"2","symbol":"PF_XBTUSD"}]}',
        "difference_json": '{"instruments":[{"absolute_delta":"0","instrument_public_id":"00000000-0000-7000-8000-000000000401","signed_delta":"0","status":"matched","symbol":"PF_XBTUSD"}]}',
        "tolerance_json": '{"instruments":[{"absolute_tolerance":"1","contract_size":"1.500000000000000001","instrument_public_id":"00000000-0000-7000-8000-000000000401","lot_step":"1","quantity_unit":"contract_count","spec_observed_at":"2026-07-14T11:59:00+00:00","spec_source":"kraken_futures:rest.get_instruments","spec_version":"s2a-v1:test","symbol":"PF_XBTUSD"}],"tolerance_lots":1}',
        "error": None,
        "session_id": _SESSION,
        "sequence_id": 7,
        "bus_time": _NOW,
    }


@pytest.mark.parametrize(
    ("internal", "side", "venue_size", "expected_status", "signed_delta"),
    [
        pytest.param(-2.0, "sell", 2.0, "matched", "0", id="short_short"),
        pytest.param(2.0, "sell", 2.0, "mismatched", "4", id="long_short"),
        pytest.param(-2.0, "buy", 2.0, "mismatched", "-4", id="short_long"),
        pytest.param(3.5, "buy", 2.0, "mismatched", "1.5", id="same_direction"),
    ],
)
def test_signed_direction_is_preserved(
    internal: float,
    side: str,
    venue_size: float,
    expected_status: str,
    signed_delta: str,
) -> None:
    """BUY is positive, SELL is negative, and absolute sizes are never compared.

    Given: Internal and venue quantities with independently varied directions.
    When: Signed futures quantities are reconciled.
    Then: The signed delta and reduced account outcome preserve both directions.

    Args:
        internal: Fill-derived signed quantity.
        side: Venue direction.
        venue_size: Venue-reported unsigned size.
        expected_status: Reduced account outcome.
        signed_delta: Exact expected signed difference string.
    """
    account = _account(positions=[_venue_position(side=side, size=venue_size)])
    result = _evaluate(projection=[_position(quantity=internal)], account=account)

    assert result["evaluation_status"] == expected_status
    assert _instrument_evidence(result["difference_json"])[0]["signed_delta"] == signed_delta


@pytest.mark.parametrize(
    ("internal", "venue_size", "lot_size", "expected_status", "absolute_delta"),
    [
        pytest.param(2.0, 2.0, 0.1, "matched", "0", id="zero"),
        pytest.param(2.09, 2.0, 0.1, "matched", "0.09", id="positive_sub_lot"),
        pytest.param(1.91, 2.0, 0.1, "matched", "0.09", id="negative_sub_lot"),
        pytest.param(2.1, 2.0, 0.1, "mismatched", "0.1", id="equal_lot"),
        pytest.param(
            2.1000000000000005, 2.0, 0.1, "mismatched", "0.1000000000000005", id="above_lot"
        ),
        pytest.param(4.0, 2.0, 0.1, "mismatched", "2", id="multi_lot"),
        pytest.param(
            0.30000000000000004, 0.2, 0.1, "mismatched", "0.10000000000000004", id="float_artifact"
        ),
    ],
)
def test_decimal_tolerance_boundary_is_exact(
    internal: float,
    venue_size: float,
    lot_size: float,
    expected_status: str,
    absolute_delta: str,
) -> None:
    """Decimal(str(value)) keeps exact thresholds and visible float artifacts.

    Given: Quantities around the certified one-lot comparison boundary.
    When: The evaluator converts each float boundary through its decimal string.
    Then: Equality and larger deltas mismatch while strictly smaller deltas match.

    SQLite-only coverage can hide PostgreSQL DOUBLE PRECISION return-value
    differences, so this test asserts the exact Decimal evidence string and
    equality-at-one-lot mismatch rule.

    Args:
        internal: Fill-derived quantity boundary value.
        venue_size: Venue quantity boundary value.
        lot_size: Certified comparison step.
        expected_status: Exact expected decision.
        absolute_delta: Exact expected Decimal string.
    """
    result = _evaluate(
        projection=[_position(quantity=internal)],
        account=_account(positions=[_venue_position(size=venue_size)]),
        specs={_INSTRUMENT: _spec(lot_size=lot_size)},
    )

    assert result["evaluation_status"] == expected_status
    assert _instrument_evidence(result["difference_json"])[0]["absolute_delta"] == absolute_delta


@pytest.mark.parametrize(
    ("projection", "positions", "expected_status", "watermark"),
    [
        pytest.param([_position(quantity=2.0)], [], "mismatched", 41, id="internal_only"),
        pytest.param([_position(quantity=0.5)], [], "matched", 41, id="internal_sub_lot"),
        pytest.param([], [_venue_position(size=2.0)], "mismatched", 0, id="venue_only"),
        pytest.param([], [_venue_position(size=0.5)], "matched", 0, id="venue_sub_lot"),
        pytest.param([], [], "matched", 0, id="flat_flat"),
    ],
)
def test_authoritative_missing_side_means_zero(
    projection: list[PositionRow],
    positions: list[AccountPositionEntry],
    expected_status: str,
    watermark: int,
) -> None:
    """Complete absence is zero while material and sub-lot residuals differ.

    Given: Complete internal and authoritative venue snapshots with one side absent.
    When: The union of their instrument identities is reconciled.
    Then: Absence becomes zero and the certified lot threshold decides the outcome.

    Args:
        projection: Complete internal account projection.
        positions: Complete authoritative venue list.
        expected_status: Expected account outcome.
        watermark: Expected aggregate BIGINT-compatible watermark.
    """
    result = _evaluate(projection=projection, account=_account(positions=positions))

    assert result["evaluation_status"] == expected_status
    assert result["source_watermark"] == watermark
    if projection or positions:
        expected = _instrument_evidence(result["expected_json"])[0]
        actual = _instrument_evidence(result["actual_json"])[0]
        assert expected["signed_qty"] == (
            str(projection[0]["quantity"]).rstrip(".0") if projection else "0"
        )
        assert actual["signed_qty"] == (str(positions[0].size).rstrip(".0") if positions else "0")


@pytest.mark.parametrize(
    "capability",
    [CapabilityStatus.UNSUPPORTED, CapabilityStatus.NOT_APPLICABLE],
)
def test_non_supported_capabilities_never_match(capability: CapabilityStatus) -> None:
    """Unsupported and structurally absent capabilities produce unsupported.

    Given: A typed capability that cannot supply supported futures positions.
    When: The evaluator performs its first method-selection gate.
    Then: It returns unsupported without presenting any full match evidence.

    Args:
        capability: Typed non-supported capability.
    """
    result = _evaluate(capability=capability)

    assert result["evaluation_status"] == "unsupported"
    assert result["method"] == "futures_position"
    assert result["expected_json"] is None
    assert result["source_watermark"] is None


def test_unavailable_projection_is_not_an_empty_projection() -> None:
    """A missing projection is incomplete and cannot use the flat watermark.

    Given: An unavailable internal projection and an authoritative empty venue book.
    When: The account is evaluated.
    Then: The outcome is incomplete with no fabricated zero watermark.
    """
    result = evaluate(
        None,
        _account(positions=[]),
        {},
        {},
        CapabilityStatus.SUPPORTED,
        _NOW,
    )

    assert result["evaluation_status"] == "incomplete"
    assert result["source_watermark"] is None


@pytest.mark.parametrize(
    "account_change",
    [
        pytest.param(
            lambda account: account.model_copy(update={"effective_status": "stale"}), id="stale"
        ),
        pytest.param(
            lambda account: account.model_copy(update={"effective_status": "clock_error"}),
            id="clock_error",
        ),
        pytest.param(
            lambda account: account.model_copy(update={"effective_status": "corrupt"}), id="corrupt"
        ),
        pytest.param(
            lambda account: account.model_copy(update={"effective_status": "error"}), id="error"
        ),
        pytest.param(
            lambda account: account.model_copy(update={"is_authoritative": False}),
            id="not_authoritative",
        ),
        pytest.param(
            lambda account: account.model_copy(update={"position_status": "unsupported"}),
            id="positions_not_observed",
        ),
        pytest.param(
            lambda account: account.model_copy(update={"open_positions": None}),
            id="missing_payload",
        ),
        pytest.param(
            lambda account: account.model_copy(update={"public_id": ""}), id="missing_state_id"
        ),
        pytest.param(
            lambda account: account.model_copy(update={"wallet_public_id": ""}), id="missing_wallet"
        ),
        pytest.param(
            lambda account: account.model_copy(update={"session_id": ""}), id="missing_session"
        ),
        pytest.param(
            lambda account: account.model_copy(update={"current_attempt_observation_id": None}),
            id="missing_observation",
        ),
        pytest.param(
            lambda account: account.model_copy(
                update={"position_payload_source_observation_id": 16}
            ),
            id="wrong_source",
        ),
        pytest.param(
            lambda account: account.model_copy(update={"authoritative_until": None}),
            id="missing_authority",
        ),
        pytest.param(
            lambda account: account.model_copy(
                update={"authoritative_until": _NOW - timedelta(seconds=1)}
            ),
            id="expired_authority",
        ),
        pytest.param(
            lambda account: account.model_copy(
                update={"authoritative_until": datetime(2026, 7, 14, 12, 1)}
            ),
            id="naive_authority",
        ),
        pytest.param(
            lambda account: account.model_copy(update={"position_observed_at": None}),
            id="missing_position_clock",
        ),
        pytest.param(
            lambda account: account.model_copy(
                update={"position_observed_at": _NOW + timedelta(seconds=1)}
            ),
            id="future_position_clock",
        ),
        pytest.param(
            lambda account: account.model_copy(
                update={"position_observed_at": datetime(2026, 7, 14, 11, 59)}
            ),
            id="naive_position_clock",
        ),
    ],
)
def test_non_authoritative_or_incoherent_account_never_matches(
    account_change: Callable[[PortfolioAccountState], PortfolioAccountState],
) -> None:
    """Every stale, unavailable, or provenance-incoherent account fails closed.

    Given: A venue account view with one authority or coherence invariant broken.
    When: A supported reconciliation is requested.
    Then: The account is incomplete and carries no full account-state provenance.

    Args:
        account_change: Mutation-free model-copy transformation under test.
    """
    result = _evaluate(account=account_change(_account()))

    assert result["evaluation_status"] == "incomplete"
    assert result["venue_account_state_public_id"] is None


def test_naive_evaluation_time_is_incomplete() -> None:
    """A naive caller clock cannot support authority or certification.

    Given: Otherwise complete matching evidence and a timezone-naive evaluation time.
    When: The evaluator checks the account authority envelope.
    Then: It returns incomplete before claiming certified comparison evidence.
    """
    result = _evaluate(now=datetime(2026, 7, 14, 12, 0))

    assert result["evaluation_status"] == "incomplete"


def test_paper_and_simulated_calls_are_explicitly_refused() -> None:
    """S1 live-only persistence cannot accept paper or simulated truth.

    Given: Paper, simulated, or unknown capability invocations outside S2's domain.
    When: The live-only futures evaluator is called directly.
    Then: It raises an explicit orchestration error rather than manufacturing a row.
    """
    paper_account = _account(mode=ExecutionModeEnum.PAPER)
    unexpected_capability = cast(CapabilityStatus, "unexpected")
    with pytest.raises(ValueError, match="live account"):
        _evaluate(account=paper_account)
    with pytest.raises(ValueError, match="simulated positions"):
        _evaluate(capability=CapabilityStatus.SIMULATED)
    with pytest.raises(ValueError, match="unknown futures"):
        _evaluate(capability=unexpected_capability)


@pytest.mark.parametrize(
    ("size", "side"),
    [
        pytest.param(0.0, "buy", id="zero"),
        pytest.param(-1.0, "buy", id="negative"),
        pytest.param(float("nan"), "buy", id="nan"),
        pytest.param(float("inf"), "buy", id="infinity"),
        pytest.param(1.0, "flat", id="bad_side"),
    ],
)
def test_invalid_venue_quantity_or_side_is_incomplete(size: float, side: str) -> None:
    """Ambiguous venue rows never normalize to an absent zero position.

    Given: A venue open-position entry with an invalid size or direction.
    When: The position is reconciled with a valid internal projection.
    Then: The instrument is incomplete and no signed delta is fabricated.

    Args:
        size: Invalid venue quantity.
        side: Venue direction under test.
    """
    position = _venue_position().model_copy(update={"size": size, "side": side})
    result = _evaluate(account=_account(positions=[position]))

    assert result["evaluation_status"] == "incomplete"
    difference = _instrument_evidence(result["difference_json"])[0]
    assert difference["status"] == "incomplete"
    assert "signed_delta" not in difference


@pytest.mark.parametrize("quantity", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_internal_quantity_is_incomplete(quantity: float) -> None:
    """Non-finite internal quantities never enter Decimal arithmetic.

    Given: A fill-derived position containing a non-finite float quantity.
    When: The evaluator validates its numeric boundary.
    Then: The result is incomplete and serialized evidence contains no non-finite token.

    Args:
        quantity: Non-finite projection quantity.
    """
    result = _evaluate(projection=[_position(quantity=quantity)])

    assert result["evaluation_status"] == "incomplete"
    assert "NaN" not in cast(str, result["expected_json"])
    assert "Infinity" not in cast(str, result["expected_json"])


@pytest.mark.parametrize(
    "specification",
    [
        pytest.param(None, id="missing"),
        pytest.param(_spec(unit_certified=False), id="stored_uncertified"),
        pytest.param(_spec(spec_observed_at=None), id="missing_observation"),
        pytest.param(_spec(spec_observed_at=_NOW - timedelta(hours=12)), id="exactly_twelve_hours"),
        pytest.param(_spec(spec_observed_at=_NOW - timedelta(hours=13)), id="stale"),
        pytest.param(_spec(spec_observed_at=_NOW + timedelta(seconds=1)), id="future"),
        pytest.param(_spec(spec_observed_at=datetime(2026, 7, 14, 11, 59)), id="naive"),
        pytest.param(_spec(lot_size=None), id="missing_lot"),
        pytest.param(_spec(lot_size=0.0), id="zero_lot"),
        pytest.param(_spec(lot_size=-1.0), id="negative_lot"),
        pytest.param(_spec(lot_size=float("nan")), id="nan_lot"),
        pytest.param(_spec(lot_size=float("inf")), id="infinite_lot"),
        pytest.param(_spec(quantity_unit="base_asset"), id="wrong_unit"),
        pytest.param(_spec(contract_size=None), id="missing_contract"),
        pytest.param(_spec(contract_size=Decimal("0")), id="zero_contract"),
        pytest.param(_spec(contract_size=Decimal("-1")), id="negative_contract"),
        pytest.param(_spec(contract_size=Decimal("NaN")), id="nan_contract"),
        pytest.param(_spec(contract_size=Decimal("Infinity")), id="infinite_contract"),
        pytest.param(_spec(spec_source=None), id="missing_source"),
        pytest.param(_spec(spec_version=None), id="missing_version"),
        pytest.param(_spec(status="inactive"), id="inactive"),
        pytest.param(_spec(instrument_kind="spot"), id="spot"),
        pytest.param(_spec(instrument_kind="etf"), id="etf"),
        pytest.param(_spec(instrument_kind="option"), id="option"),
        pytest.param(_spec(instrument_kind=None), id="unknown_kind"),
        pytest.param(_spec(instrument_public_id=_OTHER_INSTRUMENT), id="wrong_spec_identity"),
    ],
)
def test_uncertified_or_unusable_spec_is_incomplete(
    specification: InstrumentSpecRow | None,
) -> None:
    """No missing, stale, wrong-unit, or unusable spec can produce a match.

    Given: Matching quantities and one unusable S2a specification variant.
    When: Per-instrument effective-unit certification is evaluated.
    Then: The account is incomplete and no fabricated lot tolerance is claimed.

    Args:
        specification: S2a evidence variant under test.
    """
    result = _evaluate(specs={_INSTRUMENT: specification})

    assert result["evaluation_status"] == "incomplete"
    tolerance = _instrument_evidence(result["tolerance_json"])[0]
    assert "reason" in tolerance
    assert "lot_step" not in tolerance or specification is not None


@pytest.mark.parametrize("instrument_kind", ["perpetual", "future"])
def test_certified_futures_kinds_are_accepted(instrument_kind: str) -> None:
    """Perpetual and dated future specifications certify independently.

    Given: A fresh certified specification for a supported futures product kind.
    When: Matching signed quantities are evaluated.
    Then: The account produces a full match without family-based inference.

    Args:
        instrument_kind: Supported futures product kind.
    """
    result = _evaluate(specs={_INSTRUMENT: _spec(instrument_kind=instrument_kind)})

    assert result["evaluation_status"] == "matched"


def test_certification_freshness_just_below_twelve_hours_is_accepted() -> None:
    """S2a certification remains effective only strictly below twelve hours.

    Given: Unit evidence observed one microsecond less than twelve hours ago.
    When: The evaluator calls the shipped certification predicate.
    Then: Matching signed quantities remain fully certified and matched.
    """
    specification = _spec(spec_observed_at=_NOW - timedelta(hours=12) + timedelta(microseconds=1))

    assert _evaluate(specs={_INSTRUMENT: specification})["evaluation_status"] == "matched"


def test_multiple_instruments_reduce_with_incomplete_dominance_and_max_watermark() -> None:
    """Incomplete dominates mismatch and watermark selection uses the exact maximum.

    Given: Multiple instruments containing a match, a mismatch, and missing certification.
    When: Instrument outcomes and venue-event provenance are reduced account-wide.
    Then: Incomplete dominates and the exact largest watermark is retained.

    BIGINT provenance is asserted as an integer because SQLite affinity can
    conceal PostgreSQL large-integer binding and round-trip defects.
    """
    large_watermark = 9_223_372_036_854_775_000
    projection = [
        _position(source_venue_event_id=41),
        _position(
            instrument_public_id=_OTHER_INSTRUMENT,
            instrument=_OTHER_SYMBOL,
            quantity=5.0,
            source_venue_event_id=large_watermark,
        ),
    ]
    account = _account(
        positions=[_venue_position(), _venue_position(symbol=_OTHER_SYMBOL, size=1.0)]
    )
    result = _evaluate(
        projection=projection,
        account=account,
        mapping={_SYMBOL: _INSTRUMENT, _OTHER_SYMBOL: _OTHER_INSTRUMENT},
        specs={_INSTRUMENT: _spec(), _OTHER_INSTRUMENT: None},
    )

    assert result["evaluation_status"] == "incomplete"
    assert result["source_watermark"] == large_watermark
    assert [
        item["instrument_public_id"] for item in _instrument_evidence(result["difference_json"])
    ] == [
        _INSTRUMENT,
        _OTHER_INSTRUMENT,
    ]


def test_multiple_certified_instruments_reduce_to_mismatch() -> None:
    """One full instrument mismatch dominates otherwise matched instruments.

    Given: Multiple fully certified instruments with exactly one material delta.
    When: Their per-instrument decisions are reduced.
    Then: The complete account outcome is mismatched with the maximum watermark.
    """
    result = _evaluate(
        projection=[
            _position(),
            _position(
                instrument_public_id=_OTHER_INSTRUMENT,
                instrument=_OTHER_SYMBOL,
                quantity=3.0,
                source_venue_event_id=99,
            ),
        ],
        account=_account(
            positions=[_venue_position(), _venue_position(symbol=_OTHER_SYMBOL, size=1.0)]
        ),
        mapping={_SYMBOL: _INSTRUMENT, _OTHER_SYMBOL: _OTHER_INSTRUMENT},
        specs={
            _INSTRUMENT: _spec(),
            _OTHER_INSTRUMENT: _spec(instrument_public_id=_OTHER_INSTRUMENT),
        },
    )

    assert result["evaluation_status"] == "mismatched"
    assert result["source_watermark"] == 99


@pytest.mark.parametrize(
    "projection",
    [
        pytest.param([_position(wallet_public_id="other-wallet")], id="foreign_wallet"),
        pytest.param([_position(exchange="kraken")], id="foreign_exchange"),
        pytest.param([_position(mode="paper")], id="paper_row"),
        pytest.param([_position(instrument_public_id="")], id="missing_instrument_id"),
        pytest.param([_position(instrument="")], id="missing_symbol"),
        pytest.param([_position(source_venue_event_id=None)], id="unknown_watermark"),
    ],
)
def test_projection_scope_identity_and_provenance_fail_closed(
    projection: list[PositionRow],
) -> None:
    """Foreign, unidentified, or unprovenanced rows are never filtered away.

    Given: A projection row outside account scope or lacking identity or provenance.
    When: The complete caller-supplied projection is validated.
    Then: The row remains fail-closed and makes the account incomplete.

    Args:
        projection: Invalid complete projection candidate.
    """
    result = _evaluate(projection=projection)

    assert result["evaluation_status"] == "incomplete"


def test_explicit_net_zero_projection_row_is_consumed_verbatim() -> None:
    """S2 consumes one existing net row and does not reaggregate strategy shards.

    Given: One durable net-zero projection row and an authoritative empty venue book.
    When: The account is reconciled without reaching into strategy shards.
    Then: The explicit zero is matched verbatim with the empty watermark convention.
    """
    result = _evaluate(
        projection=[_position(quantity=0.0, source_venue_event_id=None)],
        account=_account(positions=[]),
    )

    assert result["evaluation_status"] == "matched"
    assert result["source_watermark"] == 0
    assert _instrument_evidence(result["expected_json"])[0]["signed_qty"] == "0"


def test_duplicate_internal_and_duplicate_venue_rows_are_incomplete() -> None:
    """Ambiguous duplicate rows are never summed and evidence is deterministic.

    Given: Duplicate internal or venue rows resolving to one instrument identity.
    When: Candidate identities are assembled.
    Then: Each ambiguity yields an incomplete reason instead of summed quantities.
    """
    internal_result = _evaluate(projection=[_position(), _position(quantity=3.0)])
    venue_result = _evaluate(
        account=_account(
            positions=[_venue_position(), _venue_position(symbol="PI_XBTUSD", size=3.0)]
        ),
        mapping={_SYMBOL: _INSTRUMENT, "PI_XBTUSD": _INSTRUMENT},
    )

    assert internal_result["evaluation_status"] == "incomplete"
    assert venue_result["evaluation_status"] == "incomplete"
    assert "duplicate_internal_position" in cast(str, internal_result["difference_json"])
    assert "duplicate_venue_position" in cast(str, venue_result["difference_json"])


def test_unresolved_venue_symbol_and_internal_mapping_conflict_remain_visible() -> None:
    """Unresolvable and contradictory symbol identities produce retained evidence.

    Given: An unresolved venue symbol or mapping that contradicts an internal row.
    When: Symbol identities are resolved for comparison.
    Then: The account is incomplete and the problematic symbol remains in evidence.
    """
    unresolved = _evaluate(mapping={})
    conflict = _evaluate(mapping={_SYMBOL: _OTHER_INSTRUMENT})

    assert unresolved["evaluation_status"] == "incomplete"
    unresolved_actual = _instrument_evidence(unresolved["actual_json"])
    assert unresolved_actual[-1]["instrument_public_id"] is None
    assert unresolved_actual[-1]["symbol"] == _SYMBOL
    assert conflict["evaluation_status"] == "incomplete"
    assert "internal_symbol_mapping_conflict" in cast(str, conflict["difference_json"])


def test_canonical_order_replay_and_inputs_are_unchanged() -> None:
    """Input order cannot change JSON and repeated evaluation is identical.

    Given: Equivalent multi-instrument inputs in opposite orders.
    When: Both are evaluated at the same captured instant.
    Then: Rows are identical and every caller-owned input remains unchanged.
    """
    first_position = _position()
    second_position = _position(
        instrument_public_id=_OTHER_INSTRUMENT,
        instrument=_OTHER_SYMBOL,
        quantity=3.0,
        source_venue_event_id=99,
    )
    first_venue = _venue_position()
    second_venue = _venue_position(symbol=_OTHER_SYMBOL, size=3.0)
    mapping = {_OTHER_SYMBOL: _OTHER_INSTRUMENT, _SYMBOL: _INSTRUMENT}
    specs = {
        _OTHER_INSTRUMENT: _spec(instrument_public_id=_OTHER_INSTRUMENT),
        _INSTRUMENT: _spec(),
    }
    projection = [second_position, first_position]
    account = _account(positions=[second_venue, first_venue])
    projection_snapshot = [row.copy() for row in projection]
    account_snapshot = account.model_copy(deep=True)

    first = _evaluate(projection=projection, account=account, mapping=mapping, specs=specs)
    second = _evaluate(
        projection=list(reversed(projection)),
        account=_account(positions=[first_venue, second_venue]),
        mapping=mapping,
        specs=specs,
    )

    assert first == second
    assert projection == projection_snapshot
    assert account == account_snapshot


def test_unexpected_evaluation_failure_returns_bounded_error() -> None:
    """An actual evaluator failure yields non-empty S1-compatible error text.

    Given: An unexpected failure inside effective-unit certification.
    When: The evaluator catches the actual comparison failure.
    Then: It returns a trimmed non-empty error bounded to S1's 512 characters.
    """
    message = "  " + "x" * 600 + "  "
    with patch(
        "snapper.application.portfolio.futures_reconciliation.is_effective_unit_certified",
        side_effect=RuntimeError(message),
    ):
        result = _evaluate()

    assert result["evaluation_status"] == "error"
    assert result["error"] == "x" * 512
    assert result["expected_json"] is None


async def test_evaluator_rows_are_accepted_by_s1_writer() -> None:
    """S1 accepts canonical rows and owns mismatch episode progression.

    Given: Three full evaluator mismatches followed by an incomplete evaluation.
    When: The test explicitly passes each immutable row to the S1 writer.
    Then: S1 opens the third-mismatch episode and incomplete does not advance it.

    This integration contract runs on SQLite in the default suite, whose type
    affinity can mask PostgreSQL checks, BIGINT, timezone, and JSON persistence
    bugs. The coordinator must rerun it against a disposable PostgreSQL
    ``TEST_DB_URL`` and separately round-trip S2a NUMERIC contract evidence.

    """
    wallet_public_id = str(uuid7())
    account_public_id = str(uuid7())
    session_id = str(uuid7())
    repo = SQLAlchemyRepository(BootstrapSettingsLoader().db_url)
    try:
        async with repo.session() as session:
            session.add_all(
                [
                    Wallet(
                        public_id=wallet_public_id,
                        label=f"futures-evaluator-{wallet_public_id}",
                        description=None,
                        is_paper=False,
                        session_id=session_id,
                        sequence_id=1,
                        timestamp=_NOW - timedelta(days=1),
                    ),
                    PortfolioReconciliationMethodConfig(
                        wallet_public_id=wallet_public_id,
                        exchange="kraken_futures",
                        mode="live",
                        method="futures_position",
                        session_id=session_id,
                        sequence_id=2,
                        timestamp=_NOW - timedelta(days=1),
                    ),
                ]
            )
            await session.commit()
        for sequence_id in (1, 2, 3):
            account = _account(
                positions=[_venue_position(size=1.0)],
                sequence_id=sequence_id,
            ).model_copy(
                update={
                    "wallet_public_id": wallet_public_id,
                    "public_id": account_public_id,
                    "session_id": session_id,
                }
            )
            mismatch = _evaluate(
                projection=[_position(wallet_public_id=wallet_public_id)],
                account=account,
            )
            assert mismatch["evaluation_status"] == "mismatched"
            await repo.record_portfolio_reconciliation(mismatch)

        states = await repo.get_portfolio_reconciliation_states([wallet_public_id])
        assert len(states) == 1
        assert states[0]["consecutive_full_mismatches"] == 3
        assert states[0]["open_drift_episode_public_id"] is not None
        assert states[0]["expected_json"] == mismatch["expected_json"]

        incomplete_account = _account(sequence_id=4).model_copy(
            update={
                "wallet_public_id": wallet_public_id,
                "public_id": account_public_id,
                "session_id": session_id,
                "is_authoritative": False,
            }
        )
        incomplete = _evaluate(
            projection=[_position(wallet_public_id=wallet_public_id)],
            account=incomplete_account,
        )
        await repo.record_portfolio_reconciliation(incomplete)
        retained = (await repo.get_portfolio_reconciliation_states([wallet_public_id]))[0]
        assert retained["current_evaluation_status"] == "incomplete"
        assert retained["consecutive_full_mismatches"] == 3
    finally:
        await repo.engine.dispose()


async def test_configured_database_numeric_and_provenance_round_trip() -> None:
    """Persist every PostgreSQL-sensitive S2 boundary and write its S1 result.

    Given: Persisted position, S2a spec, and venue-account rows at numeric boundaries.
    When: They are reread, evaluated, and the immutable result is written through S1.
    Then: Exact decimals, timestamps, JSON, BIGINT provenance, and mismatch survive.

    SQLite stores DOUBLE PRECISION-like values with dynamic affinity and uses
    an exact-text fallback for ``ExactDecimalNumeric``. It therefore cannot
    prove asyncpg's float, native NUMERIC, BIGINT, TIMESTAMP WITH TIME ZONE,
    partial-index, live-only CHECK, or Text JSON behavior. This test is
    intentionally bound to the configured test database so the coordinator's
    disposable PostgreSQL ``TEST_DB_URL`` run exercises the real driver and
    asserts every exact value after persistence.
    """
    repo = SQLAlchemyRepository(BootstrapSettingsLoader().db_url)
    symbol_public_id = str(uuid7())
    instrument_public_id = str(uuid7())
    wallet_public_id = str(uuid7())
    account_public_id = str(uuid7())
    session_id = str(uuid7())
    position_public_id = str(uuid7())
    spec_public_id = str(uuid7())
    native_symbol = f"PF_S2_{uuid7().hex[:8]}"
    persisted_at = _NOW - timedelta(hours=13)
    spec_observed_at = _NOW - timedelta(hours=12) + timedelta(microseconds=1)
    contract_size = Decimal("1.500000000000000001")
    position_watermark = 2_000_000_000
    venue_json = json.dumps(
        [
            {
                "symbol": native_symbol,
                "side": "buy",
                "size": 0.2,
                "entry_price": 1.0,
                "mark_price": 1.0,
                "unrealized_pnl": 0.0,
                "unrealized_funding": 0.0,
                "timestamp": (_NOW - timedelta(minutes=1)).isoformat(),
            }
        ],
        separators=(",", ":"),
        sort_keys=True,
    )
    try:
        async with repo.session() as session:
            session.add_all(
                [
                    Wallet(
                        public_id=wallet_public_id,
                        label=f"futures-round-trip-{wallet_public_id}",
                        description=None,
                        is_paper=False,
                        session_id=session_id,
                        sequence_id=1,
                        timestamp=persisted_at,
                    ),
                    PortfolioReconciliationMethodConfig(
                        wallet_public_id=wallet_public_id,
                        exchange="kraken_futures",
                        mode="live",
                        method="futures_position",
                        session_id=session_id,
                        sequence_id=2,
                        timestamp=persisted_at,
                    ),
                    Symbol(
                        native_symbol=native_symbol,
                        base="S2",
                        quote="USD",
                        asset_type="crypto",
                        created_at=persisted_at,
                        public_id=symbol_public_id,
                        session_id=session_id,
                        sequence_id=3,
                        timestamp=persisted_at,
                    ),
                    Instrument(
                        symbol_public_id=symbol_public_id,
                        exchange="kraken_futures",
                        source_exchange=None,
                        public_id=instrument_public_id,
                        session_id=session_id,
                        sequence_id=4,
                        timestamp=persisted_at,
                    ),
                    Position(
                        instrument_public_id=instrument_public_id,
                        mode="live",
                        wallet_public_id=wallet_public_id,
                        quantity=0.30000000000000004,
                        average_price=None,
                        unrealized_pnl=None,
                        realized_pnl=0.0,
                        mark_price=None,
                        marked_at=None,
                        source_venue_event_id=position_watermark,
                        public_id=position_public_id,
                        session_id=session_id,
                        sequence_id=5,
                        timestamp=persisted_at,
                    ),
                    InstrumentSpec(
                        instrument_public_id=instrument_public_id,
                        tick_size=0.1,
                        lot_size=0.1,
                        min_order_size=0.1,
                        max_order_size=None,
                        cost_decimals=None,
                        qty_decimals=1,
                        margin_initial=0.02,
                        position_limit_long=None,
                        position_limit_short=None,
                        status="active",
                        expiry_at=None,
                        instrument_kind="perpetual",
                        funding_type="perpetual_funding",
                        funding_frequency_hours=1,
                        rollover_rate_long=None,
                        rollover_rate_short=None,
                        max_funding_rate=None,
                        contract_size=contract_size,
                        quantity_unit="contract_count",
                        spec_source="kraken_futures:rest.get_instruments",
                        spec_version="s2a-v1:postgres-round-trip",
                        spec_observed_at=spec_observed_at,
                        unit_certified=True,
                        public_id=spec_public_id,
                        session_id=session_id,
                        sequence_id=6,
                        timestamp=persisted_at,
                    ),
                    VenueAccountState(
                        wallet_public_id=wallet_public_id,
                        exchange="kraken_futures",
                        mode="live",
                        sync_status="observed",
                        balance_status="observed",
                        position_status="observed",
                        valuation_status="native_only",
                        balances_json="[]",
                        open_positions_json=venue_json,
                        balance_observed_at=_NOW - timedelta(minutes=1),
                        position_observed_at=_NOW - timedelta(minutes=1),
                        current_attempt_observation_id=701,
                        balance_payload_source_observation_id=701,
                        position_payload_source_observation_id=701,
                        authoritative_until=_NOW + timedelta(minutes=4),
                        error=None,
                        public_id=account_public_id,
                        session_id=session_id,
                        sequence_id=7,
                        timestamp=persisted_at,
                    ),
                ]
            )
            await session.commit()

        projection = await repo.get_positions(_NOW, [wallet_public_id])
        specification = await repo.get_instrument_spec(instrument_public_id, _NOW)
        account_rows = await repo.get_venue_account_states([wallet_public_id])
        mapping = await repo.get_instrument_public_ids_by_symbols(
            {native_symbol},
            "kraken_futures",
            _NOW,
        )
        assert len(projection) == 1
        assert projection[0]["quantity"] == 0.30000000000000004
        assert projection[0]["source_venue_event_id"] == position_watermark
        assert specification is not None
        assert specification["contract_size"] == contract_size
        assert specification["spec_observed_at"] == spec_observed_at
        assert len(account_rows) == 1
        venue_account = build_portfolio_account_state(account_rows[0], _NOW)
        assert venue_account.is_authoritative
        assert venue_account.open_positions is not None
        assert venue_account.open_positions[0].size == 0.2
        assert mapping == {native_symbol: instrument_public_id}

        large_watermark = 9_223_372_036_854_775_000
        evaluator_projection = [projection[0].copy()]
        evaluator_projection[0]["source_venue_event_id"] = large_watermark
        result = evaluate(
            evaluator_projection,
            venue_account,
            mapping,
            {instrument_public_id: specification},
            CapabilityStatus.SUPPORTED,
            _NOW,
        )
        assert result["evaluation_status"] == "mismatched"
        assert result["source_watermark"] == large_watermark
        assert _instrument_evidence(result["difference_json"])[0]["absolute_delta"] == (
            "0.10000000000000004"
        )
        assert _instrument_evidence(result["tolerance_json"])[0]["contract_size"] == (
            "1.500000000000000001"
        )

        exactly_stale = evaluate(
            evaluator_projection,
            venue_account,
            mapping,
            {instrument_public_id: specification},
            CapabilityStatus.SUPPORTED,
            _NOW + timedelta(microseconds=1),
        )
        assert exactly_stale["evaluation_status"] == "incomplete"
        future_evaluation_at = spec_observed_at - timedelta(seconds=1)
        earlier_account = venue_account.model_copy(
            update={
                "position_observed_at": future_evaluation_at - timedelta(minutes=1),
                "authoritative_until": future_evaluation_at + timedelta(minutes=4),
            }
        )
        future_spec = evaluate(
            evaluator_projection,
            earlier_account,
            mapping,
            {instrument_public_id: specification},
            CapabilityStatus.SUPPORTED,
            future_evaluation_at,
        )
        assert future_spec["evaluation_status"] == "incomplete"

        await repo.record_portfolio_reconciliation(result)
        stored = (await repo.get_portfolio_reconciliation_states([wallet_public_id]))[0]
        assert stored["current_evaluation_status"] == "mismatched"
        assert stored["source_watermark"] == large_watermark
        assert stored["expected_json"] == result["expected_json"]
        assert stored["actual_json"] == result["actual_json"]
        assert stored["difference_json"] == result["difference_json"]
        assert stored["tolerance_json"] == result["tolerance_json"]
    finally:
        await repo.engine.dispose()
