"""Price contract exposure using certified explicit payoff and currency metadata."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import Overflow
from decimal import localcontext
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.application.pricing.usd_converter import PriceUnavailableError
from snapper.application.pricing.usd_converter import USDConverter
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import MarketSnapshot
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_ETL_VERSION
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE

_NOW = datetime(2026, 10, 22, 22, 0, tzinfo=UTC)
_INSTRUMENT = "contract-instrument"


def _spec(model: str) -> InstrumentSpec:
    """Build explicit current quantity and payoff evidence for one USD contract.

    Args:
        model: Explicit linear or inverse payoff model.

    Returns:
        A mutable ORM row that each test can alter before persistence.
    """
    return InstrumentSpec(
        public_id="contract-spec",
        instrument_public_id=_INSTRUMENT,
        timestamp=_NOW,
        known_to=KNOWN_TO_MAX,
        session_id="test",
        sequence_id=1,
        contract_size=Decimal(1),
        quantity_unit="contract_count",
        native_order_quantity_unit="contract_count",
        instrument_kind="perpetual",
        spec_source=FUTURES_SPEC_SOURCE,
        spec_version=f"{FUTURES_SPEC_ETL_VERSION}:test",
        spec_observed_at=_NOW,
        unit_certified=True,
        contract_payoff_model=model,
        contract_value_currency="BTC" if model == "linear" else "USD",
        settlement_currency="USD" if model == "linear" else "BTC",
    )


async def _market(
    repo: SQLAlchemyRepository,
    spec: InstrumentSpec | None,
    *,
    exchange: str = "kraken_futures",
) -> None:
    """Persist a derivative market whose ticker name supplies no payoff information.

    Args:
        repo: Isolated in-memory repository.
        spec: Optional atomic sizing and payoff evidence.
        exchange: Exact venue identity, including unknown-venue adversarial cases.
    """
    await repo.create_all()
    async with repo.session() as session:
        session.add(
            Symbol(
                public_id="contract-symbol",
                native_symbol="OPAQUE-MARKET",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=_NOW,
                timestamp=_NOW,
                known_to=KNOWN_TO_MAX,
                session_id="test",
                sequence_id=1,
            )
        )
        session.add(
            Instrument(
                public_id=_INSTRUMENT,
                symbol_public_id="contract-symbol",
                exchange=exchange,
                timestamp=_NOW,
                known_to=KNOWN_TO_MAX,
                session_id="test",
                sequence_id=1,
            )
        )
        session.add(
            MarketSnapshot(
                public_id="contract-snapshot",
                instrument_public_id=_INSTRUMENT,
                last_price=100_000,
                timestamp=_NOW,
                known_to=KNOWN_TO_MAX,
                session_id="test",
                sequence_id=1,
            )
        )
        if spec is not None:
            session.add(spec)
        await session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model,multiplier,quantity,expected",
    [
        ("linear", "1", "0.0001", "10"),
        ("linear", "0.01", "2", "2000"),
        ("inverse", "1", "1000", "1000"),
        ("inverse", "10", "1000", "10000"),
        ("inverse", "1", "0", "0"),
    ],
)
async def test_contract_notional_uses_explicit_payoff_units(
    model: str, multiplier: str, quantity: str, expected: str
) -> None:
    """Distinguish USD contract value from spot-like quantity times price.

    Given: a fresh contract with explicit multiplier denomination and payoff model.
    When: its order size is valued twice through the admission oracle.
    Then: both initial and cached results equal the exact unsigned USD notional.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    spec = _spec(model)
    spec.contract_size = Decimal(multiplier)
    await _market(repo, spec)
    converter = USDConverter(repo, now=lambda: _NOW)
    assert await converter.to_usd(_INSTRUMENT, Decimal(quantity)) == Decimal(expected)
    assert await converter.to_usd(_INSTRUMENT, Decimal(quantity)) == Decimal(expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["spec", "payoff", "old_version", "source", "uncertified"])
async def test_incomplete_derivative_evidence_never_falls_back_to_spot(missing: str) -> None:
    """Refuse unavailable semantics instead of inventing an exposure number.

    Given: a USD-quoted derivative with missing or obsolete monetary evidence.
    When: admission asks for its notional.
    Then: contract_valuation_unavailable is raised instead of multiplying by price.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    spec = _spec("inverse")
    if missing == "payoff":
        spec.contract_payoff_model = None
        spec.contract_value_currency = None
        spec.settlement_currency = None
    elif missing == "old_version":
        spec.spec_version = "s2a-v1:obsolete"
    elif missing == "source":
        spec.spec_source = "unverified"
    elif missing == "uncertified":
        spec.unit_certified = False
    await _market(repo, None if missing == "spec" else spec)
    converter = USDConverter(repo, now=lambda: _NOW)
    with pytest.raises(PriceUnavailableError) as caught:
        await converter.to_usd(_INSTRUMENT, Decimal(1000))
    assert caught.value.reason_code == "contract_valuation_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize("model", ["linear", "inverse"])
async def test_mismatched_contract_currencies_are_rejected(model: str) -> None:
    """Keep multiplier and settlement denominations bound to the exact market.

    Given: an explicit payoff model whose multiplier currency contradicts its base/quote.
    When: USD admission values the contract.
    Then: the malformed evidence is refused without a spot fallback.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    spec = _spec(model)
    spec.contract_value_currency = "EUR"
    await _market(repo, spec)
    with pytest.raises(PriceUnavailableError, match="contract_valuation_unavailable"):
        await USDConverter(repo, now=lambda: _NOW).to_usd(_INSTRUMENT, Decimal(1))


@pytest.mark.asyncio
@pytest.mark.parametrize("expiry_present", [False, True])
async def test_dated_contract_requires_unexpired_timestamp(expiry_present: bool) -> None:
    """Refuse an expired or incompletely identified fixed-maturity contract.

    Given: a future with either no expiry evidence or an expiry exactly at now.
    When: the order's USD notional is requested.
    Then: the corresponding expiry refusal is returned.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    spec = _spec("linear")
    spec.instrument_kind = "future"
    spec.expiry_at = _NOW if expiry_present else None
    await _market(repo, spec)
    expected = "contract_expired" if expiry_present else "contract_expiry_unavailable"
    with pytest.raises(PriceUnavailableError, match=expected):
        await USDConverter(repo, now=lambda: _NOW).to_usd(_INSTRUMENT, Decimal(1))


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["expiry", "certification"])
async def test_cached_price_cannot_extend_contract_evidence(boundary: str) -> None:
    """Revalidate time-dependent evidence even while the price cache stays valid.

    Given: a fresh price and contract evidence expiring within the sixty-second cache TTL.
    When: a later call crosses the evidence deadline without refreshing the price.
    Then: the cached result is refused rather than extending execution eligibility.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    spec = _spec("linear")
    if boundary == "expiry":
        spec.expiry_at = _NOW + timedelta(seconds=10)
    else:
        spec.spec_observed_at = _NOW - timedelta(hours=12) + timedelta(seconds=10)
    await _market(repo, spec)
    clock = {"now": _NOW}
    converter = USDConverter(repo, now=lambda: clock["now"])
    assert await converter.to_usd(_INSTRUMENT, Decimal(1)) == Decimal(100_000)
    clock["now"] = _NOW + timedelta(seconds=11)
    expected = "contract_expired" if boundary == "expiry" else "contract_valuation_unavailable"
    with pytest.raises(PriceUnavailableError, match=expected):
        await converter.to_usd(_INSTRUMENT, Decimal(1))


@pytest.mark.asyncio
async def test_current_valuation_metadata_is_clock_free_and_absent_identity_is_unknown() -> None:
    """Read active metadata even when a writer timestamp is ahead of the reader.

    Given: an active instrument definition whose event timestamp is ahead of now.
    When: its current valuation identity is read without a temporal cutoff.
    Then: identity still resolves, while a missing instrument returns null.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await _market(repo, _spec("linear"))
    async with repo.session() as session:
        instrument = await session.get(Instrument, 1)
        assert instrument is not None
        instrument.timestamp = _NOW + timedelta(seconds=30)
        await session.commit()
    row = await repo.get_active_instrument_valuation_metadata(_INSTRUMENT)
    assert row is not None
    assert row["exchange"] == "kraken_futures"
    assert row["base"] == "BTC"
    assert row["quote"] == "USD"
    assert row["spec"] is not None
    assert await repo.get_active_instrument_valuation_metadata("absent") is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value,asset_type",
    [("base", "", "crypto"), ("quote", "", "crypto"), ("quote", None, "index")],
)
async def test_missing_currency_identity_is_not_valuation_metadata(
    field: str, value: str | None, asset_type: str
) -> None:
    """Refuse a catalog identity that cannot label its monetary dimensions.

    Given: a persisted instrument with a missing base or quote currency.
    When: the current valuation identity is read.
    Then: it remains unavailable rather than inventing a currency or returning a malformed row.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await _market(repo, None, exchange="kraken")
    async with repo.session() as session:
        symbol = await session.get(Symbol, 1)
        assert symbol is not None
        symbol.asset_type = asset_type
        setattr(symbol, field, value)
        await session.commit()
    assert await repo.get_active_instrument_valuation_metadata(_INSTRUMENT) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["certification", "payoff", "version", "identity", "removed"])
async def test_cached_price_uses_current_contract_definition(change: str) -> None:
    """A price cache cannot preserve revoked monetary evidence or venue identity.

    Given: a successfully valued contract whose current catalog definition changes.
    When: another order is valued inside the price-cache lifetime.
    Then: the current definition is reread and the new unsafe state is refused.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await _market(repo, _spec("inverse"))
    converter = USDConverter(repo, now=lambda: _NOW)
    assert await converter.to_usd(_INSTRUMENT, Decimal(1000)) == 1000
    async with repo.session() as session:
        spec = await session.get(InstrumentSpec, 1)
        instrument = await session.get(Instrument, 1)
        assert spec is not None and instrument is not None
        if change == "certification":
            spec.unit_certified = False
        elif change == "payoff":
            spec.contract_payoff_model = None
            spec.contract_value_currency = None
            spec.settlement_currency = None
        elif change == "version":
            spec.spec_version = "s2a-v1:obsolete"
        elif change == "identity":
            instrument.exchange = "another_venue"
        else:
            instrument.known_to = _NOW
        await session.commit()
    expected = {
        "identity": "instrument_definition_changed",
        "removed": "instrument_not_found",
    }.get(change, "contract_valuation_unavailable")
    with pytest.raises(PriceUnavailableError, match=expected):
        await converter.to_usd(_INSTRUMENT, Decimal(1000))


@pytest.mark.asyncio
@pytest.mark.parametrize("quantity", [Decimal(-1), Decimal("NaN"), Decimal("Infinity"), 1, True])
async def test_invalid_order_quantity_is_never_valued(quantity: object) -> None:
    """Reject malformed admission sizes before calculating monetary exposure.

    Given: a valid contract and a negative, nonfinite or non-Decimal size.
    When: the USD cap oracle evaluates that request.
    Then: invalid_quantity identifies the unsafe input.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await _market(repo, _spec("linear"))
    with pytest.raises(PriceUnavailableError, match="invalid_quantity"):
        await USDConverter(repo, now=lambda: _NOW).to_usd(_INSTRUMENT, cast(Decimal, quantity))


@pytest.mark.asyncio
@pytest.mark.parametrize("price", [0, -1])
async def test_nonpositive_snapshot_price_is_unavailable(price: int) -> None:
    """Keep the supported positive-price contract profile explicit.

    Given: a contract snapshot with a zero or negative price.
    When: USD exposure is requested.
    Then: the oracle refuses the unsupported price instead of producing negative exposure.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await _market(repo, _spec("linear"))
    async with repo.session() as session:
        snapshot = await session.get(MarketSnapshot, 1)
        assert snapshot is not None
        snapshot.last_price = price
        await session.commit()
    with pytest.raises(PriceUnavailableError, match="last_price_invalid"):
        await USDConverter(repo, now=lambda: _NOW).to_usd(_INSTRUMENT, Decimal(1))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model,multiplier,native_unit,expected",
    [
        ("linear", "1", "base_asset", "10"),
        ("linear", "2", "base_asset", None),
        ("inverse", "1", "base_asset", None),
        ("linear", "1", None, None),
    ],
)
async def test_native_base_quantity_requires_verified_unit_linear_contract(
    model: str, multiplier: str, native_unit: str | None, expected: str | None
) -> None:
    """Keep native-base and legacy contract quantities dimensionally explicit.

    Given: a contract whose native order unit may be base assets or unknown.
    When: its order quantity is converted to USD exposure.
    Then: only the evidenced unit-multiplier linear profile uses base quantity.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    spec = _spec(model)
    spec.contract_size = Decimal(multiplier)
    spec.native_order_quantity_unit = native_unit
    await _market(repo, spec)
    converter = USDConverter(repo, now=lambda: _NOW)
    if expected is None:
        with pytest.raises(PriceUnavailableError, match="contract_valuation_unavailable"):
            await converter.to_usd(_INSTRUMENT, Decimal("0.0001"))
    else:
        assert await converter.to_usd(_INSTRUMENT, Decimal("0.0001")) == Decimal(expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("signal", ["quantity", "kind", "payoff"])
async def test_unknown_venue_derivative_signals_cannot_borrow_kraken_provenance(
    signal: str,
) -> None:
    """Refuse derivative evidence attached to an unverified execution venue.

    Given: an unknown venue with only one independent derivative signal.
    When: its USD exposure is requested with a valid positive snapshot price.
    Then: Kraken provenance cannot certify it and spot multiplication never runs.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    spec = _spec("inverse")
    spec.unit_certified = False
    spec.quantity_unit = "base_asset"
    spec.instrument_kind = "spot"
    spec.contract_payoff_model = None
    spec.contract_value_currency = None
    spec.settlement_currency = None
    if signal == "quantity":
        spec.quantity_unit = "contract_count"
    elif signal == "kind":
        spec.instrument_kind = "perpetual"
    else:
        spec.contract_payoff_model = "inverse"
        spec.contract_value_currency = "USD"
        spec.settlement_currency = "BTC"
    await _market(repo, spec, exchange="unverified_derivatives")
    with pytest.raises(PriceUnavailableError, match="contract_valuation_unavailable"):
        await USDConverter(repo, now=lambda: _NOW).to_usd(_INSTRUMENT, Decimal(1000))


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", [None, "spot", "option"])
async def test_unsupported_contract_kind_cannot_use_otherwise_complete_evidence(
    kind: str | None,
) -> None:
    """Require a supported contract family independently of quantity certification.

    Given: fresh certified quantity and inverse payoff evidence for an unsupported kind.
    When: the cap oracle evaluates an order.
    Then: missing or unsupported contract kinds are refused before valuation.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    spec = _spec("inverse")
    spec.instrument_kind = kind
    await _market(repo, spec)
    with pytest.raises(PriceUnavailableError, match="contract_valuation_unavailable"):
        await USDConverter(repo, now=lambda: _NOW).to_usd(_INSTRUMENT, Decimal(1000))


@pytest.mark.asyncio
async def test_missing_multiplier_cannot_be_hidden_by_certified_repository_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject inconsistent boundary data even when its stored certificate says true.

    Given: a repository payload missing its multiplier while retaining certification.
    When: USD exposure is requested through the public converter.
    Then: missing monetary scale is refused without producing spot-like exposure.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await _market(repo, _spec("inverse"))
    metadata = await repo.get_active_instrument_valuation_metadata(_INSTRUMENT)
    assert metadata is not None and metadata["spec"] is not None
    metadata["spec"]["contract_size"] = None
    monkeypatch.setattr(
        repo,
        "get_active_instrument_valuation_metadata",
        AsyncMock(return_value=metadata),
    )
    with pytest.raises(PriceUnavailableError, match="contract_valuation_unavailable"):
        await USDConverter(repo, now=lambda: _NOW).to_usd(_INSTRUMENT, Decimal(1000))


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange", ["unverified_derivatives", "kraken"])
async def test_cached_derivative_evidence_cannot_disappear_into_spot_fallback(
    exchange: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain a failed derivative attempt's interpretation across cached-price retries.

    Given: derivative evidence whose price was cached before its venue was refused.
    When: all current contract signals disappear inside the price cache lifetime.
    Then: retry still refuses instead of treating the same identity as spot.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await _market(repo, _spec("inverse"), exchange=exchange)
    snapshots = AsyncMock(wraps=repo.get_active_market_snapshot_price)
    monkeypatch.setattr(repo, "get_active_market_snapshot_price", snapshots)
    converter = USDConverter(repo, now=lambda: _NOW)
    with pytest.raises(PriceUnavailableError, match="contract_valuation_unavailable"):
        await converter.to_usd(_INSTRUMENT, Decimal(1000))
    async with repo.session() as session:
        spec = await session.get(InstrumentSpec, 1)
        assert spec is not None
        spec.unit_certified = False
        spec.quantity_unit = None
        spec.native_order_quantity_unit = None
        spec.instrument_kind = None
        spec.contract_size = None
        spec.spec_source = None
        spec.spec_version = None
        spec.spec_observed_at = None
        spec.contract_payoff_model = None
        spec.contract_value_currency = None
        spec.settlement_currency = None
        await session.commit()
    with pytest.raises(PriceUnavailableError, match="contract_valuation_unavailable"):
        await converter.to_usd(_INSTRUMENT, Decimal(1000))
    assert snapshots.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("spec_present", [False, True])
async def test_unknown_venue_without_derivative_signals_still_requires_provenance(
    spec_present: bool,
) -> None:
    """Refuse unknown venues even when their specification offers no derivative hints.

    Given: an unknown venue with either no spec or every derivative signal absent.
    When: the oracle receives a positive USD snapshot and a valid order size.
    Then: absent evidence cannot authorize legacy spot arithmetic for that venue.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    spec = _spec("inverse")
    spec.unit_certified = False
    spec.quantity_unit = None
    spec.native_order_quantity_unit = None
    spec.instrument_kind = None
    spec.contract_size = None
    spec.spec_source = None
    spec.spec_version = None
    spec.spec_observed_at = None
    spec.contract_payoff_model = None
    spec.contract_value_currency = None
    spec.settlement_currency = None
    await _market(repo, spec if spec_present else None, exchange="unverified_derivatives")
    with pytest.raises(PriceUnavailableError, match="contract_valuation_unavailable"):
        await USDConverter(repo, now=lambda: _NOW).to_usd(_INSTRUMENT, Decimal(1000))


@pytest.mark.asyncio
async def test_unknown_kind_on_legacy_spot_venue_cannot_use_spot_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse unknown contract kinds independently of known venue and base quantity.

    Given: Kraken metadata with base-asset quantity, no payoff and an unknown swap kind.
    When: that malformed boundary payload reaches the public oracle.
    Then: the unknown kind requires unavailable contract semantics rather than spot math.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    spec = _spec("inverse")
    spec.unit_certified = False
    spec.quantity_unit = "base_asset"
    spec.instrument_kind = "spot"
    spec.contract_payoff_model = None
    spec.contract_value_currency = None
    spec.settlement_currency = None
    await _market(repo, spec, exchange="kraken")
    metadata = await repo.get_active_instrument_valuation_metadata(_INSTRUMENT)
    assert metadata is not None and metadata["spec"] is not None
    metadata["spec"]["instrument_kind"] = "swap"
    monkeypatch.setattr(
        repo,
        "get_active_instrument_valuation_metadata",
        AsyncMock(return_value=metadata),
    )
    with pytest.raises(PriceUnavailableError, match="contract_valuation_unavailable"):
        await USDConverter(repo, now=lambda: _NOW).to_usd(_INSTRUMENT, Decimal(1000))


@pytest.mark.asyncio
@pytest.mark.parametrize("derivative", [False, True])
@pytest.mark.parametrize("trap_overflow", [False, True])
async def test_trapped_or_nonfinite_notional_is_refused_on_cached_public_path(
    derivative: bool, trap_overflow: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Contain both Decimal exceptions and untrapped non-finite exposure results.

    Given: a valid cached spot or linear price and a restricted Decimal exponent range.
    When: order valuation overflows with trapping enabled or disabled.
    Then: the public oracle returns a structured refusal and no exposure number.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await _market(
        repo,
        _spec("linear") if derivative else None,
        exchange="kraken_futures" if derivative else "kraken",
    )
    converter = USDConverter(repo, now=lambda: _NOW)
    assert await converter.to_usd(_INSTRUMENT, Decimal(1)) == 100_000
    metadata = await repo.get_active_instrument_valuation_metadata(_INSTRUMENT)
    assert metadata is not None
    monkeypatch.setattr(
        repo,
        "get_active_instrument_valuation_metadata",
        AsyncMock(return_value=metadata),
    )
    with localcontext() as context:
        context.prec = 8
        context.Emax = 4
        context.traps[Overflow] = trap_overflow
        with pytest.raises(PriceUnavailableError, match="contract_valuation_unavailable"):
            await converter.to_usd(_INSTRUMENT, Decimal(1))


@pytest.mark.asyncio
@pytest.mark.parametrize("price", [float("nan"), float("inf"), float("-inf"), "", "not-a-price"])
async def test_invalid_snapshot_boundary_values_are_refused(
    price: float | str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse malformed market data before it enters a price cache or model.

    Given: a repository boundary returning a non-finite or unparseable last price.
    When: a valid derivative order requests its USD exposure.
    Then: last_price_invalid is returned independently of model completeness.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await _market(repo, _spec("linear"))
    monkeypatch.setattr(
        repo,
        "get_active_market_snapshot_price",
        AsyncMock(return_value=(price, _NOW)),
    )
    with pytest.raises(PriceUnavailableError, match="last_price_invalid"):
        await USDConverter(repo, now=lambda: _NOW).to_usd(_INSTRUMENT, Decimal(1))
