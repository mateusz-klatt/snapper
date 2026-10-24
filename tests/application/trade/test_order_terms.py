"""Exact tokenized manual-order sizing and cash-path admission regressions."""

from collections.abc import Generator
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import localcontext
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.trade.order_terms import ManualOrderTerms
from snapper.application.trade.order_terms import manual_order_terms_refusal
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentOrderCapabilityRow
from snapper.data.repository_types import InstrumentSpecRow
from snapper.infrastructure.exchanges.tokenized_spec_provenance import TOKENIZED_MAX_DECIMALS
from snapper.infrastructure.exchanges.tokenized_spec_provenance import TOKENIZED_SPEC_ETL_VERSION
from snapper.infrastructure.exchanges.tokenized_spec_provenance import TOKENIZED_SPEC_SOURCE

_NOW = datetime(2026, 10, 23, 1, tzinfo=UTC)


@pytest.fixture
def terms() -> ManualOrderTerms:
    """Provide a valid exact tokenized limit order in manual-entry vocabulary."""
    return ManualOrderTerms(
        exchange="kraken",
        instrument="AAPL-USD",
        instrument_public_id="instrument-token",
        order_type="limit",
        quantity=Decimal("0.01"),
        price=Decimal(100),
        stop_price=None,
        leverage=None,
        reduce_only=False,
    )


@pytest.fixture
def spec() -> InstrumentSpecRow:
    """Provide complete direct REST token precision with an aware observation."""
    return {
        "instrument_public_id": "instrument-token",
        "tick_size": 0.01,
        "lot_size": 0.00000001,
        "min_order_size": 0.00000001,
        "max_order_size": None,
        "cost_decimals": 5,
        "qty_decimals": 8,
        "margin_initial": None,
        "position_limit_long": None,
        "position_limit_short": None,
        "status": "online",
        "contract_size": None,
        "quantity_unit": "base_asset",
        "spec_source": TOKENIZED_SPEC_SOURCE,
        "spec_version": f"{TOKENIZED_SPEC_ETL_VERSION}:fixture",
        "spec_observed_at": _NOW - timedelta(minutes=1),
        "unit_certified": False,
        "expiry_at": None,
        "instrument_kind": "spot",
        "funding_type": None,
        "funding_frequency_hours": None,
        "rollover_rate_long": None,
        "rollover_rate_short": None,
        "max_funding_rate": None,
    }


@pytest.fixture
def capability() -> InstrumentOrderCapabilityRow:
    """Provide the exact cash order capability and quote-cost minimum."""
    return {
        "public_id": "capability-token",
        "timestamp": _NOW - timedelta(minutes=1),
        "session_id": "session",
        "sequence_id": 1,
        "instrument_public_id": "instrument-token",
        "exchange": "kraken",
        "supported_order_types": ["market", "limit", "stop", "stop_limit"],
        "supports_post_only": True,
        "supports_reduce_only": False,
        "supports_amend_in_place": False,
        "supports_native_stop_loss": True,
        "supports_native_take_profit": False,
        "supports_trailing_stop_client_side": True,
        "supports_market_making": False,
        "supports_short_selling": False,
        "supports_leverage": False,
        "max_leverage_long": 1.0,
        "max_leverage_short": 0.0,
        "min_notional": 0.5,
        "max_order_size": None,
        "top_of_book_quality": "unknown",
    }


@pytest.fixture
def repo(spec: InstrumentSpecRow, capability: InstrumentOrderCapabilityRow) -> MagicMock:
    """Provide asynchronous catalog reads without a private venue client."""
    repository = MagicMock(spec=Repository)
    repository.get_instrument_public_id_by_symbol = AsyncMock(return_value="instrument-token")
    repository.get_instrument_spec = AsyncMock(return_value=spec)
    repository.get_instrument_capabilities = AsyncMock(return_value=[capability])
    return repository


@pytest.fixture(autouse=True)
def class_mapping() -> Generator[MagicMock]:
    """Resolve token identity through the catalog class function exclusively."""
    with patch(
        "snapper.application.trade.order_terms.native_to_kraken_asset_class",
        return_value="tokenized_asset",
    ) as mapping:
        yield mapping


@pytest.mark.parametrize("exchange", ("paper", "kraken_futures", "walutomat"))
async def test_other_venues_preserve_existing_behavior_without_catalog_reads(
    repo: MagicMock, terms: ManualOrderTerms, class_mapping: MagicMock, exchange: str
) -> None:
    """Leave ordinary non-Spot venues and paper replay to their existing guards.

    Given: A manual order for a venue outside tokenized Spot cash execution,
    When: Tokenized order terms are evaluated,
    Then: No token classification or new catalog restriction is applied.
    """
    result = await manual_order_terms_refusal(repo, replace(terms, exchange=exchange), _NOW)

    assert result is None
    class_mapping.assert_not_called()
    repo.get_instrument_spec.assert_not_awaited()


async def test_ordinary_spot_preserves_existing_behavior(
    repo: MagicMock, terms: ManualOrderTerms, class_mapping: MagicMock
) -> None:
    """Use catalog identity rather than token-like symbol spelling.

    Given: A Spot symbol whose active catalog class is currency,
    When: The symbol contains a token-like suffix,
    Then: Token-specific restrictions do not inspect its order metadata.
    """
    class_mapping.return_value = "currency"

    result = await manual_order_terms_refusal(
        repo, replace(terms, instrument="FAKEx-USD", leverage=3), _NOW
    )

    assert result is None
    repo.get_instrument_spec.assert_not_awaited()


async def test_unknown_class_does_not_fabricate_token_identity_or_trade_permission(
    repo: MagicMock, terms: ManualOrderTerms, class_mapping: MagicMock
) -> None:
    """Defer unresolved identities to the mandatory caller capability guard.

    Given: No active catalog asset-class mapping for the requested symbol,
    When: Token-specific terms are inspected,
    Then: The helper applies no guessed token classification or catalog access.
    """
    class_mapping.side_effect = ValueError("Unknown Kraken order asset class")

    assert await manual_order_terms_refusal(repo, terms, _NOW) is None
    repo.get_instrument_public_id_by_symbol.assert_not_awaited()


@pytest.mark.parametrize(("leverage", "reduce_only"), ((2, False), (1, False), (None, True)))
async def test_tokenized_margin_flags_are_refused_as_a_client_cash_path_limit(
    repo: MagicMock, terms: ManualOrderTerms, leverage: int | None, reduce_only: bool
) -> None:
    """Refuse margin modifiers without falsely claiming the venue cannot trade margin.

    Given: A catalog-proven token order requesting leverage or reduce-only,
    When: Cash-path terms are evaluated,
    Then: A truthful client cash-path refusal occurs before any catalog reads.
    """
    result = await manual_order_terms_refusal(
        repo, replace(terms, leverage=leverage, reduce_only=reduce_only), _NOW
    )

    assert result is not None
    assert result.error_code == "tokenized_cash_order_only"
    assert "Snapper" in result.message
    assert result.details["instrument_public_id"] == terms.instrument_public_id
    repo.get_instrument_spec.assert_not_awaited()


async def test_valid_limit_terms_use_exact_identity_and_temporal_catalog_reads(
    repo: MagicMock, terms: ManualOrderTerms
) -> None:
    """Accept valid token precision without requiring derivative unit certification.

    Given: Complete online token metadata with unit_certified false,
    When: A valid tokenized cash limit order is evaluated,
    Then: Exact symbol, UUID and venue reads use one shared evaluation instant.
    """
    assert await manual_order_terms_refusal(repo, terms, _NOW) is None
    repo.get_instrument_public_id_by_symbol.assert_awaited_once_with(
        terms.instrument, terms.exchange, _NOW
    )
    repo.get_instrument_spec.assert_awaited_once_with(terms.instrument_public_id, _NOW)
    repo.get_instrument_capabilities.assert_awaited_once_with(
        _NOW, exchange=terms.exchange, instrument_public_id=terms.instrument_public_id
    )


@pytest.mark.parametrize("resolved_id", (None, "other-instrument"))
async def test_missing_or_mismatched_instrument_identity_fails_closed(
    repo: MagicMock, terms: ManualOrderTerms, resolved_id: str | None
) -> None:
    """Prevent catalog precision from being borrowed from another instrument UUID.

    Given: Symbol resolution missing or identifying a different venue instrument,
    When: A token order presents the expected symbol with an unproven UUID,
    Then: Admission refuses before reading specifications or capabilities.
    """
    repo.get_instrument_public_id_by_symbol.return_value = resolved_id

    result = await manual_order_terms_refusal(repo, terms, _NOW)

    assert result is not None
    assert result.error_code == "tokenized_order_metadata_unavailable"
    repo.get_instrument_spec.assert_not_awaited()


async def test_missing_token_specification_fails_closed(
    repo: MagicMock, terms: ManualOrderTerms
) -> None:
    """Require a complete exact specification before checking token amounts.

    Given: A catalog token UUID without an active specification,
    When: Cash order terms are evaluated,
    Then: Admission refuses rather than guessing quantity or tick defaults.
    """
    repo.get_instrument_spec.return_value = None

    result = await manual_order_terms_refusal(repo, terms, _NOW)

    assert result is not None
    assert result.error_code == "tokenized_order_metadata_unavailable"


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("instrument_public_id", "other-instrument"),
        ("spec_source", None),
        ("spec_source", "kraken:ccxt.load_markets"),
        ("spec_version", None),
        ("spec_version", "tokenized-rest-v0:old"),
        ("quantity_unit", None),
        ("quantity_unit", "contract_count"),
        ("status", None),
        ("status", "post_only"),
        ("status", "cancel_only"),
        ("cost_decimals", None),
        ("cost_decimals", True),
        ("cost_decimals", "5"),
        ("cost_decimals", 5.0),
        ("cost_decimals", -1),
        ("qty_decimals", TOKENIZED_MAX_DECIMALS + 1),
        ("spec_observed_at", None),
        ("spec_observed_at", _NOW.replace(tzinfo=None)),
        ("spec_observed_at", _NOW + timedelta(seconds=1)),
        ("spec_observed_at", _NOW - timedelta(hours=12)),
        ("tick_size", None),
        ("lot_size", 0.0),
        ("min_order_size", float("nan")),
        ("min_order_size", True),
        ("min_order_size", "not-a-number"),
        ("tick_size", float("inf")),
        ("max_order_size", 0.0),
        ("max_order_size", 0.000000001),
    ),
)
async def test_unusable_token_specification_never_passes_admission(
    repo: MagicMock, terms: ManualOrderTerms, spec: InstrumentSpecRow, field: str, value: object
) -> None:
    """Reject incomplete, stale, conflicting or nonfinite catalog operands.

    Given: One required token identity, precision, status or sizing fact is unusable,
    When: Otherwise valid token cash order terms are evaluated,
    Then: Admission fails closed without manufacturing metadata.
    """
    repo.get_instrument_spec.return_value = cast(InstrumentSpecRow, {**spec, field: value})

    result = await manual_order_terms_refusal(repo, terms, _NOW)

    assert result is not None
    assert result.error_code == "tokenized_order_metadata_unavailable"


async def test_naive_evaluation_time_cannot_certify_token_metadata(
    repo: MagicMock, terms: ManualOrderTerms
) -> None:
    """Reject freshness comparisons without an aware evaluation instant.

    Given: A valid aware catalog observation and a naive caller timestamp,
    When: Tokenized cash order terms are inspected,
    Then: No freshness assertion is fabricated from incomparable clocks.
    """
    result = await manual_order_terms_refusal(repo, terms, _NOW.replace(tzinfo=None))

    assert result is not None
    assert result.error_code == "tokenized_order_metadata_unavailable"


@pytest.mark.parametrize("count", (0, 2))
async def test_missing_or_ambiguous_order_capabilities_fail_closed(
    repo: MagicMock, terms: ManualOrderTerms, capability: InstrumentOrderCapabilityRow, count: int
) -> None:
    """Require exactly one capability row for the exact requested instrument.

    Given: Missing or duplicate active token order capabilities,
    When: Cash order terms are evaluated,
    Then: Minimum cost and supported order types are never guessed.
    """
    repo.get_instrument_capabilities.return_value = [capability] * count

    result = await manual_order_terms_refusal(repo, terms, _NOW)

    assert result is not None
    assert result.error_code == "tokenized_order_metadata_unavailable"


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("instrument_public_id", "other-instrument"),
        ("exchange", "paper"),
        ("min_notional", None),
        ("min_notional", -1.0),
        ("max_order_size", float("inf")),
    ),
)
async def test_conflicting_or_incomplete_capability_evidence_fails_closed(
    repo: MagicMock,
    terms: ManualOrderTerms,
    capability: InstrumentOrderCapabilityRow,
    field: str,
    value: object,
) -> None:
    """Bind quote minima and maxima to the exact capability identity.

    Given: A capability belonging to another instrument or containing invalid limits,
    When: Token cash terms are evaluated,
    Then: Evidence from the wrong identity or invalid limit is refused.
    """
    repo.get_instrument_capabilities.return_value = [
        cast(InstrumentOrderCapabilityRow, {**capability, field: value})
    ]

    result = await manual_order_terms_refusal(repo, terms, _NOW)

    assert result is not None
    assert result.error_code == "tokenized_order_metadata_unavailable"


@pytest.mark.parametrize("order_type", ("take_profit", "unknown"))
async def test_client_cash_path_refuses_unimplemented_order_types(
    repo: MagicMock,
    terms: ManualOrderTerms,
    capability: InstrumentOrderCapabilityRow,
    order_type: str,
) -> None:
    """Require both the implemented cash path and the exact capability to support a type.

    Given: A future or unknown order type advertised by generic capability metadata,
    When: The manual cash path has no implemented price semantics for it,
    Then: Admission refuses rather than silently omitting its constraints.
    """
    capability["supported_order_types"].append(order_type)

    result = await manual_order_terms_refusal(repo, replace(terms, order_type=order_type), _NOW)

    assert result is not None
    assert result.error_code == "order_type_unsupported"


async def test_order_type_missing_from_exact_capability_is_refused(
    repo: MagicMock, terms: ManualOrderTerms, capability: InstrumentOrderCapabilityRow
) -> None:
    """Honor a narrower active capability even when the client implements the order type.

    Given: A token capability allowing only market orders,
    When: A limit order is requested for that exact instrument,
    Then: Admission refuses the unsupported capability combination.
    """
    capability["supported_order_types"] = ["market"]

    result = await manual_order_terms_refusal(repo, terms, _NOW)

    assert result is not None
    assert result.error_code == "order_type_unsupported"


@pytest.mark.parametrize(
    ("quantity", "expected_code"),
    (
        (True, "order_quantity_invalid"),
        (0.01, "order_quantity_invalid"),
        (Decimal(0), "order_quantity_invalid"),
        (Decimal("-1"), "order_quantity_invalid"),
        (Decimal("sNaN"), "order_quantity_invalid"),
        (Decimal("Infinity"), "order_quantity_invalid"),
        (Decimal("0.000000001"), "order_quantity_below_minimum"),
        (Decimal("0.000000015"), "order_quantity_step_mismatch"),
    ),
)
async def test_invalid_token_quantities_are_refused_without_rounding(
    repo: MagicMock, terms: ManualOrderTerms, quantity: object, expected_code: str
) -> None:
    """Refuse invalid operands and exact lot mismatches instead of flooring them.

    Given: An invalid, undersized or off-increment token quantity,
    When: Cash terms are checked against the authoritative lot quantum,
    Then: The original quantity remains unchanged and a precise refusal is returned.
    """
    request = replace(terms, quantity=cast(Decimal, quantity))

    result = await manual_order_terms_refusal(repo, request, _NOW)

    assert result is not None
    assert result.error_code == expected_code
    assert request.quantity is quantity


@pytest.mark.parametrize("maxima", ((1.0, None), (None, 1.0), (2.0, 1.0)))
async def test_stricter_observed_quantity_maximum_is_enforced(
    repo: MagicMock,
    terms: ManualOrderTerms,
    spec: InstrumentSpecRow,
    capability: InstrumentOrderCapabilityRow,
    maxima: tuple[float | None, float | None],
) -> None:
    """Respect quantity maxima independently of position or contract multipliers.

    Given: An optional instrument or capability maximum below the requested size,
    When: A token cash order is evaluated,
    Then: The stricter observed maximum is returned in the refusal.
    """
    spec["max_order_size"], capability["max_order_size"] = maxima

    result = await manual_order_terms_refusal(repo, replace(terms, quantity=Decimal("1.5")), _NOW)

    assert result is not None
    assert result.error_code == "order_quantity_above_maximum"
    assert result.details["maximum_quantity"] == "1.0"


@pytest.mark.parametrize(
    ("order_type", "prices", "expected_code"),
    (
        ("limit", (None, None), "order_price_invalid"),
        ("limit", (True, None), "order_price_invalid"),
        ("limit", (Decimal("NaN"), None), "order_price_invalid"),
        ("limit", (Decimal(0), None), "order_price_invalid"),
        ("limit", (Decimal("100.001"), None), "order_price_step_mismatch"),
        ("stop", (None, None), "order_price_invalid"),
        ("stop", (None, Decimal("99.001")), "order_price_step_mismatch"),
        ("stop_limit", (Decimal(100), Decimal("99.001")), "order_price_step_mismatch"),
    ),
)
async def test_only_applicable_order_prices_must_use_exact_catalog_ticks(
    repo: MagicMock,
    terms: ManualOrderTerms,
    order_type: str,
    prices: tuple[object, object],
    expected_code: str,
) -> None:
    """Validate limit legs and stop triggers without silently adjusting prices.

    Given: A missing, invalid or off-tick price required by an order type,
    When: Token cash prices are checked,
    Then: Admission reports the price problem before minimum-cost evaluation.
    """
    price, stop_price = prices
    request = replace(
        terms,
        order_type=order_type,
        price=cast(Decimal | None, price),
        stop_price=cast(Decimal | None, stop_price),
    )

    result = await manual_order_terms_refusal(repo, request, _NOW)

    assert result is not None
    assert result.error_code == expected_code


@pytest.mark.parametrize("order_type", ("limit", "stop_limit"))
async def test_limit_notional_uses_the_limit_leg_and_exact_quote_cost_minimum(
    repo: MagicMock, terms: ManualOrderTerms, order_type: str
) -> None:
    """Refuse an undersized limit notional even when the trigger suggests a larger cost.

    Given: A valid lot quantity whose limit leg is below the quote-cost minimum,
    When: The stop trigger is much higher than the limit price,
    Then: The exact limit notional determines refusal and no USD assumption is made.
    """
    request = replace(
        terms,
        order_type=order_type,
        quantity=Decimal("0.001"),
        stop_price=Decimal(10000),
    )

    result = await manual_order_terms_refusal(repo, request, _NOW)

    assert result is not None
    assert result.error_code == "order_notional_below_minimum"
    assert result.details["minimum_notional"] == "0.5"
    assert result.details["notional_unit"] == "quote_currency"


@pytest.mark.parametrize("order_type", ("market", "stop"))
async def test_market_execution_cost_is_left_to_the_venue_without_a_limit_leg(
    repo: MagicMock, terms: ManualOrderTerms, order_type: str
) -> None:
    """Avoid certifying an unknown market execution notional from a trigger price.

    Given: A valid small token quantity with no limit execution price,
    When: A market or stop-market order is checked,
    Then: Lot and trigger checks pass without inventing minimum-cost compliance.
    """
    request = replace(
        terms,
        order_type=order_type,
        quantity=Decimal("0.00000001"),
        price=None,
        stop_price=Decimal(100),
    )

    assert await manual_order_terms_refusal(repo, request, _NOW) is None


async def test_exact_limit_minimum_and_high_precision_lots_survive_small_decimal_context(
    repo: MagicMock, terms: ManualOrderTerms, spec: InstrumentSpecRow
) -> None:
    """Check exact increments and boundary cost without ambient context rounding.

    Given: A tiny authoritative lot and caller precision of one significant digit,
    When: Limit quantities are exactly at or just below the minimum quote cost,
    Then: The boundary passes and the smaller exact notional is refused.
    """
    spec["lot_size"] = 1e-256
    spec["qty_decimals"] = TOKENIZED_MAX_DECIMALS
    with localcontext() as context:
        context.prec = 1
        assert (
            await manual_order_terms_refusal(repo, replace(terms, quantity=Decimal("0.005")), _NOW)
            is None
        )
        result = await manual_order_terms_refusal(
            repo, replace(terms, quantity=Decimal("0.00499999")), _NOW
        )

    assert result is not None
    assert result.error_code == "order_notional_below_minimum"
