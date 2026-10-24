"""Only invariant raw limit-order operands qualify as a harmless xStocks view restatement."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import localcontext

import pytest
from pydantic import ValidationError

from snapper.core.xstock_rebase import XStockMultiplierWitness
from snapper.core.xstock_rebase import XStockRebaseError
from snapper.core.xstock_rebase import XStockUnitScope
from snapper.core.xstock_rebase import XStockUnitValue
from snapper.core.xstock_rebase_orders import XStockOrderRestatement
from snapper.core.xstock_rebase_orders import normalize_xstock_order_restatement

_T0 = datetime(2026, 9, 1, tzinfo=UTC)
_T1 = _T0 + timedelta(days=1)
_T2 = _T1 + timedelta(days=1)


def _scope(**updates: object) -> XStockUnitScope:
    """Build exact asset/network identifiers independently of the display symbol."""
    payload: dict[str, object] = {
        "exchange": "kraken",
        "instrument_public_id": "00000000-0000-4000-8000-000000000001",
        "spv_asset_id": "AAPLSPV",
        "underlying_asset_id": "US0378331005",
        "quote_asset_id": "USD",
        "network": "solana",
        "token_contract": "test-issuer-mint-AAPL",
    }
    return XStockUnitScope.model_validate(payload | updates)


def _witness(**updates: object) -> XStockMultiplierWitness:
    """Construct the original bounded multiplier observation and exact source clocks."""
    payload: dict[str, object] = {
        "public_id": "00000000-0000-4000-8000-000000000002",
        "scope": _scope(),
        "multiplier": "1",
        "source": "test-provider:history",
        "source_record_id": "before",
        "source_receipt_digest": "a" * 64,
        "effective_from": _T0,
        "effective_until": _T1,
        "observed_at": _T0,
        "known_at": _T0,
    }
    return XStockMultiplierWitness.model_validate(payload | updates)


def _value(**updates: object) -> XStockUnitValue:
    """Build an explicitly equity-unit quantity and the corresponding equity price."""
    payload: dict[str, object] = {
        "scope": _scope(),
        "quantity_convention": "underlying_equity",
        "price_convention": "quote_per_underlying_equity",
        "quantity": "1",
        "price": "200",
    }
    return XStockUnitValue.model_validate(payload | updates)


def _restatement(**updates: object) -> XStockOrderRestatement:
    """Provide separately witnessed before/after observations of one partial limit order."""
    payload: dict[str, object] = {
        "order_public_id": "00000000-0000-4000-8000-000000000011",
        "corporate_action_public_id": "00000000-0000-4000-8000-000000000021",
        "source_event_id": "provider:restated:001",
        "source_receipt_digest": "c" * 64,
        "source_event_kind": "restated",
        "order_type": "limit",
        "before": _value(),
        "after": _value(quantity="2", price="100"),
        "before_multiplier": _witness(),
        "after_multiplier": _witness(
            public_id="00000000-0000-4000-8000-000000000003",
            multiplier="2",
            source_record_id="after",
            effective_from=_T1,
            effective_until=_T2,
            observed_at=_T1,
            known_at=_T1,
        ),
        "before_economic_at": _T0 + timedelta(hours=1),
        "after_economic_at": _T1 + timedelta(hours=1),
        "observed_at": _T1 + timedelta(hours=2),
        "known_at": _T1 + timedelta(hours=2),
        "raw_cumulative_filled_before": "0.25",
        "raw_cumulative_filled_after": "0.25",
    }
    return XStockOrderRestatement.model_validate(payload | updates)


def test_two_for_one_view_restatement_preserves_the_original_raw_limit_order() -> None:
    """A split display update does not create a new order, fill or cash reservation.

    Given: A one-SPV limit order partially filled before its equity view doubles.
    When: Both observations are normalized using their own effective witnesses.
    Then: The raw total, progress, remaining quantity and limit price are unchanged.
    """
    evidence = _restatement()
    result = normalize_xstock_order_restatement(evidence)
    assert result.order_public_id == evidence.order_public_id
    assert result.raw_order_quantity == Decimal("1")
    assert result.raw_filled_quantity == Decimal("0.25")
    assert result.raw_remaining_quantity == Decimal("0.75")
    assert result.quote_limit_price_per_spv == Decimal("200")
    assert evidence.before.quantity == Decimal("1")
    assert evidence.after.quantity == Decimal("2")


def test_an_already_raw_observation_is_not_rebased_twice() -> None:
    """Explicit SPV observations retain their own quantity and price units.

    Given: The later endpoint already reports raw SPV quantity and price.
    When: Its view is checked against the original equity-unit observation.
    Then: The existing raw operands are retained without applying a second multiplier.
    """
    after = _value(quantity_convention="spv_token", price_convention="quote_per_spv_token")
    result = normalize_xstock_order_restatement(_restatement(after=after))
    assert result.raw_order_quantity == Decimal("1")
    assert result.quote_limit_price_per_spv == Decimal("200")


@pytest.mark.parametrize(
    ("updates", "reason"),
    [
        ({"after": _value(quantity="2.2", price="100")}, "raw_quantity_changed"),
        ({"after": _value(quantity="2", price="101")}, "raw_price_changed"),
        ({"raw_cumulative_filled_after": "0.5"}, "execution_progress_changed"),
        (
            {"raw_cumulative_filled_before": "-0.1", "raw_cumulative_filled_after": "-0.1"},
            "execution_progress_invalid",
        ),
        (
            {"raw_cumulative_filled_before": "1.1", "raw_cumulative_filled_after": "1.1"},
            "execution_progress_invalid",
        ),
    ],
)
def test_economic_changes_cannot_be_misclassified_as_a_view_rebase(
    updates: dict[str, object], reason: str
) -> None:
    """An amendment or intervening execution requires its own durable interpretation.

    Given: A restatement whose raw order, price or execution progress changed.
    When: The narrow harmless-view consistency check is applied.
    Then: It refuses that classification without modifying the receipt.
    """
    evidence = _restatement(**updates)
    original = evidence.model_dump(mode="json")
    with pytest.raises(XStockRebaseError, match=reason):
        normalize_xstock_order_restatement(evidence)
    assert evidence.model_dump(mode="json") == original


def test_matching_local_operands_do_not_hide_a_different_quote_or_market_scope() -> None:
    """Two individually valid endpoint views still need one common instrument scope.

    Given: Equal numeric views but a separately scoped later quote market.
    When: Both are normalized using their matching local witnesses.
    Then: The combined order restatement refuses the cross-scope substitution.
    """
    source = _restatement()
    other = _scope(quote_asset_id="EUR")
    evidence = _restatement(
        after=_value(scope=other, quantity="2", price="100"),
        after_multiplier=source.after_multiplier.model_copy(update={"scope": other}),
    )
    with pytest.raises(XStockRebaseError, match="scope_mismatch"):
        normalize_xstock_order_restatement(evidence)


@pytest.mark.parametrize(
    "updates",
    [
        {"source_event_kind": "amended"},
        {"order_type": "market"},
        {"after_economic_at": _T0},
        {"after_economic_at": _T0 + timedelta(hours=1)},
        {"observed_at": _T2},
        {"known_at": _T0, "observed_at": _T0},
    ],
)
def test_unproven_event_kinds_and_invalid_clocks_are_not_accepted(
    updates: dict[str, object],
) -> None:
    """An event label and receipt clocks are part of the narrow source contract.

    Given: A user amendment, unsupported order type or inconsistent observation ordering.
    When: The immutable restatement proposal is constructed.
    Then: Validation refuses before any original command could be reinterpreted.
    """
    with pytest.raises(ValidationError):
        _restatement(**updates)


def test_exact_remaining_quantity_does_not_use_ambient_decimal_subtraction() -> None:
    """A low display precision cannot alter the retained open raw quantity.

    Given: Many-digit raw order and cumulative-fill operands across a split.
    When: Normalization runs with Decimal precision two.
    Then: The exact raw remainder is preserved without rounding.
    """
    source = _restatement(
        before=_value(quantity="123456789.12345678"),
        after=_value(quantity="246913578.24691356", price="100"),
        raw_cumulative_filled_before="0.12345678",
        raw_cumulative_filled_after="0.12345678",
    )
    with localcontext() as context:
        context.prec = 2
        result = normalize_xstock_order_restatement(source)
    assert result.raw_remaining_quantity == Decimal("123456789")


def test_unchecked_event_copy_is_revalidated_before_normalization() -> None:
    """A frozen-model copy cannot turn a user amendment into an engine restatement.

    Given: An unchecked copy with the unsupported source event kind.
    When: The normalization boundary receives that object.
    Then: Revalidation refuses before returning canonical order operands.
    """
    invalid = _restatement().model_copy(update={"source_event_kind": "amended"})
    with pytest.raises(ValidationError):
        normalize_xstock_order_restatement(invalid)
