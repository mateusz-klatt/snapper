"""Golden position-only contract folds and explicit cash-settlement basis resets."""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import Inexact
from decimal import localcontext
from typing import Literal
from typing import cast

import pytest
from pydantic import ValidationError

from snapper.application.portfolio.contract_economics import ContractEconomicsError
from snapper.application.portfolio.contract_economics import ContractEconomicState
from snapper.application.portfolio.contract_economics import ContractSettlementObservation
from snapper.application.portfolio.contract_economics import contract_model
from snapper.application.portfolio.contract_economics import fold_contract_fill
from snapper.application.portfolio.contract_economics import fold_contract_settlement
from snapper.core.contract_position import ContractPosition
from snapper.core.contract_valuation import LinearContractValuation
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload

_WALLET = "aabbccdd-eeff-4011-8233-445566778899"
_INSTRUMENT = "11223344-5566-4788-99aa-bbccddeeff00"
_EXECUTION = "01234567-89ab-4cde-8fab-0123456789ab"
_EVENT = "fedcba98-7654-4321-8fed-cba987654321"
_OBSERVED = datetime(2026, 10, 23, 6, 0, tzinfo=UTC)


def _snapshot(
    quantity: str = "1",
    price: str = "100",
    payoff: Literal["linear", "inverse"] = "linear",
    multiplier: str = "1",
) -> ExecutionUnitSnapshotPayload:
    """Build a source-tagged fill with independently explicit monetary dimensions.

    Args:
        quantity: Exact signed native quantity with no currency conversion.
        price: Exact quote-per-base price supplied by the venue observation.
        payoff: Explicit linear or reciprocal payoff interpretation.
        multiplier: Native model multiplier with its declared currency dimension.

    Returns:
        A structurally valid fill snapshot, without claiming database authenticity.
    """
    return ExecutionUnitSnapshotPayload.model_validate(
        {
            "target": {
                "phase": "fill",
                "execution_public_id": _EXECUTION,
                "execution_digest": "a" * 64,
                "venue_event_public_id": _EVENT,
                "venue_event_digest": "b" * 64,
            },
            "scope": {
                "wallet_public_id": _WALLET,
                "exchange": "kraken_futures",
                "mode": "live",
                "instrument_public_id": _INSTRUMENT,
            },
            "catalog": {
                "spec_public_id": _INSTRUMENT,
                "spec_timestamp": _OBSERVED,
                "source": "native-instruments",
                "version": "observed-v1",
                "observed_at": _OBSERVED,
            },
            "dimensions": {
                "base_currency": "BTC",
                "quote_currency": "USD",
                "native_order_quantity_unit": "contract_count",
                "internal_quantity_unit": "contract_count",
                "signed_quantity_unit": "contract_count",
                "side": "buy" if Decimal(quantity) > 0 else "sell",
                "signed_quantity": quantity,
                "quantity_quantum": "0.001",
                "price": price,
                "price_quantum": "0.01",
                "price_currency": "USD",
                "price_quantity_convention": "quote_per_base",
                "catalog_contract_multiplier": multiplier,
                "fee": "1",
                "fee_currency": "USD" if payoff == "linear" else "BTC",
                "payoff_model": payoff,
                "contract_multiplier": multiplier,
                "contract_value_currency": "BTC" if payoff == "linear" else "USD",
                "settlement_currency": "USD" if payoff == "linear" else "BTC",
            },
            "venue_identity": {
                "native_symbol": "contract-native-id",
                "source": "native-instruments",
                "source_version": "observed-v1",
                "observed_at": _OBSERVED,
            },
            "rebase": {"quantity_convention": "not_applicable"},
            "receipt": {
                "representation": "decoded_json_text",
                "material": '{"execution":"native-id"}',
                "endpoint_or_channel": "futures.fills",
                "observed_at": _OBSERVED,
                "reported_quantity_unit": "contract_count",
                "reported_quantity_convention": "not_applicable",
            },
            "interpreted_at": _OBSERVED + timedelta(seconds=1),
        }
    )


def _changed(
    snapshot: ExecutionUnitSnapshotPayload, section: str, updates: dict[str, object]
) -> ExecutionUnitSnapshotPayload:
    """Replace one source section while revalidating every frozen model.

    Args:
        snapshot: Original validated evidence.
        section: One dictionary-valued source section.
        updates: Explicit changed source facts.

    Returns:
        A new validated snapshot retaining the original object unchanged.
    """
    data: dict[str, object] = snapshot.model_dump(mode="python")
    cast(dict[str, object], data[section]).update(updates)
    return ExecutionUnitSnapshotPayload.model_validate(data)


def _state(
    snapshot: ExecutionUnitSnapshotPayload, quantity: str = "0", entry: str | None = None
) -> ContractEconomicState:
    """Provide an explicit test seed without implying a production bootstrap.

    Args:
        snapshot: Exact unit profile for this test's position pool.
        quantity: Independently specified seed position quantity.
        entry: Independently specified seed entry price, absent when flat.

    Returns:
        A validated typed economic position with no cash field.
    """
    return ContractEconomicState(
        snapshot.scope,
        snapshot.dimensions,
        ContractPosition(
            contract_model(snapshot.dimensions),
            Decimal(quantity),
            Decimal(entry) if entry is not None else None,
        ),
    )


def _settlement(
    state: ContractEconomicState, kind: str, price: str, quantity: str
) -> ContractSettlementObservation:
    """Build an explicit cash-settlement observation, independent of execution fills.

    Args:
        state: Exact test account/instrument scope.
        kind: Variation basis reset or expiry close.
        price: Exact quoted settlement price.
        quantity: Explicit resulting contract-count position.

    Returns:
        A source-tagged settlement observation awaiting durable binding.
    """
    return ContractSettlementObservation.model_validate(
        {
            "scope": state.scope,
            "kind": kind,
            "price": price,
            "price_currency": "USD",
            "price_quantity_convention": "quote_per_base",
            "quantity_unit": "contract_count",
            "settlement_type": "cash",
            "resulting_quantity": quantity,
            "receipt": _snapshot().receipt,
            "observed_at": _OBSERVED,
        }
    )


def test_linear_open_partial_close_and_flip_do_not_transfer_contract_principal() -> None:
    """Use explicit contract economics without a spot cash debit.

    Given: Two contracts with half a BTC of base exposure per contract.
    When: They open at100, partially close at110 and flip at90.
    Then: P&L, entry and turnover follow the contract model and no cash is invented.
    """
    opening = _snapshot("2", "100", multiplier="0.5")
    initial = _state(opening)
    opened = fold_contract_fill(initial, opening)
    partial = fold_contract_fill(opened.state, _snapshot("-1", "110", multiplier="0.5"))
    flipped = fold_contract_fill(partial.state, _snapshot("-2", "90", multiplier="0.5"))
    assert opened.state.position.quantity == 2
    assert opened.state.quote_turnover == 100
    assert opened.realized_delta == 0
    assert partial.realized_delta == 5
    assert partial.state.position.entry_price == 100
    assert flipped.realized_delta == -5
    assert flipped.state.realized_pnl == 0
    assert flipped.state.position.quantity == -1
    assert flipped.state.position.entry_price == 90
    assert flipped.closed_quantity == 1
    assert flipped.opened_quantity == 1
    assert flipped.state.quote_turnover == 245
    assert flipped.state.settlement_currency == "USD"
    assert not hasattr(flipped.state, "cash")
    assert initial.position.quantity == 0


def test_inverse_partial_close_and_flip_realize_base_currency_pnl() -> None:
    """Keep reciprocal P&L in base currency and quote notional independent of price.

    Given: One hundred inverse contracts worth one USD each at10000.
    When: Fifty close at20000 and the remainder flips at5000.
    Then: The realized result is negative0.0025BTC and no USD cash balance is derived.
    """
    opening = _snapshot("100", "10000", "inverse")
    opened = fold_contract_fill(_state(opening), opening)
    partial = fold_contract_fill(opened.state, _snapshot("-50", "20000", "inverse"))
    flipped = fold_contract_fill(partial.state, _snapshot("-100", "5000", "inverse"))
    assert partial.realized_delta == Decimal("0.0025")
    assert flipped.realized_delta == Decimal("-0.005")
    assert flipped.state.realized_pnl == Decimal("-0.0025")
    assert flipped.state.position.quantity == -50
    assert flipped.state.position.entry_price == 5000
    assert flipped.state.quote_turnover == 250
    assert flipped.state.settlement_currency == "BTC"


def test_inverse_same_side_entries_use_harmonic_basis() -> None:
    """Consume the shared reciprocal position fold rather than arithmetic averaging.

    Given: Equal inverse quantities entered at10000 and20000.
    When: The second fill increases the position.
    Then: Entry equals200 divided by the sum of each quantity over its price.
    """
    first = _snapshot("100", "10000", "inverse")
    opened = fold_contract_fill(_state(first), first)
    result = fold_contract_fill(opened.state, _snapshot("100", "20000", "inverse"))
    assert result.state.position.entry_price == Decimal(200) / Decimal("0.015")
    assert result.state.position.entry_price != 15000


def test_fee_absence_or_foreign_fee_does_not_invent_a_position_cash_posting() -> None:
    """Keep fee/cash interpretation outside the position economics projection.

    Given: A known contract fill with unknown fees or a separately denominated fee.
    When: Position economics are folded.
    Then: Quantity and gross economic P&L are retained without fabricating fee conversion.
    """
    snapshot = _snapshot()
    initial = _state(snapshot)
    missing = _changed(snapshot, "dimensions", {"fee": None, "fee_currency": None})
    foreign = _changed(snapshot, "dimensions", {"fee": Decimal("-0.01"), "fee_currency": "ETH"})
    assert fold_contract_fill(initial, missing).state == fold_contract_fill(initial, foreign).state
    assert fold_contract_fill(initial, foreign).realized_delta == 0


def test_equivalent_catalog_refresh_does_not_change_the_economic_pool() -> None:
    """Separate specification observation revisions from unchanged contract dimensions.

    Given: New catalog clocks/version and tick evidence with unchanged economics.
    When: A later observed fill is folded into the existing position.
    Then: It increases that same pool without guessing a new monetary interpretation.
    """
    snapshot = _snapshot()
    changed = _changed(snapshot, "catalog", {"version": "observed-v2"})
    changed = _changed(changed, "dimensions", {"price_quantum": Decimal("0.001")})
    first = fold_contract_fill(_state(snapshot), snapshot)
    assert fold_contract_fill(first.state, changed).state.position.quantity == 2


@pytest.mark.parametrize(
    ("updates", "reason"),
    (
        ({"internal_quantity_unit": "base_asset"}, "internal_unit"),
        ({"native_order_quantity_unit": None}, "quantity_conversion"),
        ({"signed_quantity_unit": None}, "quantity_conversion"),
        ({"native_order_quantity_unit": "base_asset"}, "quantity_conversion"),
        ({"price_quantity_convention": "unknown", "price_currency": None}, "price_convention"),
        ({"contract_value_currency": "ETH"}, "linear_currency"),
        ({"settlement_currency": "ETH"}, "linear_currency"),
        ({"base_currency": "USD", "contract_value_currency": "USD"}, "currencies_equal"),
    ),
)
def test_unknown_or_incompatible_unit_profiles_are_refused(
    updates: dict[str, object], reason: str
) -> None:
    """Refuse ambiguous financial dimensions before any result can be proposed.

    Given: An incomplete or inconsistent monetary/quantity interpretation.
    When: A contract position seed is requested.
    Then: The profile refuses rather than falling back to spot or multiplier guesses.
    """
    snapshot = _changed(_snapshot(), "dimensions", updates)
    with pytest.raises(ContractEconomicsError, match=reason):
        _state(snapshot)


def test_unknown_payoff_and_inverse_currency_mismatch_are_refused() -> None:
    """Require the complete explicit payoff and denomination tuple.

    Given: An entirely unknown monetary tuple or an inverse tuple with quote settlement.
    When: The unit bridge resolves a valuation model.
    Then: Neither can establish economic accounting.
    """
    missing = _changed(
        _snapshot(),
        "dimensions",
        dict.fromkeys(
            (
                "payoff_model",
                "contract_multiplier",
                "contract_value_currency",
                "settlement_currency",
            )
        ),
    )
    with pytest.raises(ContractEconomicsError, match="payoff_evidence"):
        contract_model(missing.dimensions)
    mismatch = _changed(_snapshot(payoff="inverse"), "dimensions", {"settlement_currency": "USD"})
    with pytest.raises(ContractEconomicsError, match="inverse_currency"):
        contract_model(mismatch.dimensions)


def test_explicit_native_base_identity_requires_linear_multiplier_one() -> None:
    """Accept only the proven identity mapping between native base and internal contracts.

    Given: Native base quantities, normalized contract positions and an explicit multiplier.
    When: A fill with multiplier one is folded and a multiplier two profile is attempted.
    Then: Identity succeeds and the unimplemented conversion is refused.
    """
    snapshot = _changed(
        _snapshot(),
        "dimensions",
        {"native_order_quantity_unit": "base_asset", "signed_quantity_unit": "base_asset"},
    )
    snapshot = _changed(snapshot, "receipt", {"reported_quantity_unit": "base_asset"})
    assert fold_contract_fill(_state(snapshot), snapshot).state.position.quantity == 1
    changed = _changed(
        snapshot,
        "dimensions",
        {"contract_multiplier": Decimal(2), "catalog_contract_multiplier": Decimal(2)},
    )
    with pytest.raises(ContractEconomicsError, match="quantity_conversion"):
        _state(changed)


@pytest.mark.parametrize("price", (None, Decimal(0), Decimal(-1)))
def test_missing_or_nonpositive_fill_price_is_refused(price: Decimal | None) -> None:
    """Reject unavailable prices without returning a partially changed state.

    Given: A valid source scope and an unknown or unsupported fill price.
    When: Position accounting is attempted.
    Then: No economic result is proposed and the original flat state survives.
    """
    original = _snapshot()
    state = _state(original)
    changed = _changed(original, "dimensions", {"price": price})
    with pytest.raises(ContractEconomicsError, match="price_unavailable|arithmetic_unavailable"):
        fold_contract_fill(state, changed)
    assert state.position.quantity == 0


@pytest.mark.parametrize(
    ("section", "updates", "reason"),
    (
        ("scope", {"wallet_public_id": _INSTRUMENT}, "scope_mismatch"),
        (
            "dimensions",
            {"contract_multiplier": Decimal(2), "catalog_contract_multiplier": Decimal(2)},
            "payoff_changed",
        ),
        ("receipt", {"reported_quantity_unit": None}, "operand_unit"),
        ("receipt", {"reported_quantity_convention": "unknown"}, "rebase"),
        ("rebase", {"quantity_convention": "underlying_equity"}, "rebase"),
    ),
)
def test_fill_evidence_must_match_exact_scope_profile_and_reported_units(
    section: str, updates: dict[str, object], reason: str
) -> None:
    """Refuse a changed source meaning before applying the fill.

    Given: One independently changed scope, profile or quantity convention.
    When: A later fill is proposed against the original state.
    Then: It refuses with the relevant evidence boundary.
    """
    snapshot = _snapshot()
    with pytest.raises(ContractEconomicsError, match=reason):
        fold_contract_fill(_state(snapshot), _changed(snapshot, section, updates))


def test_submission_snapshot_cannot_be_counted_as_a_confirmed_fill() -> None:
    """Keep intent and actual execution targets distinct.

    Given: Structurally valid source evidence targeting a submission command.
    When: The position fold is asked to consume it.
    Then: The request is refused before accounting any quantity.
    """
    snapshot = _snapshot()
    data = snapshot.model_dump(mode="python")
    data["target"] = {
        "phase": "submission",
        "command_public_id": _WALLET,
        "client_order_id": "intent",
        "request_digest": "c" * 64,
    }
    submission = ExecutionUnitSnapshotPayload.model_validate(data)
    with pytest.raises(ContractEconomicsError, match="fill_snapshot"):
        fold_contract_fill(_state(snapshot), submission)


@pytest.mark.parametrize("kind", ("variation", "expiry"))
@pytest.mark.parametrize("payoff", ("linear", "inverse"))
def test_cash_settlement_realizes_economics_without_synthetic_trade_turnover(
    kind: str, payoff: Literal["linear", "inverse"]
) -> None:
    """Model dated variation and expiry independently of perpetual funding and trades.

    Given: A position entered at100 and an explicit cash settlement at125.
    When: Variation retains its contracts or expiry explicitly reports zero.
    Then: P&L is realized, the basis resets or closes and turnover stays unchanged.
    """
    snapshot = _snapshot("2", "100", payoff)
    state = _state(snapshot, "2", "100")
    quantity = "2" if kind == "variation" else "0"
    result = fold_contract_settlement(state, _settlement(state, kind, "125", quantity))
    expected_pnl = Decimal(50) if payoff == "linear" else Decimal("0.004")
    assert result.realized_delta == expected_pnl
    assert result.state.realized_pnl == expected_pnl
    assert result.quote_notional == 0
    assert result.state.quote_turnover == 0
    assert result.state.position.quantity == Decimal(quantity)
    assert result.state.position.entry_price == (125 if kind == "variation" else None)
    assert result.closed_quantity == (2 if kind == "expiry" else 0)
    assert result.opened_quantity == 0


def test_variation_reset_prevents_realizing_the_same_price_move_twice() -> None:
    """Apply an actual reset basis before subsequent fills.

    Given: A linear contract entered at100 and varied at110.
    When: It closes by trade at115 after the basis reset.
    Then: Settlement realizes10 and the later trade realizes only5.
    """
    snapshot = _snapshot()
    state = _state(snapshot, "1", "100")
    variation = fold_contract_settlement(state, _settlement(state, "variation", "110", "1"))
    closed = fold_contract_fill(variation.state, _snapshot("-1", "115"))
    assert variation.realized_delta == 10
    assert closed.realized_delta == 5
    assert closed.state.realized_pnl == 15
    assert closed.state.quote_turnover == 115


def test_flat_settlement_retains_zero_economics_and_no_entry() -> None:
    """Keep a proven flat source event from inventing a position or cash movement.

    Given: An explicit flat instrument state.
    When: A cash-expiry observation also reports zero resulting quantity.
    Then: Economics and turnover remain zero and entry remains absent.
    """
    state = _state(_snapshot())
    result = fold_contract_settlement(state, _settlement(state, "expiry", "100", "0"))
    assert result.realized_delta == 0
    assert result.state.position.entry_price is None


@pytest.mark.parametrize("kind", ("variation", "expiry"))
def test_settlement_requires_the_explicit_expected_resulting_quantity(kind: str) -> None:
    """Do not infer a close or retained quantity from the event label.

    Given: A nonflat position and contradictory reported resulting quantity.
    When: A variation or expiry is interpreted.
    Then: The evidence refuses instead of manufacturing an offsetting trade.
    """
    state = _state(_snapshot(), "1", "100")
    with pytest.raises(ContractEconomicsError, match="quantity_mismatch"):
        fold_contract_settlement(state, _settlement(state, kind, "110", "2"))


@pytest.mark.parametrize("updates", ({"price_currency": "EUR"}, {"scope": None}))
def test_settlement_currency_or_scope_mismatch_is_refused(updates: dict[str, object]) -> None:
    """Retain settlement denominations and exact instrument/account scope.

    Given: A changed source price currency or another wallet's scope.
    When: Settlement is proposed against an existing pool.
    Then: The economic interpretation is refused.
    """
    state = _state(_snapshot(), "1", "100")
    observation = _settlement(state, "variation", "110", "1")
    data = observation.model_dump(mode="python")
    if "scope" in updates:
        data["scope"] = {**state.scope.model_dump(), "wallet_public_id": _INSTRUMENT}
    else:
        data.update(updates)
    changed = ContractSettlementObservation.model_validate(data)
    with pytest.raises(ContractEconomicsError, match="currency_mismatch|scope_mismatch"):
        fold_contract_settlement(state, changed)


def test_settlement_receipt_must_have_been_observed() -> None:
    """Keep interpretation after its supplied observation source.

    Given: An observation time earlier than the receipt it claims to use.
    When: A settlement envelope is validated.
    Then: The impossible chronology is rejected.
    """
    state = _state(_snapshot())
    data = _settlement(state, "expiry", "100", "0").model_dump(mode="python")
    data["observed_at"] = _OBSERVED - timedelta(seconds=1)
    with pytest.raises(ValidationError, match="precedes_receipt"):
        ContractSettlementObservation.model_validate(data)


@pytest.mark.parametrize("field", ("price", "resulting_quantity"))
def test_nonfinite_settlement_numbers_are_rejected(field: str) -> None:
    """Require exact finite settlement operands before any fold.

    Given: A nonfinite settlement price or resulting quantity.
    When: The source envelope is validated.
    Then: The number is rejected without changing the original position.
    """
    state = _state(_snapshot())
    data = _settlement(state, "expiry", "100", "0").model_dump(mode="python")
    data[field] = Decimal("NaN")
    with pytest.raises(ValidationError, match="finite"):
        ContractSettlementObservation.model_validate(data)


def test_state_rejects_untyped_positions_changed_models_and_invalid_totals() -> None:
    """Revalidate retained state before live or replay consumers can use it.

    Given: An untyped position, mismatched multiplier or invalid cumulative totals.
    When: The immutable economic state is constructed.
    Then: Each invalid seed refuses without establishing an accounting anchor.
    """
    state = _state(_snapshot())
    with pytest.raises(ContractEconomicsError, match="typed_state"):
        replace(state, position=cast(ContractPosition, {}))
    with pytest.raises(ContractEconomicsError, match="payoff_mismatch"):
        replace(
            state, position=ContractPosition(LinearContractValuation(Decimal(2)), Decimal(0), None)
        )
    with pytest.raises(ContractEconomicsError, match="requires_decimal"):
        replace(state, realized_pnl=cast(Decimal, 0.0))
    with pytest.raises(ContractEconomicsError, match="nonfinite"):
        replace(state, realized_pnl=Decimal("Infinity"))
    with pytest.raises(ContractEconomicsError, match="negative"):
        replace(state, quote_turnover=Decimal(-1))


def test_fill_refuses_an_untyped_seed_before_accessing_its_fields() -> None:
    """Require an explicit proven-state object at the application accounting boundary.

    Given: No typed retained economic position state.
    When: A fill is proposed against that missing seed.
    Then: The bridge refuses before reading fields or inventing a flat position.
    """
    with pytest.raises(ContractEconomicsError, match="economic_state_required"):
        fold_contract_fill(cast(ContractEconomicState, None), _snapshot())


def test_insufficient_decimal_precision_refuses_cumulative_economic_loss() -> None:
    """Preserve small realized deltas rather than silently rounding them away.

    Given: A large retained total and a small independently realized gain.
    When: The caller's Decimal context cannot represent their exact sum.
    Then: Accumulation refuses and the original state remains unchanged.
    """
    snapshot = _snapshot()
    state = replace(_state(snapshot, "1", "100"), realized_pnl=Decimal("1e10"))
    with localcontext() as context:
        context.prec = 3
        with pytest.raises(ContractEconomicsError, match="accumulation_unavailable"):
            fold_contract_fill(state, _snapshot("-1", "101"))
    assert state.realized_pnl == Decimal("1e10")


def test_settlement_arithmetic_failure_preserves_original_state() -> None:
    """Do not partially advance a position basis when settlement arithmetic traps.

    Given: An inverse position and a context unable to represent reciprocal division.
    When: Its supplied cash settlement is folded with a trapped precision loss.
    Then: The proposal refuses and the original entry survives.
    """
    snapshot = _snapshot("1", "3", "inverse")
    state = _state(snapshot, "1", "3")
    with localcontext() as context:
        context.prec = 3
        context.traps[Inexact] = True
        with pytest.raises(ContractEconomicsError, match="settlement_arithmetic"):
            fold_contract_settlement(state, _settlement(state, "expiry", "7", "0"))
    assert state.position.entry_price == 3
