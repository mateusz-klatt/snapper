"""Verify partial-account projections preserve native units, missing facts and cash uncertainty."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from snapper.core.contract_account_projection import ContractAccountSnapshot
from snapper.core.contract_account_projection import ContractJournalReference
from snapper.core.contract_account_projection import ObservedContractCash
from snapper.core.contract_account_projection import ObservedContractPosition
from snapper.core.contract_account_projection import ReportedPositionCashComponents
from snapper.core.contract_account_projection import contract_account_snapshot_digest
from snapper.core.execution_route_authority import ExecutionAccountScope

_NOW = datetime(2026, 10, 23, 14, tzinfo=UTC)


def _id(number: int) -> str:
    """Return an actual canonical UUID string for isolated projection evidence."""
    return str(UUID(int=number))


def _snapshot() -> ContractAccountSnapshot:
    """Build a typed partial account whose reported fee is not a native cash movement."""
    return ContractAccountSnapshot(
        scope=ExecutionAccountScope(
            wallet_public_id=_id(1),
            exchange="kraken_futures",
            mode="live",
            environment="production",
            margin_account="futures.flex",
        ),
        as_of=_NOW,
        journal=(
            ContractJournalReference(
                public_id=_id(2),
                content_digest="a" * 64,
                ledger_sequence=1,
                recorded_at=_NOW,
                kind="contract_position_fill",
                instrument_public_id=_id(3),
            ),
        ),
        positions=(
            ObservedContractPosition(
                instrument_public_id=_id(3),
                latest_event_public_id=_id(2),
                anchor_public_id=_id(4),
                unit_snapshot_public_id=_id(5),
                route_authority_public_id=_id(6),
                ledger_sequence=1,
                quantity=Decimal("2"),
                entry_price=Decimal("100"),
                settlement_currency="USD",
                quote_currency="USD",
                modeled_realized_pnl_since_anchor=Decimal("0.05"),
                modeled_quote_turnover_since_anchor=Decimal("200"),
                basis_status="equal",
            ),
        ),
        native_cash=(),
        reported_cash_components=(
            ReportedPositionCashComponents(
                position_event_public_id=_id(2),
                source_receipt_public_id=_id(7),
                target_json_pointer="/new_entry",
                source_log_id="17000000000000000000000000000",
                booking_uid="opaque-booking",
                execution_id="opaque-execution",
                reported_collateral_currency="USD",
                fee=Decimal("0.1"),
                realized_pnl=None,
                realized_funding=None,
                liquidation_fee=None,
                funding_rate=None,
                conversion_spread_percentage=None,
            ),
        ),
        unresolved_holds=(),
    )


def test_reported_position_cash_never_creates_an_observed_balance() -> None:
    """Retain actual source costs and modeled position totals without manufacturing cash.

    Given: A native position reports a fee and the price model reports realized PnL.
    When: The typed account has no actual native balance transition.
    Then: Cash remains absent and reconciliation, inventory and readiness remain held.
    """
    snapshot = _snapshot()
    assert snapshot.native_cash == ()
    assert snapshot.positions[0].modeled_realized_pnl_since_anchor == Decimal("0.05")
    assert snapshot.reported_cash_components[0].realized_pnl is None
    assert snapshot.reported_cash_components[0].cash_reconciliation_status == "unproven"
    assert snapshot.readiness == "held"
    assert snapshot.inventory_status == "unproven"
    assert snapshot.owner_identity_status == "unproven"
    assert snapshot.execution_prefix_status == "unavailable"


def test_snapshot_fingerprint_ignores_read_clock_and_preserves_source_components() -> None:
    """Fingerprint evidence rather than a changing cache-refresh wall clock.

    Given: One actual partial journal is read twice at different local clocks.
    When: A source component is then changed in a separate projection candidate.
    Then: Clock-only refresh has the same digest and changed evidence has a different digest.
    """
    first = _snapshot()
    later = ContractAccountSnapshot.model_validate(
        {**first.model_dump(), "as_of": _NOW + timedelta(seconds=1)}
    )
    component = first.reported_cash_components[0].model_copy(update={"fee": Decimal("0.2")})
    changed = ContractAccountSnapshot.model_validate(
        {**first.model_dump(), "reported_cash_components": (component,)}
    )
    assert contract_account_snapshot_digest(first) == contract_account_snapshot_digest(later)
    assert contract_account_snapshot_digest(first) != contract_account_snapshot_digest(changed)
    assert ContractAccountSnapshot.model_validate_json(first.model_dump_json()) == first


@pytest.mark.parametrize(
    ("instrument", "currency"),
    [(None, None), (_id(3), "USD"), (None, "USD")],
)
def test_position_reference_refuses_currency_or_missing_instrument(
    instrument: str | None, currency: str | None
) -> None:
    """Refuse contract facts with currency dimensions or an absent immutable instrument.

    Given: A candidate local journal reference is labeled as a position event.
    When: Its native dimensions are missing or describe a currency.
    Then: Strict projection validation rejects it before any account state exists.
    """
    payload = _snapshot().journal[0].model_dump()
    with pytest.raises(ValidationError, match="native units"):
        ContractJournalReference.model_validate(
            {**payload, "instrument_public_id": instrument, "native_currency": currency}
        )


@pytest.mark.parametrize(
    ("instrument", "currency"),
    [(None, None), (_id(3), "USD"), (_id(3), None)],
)
def test_cash_reference_refuses_instrument_or_missing_currency(
    instrument: str | None, currency: str | None
) -> None:
    """Refuse native cash facts without an exact currency-only dimension.

    Given: A candidate local journal reference is labeled as a native cash event.
    When: Its dimensions name a contract or omit the currency.
    Then: Strict projection validation refuses the incompatible units.
    """
    payload = _snapshot().journal[0].model_dump()
    with pytest.raises(ValidationError, match="native units"):
        ContractJournalReference.model_validate(
            {
                **payload,
                "kind": "native_cash",
                "instrument_public_id": instrument,
                "native_currency": currency,
            }
        )


@pytest.mark.parametrize(
    ("quantity", "basis", "turnover"),
    [
        (Decimal("0"), Decimal("100"), Decimal("0")),
        (Decimal("1"), None, Decimal("0")),
        (Decimal("1"), Decimal("0"), Decimal("0")),
        (Decimal("1"), Decimal("-100"), Decimal("0")),
        (Decimal("1"), Decimal("100"), Decimal("-1")),
    ],
)
def test_native_position_basis_and_modeled_turnover_remain_constrained(
    quantity: Decimal, basis: Decimal | None, turnover: Decimal
) -> None:
    """Preserve explicit source flatness and valid modeled quote units.

    Given: A valid observed contract position has a positive source entry basis.
    When: A projection candidate has contradictory flatness, nonpositive basis or turnover.
    Then: Validation refuses the candidate without interpreting it as cash.
    """
    payload = _snapshot().positions[0].model_dump()
    with pytest.raises(ValidationError):
        ObservedContractPosition.model_validate(
            {
                **payload,
                "quantity": quantity,
                "entry_price": basis,
                "modeled_quote_turnover_since_anchor": turnover,
            }
        )


@pytest.mark.parametrize("sequence", [True, 1.0, 0, -1])
def test_local_ledger_sequence_rejects_aliases_and_nonpositive_values(
    sequence: bool | float | int,
) -> None:
    """Keep local journal coordinates strict and positive.

    Given: A position reference has a genuine positive integer ledger sequence.
    When: A boolean, float or nonpositive coordinate is supplied.
    Then: Validation rejects it instead of aliasing a real retained fact.
    """
    with pytest.raises(ValidationError):
        ContractJournalReference.model_validate(
            {**_snapshot().journal[0].model_dump(), "ledger_sequence": sequence}
        )


@pytest.mark.parametrize(
    "change", ["duplicate_id", "duplicate_sequence", "future", "missing_position"]
)
def test_snapshot_rejects_ambiguous_or_incomplete_retained_state(change: str) -> None:
    """Refuse stale, duplicate or missing local financial projections.

    Given: A valid retained position journal has one explicit latest state.
    When: Facts duplicate identities or sequences, exceed knowledge or lose latest state.
    Then: The partial snapshot refuses instead of silently dropping financial evidence.
    """
    snapshot = _snapshot()
    payload = snapshot.model_dump()
    journal = snapshot.journal[0]
    if change == "duplicate_id":
        payload["journal"] = (journal, journal.model_copy(update={"ledger_sequence": 2}))
    elif change == "duplicate_sequence":
        payload["journal"] = (journal, journal.model_copy(update={"public_id": _id(8)}))
    elif change == "future":
        payload["as_of"] = _NOW - timedelta(microseconds=1)
    else:
        payload["positions"] = ()
    with pytest.raises(ValidationError):
        ContractAccountSnapshot.model_validate(payload)


@pytest.mark.parametrize("change", ["missing", "duplicate", "foreign", "duplicate_position"])
def test_every_retained_position_keeps_its_reported_cash_uncertainty(change: str) -> None:
    """Preserve one cash-component observation per actual retained position fact.

    Given: The snapshot retains a source position with explicit unproven cash reconciliation.
    When: Components are missing, duplicated, foreign or the position is duplicated.
    Then: Validation refuses the incomplete or ambiguous account view.
    """
    snapshot = _snapshot()
    payload = snapshot.model_dump()
    component = snapshot.reported_cash_components[0]
    if change == "missing":
        payload["reported_cash_components"] = ()
    elif change == "duplicate":
        payload["reported_cash_components"] = (component, component)
    elif change == "foreign":
        payload["reported_cash_components"] = (
            component.model_copy(update={"position_event_public_id": _id(8)}),
        )
    else:
        payload["positions"] = snapshot.positions * 2
    with pytest.raises(ValidationError):
        ContractAccountSnapshot.model_validate(payload)


def test_absolute_native_currency_projection_has_no_cross_currency_sum() -> None:
    """Preserve an actual native balance independently from position components.

    Given: One actual USD cash transition follows an independently retained position fact.
    When: The latest native balance is included in the partial snapshot.
    Then: It remains the exact source absolute value and duplicate currency state refuses.
    """
    snapshot = _snapshot()
    native = ContractJournalReference(
        public_id=_id(9),
        content_digest="b" * 64,
        ledger_sequence=2,
        recorded_at=_NOW,
        kind="native_cash",
        native_currency="USD",
    )
    cash = ObservedContractCash(
        native_currency="USD",
        balance=Decimal("99.75"),
        latest_event_public_id=_id(9),
        anchor_public_id=_id(10),
        route_authority_public_id=_id(6),
        ledger_sequence=2,
    )
    payload = {
        **snapshot.model_dump(),
        "journal": (*snapshot.journal, native),
        "native_cash": (cash,),
    }
    actual = ContractAccountSnapshot.model_validate(payload)
    assert actual.native_cash[0].balance == Decimal("99.75")
    with pytest.raises(ValidationError, match="duplicate native currencies"):
        ContractAccountSnapshot.model_validate({**payload, "native_cash": (cash, cash)})
    with pytest.raises(ValidationError, match="latest state"):
        ContractAccountSnapshot.model_validate({**payload, "native_cash": ()})


def test_closed_source_position_remains_an_explicit_observed_position() -> None:
    """Distinguish an observed flat contract from an absent source position.

    Given: A genuine retained contract identity has an explicit zero source quantity.
    When: Its observed basis is absent and the partial snapshot is validated.
    Then: The position remains present and flat without creating native cash.
    """
    snapshot = _snapshot()
    flat = snapshot.positions[0].model_copy(update={"quantity": Decimal("0"), "entry_price": None})
    actual = ContractAccountSnapshot.model_validate({**snapshot.model_dump(), "positions": (flat,)})
    assert actual.positions[0].quantity == Decimal("0")
    assert actual.positions[0].entry_price is None
    assert actual.native_cash == ()


def test_earlier_position_components_survive_replacement_by_a_later_position_fact() -> None:
    """Retain cash uncertainty from every native position fact rather than only the latest state.

    Given: Two retained events affect the same contract and each reports source cash components.
    When: The latest observed position replaces the prior quantity and basis in the view.
    Then: Both component records remain required and removing the earlier record refuses.
    """
    snapshot = _snapshot()
    later = snapshot.journal[0].model_copy(update={"public_id": _id(8), "ledger_sequence": 2})
    position = snapshot.positions[0].model_copy(
        update={"latest_event_public_id": _id(8), "ledger_sequence": 2}
    )
    component = snapshot.reported_cash_components[0].model_copy(
        update={"position_event_public_id": _id(8), "source_log_id": "18"}
    )
    payload = {
        **snapshot.model_dump(),
        "journal": (*snapshot.journal, later),
        "positions": (position,),
        "reported_cash_components": (*snapshot.reported_cash_components, component),
    }
    actual = ContractAccountSnapshot.model_validate(payload)
    assert len(actual.positions) == 1
    assert len(actual.reported_cash_components) == 2
    with pytest.raises(ValidationError, match="every position"):
        ContractAccountSnapshot.model_validate(
            {**payload, "reported_cash_components": (component,)}
        )
