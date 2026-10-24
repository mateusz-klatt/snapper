"""Adversarial source-position proof validation without asserting any database authority."""

from datetime import timedelta
from decimal import Decimal
from typing import cast

import pytest

from snapper.application.portfolio.contract_position_ledger import CURRENT_POSITION_DECIMAL_POLICY
from snapper.application.portfolio.contract_position_ledger import ContractPositionAnchorPayload
from snapper.application.portfolio.contract_position_ledger import ContractPositionEventPayload
from snapper.application.portfolio.contract_position_ledger import ContractPositionValue
from snapper.application.portfolio.contract_position_ledger import PositionDecimalPolicy
from snapper.application.portfolio.contract_position_ledger import canonical_position_payload
from snapper.application.portfolio.contract_position_ledger import fold_position_source
from snapper.application.portfolio.contract_position_ledger import position_payload_digest
from snapper.application.portfolio.futures_account_log import FuturesAccountLogEntry
from snapper.application.portfolio.futures_position_source import FuturesPositionSourceLeg
from snapper.application.portfolio.futures_position_source import native_futures_position_leg
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from tests.application.portfolio.test_contract_economics import _OBSERVED
from tests.application.portfolio.test_contract_position_ledger import _source
from tests.application.portfolio.test_contract_position_ledger import _unit
from tests.application.portfolio.test_contract_position_ledger import _value


def _anchor() -> ContractPositionAnchorPayload:
    """Provide structural pure-model evidence without claiming persisted targets or authority."""
    unit = _unit()
    reference = unit.scope.instrument_public_id
    return ContractPositionAnchorPayload(
        scope=ExecutionAccountScope(
            wallet_public_id=unit.scope.wallet_public_id,
            exchange="kraken_futures",
            mode="live",
            environment="production",
            margin_account="account-1",
        ),
        instrument_public_id=reference,
        route_authority_public_id=reference,
        position_route_authority_public_id=reference,
        source_receipt_public_id=reference,
        unit_snapshot_public_id=reference,
        source_alias_public_id=reference,
        source_alias_timestamp=_OBSERVED,
        unit=unit,
        source_position=_source(unit, ("0", None), ("1", "100")),
        opening=_value("0", None),
    )


def _event() -> ContractPositionEventPayload:
    """Build conserved structural projections for independent adversarial field replacement."""
    anchor = _anchor()
    derived, observed = fold_position_source(anchor.opening, anchor.unit, anchor.source_position)
    reference = anchor.instrument_public_id
    return ContractPositionEventPayload(
        scope=anchor.scope,
        instrument_public_id=reference,
        route_authority_public_id=reference,
        position_route_authority_public_id=reference,
        anchor_public_id=reference,
        raw_receipt_public_id=reference,
        position_receipt_public_id=reference,
        execution_public_id=cast(FillSnapshotTarget, anchor.unit.target).execution_public_id,
        execution_scope_sequence=1,
        unit_snapshot_public_id=reference,
        source_alias_public_id=reference,
        source_alias_timestamp=_OBSERVED,
        venue_execution_id="venue-fill-1",
        executed_at=_OBSERVED,
        execution_known_at=_OBSERVED + timedelta(seconds=1),
        ledger_sequence=1,
        unit=anchor.unit,
        profile_unit=anchor.unit,
        source_position=anchor.source_position,
        before=anchor.opening,
        derived_after=derived,
        observed_after=observed,
        basis_status="equal",
    )


@pytest.mark.parametrize(
    "policy", ("contract_context_50_half_even_v1", CURRENT_POSITION_DECIMAL_POLICY)
)
def test_recorded_position_policy_roundtrip_keeps_its_explicit_version(
    policy: PositionDecimalPolicy,
) -> None:
    """Keep historical and current arithmetic identities distinct in immutable event evidence.

    Given: A genuine structural transition whose opening arithmetic agrees under both policies.
    When: Each explicit policy is serialized and restored as a retained event.
    Then: Its exact policy survives and the two policy versions have different content identities.
    """
    original = _event()
    event = ContractPositionEventPayload.model_validate(
        {**original.model_dump(), "decimal_policy": policy}
    )
    restored = ContractPositionEventPayload.model_validate_json(canonical_position_payload(event))
    assert restored == event
    assert restored.decimal_policy == policy
    alternate = ContractPositionEventPayload.model_validate(
        {
            **event.model_dump(),
            "decimal_policy": (
                CURRENT_POSITION_DECIMAL_POLICY
                if policy == "contract_context_50_half_even_v1"
                else "contract_context_50_half_even_v1"
            ),
        }
    )
    assert position_payload_digest(event) != position_payload_digest(alternate)


def test_native_result_must_equal_one_actual_fill_instead_of_a_cumulative_quantity_gap() -> None:
    """Refuse plausible source changes that combine more than the selected genuine fill.

    Given: The real fill quantity is one but the source position changes by two.
    When: A caller directly folds the source operands.
    Then: The contradiction refuses without fabricating an aggregate execution.
    """
    unit = _unit()
    with pytest.raises(ValueError, match="quantity contradicts"):
        fold_position_source(_value("0", None), unit, _source(unit, ("0", None), ("2", "100")))


@pytest.mark.parametrize("version", (True, 1.0, "1"))
def test_position_schema_versions_cannot_be_coerced(version: object) -> None:
    """Keep immutable position schemas distinct from boolean, float and textual aliases.

    Given: A valid position with an ambiguous schema discriminator.
    When: It is revalidated as financial source evidence.
    Then: The noncanonical version is rejected.
    """
    with pytest.raises(ValueError, match="schema version must be an integer"):
        ContractPositionValue.model_validate(
            {**_value("0", None).model_dump(), "schema_version": version}
        )


@pytest.mark.parametrize("quantity", ("0", "1"))
def test_position_basis_cannot_be_present_when_flat_or_missing_when_nonflat(quantity: str) -> None:
    """Distinguish proved flat inventory from an unknown entry basis.

    Given: Native quantity and entry basis contradict each other.
    When: The immutable state value is constructed.
    Then: It refuses a fake flat basis or missing nonflat basis.
    """
    with pytest.raises(ValueError, match="quantity and entry basis disagree"):
        _value(quantity, "100" if quantity == "0" else None)


@pytest.mark.parametrize("change", ("opening", "instrument"))
def test_anchor_cannot_replace_native_opening_or_change_source_instrument(change: str) -> None:
    """Check anchor projections instead of trusting a well-formed UUID alone.

    Given: Explicit native opening evidence and its captured instrument.
    When: A caller substitutes opening state or source instrument identity.
    Then: The unsupported anchor refuses.
    """
    anchor = _anchor()
    updates: dict[str, object] = (
        {"opening": _value("1", "100")}
        if change == "opening"
        else {"instrument_public_id": "aabbccdd-eeff-4011-8233-445566778800"}
    )
    with pytest.raises(ValueError):
        ContractPositionAnchorPayload.model_validate({**anchor.model_dump(), **updates})


@pytest.mark.parametrize("field", ("execution_public_id", "venue_execution_id", "basis_status"))
def test_event_cannot_change_source_execution_or_declared_model_outcome(field: str) -> None:
    """Revalidate redundant event fields against source identity and arithmetic operands.

    Given: One structurally conserved source position event.
    When: Actual execution identity or model comparison status is replaced.
    Then: The contradictory event refuses before persistence.
    """
    event = _event()
    replacement = {
        "execution_public_id": "aabbccdd-eeff-4011-8233-445566778800",
        "venue_execution_id": "different-source-fill",
        "basis_status": "source_precision_unproven",
    }[field]
    with pytest.raises(ValueError):
        ContractPositionEventPayload.model_validate({**event.model_dump(), field: replacement})


@pytest.mark.parametrize(
    "updates",
    (
        {"old_average_entry_price": Decimal(1)},
        {"old_balance": Decimal(1), "old_average_entry_price": None},
        {"old_balance": Decimal(1), "old_average_entry_price": Decimal(0)},
        {"asset": "flex_collateral_value"},
        {"contract": "another-provider-contract"},
        {"old_balance": Decimal("1e100"), "old_average_entry_price": Decimal(100)},
    ),
)
def test_invalid_native_basis_valuation_and_precision_never_become_position_units(
    updates: dict[str, object],
) -> None:
    """Refuse unsupported native operands while their original raw receipt remains held.

    Given: Source fields contain impossible basis, valuation units or inexact quantity change.
    When: Native position interpretation is requested.
    Then: It refuses instead of substituting cash, guessed basis or rounded quantity.
    """
    unit = _unit()
    source = _source(unit, ("0", None), ("1", "100"))
    entry = FuturesAccountLogEntry.model_validate({**source.source_entry.model_dump(), **updates})
    with pytest.raises(ValueError):
        native_futures_position_leg(entry, "provider-contract", unit.scope.instrument_public_id)


def test_native_projection_cannot_be_replaced_after_source_validation() -> None:
    """Require redundant quantity fields to retain the exact native source transition.

    Given: A verified native source leg with an exact quantity delta.
    When: A caller replaces the projected delta while retaining the source entry.
    Then: The immutable source DTO refuses the contradiction.
    """
    unit = _unit()
    source = _source(unit, ("0", None), ("1", "100"))
    with pytest.raises(ValueError, match="projection_contradiction"):
        FuturesPositionSourceLeg.model_validate(
            {**source.model_dump(), "quantity_delta": Decimal(2)}
        )
