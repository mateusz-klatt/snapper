"""Native balance evidence stays separate from inferred margin aliases and spendable funds."""

from datetime import UTC
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.infrastructure.exchanges.native_balance_source import NativeBalanceProfile
from snapper.infrastructure.exchanges.native_balance_source import NativeBalanceProfileObservation
from snapper.infrastructure.exchanges.native_balance_source import NativeBalanceStreamProof
from snapper.infrastructure.exchanges.native_balance_source import NativeCurrencyQuantity

_NOW = datetime(2026, 10, 23, 10, tzinfo=UTC)


def _selection() -> ExecutionClientSelection:
    """Construct an explicit isolated demo source, without a margin-account assumption."""
    return ExecutionClientSelection(
        wallet_public_id=str(UUID(int=1)),
        exchange="kraken_futures",
        mode="live",
        environment="demo",
        credential_public_id=str(UUID(int=2)),
        credential_timestamp=_NOW,
        public_api_origin="https://demo-futures.kraken.com",
    )


def _proof(**updates: object) -> NativeBalanceStreamProof:
    """Describe two retained source receipts while allowing deliberate malformed boundary inputs."""
    values: dict[str, object] = {
        "connection_epoch_public_id": str(UUID(int=3)),
        "client_selection": _selection(),
        "receipt_public_ids": (str(UUID(int=4)), str(UUID(int=5))),
        "venue_account_id": "actual-native-header",
        "snapshot_sequence": 0,
        "tip_sequence": 1,
        "observed_at": _NOW,
        "venue_timestamp": _NOW,
    }
    values.update(updates)
    return NativeBalanceStreamProof.model_validate(values)


def _single_profile() -> NativeBalanceProfile:
    """Keep the exact constructed native label without treating it as an account-log alias."""
    return NativeBalanceProfile(
        kind="single_collateral",
        source_key="F-XBT:USD",
        source_name="F-XBT:USD",
        source_pair="XBT/USD",
        settlement_unit="XBT",
    )


@pytest.mark.parametrize("kind", ("holding", "multi_collateral"))
def test_non_single_profiles_do_not_acquire_invented_margin_aliases(kind: str) -> None:
    """A source profile kind cannot silently become a named ledger margin account.

    Given: A holding or multi-collateral source profile,
    When: Its immutable native observation is constructed,
    Then: Its exact quantities remain independent of account-log naming and availability.
    """
    profile = NativeBalanceProfile.model_validate({"kind": kind})
    observation = NativeBalanceProfileObservation(
        profile=profile,
        quantities=(NativeCurrencyQuantity(currency="USD", quantity=Decimal("-0.25")),),
        source_proof=_proof(),
    )
    assert observation.profile.source_name is None
    assert observation.quantities[0].quantity == Decimal("-0.25")
    assert "margin_account" not in observation.model_dump()
    assert "spendable" not in observation.model_dump()


@pytest.mark.parametrize("field", ("source_key", "source_name", "source_pair", "settlement_unit"))
def test_single_collateral_profile_requires_complete_verbatim_source_metadata(field: str) -> None:
    """A partially identified source cannot silently claim a complete coin-margin profile.

    Given: An otherwise valid native single-collateral descriptor,
    When: One observed identity component is absent,
    Then: The source candidate refuses rather than inventing a label or unit.
    """
    values = _single_profile().model_dump()
    values[field] = None
    with pytest.raises(ValidationError, match="single_profile_metadata_missing"):
        NativeBalanceProfile.model_validate(values)


@pytest.mark.parametrize("kind", ("holding", "multi_collateral"))
def test_other_native_profile_kinds_refuse_caller_supplied_named_aliases(kind: str) -> None:
    """An observed kind does not certify an arbitrary configured margin account.

    Given: A non-single native profile and a proposed unproven source name,
    When: The immutable descriptor is validated,
    Then: It refuses the alias.
    """
    with pytest.raises(ValidationError, match="profile_alias_unproven"):
        NativeBalanceProfile.model_validate({"kind": kind, "source_name": "flex"})


def test_single_profile_exposes_its_native_settlement_unit_and_exact_quantity() -> None:
    """A coin-margin quantity remains denominated in the actually observed settlement unit.

    Given: A source single-collateral account with an exact XBT balance,
    When: Its native profile candidate is constructed,
    Then: The long Decimal quantity survives without a fabricated USD amount.
    """
    quantity = Decimal("0.100000000000000001")
    observation = NativeBalanceProfileObservation(
        profile=_single_profile(),
        quantities=(NativeCurrencyQuantity(currency="XBT", quantity=quantity),),
        source_proof=_proof(),
    )
    assert observation.quantities[0].quantity == quantity
    assert observation.profile.source_key == "F-XBT:USD"


@pytest.mark.parametrize("currencies", ((), ("USD",), ("XBT", "USD")))
def test_single_profile_refuses_missing_or_wrong_native_settlement_inventory(
    currencies: tuple[str, ...],
) -> None:
    """A settlement profile cannot claim unrelated currency quantities.

    Given: A single-collateral profile whose actual settlement unit is XBT,
    When: Its candidate omits XBT or adds an unrelated currency,
    Then: It refuses the contradictory native inventory.
    """
    with pytest.raises(ValidationError, match="settlement_quantity_mismatch"):
        NativeBalanceProfileObservation(
            profile=_single_profile(),
            quantities=tuple(
                NativeCurrencyQuantity(currency=currency, quantity=Decimal("1"))
                for currency in currencies
            ),
            source_proof=_proof(),
        )


def test_profile_refuses_repeated_currency_instead_of_flattening_or_summing_it() -> None:
    """Repeated native currencies require explicit source resolution rather than aggregation.

    Given: Two USD quantities inside the same native profile,
    When: Its candidate is validated,
    Then: It refuses duplicate entries instead of summing account state.
    """
    with pytest.raises(ValidationError, match="profile_currency_repeated"):
        NativeBalanceProfileObservation(
            profile=NativeBalanceProfile(kind="multi_collateral"),
            quantities=(
                NativeCurrencyQuantity(currency="USD", quantity=Decimal("1")),
                NativeCurrencyQuantity(currency="USD", quantity=Decimal("2")),
            ),
            source_proof=_proof(),
        )


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    (
        ("receipt_public_ids", (), "at least 1 item"),
        ("receipt_public_ids", (str(UUID(int=4)), str(UUID(int=4))), "receipt_repeated"),
        ("tip_sequence", 2, "sequence_projection_mismatch"),
        ("snapshot_sequence", True, "valid integer"),
        ("snapshot_sequence", -1, "greater than or equal to 0"),
        ("connection_epoch_public_id", "not-an-epoch", "UUID"),
    ),
)
def test_invalid_source_chain_metadata_cannot_become_native_proof(
    field: str, value: object, reason: str
) -> None:
    """Transport metadata is validated without pretending it validates retained wire contents.

    Given: A malformed or internally contradictory retained-chain descriptor,
    When: The candidate is constructed,
    Then: Its explicit source invariant rejects it before durable replay.
    """
    with pytest.raises(ValidationError, match=reason):
        _proof(**{field: value})


def test_futures_native_source_refuses_another_provider_client_selection() -> None:
    """Identical source shapes cannot relabel another venue's actual authenticated route.

    Given: An otherwise valid Spot source selection,
    When: It is supplied to a Futures balance-chain candidate,
    Then: The provider mismatch refuses.
    """
    selection = _selection().model_dump()
    selection.update(exchange="kraken", public_api_origin="https://api.kraken.com")
    with pytest.raises(ValidationError, match="provider_mismatch"):
        _proof(client_selection=ExecutionClientSelection.model_validate(selection))
