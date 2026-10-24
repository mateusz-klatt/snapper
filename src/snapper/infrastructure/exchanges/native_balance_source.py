"""Describe retained native profile evidence without granting spendable-funds authority."""

from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID

NATIVE_BALANCE_RECEIPT_LIMIT = 4096


class _NativeBalanceEvidence(BaseModel):
    """Revalidate immutable source candidates rather than accepting mutable decoded state."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )


class NativeBalanceProfile(_NativeBalanceEvidence):
    """One verbatim native source profile, with no inferred account-log alias."""

    kind: Literal["holding", "single_collateral", "multi_collateral"]
    source_key: EvidenceText | None = None
    source_name: EvidenceText | None = None
    source_pair: EvidenceText | None = None
    settlement_unit: EvidenceText | None = None

    @model_validator(mode="after")
    def validate_profile(self) -> Self:
        """Keep a single-collateral profile complete and other profile kinds unaliased.

        Returns:
            The same unnormalized profile descriptor.
        """
        metadata = (
            self.source_key,
            self.source_name,
            self.source_pair,
            self.settlement_unit,
        )
        if self.kind == "single_collateral":
            if any(value is None for value in metadata):
                raise ValueError("native_balance_single_profile_metadata_missing")
        elif any(value is not None for value in metadata):
            raise ValueError("native_balance_profile_alias_unproven")
        return self


class NativeCurrencyQuantity(_NativeBalanceEvidence):
    """An exact native quantity, without valuation, availability or a free/used split."""

    currency: EvidenceText
    quantity: EvidenceDecimal


class NativeBalanceStreamProof(_NativeBalanceEvidence):
    """A bounded replay candidate for retained bytes from one actual WS subscription epoch."""

    policy_version: Literal["kraken_futures_balances_v1"] = "kraken_futures_balances_v1"
    connection_epoch_public_id: EvidenceUUID
    client_selection: ExecutionClientSelection
    receipt_public_ids: tuple[EvidenceUUID, ...] = Field(
        min_length=1, max_length=NATIVE_BALANCE_RECEIPT_LIMIT
    )
    venue_account_id: EvidenceText
    snapshot_sequence: int = Field(strict=True, ge=0)
    tip_sequence: int = Field(strict=True, ge=0)
    observed_at: EvidenceInstant
    venue_timestamp: EvidenceInstant

    @model_validator(mode="after")
    def validate_source_chain(self) -> Self:
        """Refuse duplicated receipts and impossible sequence projections before durable replay.

        Returns:
            The same candidate, still requiring full immutable receipt validation.
        """
        if self.client_selection.exchange != "kraken_futures":
            raise ValueError("native_balance_provider_mismatch")
        if len(set(self.receipt_public_ids)) != len(self.receipt_public_ids):
            raise ValueError("native_balance_receipt_repeated")
        if self.tip_sequence - self.snapshot_sequence != len(self.receipt_public_ids) - 1:
            raise ValueError("native_balance_sequence_projection_mismatch")
        return self


class NativeBalanceProfileObservation(_NativeBalanceEvidence):
    """One profile's source quantities, independently replayable and not yet account mapped."""

    profile: NativeBalanceProfile
    quantities: tuple[NativeCurrencyQuantity, ...] = Field(max_length=4096)
    source_proof: NativeBalanceStreamProof

    @model_validator(mode="after")
    def validate_unique_currency(self) -> Self:
        """Keep same-currency quantities separate across profiles and unique within one profile.

        Returns:
            The same explicit native profile observation.
        """
        currencies = tuple(item.currency for item in self.quantities)
        if len(set(currencies)) != len(currencies):
            raise ValueError("native_balance_profile_currency_repeated")
        if self.profile.kind == "single_collateral" and currencies != (
            self.profile.settlement_unit,
        ):
            raise ValueError("native_balance_settlement_quantity_mismatch")
        return self
