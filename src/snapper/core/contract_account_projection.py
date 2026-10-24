"""Describe partial source-bound contract accounts without granting spending authority."""

import hashlib
import json
from typing import Annotated
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import StrictInt
from pydantic import model_validator

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID

type ProjectionSequence = Annotated[StrictInt, Field(gt=0)]


class _ProjectionPayload(BaseModel):
    """Keep projected evidence immutable and reject unknown or coercible fields."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )


class ContractJournalReference(_ProjectionPayload):
    """Identify one retained local financial fact without inventing a source prefix."""

    public_id: EvidenceUUID
    content_digest: EvidenceDigest
    ledger_sequence: ProjectionSequence
    recorded_at: EvidenceInstant
    kind: Literal["contract_position_fill", "native_cash"]
    instrument_public_id: EvidenceUUID | None = None
    native_currency: EvidenceText | None = None

    @model_validator(mode="after")
    def validate_units(self) -> Self:
        """Require the actual instrument or currency appropriate for this financial fact.

        Returns:
            The same reference with mutually exclusive native dimensions.
        """
        position = self.kind == "contract_position_fill"
        if position != (self.instrument_public_id is not None) or position == (
            self.native_currency is not None
        ):
            raise ValueError("Projection journal reference has inconsistent native units")
        return self


class ObservedContractPosition(_ProjectionPayload):
    """Keep native quantity and basis distinct from modeled position economics."""

    instrument_public_id: EvidenceUUID
    latest_event_public_id: EvidenceUUID
    anchor_public_id: EvidenceUUID
    unit_snapshot_public_id: EvidenceUUID
    route_authority_public_id: EvidenceUUID
    ledger_sequence: ProjectionSequence
    quantity_unit: Literal["contract_count"] = "contract_count"
    quantity: EvidenceDecimal
    entry_price: EvidenceDecimal | None
    settlement_currency: EvidenceText
    quote_currency: EvidenceText
    modeled_realized_pnl_since_anchor: EvidenceDecimal
    modeled_quote_turnover_since_anchor: EvidenceDecimal
    basis_status: Literal["equal", "source_precision_unproven"]

    @model_validator(mode="after")
    def validate_source_basis(self) -> Self:
        """Preserve explicit flatness and positive source entry basis.

        Returns:
            The same observed position without inventing an entry price.
        """
        if self.quantity.is_zero() != (self.entry_price is None):
            raise ValueError("Projected position quantity and basis disagree")
        if self.entry_price is not None and self.entry_price <= 0:
            raise ValueError("Projected position basis must be positive")
        if self.modeled_quote_turnover_since_anchor < 0:
            raise ValueError("Projected modeled turnover cannot be negative")
        return self


class ObservedContractCash(_ProjectionPayload):
    """One latest absolute balance from an explicitly partial native currency chain."""

    native_currency: EvidenceText
    balance: EvidenceDecimal
    latest_event_public_id: EvidenceUUID
    anchor_public_id: EvidenceUUID
    route_authority_public_id: EvidenceUUID
    ledger_sequence: ProjectionSequence


class ReportedPositionCashComponents(_ProjectionPayload):
    """Retain source components without asserting a cash row or a complete pairing rule."""

    position_event_public_id: EvidenceUUID
    source_receipt_public_id: EvidenceUUID
    target_json_pointer: EvidenceText
    source_log_id: Annotated[EvidenceText, Field(pattern=r"^[1-9][0-9]*$")]
    booking_uid: EvidenceText
    execution_id: EvidenceText
    reported_collateral_currency: EvidenceText | None
    fee: EvidenceDecimal | None
    realized_pnl: EvidenceDecimal | None
    realized_funding: EvidenceDecimal | None
    liquidation_fee: EvidenceDecimal | None
    funding_rate: EvidenceDecimal | None
    conversion_spread_percentage: EvidenceDecimal | None
    cash_reconciliation_status: Literal["unproven"] = "unproven"


class ContractProjectionHold(_ProjectionPayload):
    """Expose an unresolved broad or selected-margin refusal without resolving it."""

    public_id: EvidenceUUID
    raw_receipt_public_id: EvidenceUUID
    reason: EvidenceText
    margin_account: EvidenceText | None


class ContractAccountSnapshot(_ProjectionPayload):
    """Read retained facts while inventory, ownership and cash reconciliation remain unproven."""

    scope: ExecutionAccountScope
    as_of: EvidenceInstant
    journal: tuple[ContractJournalReference, ...]
    positions: tuple[ObservedContractPosition, ...]
    native_cash: tuple[ObservedContractCash, ...]
    reported_cash_components: tuple[ReportedPositionCashComponents, ...]
    unresolved_holds: tuple[ContractProjectionHold, ...]
    inventory_status: Literal["unproven"] = "unproven"
    owner_identity_status: Literal["unproven"] = "unproven"
    execution_prefix_status: Literal["unavailable"] = "unavailable"
    readiness: Literal["held"] = "held"

    @model_validator(mode="after")
    def validate_retained_projection(self) -> Self:
        """Require unique ordered facts and complete coverage of the selected retained journal.

        Returns:
            A partial external-account view that preserves every retained position's components.
        """
        ids = [item.public_id for item in self.journal]
        sequences = [item.ledger_sequence for item in self.journal]
        if len(set(ids)) != len(ids) or sequences != sorted(set(sequences)):
            raise ValueError("Projection journal must have unique ordered identities")
        if any(item.recorded_at > self.as_of for item in self.journal):
            raise ValueError("Projection cannot expose future retained knowledge")
        self._verify_latest()
        return self

    def _verify_latest(self) -> None:
        """Require the latest projected state and all cash-component observations to have facts."""
        position_facts = {
            item.instrument_public_id: item
            for item in self.journal
            if item.kind == "contract_position_fill"
        }
        cash_facts = {
            item.native_currency: item for item in self.journal if item.kind == "native_cash"
        }
        if len({item.instrument_public_id for item in self.positions}) != len(self.positions):
            raise ValueError("Projection contains duplicate native positions")
        if len({item.native_currency for item in self.native_cash}) != len(self.native_cash):
            raise ValueError("Projection contains duplicate native currencies")
        actual_positions = {
            item.instrument_public_id: (item.latest_event_public_id, item.ledger_sequence)
            for item in self.positions
        }
        actual_cash = {
            item.native_currency: (item.latest_event_public_id, item.ledger_sequence)
            for item in self.native_cash
        }
        if actual_positions != {
            key: (item.public_id, item.ledger_sequence) for key, item in position_facts.items()
        } or actual_cash != {
            key: (item.public_id, item.ledger_sequence) for key, item in cash_facts.items()
        }:
            raise ValueError("Projection latest state does not cover its retained journal")
        reported = [item.position_event_public_id for item in self.reported_cash_components]
        expected = {
            item.public_id for item in self.journal if item.kind == "contract_position_fill"
        }
        if len(set(reported)) != len(reported) or set(reported) != expected:
            raise ValueError("Projection must retain every position's reported cash components")


def contract_account_snapshot_digest(snapshot: ContractAccountSnapshot) -> str:
    """Fingerprint retained partial evidence independently of its changing read clock.

    Args:
        snapshot: Strict source-bound snapshot built by the actual financial journal reader.

    Returns:
        A deterministic content identity, never an ordered revision or spending capability.
    """
    evidence = ContractAccountSnapshot.model_validate(snapshot)
    payload = json.dumps(
        evidence.model_dump(mode="json", exclude={"as_of"}),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(
        b"snapper:contract-account-projection:v1\x00" + payload.encode()
    ).hexdigest()
