"""Native financial evidence reusing unchanged, authority-neutral v1 arithmetic operands.

No v1 row, canonical serializer or hash domain changes. These new wrappers
require actual source/policy graph binding; constructing a strict model supplies
neither transport ownership nor current financial admission. Nested v1 operand
models are arithmetic/source codecs, not inserted legacy financial records.
"""

import hashlib
import json
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic import model_validator

from snapper.application.portfolio.contract_position_ledger import CURRENT_POSITION_DECIMAL_POLICY
from snapper.application.portfolio.contract_position_ledger import ContractPositionAnchorPayload
from snapper.application.portfolio.contract_position_ledger import ContractPositionEventPayload
from snapper.application.portfolio.futures_account_log import FuturesAccountLogEntry
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashAnchor
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashEvent
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.financial_account_owner import FinancialAccountOwnerEvidence
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_account_authority import native_account_authority_digest
from snapper.core.private_source_ownership import PrivateSourceOwnershipReference


class _FrozenNativeFinancialEvidence(BaseModel):
    """Reject unknown grant fields, implicit coercion and forged unchecked nested updates."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class NativeFinancialSourceEvidence(_FrozenNativeFinancialEvidence):
    """Exact account-log authority, actual sidecar and policy version at one posting clock.

    The sidecar's authenticated UID is independently rebound by the actual data
    reader. A provider account label is deliberately not compared with the safe
    key-check accountUid. Historical source proof grants no current spending.
    """

    recorded_at: EvidenceInstant
    authority_reference: AccountAuthorityReference
    authority: NativeAccountAuthorityPayload
    owner_evidence: FinancialAccountOwnerEvidence
    ownership_reference: PrivateSourceOwnershipReference

    @model_validator(mode="after")
    def validate_native_source(self) -> Self:
        """Require mutually consistent retained references without inventing actual source rows.

        Returns:
            The same immutable evidence awaiting all actual bound data readers.
        """
        reference = self.authority_reference
        if (
            reference.family != "native_accounts_v1"
            or reference.content_digest != native_account_authority_digest(self.authority)
            or reference.recorded_at < self.authority.native_read.source_recorded_at
        ):
            raise ValueError("Native financial authority reference contradicts its payload")
        owner = self.owner_evidence
        if (
            owner.authority_reference != reference
            or owner.decided_at != self.recorded_at
            or owner.assignment.owner_key
            != financial_account_owner_key_from_authority(self.authority)
            or owner.assignment.owner_wallet_public_id != self.authority.scope.wallet_public_id
        ):
            raise ValueError("Native financial owner decision does not belong to this posting")
        if max(reference.recorded_at, self.ownership_reference.recorded_at) > self.recorded_at:
            raise ValueError("Native financial posting precedes retained source knowledge")
        return self


class _NativeFinancialPayload(_FrozenNativeFinancialEvidence):
    """A distinct financial schema whose source identity remains independent of version or kind."""

    schema_version: Literal[2] = 2
    source: NativeFinancialSourceEvidence

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_strict_version(cls, value: object) -> object:
        """Refuse bool, float and string aliases before assigning the new financial hash domain.

        Args:
            value: Uncoerced new financial wrapper version.

        Returns:
            The same exact integer before supported-version validation.
        """
        if type(value) is not int:
            raise ValueError("Native financial version must be an integer")
        return value


def _validate_account_log_source(
    source: NativeFinancialSourceEvidence,
    scope: ExecutionAccountScope,
    entry: FuturesAccountLogEntry,
    raw_public_id: str,
) -> None:
    """Keep actual scope and child selection explicit; header labels never establish an owner UID."""
    reference = source.ownership_reference
    if (
        source.authority.scope != scope
        or entry.margin_account != scope.margin_account
        or reference.raw_receipt_public_id != raw_public_id
        or reference.target_json_pointer != entry.target_json_pointer
    ):
        raise ValueError("Native financial operands contradict the selected account-log source")


class FuturesNativeCashAnchorV2(_NativeFinancialPayload):
    """One actual native old-balance seed, still without any complete inventory or local prefix."""

    anchor_kind: Literal["native_cash_old_balance"] = "native_cash_old_balance"
    operands: FuturesNativeCashAnchor

    @model_validator(mode="after")
    def validate_native_anchor(self) -> Self:
        """Reuse exact source-conservation validation without claiming a legacy authority row.

        Returns:
            The same native source seed requiring actual history and policy binding.
        """
        value = self.operands
        _validate_account_log_source(
            self.source, value.scope, value.source_entry, value.source_receipt_public_id
        )
        if value.route_authority_public_id != self.source.authority_reference.public_id:
            raise ValueError("Native cash anchor authority copies disagree")
        return self


class FuturesNativeCashEventV2(_NativeFinancialPayload):
    """One authoritative native balance transition, never modeled fill PnL or duplicated components."""

    kind: Literal["native_cash"] = "native_cash"
    operands: FuturesNativeCashEvent

    @model_validator(mode="after")
    def validate_native_cash(self) -> Self:
        """Bind the exact observed delta to its actual raw and native account references.

        Returns:
            The same single cash fact pending actual source/claim/anchor chain verification.
        """
        value = self.operands
        _validate_account_log_source(
            self.source, value.scope, value.source_entry, value.raw_receipt_public_id
        )
        if value.route_authority_public_id != self.source.authority_reference.public_id:
            raise ValueError("Native cash event authority copies disagree")
        return self


class _NativePositionPayload(_NativeFinancialPayload):
    """Keep original fill authority/ownership separate from the later native account-log source."""

    fill_authority_reference: AccountAuthorityReference
    fill_ownership_reference: PrivateSourceOwnershipReference


def _validate_position_references(
    payload: _NativePositionPayload,
    operands: ContractPositionAnchorPayload | ContractPositionEventPayload,
    position_receipt_public_id: str,
) -> None:
    """Retain actual original fill identity without moving later log knowledge into Execution time."""
    _validate_account_log_source(
        payload.source,
        operands.scope,
        operands.source_position.source_entry,
        position_receipt_public_id,
    )
    if (
        operands.route_authority_public_id != payload.fill_authority_reference.public_id
        or operands.position_route_authority_public_id
        != payload.source.authority_reference.public_id
        or payload.fill_ownership_reference.recorded_at > payload.source.recorded_at
    ):
        raise ValueError("Native position original fill and later log references disagree")


class ContractPositionAnchorV2(_NativePositionPayload):
    """An actual old-position basis whose seed remains explicitly partial and prefix-unavailable."""

    anchor_kind: Literal["position_source_seed"] = "position_source_seed"
    operands: ContractPositionAnchorPayload

    @model_validator(mode="after")
    def validate_native_position_anchor(self) -> Self:
        """Retain the original unit witness and actual native old quantity without an invented trade.

        Returns:
            The same source seed requiring actual raw, unit, alias and sidecar data binding.
        """
        _validate_position_references(self, self.operands, self.operands.source_receipt_public_id)
        return self


class ContractPositionEventV2(_NativePositionPayload):
    """An actual fill's position-only fold with exact totals and separately proven account ownership."""

    kind: Literal["contract_position_fill"] = "contract_position_fill"
    operands: ContractPositionEventPayload

    @model_validator(mode="after")
    def validate_native_position_event(self) -> Self:
        """Reuse current position arithmetic while keeping all source clocks and references explicit.

        Returns:
            The same actual position transition, without any principal or modeled cash posting.
        """
        value = self.operands
        _validate_position_references(self, value, value.position_receipt_public_id)
        if (
            value.raw_receipt_public_id != self.fill_ownership_reference.raw_receipt_public_id
            or self.fill_authority_reference.recorded_at > value.execution_known_at
            or value.execution_known_at > self.source.recorded_at
            or value.decimal_policy != CURRENT_POSITION_DECIMAL_POLICY
        ):
            raise ValueError(
                "Native position execution clock, raw source or decimal policy disagrees"
            )
        return self


type NativeFinancialEventV2 = FuturesNativeCashEventV2 | ContractPositionEventV2
type NativeFinancialAnchorV2 = FuturesNativeCashAnchorV2 | ContractPositionAnchorV2
type NativeFinancialPayloadV2 = NativeFinancialEventV2 | NativeFinancialAnchorV2


def canonical_native_financial_payload_v2(payload: NativeFinancialPayloadV2) -> str:
    """Serialize a new wrapper without altering any nested v1 arithmetic or old record serializer.

    Args:
        payload: Complete strict source evidence and independently validated financial operands.

    Returns:
        Canonical new-schema JSON, still requiring all actual immutable row/source readers.
    """
    proven = type(payload).model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def native_financial_payload_digest_v2(payload: NativeFinancialPayloadV2) -> str:
    """Hash each new financial record kind under its own v2 domain, never the legacy v1 domains.

    Args:
        payload: Strict new financial event or partial source anchor.

    Returns:
        The complete new record digest; provider economic source uniqueness remains unchanged.
    """
    domains = {
        FuturesNativeCashAnchorV2: b"snapper:futures-native-cash-anchor:v2\x00",
        FuturesNativeCashEventV2: b"snapper:futures-native-cash-event:v2\x00",
        ContractPositionAnchorV2: b"snapper:contract-position-anchor:v2\x00",
        ContractPositionEventV2: b"snapper:contract-position-event:v2\x00",
    }
    return hashlib.sha256(
        domains[type(payload)] + canonical_native_financial_payload_v2(payload).encode()
    ).hexdigest()
