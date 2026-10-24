"""Describe observed native cash without asserting complete account or spendable equity.

An account-log old balance can anchor the observed currency chain. It does not
prove a complete currency inventory or alignment with any local execution
prefix. Cash entries retain the source components while posting the observed
balance delta exactly once. Position economics are a separate ledger family.
"""

import hashlib
import json
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from snapper.application.portfolio.futures_account_log import FuturesAccountLogEntry
from snapper.application.portfolio.futures_account_log import native_futures_cash_leg
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID

_ANCHOR_HASH_DOMAIN = b"snapper:futures-native-cash-anchor:v1\x00"
_EVENT_HASH_DOMAIN = b"snapper:futures-native-cash-event:v1\x00"


class _ImmutableCashPayload(BaseModel):
    """Revalidate nested evidence and reject ambiguous schema discriminators."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    schema_version: Literal[1] = 1

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_schema_version(cls, value: object) -> object:
        """Reject bool and float aliases for the immutable payload schema."""
        if type(value) is not int:
            raise ValueError("Cash ledger schema version must be an integer")
        return value


class FuturesNativeCashAnchor(_ImmutableCashPayload):
    """A source-proven currency opening balance with explicitly unavailable local alignment."""

    scope: ExecutionAccountScope
    route_authority_public_id: EvidenceUUID
    source_receipt_public_id: EvidenceUUID
    anchor_kind: Literal["native_cash_old_balance"] = "native_cash_old_balance"
    source_execution_prefix_status: Literal["unavailable"] = "unavailable"
    source_execution_scope_sequence: None = None
    native_currency: EvidenceText
    balance: EvidenceDecimal
    source_entry: FuturesAccountLogEntry

    @model_validator(mode="after")
    def validate_native_anchor(self) -> Self:
        """Attach the exact old balance and currency to its actual margin account.

        Returns:
            The same immutable anchor after checking all projected source fields.
        """
        leg = native_futures_cash_leg(self.source_entry)
        if (
            self.scope.exchange != "kraken_futures"
            or self.scope.margin_account != self.source_entry.margin_account
            or self.native_currency != leg.native_currency
            or self.balance != self.source_entry.old_balance
        ):
            raise ValueError("Native cash anchor contradicts its source")
        return self


def account_log_cash_source_identity(entry: FuturesAccountLogEntry) -> str:
    """Name an exact source log, keeping booking groups distinct from individual cash legs.

    Args:
        entry: One strict selected native source entry.

    Returns:
        The opaque source-log identity within the already selected actual account scope.
    """
    evidence = FuturesAccountLogEntry.model_validate(entry)
    return f"kraken_futures:account_log:{evidence.source_log_id}"


class FuturesNativeCashEvent(_ImmutableCashPayload):
    """One native balance transition with durable source and per-account ledger coordinates."""

    scope: ExecutionAccountScope
    route_authority_public_id: EvidenceUUID
    anchor_public_id: EvidenceUUID
    raw_receipt_public_id: EvidenceUUID
    ledger_sequence: int = Field(gt=0)
    kind: Literal["native_cash"] = "native_cash"
    source_identity: EvidenceText
    native_currency: EvidenceText
    cash_before: EvidenceDecimal
    cash_after: EvidenceDecimal
    cash_delta: EvidenceDecimal
    source_entry: FuturesAccountLogEntry

    @model_validator(mode="after")
    def validate_native_event(self) -> Self:
        """Require exact native units, source identity and observed balance conservation.

        Returns:
            The same event without turning fee or funding components into extra postings.
        """
        leg = native_futures_cash_leg(self.source_entry)
        projected = (
            self.scope.exchange,
            self.scope.margin_account,
            self.source_identity,
            self.native_currency,
            self.cash_before,
            self.cash_after,
            self.cash_delta,
        )
        expected = (
            "kraken_futures",
            self.source_entry.margin_account,
            account_log_cash_source_identity(self.source_entry),
            leg.native_currency,
            self.source_entry.old_balance,
            self.source_entry.new_balance,
            leg.cash_delta,
        )
        if projected != expected:
            raise ValueError("Native cash event contradicts its source")
        return self


def canonical_native_cash_anchor(payload: FuturesNativeCashAnchor) -> str:
    """Serialize the complete immutable source anchor for durable replay.

    Args:
        payload: The validated native source anchor.

    Returns:
        Stable JSON preserving native Decimal operands and source identities.
    """
    evidence = FuturesNativeCashAnchor.model_validate(payload)
    return json.dumps(evidence.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def native_cash_anchor_digest(payload: FuturesNativeCashAnchor) -> str:
    """Hash the complete native cash anchor independently of insertion provenance.

    Args:
        payload: The exact validated source anchor.

    Returns:
        Its versioned SHA-256 content digest.
    """
    return hashlib.sha256(
        _ANCHOR_HASH_DOMAIN + canonical_native_cash_anchor(payload).encode()
    ).hexdigest()


def canonical_native_cash_event(payload: FuturesNativeCashEvent) -> str:
    """Serialize an exact cash posting without floating point conversion.

    Args:
        payload: The validated native cash posting.

    Returns:
        Stable JSON retaining source components and immutable ledger coordinates.
    """
    evidence = FuturesNativeCashEvent.model_validate(payload)
    return json.dumps(evidence.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def native_cash_event_digest(payload: FuturesNativeCashEvent) -> str:
    """Hash an immutable observed native cash transition.

    Args:
        payload: The exact validated cash posting.

    Returns:
        Its versioned SHA-256 content digest.
    """
    return hashlib.sha256(
        _EVENT_HASH_DOMAIN + canonical_native_cash_event(payload).encode()
    ).hexdigest()
