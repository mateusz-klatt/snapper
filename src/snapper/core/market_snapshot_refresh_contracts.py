"""Exact write operands and version receipts for selected snapshot refresh."""

import hashlib
import json
import math
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator


class RefreshAliasProof(BaseModel):
    """Exact selected wire alias version, separate from the public scope digest."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    instrument_public_id: str
    symbol_public_id: str
    alias_public_id: str
    alias_row_id: int = Field(gt=0)
    exchange: Literal["kraken"]
    channel: Literal["ws"]
    exchange_symbol: str


class RefreshSnapshot(BaseModel):
    """One collected snapshot at its original receive time, without SCD2 bookkeeping."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    instrument_public_id: str
    timestamp: datetime
    session_id: str
    sequence_id: int
    last_price: float
    bid: float | None = None
    bid_volume: float | None = None
    ask: float | None = None
    ask_volume: float | None = None
    volume_24h: float | None = None
    vwap_24h: float | None = None
    low_24h: float | None = None
    high_24h: float | None = None
    change_24h: float | None = None
    spread: float | None = None
    spread_pct: float | None = None

    @field_validator("timestamp")
    @classmethod
    def aware_timestamp(cls, value: datetime) -> datetime:
        """Preserve the instant as UTC and refuse naive receive times.

        Args:
            value: Original receive timestamp whose timezone and instant must be retained.

        Returns:
            The same receive instant normalized to UTC, or a refusal for a naive value.
        """
        if value.utcoffset() is None:
            raise ValueError("Snapshot receive time must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("last_price")
    @classmethod
    def positive_price(cls, value: float) -> float:
        """Require usable finite positive last-price evidence.

        Args:
            value: Last-trade price evidence requiring a finite strictly positive value.

        Returns:
            The original finite positive last price.
        """
        if not math.isfinite(value) or value <= 0:
            raise ValueError("Snapshot last price must be finite and positive")
        return value

    @field_validator(
        "bid",
        "bid_volume",
        "ask",
        "ask_volume",
        "volume_24h",
        "vwap_24h",
        "low_24h",
        "high_24h",
        "change_24h",
        "spread",
        "spread_pct",
    )
    @classmethod
    def finite_optional_value(cls, value: float | None) -> float | None:
        """Refuse nonfinite optional market evidence without inventing replacements.

        Args:
            value: Optional numeric market evidence, including None for an absent field.

        Returns:
            Finite evidence or None, with equivalent signed zero normalized to 0.0.
        """
        if value is not None and not math.isfinite(value):
            raise ValueError("Snapshot numeric evidence must be finite")
        return 0.0 if value == 0.0 else value

    @property
    def payload_digest(self) -> str:
        """Bind every operand while retaining no clear financial values in the receipt.

        Returns:
            The SHA256 fingerprint of all original collection operands.
        """
        encoded = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def snapshot_batch_digest(
    scope_digest: str,
    collection_started_at: datetime,
    alias_proofs: Sequence[RefreshAliasProof],
    snapshot_digests: Sequence[tuple[str, str]],
) -> str:
    """Hash the canonical exact batch intent without requiring clear price operands.

    Args:
        scope_digest: Frozen required and excluded instrument metadata digest.
        collection_started_at: Original normalized UTC subscription start.
        alias_proofs: Exact selected wire alias versions, independent of input order.
        snapshot_digests: Instrument identities and complete operand fingerprints.

    Returns:
        The established SHA256 digest of the canonical batch payload.
    """
    payload = [
        scope_digest,
        collection_started_at.isoformat(),
        [
            proof.model_dump(mode="json")
            for proof in sorted(alias_proofs, key=lambda proof: proof.instrument_public_id)
        ],
        sorted(snapshot_digests),
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


class RefreshSnapshotBatch(BaseModel):
    """An exact frozen scope and its one-per-instrument collected operands."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    scope_digest: str
    collection_started_at: datetime
    alias_proofs: tuple[RefreshAliasProof, ...]
    snapshots: tuple[RefreshSnapshot, ...]

    @field_validator("collection_started_at")
    @classmethod
    def aware_collection_start(cls, value: datetime) -> datetime:
        """Normalize the actual subscription start without inferring a missing timezone.

        Args:
            value: Actual subscription-start timestamp with an explicit timezone.

        Returns:
            The original subscription-start instant normalized to UTC.
        """
        if value.utcoffset() is None:
            raise ValueError("Refresh collection start must be timezone-aware")
        return value.astimezone(UTC)

    @property
    def batch_digest(self) -> str:
        """Hash scope and sorted operand fingerprints independently of input order.

        Returns:
            The canonical fingerprint of scope, start time, alias proofs and operands.
        """
        return snapshot_batch_digest(
            self.scope_digest,
            self.collection_started_at,
            self.alias_proofs,
            [(row.instrument_public_id, row.payload_digest) for row in self.snapshots],
        )


class RefreshSnapshotReceipt(BaseModel):
    """Exact retained version identity and operand digest for one inserted snapshot."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    instrument_public_id: str
    row_id: int
    public_id: str
    session_id: str
    sequence_id: int = Field(gt=0)
    timestamp: datetime
    payload_digest: str

    @field_validator("timestamp")
    @classmethod
    def aware_timestamp(cls, value: datetime) -> datetime:
        """Require the original aware receive instant with the operand's UTC normalization.

        Args:
            value: Original receive timestamp whose timezone and instant must be retained.

        Returns:
            The same receive instant normalized to UTC, or a refusal for a naive value.
        """
        return RefreshSnapshot.aware_timestamp(value)


class RefreshCommitReceipt(BaseModel):
    """Exact versions returned only after the whole batch commit is acknowledged."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    run_id: str
    scope_digest: str
    batch_digest: str
    collection_started_at: datetime
    alias_proofs: tuple[RefreshAliasProof, ...]
    recorded_at: datetime
    snapshots: tuple[RefreshSnapshotReceipt, ...]
