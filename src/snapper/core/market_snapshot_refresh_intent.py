"""Immutable price-free durable intent exchanged between refresh work and its journal."""

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from pydantic import ConfigDict

from snapper.core.market_snapshot_refresh_contracts import RefreshAliasProof
from snapper.core.market_snapshot_refresh_scope import RefreshTarget


@dataclass(frozen=True, slots=True)
class RefreshIntentSnapshot:
    """Price-free identity and original provenance for one pending operand."""

    __pydantic_config__: ClassVar[ConfigDict] = ConfigDict(extra="forbid", strict=True)

    instrument_public_id: str
    session_id: str
    sequence_id: int
    received_at: datetime
    payload_digest: str


@dataclass(frozen=True, slots=True)
class RefreshWriteIntent:
    """Durable price-free intent that must be acknowledged before persistence."""

    __pydantic_config__: ClassVar[ConfigDict] = ConfigDict(extra="forbid", strict=True)

    host_invocation_id: str
    run_id: str
    scope_digest: str
    batch_digest: str
    collection_started_at: datetime
    requested_instrument_public_ids: tuple[str, ...]
    configured_fx_exchanges: tuple[str, ...]
    required: tuple[RefreshTarget, ...]
    excluded: tuple[RefreshTarget, ...]
    alias_proofs: tuple[RefreshAliasProof, ...]
    snapshots: tuple[RefreshIntentSnapshot, ...]
