"""Pure explicit refresh-scope construction shared by observation and persistence."""

import hashlib
import json
from collections.abc import Sequence
from typing import TypedDict
from uuid import UUID

from pydantic import BaseModel
from pydantic import ConfigDict

from snapper.core.market_snapshot_freshness import MAX_REFRESH_TARGETS

MAX_REQUESTED_INSTRUMENTS = 100
SNAPSHOT_REFRESH_EXCHANGES = frozenset({"kraken", "kraken_futures", "kraken_equities", "walutomat"})


class RefreshTargetMetadata(TypedDict):
    """Exact current instrument metadata used in the established scope digest."""

    instrument_public_id: str
    native_symbol: str
    exchange: str
    base: str
    quote: str
    asset_type: str


class RefreshTarget(BaseModel):
    """Immutable resolved metadata, retaining exact source identity spellings."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    instrument_public_id: str
    native_symbol: str
    exchange: str
    base: str
    quote: str
    asset_type: str

    def metadata(self) -> RefreshTargetMetadata:
        """Return the six established metadata keys without serialization additions.

        Returns:
            The six existing resolver metadata fields with their exact source spellings.
        """
        return {
            "instrument_public_id": self.instrument_public_id,
            "native_symbol": self.native_symbol,
            "exchange": self.exchange,
            "base": self.base,
            "quote": self.quote,
            "asset_type": self.asset_type,
        }


def normalize_refresh_instrument_ids(values: Sequence[str]) -> tuple[str, ...]:
    """Require bounded UUID inputs and deduplicate their canonical spellings.

    Args:
        values: Explicit bounded instrument UUID strings, including accepted UUID spelling aliases.

    Returns:
        Canonical distinct UUID strings in first-occurrence order.
    """
    if not values or len(values) > MAX_REQUESTED_INSTRUMENTS:
        raise ValueError(f"Request between 1 and {MAX_REQUESTED_INSTRUMENTS} instruments")
    if any(not value or value.strip() != value for value in values):
        raise ValueError(
            "Instrument identities must be nonempty and have no surrounding whitespace"
        )
    try:
        canonical = [str(UUID(value)) for value in values]
    except ValueError as exc:
        raise ValueError("Instrument identities must be valid UUIDs") from exc
    return tuple(dict.fromkeys(canonical))


def normalize_refresh_fx_exchanges(values: Sequence[str]) -> tuple[str, ...]:
    """Require an explicit bounded selection of supported FX refresh venues.

    Args:
        values: Explicit bounded supported FX venue names; an empty selection is invalid.

    Returns:
        Distinct explicitly selected venue names in first-occurrence order.
    """
    if not values or len(values) > MAX_REQUESTED_INSTRUMENTS:
        raise ValueError("An explicit bounded nonempty FX exchange list is required")
    if any(value not in SNAPSHOT_REFRESH_EXCHANGES for value in values):
        raise ValueError("FX exchanges must name supported snapshot refresh venues")
    return tuple(dict.fromkeys(values))


class FrozenRefreshScope(BaseModel):
    """Resolved required and excluded metadata with the existing byte-stable digest."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    requested_instrument_public_ids: tuple[str, ...]
    configured_fx_exchanges: tuple[str, ...]
    required: tuple[RefreshTarget, ...]
    excluded: tuple[RefreshTarget, ...]

    @property
    def scope_digest(self) -> str:
        """Hash exactly the payload keys and canonical JSON used by the freshness API.

        Returns:
            The established SHA256 scope digest, independent of input ordering.
        """
        payload = {
            "requested_instrument_public_ids": sorted(set(self.requested_instrument_public_ids)),
            "configured_fx_exchanges": sorted(set(self.configured_fx_exchanges)),
            "required_instruments": [
                target.metadata() for target in sorted(self.required, key=_target_identity)
            ],
            "excluded_fx_instruments": [
                target.metadata() for target in sorted(self.excluded, key=_target_identity)
            ],
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @property
    def required_instrument_ids(self) -> tuple[str, ...]:
        """Return the exact sorted required snapshot identities.

        Returns:
            All required instrument identities in deterministic sorted order.
        """
        return tuple(sorted(target.instrument_public_id for target in self.required))

    @property
    def unresolved_fx_currencies(self) -> tuple[str, ...]:
        """Find requested non-USD quotes without a configured direct or inverse FX target.

        Returns:
            Sorted requested quote currencies without a configured USD conversion target.
        """
        requested = set(self.requested_instrument_public_ids)
        currencies = {row.quote for row in self.required if row.instrument_public_id in requested}
        return tuple(
            sorted(currency for currency in currencies - {"USD"} if not self._has_fx(currency))
        )

    def _has_fx(self, currency: str) -> bool:
        """Require a configured forex identity rather than relying on native-symbol spelling.

        Args:
            currency: Requested non-USD quote currency whose conversion dependency must be resolved.

        Returns:
            Whether a configured forex target supplies the direct or inverse USD pair.
        """
        return any(
            row.asset_type == "forex"
            and row.exchange in self.configured_fx_exchanges
            and (row.base, row.quote) in {(currency, "USD"), ("USD", currency)}
            for row in self.required
        )


def _target_identity(target: RefreshTarget) -> str:
    """Select the stable ordering key used by the established scope digest.

    Args:
        target: One resolved instrument and its original metadata spellings.

    Returns:
        The exact public instrument identity used for stable sorting.
    """
    return target.instrument_public_id


def _validated_targets(
    requested: tuple[str, ...], targets: Sequence[RefreshTargetMetadata]
) -> dict[str, RefreshTarget]:
    """Reject incomplete or ambiguous metadata before required/excluded classification.

    Args:
        requested: Canonical explicitly requested instrument identities.
        targets: Complete resolved metadata, including required and excluded FX candidates.

    Returns:
        Validated complete target metadata indexed by unique instrument identity.
    """
    if len(targets) > MAX_REFRESH_TARGETS:
        raise ValueError(f"Expanded refresh scope exceeds {MAX_REFRESH_TARGETS} instruments")
    rows = [RefreshTarget(**target) for target in targets]
    by_id = {target.instrument_public_id: target for target in rows}
    if len(by_id) != len(rows):
        raise ValueError("Refresh resolution returned duplicate instrument identities")
    if any(not _explicit_target(target) for target in rows):
        raise ValueError("Refresh resolution returned incomplete instrument metadata")
    missing = sorted(set(requested).difference(by_id))
    if missing:
        raise ValueError(f"Unknown or unsupported requested instruments: {', '.join(missing)}")
    if any(by_id[identity].exchange not in SNAPSHOT_REFRESH_EXCHANGES for identity in requested):
        raise ValueError("Unsupported requested instrument venues")
    return by_id


def _explicit_target(target: RefreshTarget) -> bool:
    """Match the selected collector's exact nonblank, unpadded, nonwildcard identities.

    Args:
        target: One resolved instrument and its original metadata spellings.

    Returns:
        Whether identity, native symbol and currencies are explicit and unpadded.
    """
    values = (target.instrument_public_id, target.native_symbol, target.base, target.quote)
    return (
        all(value and value == value.strip() for value in values)
        and "*" not in target.native_symbol
    )


def freeze_refresh_scope(
    requested_instrument_public_ids: Sequence[str],
    configured_fx_exchanges: Sequence[str],
    targets: Sequence[RefreshTargetMetadata],
) -> FrozenRefreshScope:
    """Resolve one complete explicit scope without silently discarding excluded FX metadata.

    Args:
        requested_instrument_public_ids: Explicit main instrument UUIDs, with no implicit universe.
        configured_fx_exchanges: Explicit supported venues whose FX candidates must be refreshed.
        targets: Complete resolved metadata, including required and excluded FX candidates.

    Returns:
        An immutable complete scope with separately retained excluded FX candidates.
    """
    requested = normalize_refresh_instrument_ids(requested_instrument_public_ids)
    configured = normalize_refresh_fx_exchanges(configured_fx_exchanges)
    by_id = _validated_targets(requested, targets)
    required: list[RefreshTarget] = []
    excluded: list[RefreshTarget] = []
    for identity in sorted(by_id):
        target = by_id[identity]
        if identity in requested or target.exchange in configured:
            required.append(target)
        else:
            excluded.append(target)
    return FrozenRefreshScope(
        requested_instrument_public_ids=requested,
        configured_fx_exchanges=configured,
        required=tuple(required),
        excluded=tuple(excluded),
    )
