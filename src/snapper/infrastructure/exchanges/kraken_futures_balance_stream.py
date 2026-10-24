"""Replay retained Futures native quantities within a bounded physical source epoch.

The cache produces auditable evidence candidates, never margin-account aliases,
spendable balances or execution-prefix alignment. Financial callers must reload
the immutable receipt chain and replay it before using the candidate.
"""

import base64
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Literal
from typing import cast
from uuid import UUID

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import is_execution_control_message
from snapper.infrastructure.exchanges.native_balance_source import NATIVE_BALANCE_RECEIPT_LIMIT
from snapper.infrastructure.exchanges.native_balance_source import NativeBalanceProfile
from snapper.infrastructure.exchanges.native_balance_source import NativeBalanceProfileObservation
from snapper.infrastructure.exchanges.native_balance_source import NativeBalanceStreamProof
from snapper.infrastructure.exchanges.native_balance_source import NativeCurrencyQuantity

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
type _ProfileState = dict[NativeBalanceProfile, dict[str, Decimal]]


class NativeBalanceStreamError(ValueError):
    """Retained bytes cannot establish this native source chain or profile inventory."""


@dataclass(frozen=True)
class CapturedNativeBalanceFrame:
    """One committed whole-frame receipt paired with its actual local transport selection."""

    observation: PrivateExecutionObservation
    receipt_public_id: str
    client_selection: ExecutionClientSelection
    connection_epoch_public_id: str


@dataclass(frozen=True)
class _DecodedFrame:
    """Strict source metadata and exact numeric fields decoded from retained original bytes."""

    source: CapturedNativeBalanceFrame
    message: Mapping[str, object]
    snapshot: bool
    account: str
    sequence: int
    source_timestamp: datetime


def futures_balance_channel(
    feed: Literal["balances", "balances_snapshot"], epoch_public_id: str
) -> str:
    """Name the actual physical source epoch without changing its provider payload.

    Args:
        feed: Actual provider balance feed discriminant.
        epoch_public_id: Canonical UUID attached by the physical receive boundary.

    Returns:
        Versioned local observation channel retaining the actual source feed.

    Raises:
        NativeBalanceStreamError: If the local physical epoch is not canonical.
    """
    try:
        canonical = str(UUID(epoch_public_id))
    except ValueError as error:
        raise NativeBalanceStreamError("native_balance_epoch_noncanonical") from error
    if canonical != epoch_public_id:
        raise NativeBalanceStreamError("native_balance_epoch_noncanonical")
    return f"kraken_futures.ws.{feed}.v1/{epoch_public_id}"


def _object(value: object) -> Mapping[str, object]:
    """Refuse malformed financial maps without converting or dropping their retained raw bytes."""
    if not isinstance(value, dict):
        raise NativeBalanceStreamError("native_balance_object_required")
    return cast(dict[str, object], value)


def _text(value: object) -> str:
    """Preserve a verbatim nonblank source identity rather than normalizing a margin label."""
    if not isinstance(value, str) or not value.strip():
        raise NativeBalanceStreamError("native_balance_identity_missing")
    return value


def _quantity(value: object) -> Decimal:
    """Accept exact JSON integer/Decimal quantities without promoting floats or booleans."""
    if isinstance(value, bool) or not isinstance(value, int | Decimal):
        raise NativeBalanceStreamError("native_balance_exact_quantity_required")
    amount = Decimal(value)
    if not amount.is_finite():
        raise NativeBalanceStreamError("native_balance_quantity_nonfinite")
    return amount


def _integer(value: object) -> int:
    """Require an actual nonnegative source integer without bool or float aliases."""
    if type(value) is not int or value < 0:
        raise NativeBalanceStreamError("native_balance_source_integer_required")
    return value


def _decode(source: CapturedNativeBalanceFrame) -> _DecodedFrame:
    """Read source metadata only from original duplicate-key-checked bytes."""
    observation = source.observation
    if observation.representation != "wire_bytes_base64":
        raise NativeBalanceStreamError("native_balance_original_wire_required")
    message = decode_private_execution_receipt(
        base64.b64decode(observation.material, validate=True)
    )
    feed = message.get("feed")
    if feed == "balances":
        expected = futures_balance_channel("balances", source.connection_epoch_public_id)
    elif feed == "balances_snapshot":
        expected = futures_balance_channel("balances_snapshot", source.connection_epoch_public_id)
    else:
        raise NativeBalanceStreamError("native_balance_feed_unproven")
    if is_execution_control_message(message):
        raise NativeBalanceStreamError("native_balance_feed_unproven")
    if (
        observation.context.kind != "account_cash"
        or observation.context.channel != expected
        or observation.context.target_json_pointer is not None
    ):
        raise NativeBalanceStreamError("native_balance_receipt_context_mismatch")
    return _DecodedFrame(
        source=source,
        message=message,
        snapshot=feed == "balances_snapshot",
        account=_text(message.get("account")),
        sequence=_integer(message.get("seq")),
        source_timestamp=_EPOCH + timedelta(milliseconds=_integer(message.get("timestamp"))),
    )


def _holding(state: _ProfileState, values: object, snapshot: bool) -> None:
    """Replace reported native holding quantities while preserving omitted delta currencies."""
    profile = NativeBalanceProfile(kind="holding")
    if profile not in state and not snapshot:
        raise NativeBalanceStreamError("native_balance_profile_requires_snapshot")
    quantities = state.setdefault(profile, {})
    for currency, quantity in _object(values).items():
        quantities[_text(currency)] = _quantity(quantity)


def _single(state: _ProfileState, values: object, snapshot: bool) -> None:
    """Keep each single-collateral profile's exact source key, metadata and settlement balance."""
    for key, item in _object(values).items():
        raw = _object(item)
        prior = next((profile for profile in state if profile.source_key == key), None)
        if prior is None:
            if not snapshot:
                raise NativeBalanceStreamError("native_balance_profile_requires_snapshot")
            profile = NativeBalanceProfile(
                kind="single_collateral",
                source_key=_text(key),
                source_name=_text(raw.get("name")),
                source_pair=_text(raw.get("pair")),
                settlement_unit=_text(raw.get("unit")),
            )
            state[profile] = {_text(raw.get("unit")): _quantity(raw.get("balance"))}
        else:
            expected = {
                "name": prior.source_name,
                "pair": prior.source_pair,
                "unit": prior.settlement_unit,
            }
            if any(field in raw and raw[field] != value for field, value in expected.items()):
                raise NativeBalanceStreamError("native_balance_profile_identity_changed")
            if "balance" in raw:
                state[prior][_text(prior.settlement_unit)] = _quantity(raw["balance"])


def _multi(state: _ProfileState, values: object, snapshot: bool) -> None:
    """Read native multi-collateral quantities independently of USD valuation fields."""
    profile = NativeBalanceProfile(kind="multi_collateral")
    raw = _object(values)
    if profile not in state:
        if not snapshot or "currencies" not in raw:
            raise NativeBalanceStreamError("native_balance_profile_requires_snapshot")
        state[profile] = {}
    quantities = state[profile]
    if "currencies" not in raw:
        return
    for currency, item in _object(raw["currencies"]).items():
        entry = _object(item)
        asset = _text(currency)
        if "quantity" in entry:
            quantities[asset] = _quantity(entry["quantity"])
        elif asset not in quantities:
            raise NativeBalanceStreamError("native_balance_new_currency_quantity_missing")


def _fold(state: _ProfileState, frame: _DecodedFrame) -> _ProfileState:
    """Build a replacement candidate without exposing a partly applied failed delta."""
    candidate = (
        {} if frame.snapshot else {profile: dict(values) for profile, values in state.items()}
    )
    for field, apply in (("holding", _holding), ("futures", _single), ("flex_futures", _multi)):
        if field in frame.message:
            apply(candidate, frame.message[field], frame.snapshot)
    if not candidate:
        raise NativeBalanceStreamError("native_balance_no_profile_snapshot")
    return candidate


def _profile_observations(
    state: _ProfileState, proof: NativeBalanceStreamProof
) -> tuple[NativeBalanceProfileObservation, ...]:
    """Validate complete exact immutable candidates before replacing the cache's prior state."""
    return tuple(
        NativeBalanceProfileObservation(
            profile=profile,
            quantities=tuple(
                NativeCurrencyQuantity(currency=currency, quantity=values[currency])
                for currency in sorted(values)
            ),
            source_proof=proof,
        )
        for profile, values in state.items()
    )


class KrakenFuturesBalanceStream:
    """Cache one strict native source chain while keeping all financial authority elsewhere."""

    def __init__(self, *, max_receipts: int = NATIVE_BALANCE_RECEIPT_LIMIT) -> None:
        """Start with no observed profile, source epoch or spendable balance.

        Args:
            max_receipts: Bounded replay budget, never exceeding the versioned source policy.
        """
        if type(max_receipts) is not int or not 1 <= max_receipts <= NATIVE_BALANCE_RECEIPT_LIMIT:
            raise NativeBalanceStreamError("native_balance_chain_bound_invalid")
        self._max_receipts = max_receipts
        self._state: _ProfileState = {}
        self._proof: NativeBalanceStreamProof | None = None

    def clear(self) -> None:
        """Invalidate all candidates on explicit source lifecycle changes."""
        self._state = {}
        self._proof = None

    def observe(self, source: CapturedNativeBalanceFrame) -> None:
        """Apply one committed source frame or invalidate the entire candidate on failure.

        Args:
            source: Exact durable receipt plus actual transport-bound source identity.

        Raises:
            ValueError: If source bytes or sequence metadata cannot certify this candidate.
        """
        try:
            frame = _decode(source)
            self._check_continuity(frame)
            state = _fold(self._state, frame)
            prior = None if frame.snapshot else self._proof
            proof = NativeBalanceStreamProof(
                connection_epoch_public_id=source.connection_epoch_public_id,
                client_selection=source.client_selection,
                receipt_public_ids=(
                    (source.receipt_public_id,)
                    if prior is None
                    else (*prior.receipt_public_ids, source.receipt_public_id)
                ),
                venue_account_id=frame.account,
                snapshot_sequence=frame.sequence if prior is None else prior.snapshot_sequence,
                tip_sequence=frame.sequence,
                observed_at=source.observation.context.observed_at,
                venue_timestamp=frame.source_timestamp,
            )
            _profile_observations(state, proof)
            self._state, self._proof = state, proof
        except ArithmeticError as error:
            self.clear()
            raise NativeBalanceStreamError("native_balance_source_arithmetic_invalid") from error
        except ValueError:
            self.clear()
            raise

    def _check_continuity(self, frame: _DecodedFrame) -> None:
        """Never splice missing snapshots, physical epochs, account identities or source clocks."""
        prior = self._proof
        source = frame.source
        if prior is not None and (
            source.observation.context.observed_at < prior.observed_at
            or frame.source_timestamp < prior.venue_timestamp
        ):
            raise NativeBalanceStreamError("native_balance_source_clock_regressed")
        if frame.snapshot:
            return
        if prior is None:
            raise NativeBalanceStreamError("native_balance_snapshot_required")
        if (
            prior.connection_epoch_public_id != source.connection_epoch_public_id
            or prior.client_selection != source.client_selection
            or prior.venue_account_id != frame.account
            or prior.tip_sequence + 1 != frame.sequence
            or source.receipt_public_id in prior.receipt_public_ids
            or len(prior.receipt_public_ids) >= self._max_receipts
        ):
            raise NativeBalanceStreamError("native_balance_source_chain_broken")

    def profiles(self) -> tuple[NativeBalanceProfileObservation, ...]:
        """Return immutable profile candidates while preserving the actual tip clock.

        Returns:
            Per-profile quantities and source proof, empty before a verified snapshot.
        """
        if self._proof is None:
            return ()
        return _profile_observations(self._state, self._proof)
