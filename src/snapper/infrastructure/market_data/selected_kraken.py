"""Prepare anonymous Kraken ticker collection from bounded current alias evidence."""

import asyncio
import math
from collections.abc import AsyncIterator
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from types import MappingProxyType

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.market_snapshot_freshness import MAX_REFRESH_TARGETS
from snapper.core.market_snapshot_refresh_contracts import RefreshAliasProof
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow
from snapper.data.selected_market_snapshot_aliases import SelectedKrakenTickerAlias
from snapper.data.selected_market_snapshot_aliases import read_selected_kraken_ticker_aliases
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTickerSchema


def _explicit_text(value: str) -> bool:
    """Recognize exact nonblank names without wildcard expansion.

    Args:
        value: One resolved identity or symbol.

    Returns:
        Whether the value is usable without normalization or inference.
    """
    return bool(value) and value.strip() == value and "*" not in value


def _selected_targets(
    targets: Sequence[MarketSnapshotRefreshTargetRow],
) -> dict[str, MarketSnapshotRefreshTargetRow]:
    """Require a bounded, unambiguous Kraken-only target set before database access.

    Args:
        targets: Required instruments selected by the refresh scope policy.

    Returns:
        Exact targets indexed by instrument identity.

    Raises:
        ValueError: If any required target is empty, malformed or unsupported.
    """
    if not targets or len(targets) > MAX_REFRESH_TARGETS:
        raise ValueError("Selected Kraken collection requires a bounded nonempty target set")
    by_id: dict[str, MarketSnapshotRefreshTargetRow] = {}
    native_ids: dict[str, str] = {}
    for target in targets:
        if target["exchange"] != "kraken" or not all(
            _explicit_text(value) for value in target.values()
        ):
            raise ValueError("Only explicit Kraken snapshot targets are supported")
        identity = target["instrument_public_id"]
        native = target["native_symbol"]
        if identity in by_id and by_id[identity] != target:
            raise ValueError("Selected Kraken instrument metadata is ambiguous")
        if native in native_ids and native_ids[native] != identity:
            raise ValueError("Selected Kraken native symbol identifies multiple instruments")
        by_id[identity] = target.copy()
        native_ids[native] = identity
    return by_id


def _tick(schema: KrakenTickerSchema, native: str) -> TickerUpdate:
    """Convert validated venue data using only a previously admitted native identity.

    Args:
        schema: Validated Kraken ticker fields.
        native: Exact selected native symbol corresponding to the wire alias.

    Returns:
        The standard typed ticker consumed by the selected snapshot collector.
    """
    return TickerUpdate(
        symbol=native,
        bid=schema.bid or 0.0,
        bid_qty=schema.bid_qty or 0.0,
        ask=schema.ask or 0.0,
        ask_qty=schema.ask_qty or 0.0,
        last=schema.last or 0.0,
        volume=schema.volume or 0.0,
        vwap=schema.vwap or 0.0,
        low=schema.low or 0.0,
        high=schema.high or 0.0,
        change=schema.change or 0.0,
        change_pct=schema.change_pct or 0.0,
    )


@dataclass(frozen=True)
class SelectedKrakenTickerCodec:
    """Immutable selected-native and wire-alias maps with no catalog fallback."""

    native_to_wire: Mapping[str, str]
    wire_to_native: Mapping[str, str]

    def to_wire_symbols(self, symbols: list[str]) -> list[str]:
        """Translate exactly the admitted native set before a connection is started.

        Args:
            symbols: Explicit selected native subscription list.

        Returns:
            Stable unique wire aliases.

        Raises:
            ValueError: If the caller changes, expands or empties the admitted scope.
        """
        if set(symbols) != set(self.native_to_wire) or len(symbols) != len(self.native_to_wire):
            raise ValueError("Kraken ticker subscription must match the complete selected scope")
        return [self.native_to_wire[native] for native in sorted(symbols)]

    def parse_ticks(self, payload: object) -> list[TickerUpdate]:
        """Keep valid selected ticks without consulting the global symbol mapper.

        Args:
            payload: Untrusted Kraken ticker message payload.

        Returns:
            Selected native ticks; malformed and unrelated rows are omitted.
        """
        if not isinstance(payload, list):
            return []
        ticks: list[TickerUpdate] = []
        for item in payload:
            try:
                schema = KrakenTickerSchema.model_validate(item)
            except ValidationError:
                continue
            native = self.wire_to_native.get(schema.symbol)
            if native is not None:
                ticks.append(_tick(schema, native))
        return ticks


def _codec(
    targets: dict[str, MarketSnapshotRefreshTargetRow],
    aliases: tuple[SelectedKrakenTickerAlias, ...],
) -> SelectedKrakenTickerCodec:
    """Bind alias evidence to the previously resolved complete instrument metadata.

    Args:
        targets: Validated selected instrument metadata.
        aliases: Complete current alias evidence returned by the bounded reader.

    Returns:
        An immutable bijection for the selected native and wire symbols.

    Raises:
        ValueError: If current metadata changed or an alias is blank or ambiguous.
    """
    native_to_wire: dict[str, str] = {}
    wire_to_native: dict[str, str] = {}
    for alias in aliases:
        target = targets[alias.instrument_public_id]
        metadata = (alias.native_symbol, alias.base, alias.quote, alias.asset_type)
        expected = (target["native_symbol"], target["base"], target["quote"], target["asset_type"])
        if metadata != expected or not _explicit_text(alias.exchange_symbol):
            raise ValueError("Selected Kraken metadata or ticker alias changed during resolution")
        if alias.native_symbol in native_to_wire or alias.exchange_symbol in wire_to_native:
            raise ValueError("Selected Kraken ticker aliases are ambiguous")
        native_to_wire[alias.native_symbol] = alias.exchange_symbol
        wire_to_native[alias.exchange_symbol] = alias.native_symbol
    return SelectedKrakenTickerCodec(
        MappingProxyType(native_to_wire), MappingProxyType(wire_to_native)
    )


class SelectedKrakenSnapshotAdapter:
    """Own one anonymous selected client; the collector bounds subscription and closure."""

    def __init__(
        self,
        client: KrakenExchangeClient,
        codec: SelectedKrakenTickerCodec,
        deadline: float,
        alias_proofs: tuple[RefreshAliasProof, ...],
    ) -> None:
        """Bind a fresh client without opening REST or WebSocket connections.

        Args:
            client: Fresh anonymous Kraken public-data client.
            codec: Complete prevalidated selected ticker mapping.
            deadline: Absolute loop deadline for this adapter's remaining lifecycle.
            alias_proofs: Exact current alias versions admitted before collection.
        """
        self._client = client
        self._codec = codec
        self.deadline = deadline
        self._alias_proofs = alias_proofs
        client.configure_ticker_symbol_codec(codec)

    @property
    def alias_proofs(self) -> tuple[RefreshAliasProof, ...]:
        """Expose immutable alias evidence for atomic write-intent and receipt binding.

        Returns:
            The complete admitted proof tuple in instrument identity order.
        """
        return self._alias_proofs

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Return the selected stream for the collector's absolute-deadline context.

        Connection establishment occurs lazily inside the existing client's
        iterator and is therefore covered by collect_selected_snapshots. The
        caller must pass that collector a deadline no later than this adapter's.

        Args:
            symbols: Complete selected native subscription list.

        Returns:
            The existing client's explicitly closable asynchronous ticker stream.

        Raises:
            TimeoutError: If the caller's lifecycle budget already elapsed.
            ValueError: If the supplied symbols differ from the admitted scope.
        """
        self._codec.to_wire_symbols(symbols)
        if asyncio.get_running_loop().time() >= self.deadline:
            raise TimeoutError("Selected Kraken subscription deadline elapsed")
        return self._client.subscribe_ticks(symbols)

    async def disconnect(self, deadline: float) -> None:
        """Close the selected client within the caller's remaining cooperative budget.

        Cancellation-resistant SDK cleanup still requires the runner's external
        process bound. A later caller deadline cannot extend the admitted budget.

        Args:
            deadline: Absolute loop deadline available for cleanup.

        Raises:
            ValueError: If the cleanup deadline is not finite.
            TimeoutError: If cleanup reaches its absolute deadline.
            RuntimeError: If there is no owning asynchronous task.
        """
        if not math.isfinite(deadline):
            raise ValueError("Selected Kraken cleanup requires a finite deadline")
        effective_deadline = min(deadline, self.deadline)
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("Selected Kraken cleanup requires an owning task")
        initial_requests = task.cancelling()
        try:
            async with asyncio.timeout_at(effective_deadline):
                await self._client.disconnect()
        finally:
            if task.cancelling() > initial_requests:
                raise asyncio.CancelledError("Selected Kraken cleanup cancelled by caller")
        if asyncio.get_running_loop().time() >= effective_deadline:
            raise TimeoutError("Selected Kraken cleanup deadline elapsed")


async def prepare_selected_kraken_adapter(
    session: AsyncSession,
    targets: Sequence[MarketSnapshotRefreshTargetRow],
    deadline: float,
) -> SelectedKrakenSnapshotAdapter:
    """Resolve aliases and prepare the sole supported public refresh adapter.

    Args:
        session: Caller-owned session with database timeouts already bounded.
        targets: Complete required Kraken targets from refresh-scope resolution.
        deadline: Absolute running-loop deadline, never a fresh duration grant.

    Returns:
        A disconnected anonymous client adapter with immutable selected mappings.

    Raises:
        ValueError: If any requested venue, identity or alias is unusable.
        TimeoutError: If alias resolution consumes the remaining budget.
    """
    by_id = _selected_targets(targets)
    aliases = await read_selected_kraken_ticker_aliases(session, sorted(by_id), deadline)
    codec = _codec(by_id, aliases)
    proofs = tuple(
        alias.proof for alias in sorted(aliases, key=lambda row: row.instrument_public_id)
    )
    return SelectedKrakenSnapshotAdapter(KrakenExchangeClient(), codec, deadline, proofs)
