"""Read only current Kraken ticker aliases for an explicit bounded instrument set."""

import asyncio
import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

from sqlalchemy import Row
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.market_snapshot_freshness import MAX_REFRESH_TARGETS
from snapper.core.market_snapshot_refresh_contracts import RefreshAliasProof
from snapper.data.repository import _selected_kraken_ticker_aliases_query

type _AliasProjection = tuple[str, str, str, str, str | None, str, str, str, int]


@dataclass(frozen=True)
class SelectedKrakenTickerAlias:
    """One current instrument identity and its exact public ticker wire alias."""

    instrument_public_id: str
    symbol_public_id: str
    native_symbol: str
    base: str
    quote: str
    asset_type: str
    alias_public_id: str
    exchange_symbol: str
    alias_row_id: int

    @property
    def proof(self) -> RefreshAliasProof:
        """Bind the exact current alias version independently of the API scope digest.

        Returns:
            Frozen selected Kraken WebSocket alias identity and exact version evidence.
        """
        return RefreshAliasProof(
            instrument_public_id=self.instrument_public_id,
            symbol_public_id=self.symbol_public_id,
            alias_public_id=self.alias_public_id,
            alias_row_id=self.alias_row_id,
            exchange="kraken",
            channel="ws",
            exchange_symbol=self.exchange_symbol,
        )


def _selected_ids(instrument_public_ids: Sequence[str]) -> list[str]:
    """Validate explicit identities before constructing a bounded SQL query.

    Args:
        instrument_public_ids: Exact requested instrument identities.

    Returns:
        Sorted distinct identities.

    Raises:
        ValueError: If the request is empty, oversized or contains blank identities.
    """
    if not instrument_public_ids or len(instrument_public_ids) > MAX_REFRESH_TARGETS:
        raise ValueError("An explicit bounded nonempty Kraken instrument set is required")
    if any(not identity or identity.strip() != identity for identity in instrument_public_ids):
        raise ValueError("Kraken instrument identities must be nonblank and unpadded")
    return sorted(set(instrument_public_ids))


def _alias_row(row: Row[*_AliasProjection]) -> SelectedKrakenTickerAlias:
    """Project a SQL row whose quote is explicitly constrained to be non-null.

    Args:
        row: One typed selected projection.

    Returns:
        Exact current identity and alias evidence.
    """
    instrument_id, symbol_id, native, base, quote, asset_type, alias_id, wire, alias_row_id = row
    return SelectedKrakenTickerAlias(
        instrument_public_id=instrument_id,
        symbol_public_id=symbol_id,
        native_symbol=native,
        base=base,
        quote=cast(str, quote),
        asset_type=asset_type,
        alias_public_id=alias_id,
        exchange_symbol=wire,
        alias_row_id=alias_row_id,
    )


async def read_selected_kraken_ticker_aliases(
    session: AsyncSession, instrument_public_ids: Sequence[str], deadline: float
) -> tuple[SelectedKrakenTickerAlias, ...]:
    """Resolve complete current alias evidence within a caller-owned async budget.

    The caller owns the session, its database statement/lock limits and its
    transaction. Cancellation cannot establish a hard server-side time bound.

    Args:
        session: Caller-owned, already bounded database session.
        instrument_public_ids: Explicit instrument scope, never an implicit catalog.
        deadline: Absolute deadline in the running event loop's clock domain.

    Returns:
        Deterministic complete alias evidence for the requested identities.

    Raises:
        ValueError: If identities, deadline or current aliases are unusable.
        TimeoutError: If the query exhausts the caller's remaining budget.
    """
    identities = _selected_ids(instrument_public_ids)
    if not math.isfinite(deadline):
        raise ValueError("Kraken alias resolution requires a finite deadline")
    loop = asyncio.get_running_loop()
    if loop.time() >= deadline:
        raise TimeoutError("Kraken alias resolution deadline elapsed")
    async with asyncio.timeout_at(deadline):
        result = await session.execute(_selected_kraken_ticker_aliases_query(identities))
        rows = tuple(_alias_row(row) for row in result.all())
    if loop.time() >= deadline:
        raise TimeoutError("Kraken alias resolution deadline elapsed")
    if len(rows) != len(identities) or {row.instrument_public_id for row in rows} != set(
        identities
    ):
        raise ValueError("Selected Kraken instruments lack unambiguous current ticker aliases")
    return rows


async def read_selected_kraken_alias_proofs(
    session: AsyncSession, instrument_public_ids: Sequence[str], deadline: float
) -> tuple[RefreshAliasProof, ...]:
    """Read exact alias proofs through the caller's existing bounded transaction.

    The writer must hold its metadata and symbol-alias locks on this same
    session until commit. This reader does not open another session or acquire
    its own authority to write.

    Args:
        session: Existing caller-owned session with server-side query bounds.
        instrument_public_ids: Exact nonempty selected Kraken instrument identities.
        deadline: Remaining absolute running-loop deadline shared with the caller.

    Returns:
        Complete current alias-version proofs in deterministic instrument order.

    Raises:
        ValueError: If current alias evidence is incomplete or ambiguous.
        TimeoutError: If alias resolution exhausts the caller's budget.
    """
    aliases = await read_selected_kraken_ticker_aliases(session, instrument_public_ids, deadline)
    return tuple(alias.proof for alias in aliases)
