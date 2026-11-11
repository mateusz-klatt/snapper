"""Whitelisted transfer of Kraken spot aliases between catalog symbols.

The Kraken spot updater refuses an alias string that another symbol still
holds: the partial unique index on active ``(exchange, channel,
exchange_symbol)`` alias rows fails the whole run. Item C2 of the Kraken
catalog specification (proprietary
``plans/spec_2026_11_11_kraken_catalog_identity.md``) allows exactly the
transfers in :data:`KRAKEN_ALIAS_TRANSFERS`. Those are the ZCHF REST and
CCXT aliases that the mislabelled ``CHF-*`` symbols hold, plus their exact
inverses so that a rollback of C1 can move them back. Every other
cross-owner collision still aborts the run.

A transfer first closes the previous owner's row with a plain SCD2 close,
executed at once, and only then lets the ordinary destination upsert write
the new owner's row. The previous owner therefore keeps its historical row
and its alias identity, and no ``public_id`` crosses between symbols.

:class:`AliasClaims` records, for one updater transaction, which symbol each
alias string was written for. A string claimed by two symbols in one run
raises, in either order, even when the first write left its row unchanged.
"""

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Final

from loguru import logger
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.orm import Session as SyncSession

from snapper.core.types import AliasChannelEnum
from snapper.core.types import ExchangeEnum
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias


@dataclass(frozen=True)
class AliasTransfer:
    """One permitted move of a Kraken alias string between two native symbols.

    Attributes:
        channel: Alias channel of the string.
        exchange_symbol: The alias string.
        source_native: Native symbol that holds the string before the move.
        destination_native: Native symbol that holds it after the move.
    """

    channel: AliasChannelEnum
    exchange_symbol: str
    source_native: str
    destination_native: str


def _with_inverses(transfers: tuple[AliasTransfer, ...]) -> frozenset[AliasTransfer]:
    """Return the transfers together with their exact inverses.

    Args:
        transfers: The forward transfers.

    Returns:
        The forward transfers and, for each, the move back.
    """
    inverses = tuple(
        AliasTransfer(
            transfer.channel,
            transfer.exchange_symbol,
            transfer.destination_native,
            transfer.source_native,
        )
        for transfer in transfers
    )
    return frozenset(transfers + inverses)


KRAKEN_ALIAS_TRANSFERS: Final[frozenset[AliasTransfer]] = _with_inverses(
    (
        AliasTransfer(AliasChannelEnum.REST, "ZCHFUSD", "CHF-USD", "ZCHF-USD"),
        AliasTransfer(AliasChannelEnum.REST, "ZCHFCHF", "CHF-CHF", "ZCHF-CHF"),
        AliasTransfer(AliasChannelEnum.CCXT, "ZCHF/USD", "CHF-USD", "ZCHF-USD"),
        AliasTransfer(AliasChannelEnum.CCXT, "ZCHF/CHF", "CHF-CHF", "ZCHF-CHF"),
    )
)
"""The only alias transfers the Kraken spot updater performs, forward and inverse."""


@dataclass(frozen=True)
class KrakenAliasWrite:
    """One Kraken alias write of the spot updater.

    Attributes:
        symbol_public_id: Public id of the symbol the alias is written for.
        native_symbol: Native symbol of that owner.
        channel: Alias channel.
        exchange_symbol: The alias string.
        now: Timestamp of the updater transaction.
    """

    symbol_public_id: str
    native_symbol: str
    channel: AliasChannelEnum
    exchange_symbol: str
    now: datetime


@dataclass
class AliasClaims:
    """Kraken alias strings written in one updater transaction, by owner.

    Attributes:
        owners: Owner public id of each claimed ``(channel, string)``.
    """

    owners: dict[tuple[AliasChannelEnum, str], str] = field(default_factory=dict)

    def claim(self, write: KrakenAliasWrite) -> None:
        """Register the write's alias string for its owner.

        Args:
            write: The alias write about to be persisted.

        Raises:
            ValueError: When another symbol already claimed the string in this run.
        """
        key = (write.channel, write.exchange_symbol)
        owner = self.owners.setdefault(key, write.symbol_public_id)
        if owner != write.symbol_public_id:
            raise ValueError(
                f"Kraken {write.channel} alias {write.exchange_symbol!r} claimed twice in one "
                f"run, by {owner} and by {write.symbol_public_id} ({write.native_symbol})"
            )


def retire_transferred_alias(session: SyncSession, write: KrakenAliasWrite) -> bool:
    """Close the alias string on its previous owner when the move is whitelisted.

    Args:
        session: The updater transaction's session.
        write: The alias write about to be persisted.

    Returns:
        False when no other symbol holds the string; True after a whitelisted
        move closed the previous owner's row.

    Raises:
        ValueError: When another symbol holds the string and the move is not
            in :data:`KRAKEN_ALIAS_TRANSFERS`.
    """
    holder = session.execute(
        select(SymbolAlias.id, SymbolAlias.symbol_public_id).where(
            SymbolAlias.exchange == ExchangeEnum.KRAKEN,
            SymbolAlias.channel == write.channel,
            SymbolAlias.exchange_symbol == write.exchange_symbol,
            SymbolAlias.symbol_public_id != write.symbol_public_id,
            SymbolAlias.timestamp <= write.now,
            SymbolAlias.known_to > write.now,
        )
    ).first()
    if holder is None:
        return False
    alias_row_id, source_public_id = holder
    source_native = session.scalar(
        select(Symbol.native_symbol).where(
            Symbol.public_id == source_public_id,
            Symbol.timestamp <= write.now,
            Symbol.known_to > write.now,
        )
    )
    transfer = AliasTransfer(
        write.channel, write.exchange_symbol, source_native or "", write.native_symbol
    )
    if transfer not in KRAKEN_ALIAS_TRANSFERS:
        raise ValueError(
            f"Kraken {write.channel} alias {write.exchange_symbol!r} is active on "
            f"{source_native or source_public_id}; moving it to {write.native_symbol} "
            "is not a whitelisted transfer"
        )
    session.execute(
        update(SymbolAlias).where(SymbolAlias.id == alias_row_id).values(known_to=write.now)
    )
    logger.warning(
        f"Kraken {write.channel} alias {write.exchange_symbol!r} moved from {source_native} "
        f"to {write.native_symbol}"
    )
    return True
