"""Control items of the trade-built candle stream, delivered beside its bars.

The Kraken spot trade-built producer puts marks into the same FIFO queue as its
bars, and the live candle loop handles each item completely before it asks for
the next one. A mark is therefore an in-band boundary: every minute it certifies
is swept, and every window that ends at it is sealed, before any later bar of the
same generation is observed. That is what lets minute completion and the
higher-timeframe flush follow the consumer instead of the wall clock.

A break replaces items the queue lost or discarded. It names the lost minutes,
starts a new generation, and gives the arming floor below which a bar may no
longer arm a carried close.

Classes:
    TradeCandleMark: An in-band completion boundary.
    TradeCandleBreakReason: Why a break happened.
    TradeCandleBreak: The record of lost or discarded items.
    LostBars: Count and minute range of bars lost before a break.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from snapper.infrastructure.exchanges.contracts import CandleUpdate


@dataclass(frozen=True, slots=True)
class TradeCandleMark:
    """An in-band boundary that certifies the evidence present at a producer tick.

    Every minute before ``through`` that had a trade of a symbol by the tick has
    had its first emission for that symbol, earlier in the queue. A trade arriving
    later for such a minute produces a retained correction, or a late first
    emission if the symbol had no trade in that minute before; both are late bars.

    Attributes:
        through: UTC minute boundary the mark certifies.
        generation: Producer generation in which the mark was enqueued.
        produced_at: UTC wall clock of the producer tick.
    """

    through: datetime
    generation: int
    produced_at: datetime


class TradeCandleBreakReason(StrEnum):
    """Why the stream lost or discarded queued items."""

    OVERFLOW = "overflow"
    """The bounded queue refused an item."""

    LAG = "lag"
    """The consumer discarded a backlog older than its lag bound."""

    RESET = "reset"
    """A feed break replaced the builder and emptied the queue."""


@dataclass(frozen=True, slots=True)
class TradeCandleBreak:
    """The record of a break: what was lost, and from where the stream restarts.

    Attributes:
        reason: Why the break happened.
        generation: The new producer generation.
        lost_bars: Bars drained from the queue or refused by it.
        lost_from: Oldest minute among the lost bars, or ``None``.
        lost_through: Newest minute among the lost bars, or ``None``.
        builder_kept: Whether the trade candle builder survived the break.
        arm_after: The producer's newest popped boundary at the break, or
            ``None`` before its first completed tick. Every bar of an earlier
            minute that existed at that tick has had its first emission, so no
            bar below it may arm a carried close after the break.
    """

    reason: TradeCandleBreakReason
    generation: int
    lost_bars: int
    lost_from: datetime | None
    lost_through: datetime | None
    builder_kept: bool
    arm_after: datetime | None


type TradeCandleItem = CandleUpdate | TradeCandleMark | TradeCandleBreak
"""One item of the trade-built candle stream."""


@dataclass(slots=True)
class LostBars:
    """Count and minute range of bars lost before a break.

    Attributes:
        count: Bars counted so far.
        oldest: Oldest minute counted, or ``None``.
        newest: Newest minute counted, or ``None``.
    """

    count: int = 0
    oldest: datetime | None = None
    newest: datetime | None = None

    def add(self, minute: datetime) -> None:
        """Count one lost bar.

        Args:
            minute: Opening of the lost bar's minute.
        """
        self.count += 1
        if self.oldest is None or minute < self.oldest:
            self.oldest = minute
        if self.newest is None or minute > self.newest:
            self.newest = minute

    def add_item(self, item: CandleUpdate | TradeCandleMark) -> None:
        """Count a lost queue item if it is a bar; a lost mark loses no evidence.

        Args:
            item: An item drained from, or refused by, the queue.
        """
        if isinstance(item, CandleUpdate):
            self.add(item.interval_begin)
