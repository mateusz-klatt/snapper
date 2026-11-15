"""Typed values of the guarded candle void (catalog item C5).

The void closes every active candle of a voted set of instruments at one
database instant. These values travel between the operator command, its
application module and the repository, which alone performs the closure under
the candle writer fence.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime


class CandleVoidRefusedError(Exception):
    """The void refused before closing anything, for a named reason.

    Attributes:
        code: Stable reason code, for example ``cohort_changed``.
    """

    def __init__(self, code: str) -> None:
        """Record the reason code.

        Args:
            code: Stable reason code.
        """
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True, order=True)
class CandleVersionKey:
    """The full identity of one stored candle version.

    Physical ``id`` values are unique only within one daily partition leaf on
    PostgreSQL, so a version is identified by its natural key together with its
    physical id, public identity, author and bus time.

    Attributes:
        instrument_public_id: Instrument of the candle.
        timeframe: Timeframe label.
        open_at: Candle open instant.
        id: Physical row id.
        public_id: Public identity of the version.
        session_id: Authoring session.
        sequence_id: Authoring sequence number.
        timestamp: Bus time the version was written at.
    """

    instrument_public_id: str
    timeframe: str
    open_at: datetime
    id: int
    public_id: str
    session_id: str
    sequence_id: int
    timestamp: datetime


@dataclass(frozen=True, slots=True, order=True)
class CatalogExpectation:
    """The catalog state of one instrument's symbol on its exchange.

    Attributes:
        instrument_public_id: Instrument the expectation belongs to.
        capability_public_id: Public identity of the active capability version,
            or None when no capability row is active.
        capability_timestamp: Bus time of that version, or None.
        can_market_data: Its market data flag, or None.
        can_trade: Its trading flag, or None.
        active_alias_count: Active aliases of the symbol on the exchange.
    """

    instrument_public_id: str
    capability_public_id: str | None
    capability_timestamp: datetime | None
    can_market_data: bool | None
    can_trade: bool | None
    active_alias_count: int


@dataclass(frozen=True, slots=True)
class CandleVoidTarget:
    """One requested instrument, resolved for the dry run.

    Attributes:
        instrument_public_id: Instrument public identity.
        exchange: Exchange of the active instrument version.
        native_symbol: Native symbol of its active symbol version.
        catalog: Catalog state of the symbol on that exchange.
    """

    instrument_public_id: str
    exchange: str
    native_symbol: str
    catalog: CatalogExpectation


@dataclass(frozen=True, slots=True)
class CandleVoidReferences:
    """Counts the vote sees about everything the void does not change.

    Attributes:
        elections_active: Active FX election versions selecting an instrument.
        elections_historical: Closed FX election versions selecting one.
        proofs_active: Active FX proof versions sourced from an instrument.
        proofs_historical: Closed FX proof versions sourced from one.
        shadow_candles: Active shadow candles per instrument, timeframe and
            source, keyed ``instrument|timeframe|source``.
    """

    elections_active: int
    elections_historical: int
    proofs_active: int
    proofs_historical: int
    shadow_candles: dict[str, int]


@dataclass(frozen=True, slots=True)
class CandleVoidRequest:
    """Everything the repository needs to close exactly one voted set.

    Attributes:
        instrument_public_ids: Instruments whose active candles are closed.
        expected: Every active version the vote covers, and nothing else.
        catalog: The approved catalog state of each instrument.
        on_prepared: Called with the chosen ``void_at`` under the fence and
            before any row is closed; raising aborts the transaction.
    """

    instrument_public_ids: tuple[str, ...]
    expected: tuple[CandleVersionKey, ...]
    catalog: tuple[CatalogExpectation, ...]
    on_prepared: Callable[[datetime], None]


@dataclass(frozen=True, slots=True)
class CandleVersionReading:
    """What a fenced read finds of one expected version.

    Attributes:
        known_to: The stored ``known_to``, or None when the version is absent.
        active: Whether that ``known_to`` is the active sentinel.
    """

    known_to: datetime | None
    active: bool


@dataclass(frozen=True, slots=True)
class CandleVoidResult:
    """The committed closure.

    Attributes:
        void_at: The database instant every closed version now ends at.
        closed: The closed versions, sorted.
    """

    void_at: datetime
    closed: tuple[CandleVersionKey, ...]


@dataclass(frozen=True, slots=True)
class CandleRestoreCounts:
    """What one fenced candle restore batch did.

    Attributes:
        inserted: Archived versions absent from the database, now inserted.
        skipped_exact: Versions already stored with the same ``known_to``.
        skipped_known_to_differs: Versions stored with another ``known_to``.
    """

    inserted: int
    skipped_exact: int
    skipped_known_to_differs: int
