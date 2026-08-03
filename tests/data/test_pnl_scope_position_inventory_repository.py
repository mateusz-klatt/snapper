"""Tests for the Phase-5B scope position-inventory repository read (R6/A2).

Pins the read that labels the position/cash partition: the ``(wallet, mode)``
scope filter at the knowledge horizon, the base currency and venue from the
as-of-active symbol and instrument, and the fail-closed ``is_spot_margin`` proof
— ``False`` ONLY when the as-of-active spec proves a spot, non-spot-margin
instrument, and ``True`` for a missing spec, a non-spot kind, or the spot-margin
funding model.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Position
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_TS = _NOW - timedelta(hours=6)
_WINDOW_START = _NOW - timedelta(minutes=20)
_OPEN = _NOW - timedelta(minutes=10)
_WALLET = "0000face-0000-7000-8000-0000000000a1"
_OTHER_WALLET = "0000face-0000-7000-8000-0000000000b2"
_SESSION = "00000000-0000-7000-8000-000000000901"


def _symbol(public_id: str, base: str, quote: str) -> Symbol:
    """Build one active symbol carrying the base currency the read projects."""
    return Symbol(
        public_id=public_id,
        native_symbol=f"{base}-{quote}",
        base=base,
        quote=quote,
        asset_type="crypto",
        created_at=_TS,
        timestamp=_TS,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _instrument(public_id: str, symbol_public_id: str, exchange: str) -> Instrument:
    """Build one active instrument listing a symbol on a venue."""
    return Instrument(
        public_id=public_id,
        symbol_public_id=symbol_public_id,
        exchange=exchange,
        source_exchange=None,
        requires_ai_review=False,
        timestamp=_TS,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _spec(
    instrument_public_id: str,
    instrument_kind: str | None = "spot",
    funding_type: str | None = None,
) -> InstrumentSpec:
    """Build one active instrument spec proving the spot / funding classification."""
    return InstrumentSpec(
        instrument_public_id=instrument_public_id,
        instrument_kind=instrument_kind,
        funding_type=funding_type,
        timestamp=_TS,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _position(
    public_id: str,
    instrument_public_id: str,
    quantity: float,
    *,
    owner: tuple[str, str] = (_WALLET, "live"),
    window: tuple[datetime, datetime] = (_TS, KNOWN_TO_MAX),
) -> Position:
    """Build one position version for a ``(wallet, mode)`` owner and interval."""
    return Position(
        public_id=public_id,
        instrument_public_id=instrument_public_id,
        wallet_public_id=owner[0],
        mode=owner[1],
        quantity=quantity,
        average_price=None,
        unrealized_pnl=None,
        realized_pnl=0.0,
        timestamp=window[0],
        known_to=window[1],
        session_id=_SESSION,
        sequence_id=1,
    )


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated repository seeded with proven and unproven positions."""
    db_path = tmp_path / "pnl-positions.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Symbol.__table__.create(schema_engine)
    Instrument.__table__.create(schema_engine)
    InstrumentSpec.__table__.create(schema_engine)
    Position.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    async with repo.session() as s:
        s.add_all(
            [
                _symbol("sym-eur", "EUR", "USD"),
                _symbol("sym-btc", "BTC", "USD"),
                _symbol("sym-sol", "SOL", "USD"),
                _symbol("sym-xrp", "XRP", "USD"),
                _symbol("sym-ada", "ADA", "USD"),
                _symbol("sym-doge", "DOGE", "USD"),
                _instrument("ins-eur", "sym-eur", "walutomat"),
                _instrument("ins-btc", "sym-btc", "kraken"),
                _instrument("ins-sol", "sym-sol", "kraken"),
                _instrument("ins-xrp", "sym-xrp", "kraken"),
                _instrument("ins-ada", "sym-ada", "kraken"),
                _instrument("ins-doge", "sym-doge", "kraken"),
                _spec("ins-eur", instrument_kind="spot", funding_type=None),
                _spec("ins-sol", instrument_kind="spot", funding_type="spot_margin_rollover"),
                _spec("ins-xrp", instrument_kind="perpetual"),
                _position("pos-eur", "ins-eur", 20.0),
                _position("pos-btc", "ins-btc", 1.5),
                _position("pos-sol", "ins-sol", 3.0),
                _position("pos-xrp", "ins-xrp", 4.0),
                _position("pos-ada", "ins-ada", 5.0, window=(_OPEN, KNOWN_TO_MAX)),
                _position("pos-other-wallet", "ins-btc", 9.0, owner=(_OTHER_WALLET, "live")),
                _position("pos-paper", "ins-btc", 7.0, owner=(_WALLET, "paper")),
                _position("pos-closed", "ins-doge", 8.0, window=(_TS, _WINDOW_START)),
                _position(
                    "pos-future", "ins-doge", 6.0, window=(_NOW + timedelta(hours=1), KNOWN_TO_MAX)
                ),
            ]
        )
        await s.commit()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _inventory(repo: SQLAlchemyRepository) -> dict[str, dict[str, object]]:
    """Return the window position inventory keyed by base currency for assertions."""
    rows = await repo.get_pnl_scope_position_inventory_window(_WALLET, "live", _WINDOW_START, _NOW)
    return {row["base_currency"]: dict(row) for row in rows}


class TestScopePositionInventoryWindow:
    """The temporal spot-labelling inventory read and its fail-closed spec proof."""

    async def test_proven_spot_is_not_margin_with_interval(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A spec-proven spot, non-margin version carries its knowledge interval."""
        eur = (await _inventory(repository))["EUR"]
        assert eur["exchange"] == "walutomat"
        assert eur["quantity"] == pytest.approx(20.0)
        assert eur["is_spot_margin"] is False
        assert eur["valid_from"] == _TS
        assert eur["valid_to"] == KNOWN_TO_MAX

    async def test_missing_spec_fails_closed_to_margin(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A position whose instrument has no spec is not proven spot."""
        assert (await _inventory(repository))["BTC"]["is_spot_margin"] is True

    async def test_spot_margin_rollover_is_margin(self, repository: SQLAlchemyRepository) -> None:
        """The spot-margin funding model surfaces as leveraged inventory."""
        assert (await _inventory(repository))["SOL"]["is_spot_margin"] is True

    async def test_non_spot_kind_is_margin(self, repository: SQLAlchemyRepository) -> None:
        """A non-spot instrument kind is never proven spot."""
        assert (await _inventory(repository))["XRP"]["is_spot_margin"] is True

    async def test_later_opened_version_reports_its_valid_from(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """A version that opened mid-window reports its later ``valid_from`` (A2)."""
        assert (await _inventory(repository))["ADA"]["valid_from"] == _OPEN

    async def test_scope_excludes_other_wallet_mode_and_out_of_window(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """The read returns only in-window live versions of the requested wallet."""
        inventory = await _inventory(repository)
        assert set(inventory) == {"EUR", "BTC", "SOL", "XRP", "ADA"}

    async def test_empty_scope_returns_empty(self, repository: SQLAlchemyRepository) -> None:
        """A wallet with no positions returns an empty inventory."""
        assert (
            await repository.get_pnl_scope_position_inventory_window(
                "no-such-wallet", "live", _WINDOW_START, _NOW
            )
            == []
        )
