"""Source→paper instrument identity mapping (PnL Phase 1).

Pins ``ensure_instrument``'s ``source_exchange`` authoring semantics
(create-with-source, idempotence, SCD2 revision preserving identity and
``requires_ai_review``, None-never-clears) and the clock-free
``resolve_source_instrument_public_id`` resolver.
"""

from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id


async def _repo_with_symbol(tmp_path: Path, name: str) -> tuple[SQLAlchemyRepository, str]:
    """Create a repo seeded with one BTC-USD symbol row.

    Args:
        tmp_path: Pytest temporary directory.
        name: Database file name.

    Returns:
        Tuple of repository and the symbol's public_id.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / name}")
    await repo.create_all()
    now = datetime.now(UTC)
    async with repo.session() as s:
        s.add(
            Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=now,
                timestamp=now,
                session_id="seed",
                sequence_id=1,
            )
        )
        await s.commit()
    spid = await resolve_symbol_public_id(repo, "BTC-USD", as_of=now)
    assert spid is not None
    return repo, spid


async def _active_instrument(repo: SQLAlchemyRepository, public_id: str) -> Instrument:
    """Load the single active version of an instrument.

    Args:
        repo: Repository owning the session factory.
        public_id: Stable instrument identity.

    Returns:
        The active ORM row.
    """
    async with repo.session() as s:
        result = await s.execute(
            select(Instrument).where(
                Instrument.public_id == public_id,
                Instrument.known_to == KNOWN_TO_MAX,
            )
        )
        return result.scalars().one()


@pytest.mark.asyncio
async def test_ensure_instrument_creates_with_source_mapping(tmp_path: Path) -> None:
    """A new paper instrument records its source venue at creation.

    Given: no paper instrument for the symbol,
    When: ``ensure_instrument`` runs with ``source_exchange='kraken'``,
    Then: the created row carries the mapping.
    """
    repo, spid = await _repo_with_symbol(tmp_path, "src_create.db")
    now = datetime.now(UTC)
    _, pid = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="paper",
        session_id="s1",
        sequence_id=1,
        timestamp=now,
        source_exchange="kraken",
    )
    row = await _active_instrument(repo, pid)
    assert row.source_exchange == "kraken"


@pytest.mark.asyncio
async def test_ensure_instrument_same_source_is_idempotent(tmp_path: Path) -> None:
    """Re-ensuring with the SAME source never versions the row.

    Given: a paper instrument already mapped to kraken,
    When: ``ensure_instrument`` runs again with the same source,
    Then: the same row id returns and no SCD2 successor is created.
    """
    repo, spid = await _repo_with_symbol(tmp_path, "src_idem.db")
    now = datetime.now(UTC)
    first_id, pid = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="paper",
        session_id="s1",
        sequence_id=1,
        timestamp=now,
        source_exchange="kraken",
    )
    second_id, second_pid = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="paper",
        session_id="s1",
        sequence_id=2,
        timestamp=now,
        source_exchange="kraken",
    )
    assert (first_id, pid) == (second_id, second_pid)


@pytest.mark.asyncio
async def test_ensure_instrument_none_never_clears_mapping(tmp_path: Path) -> None:
    """A source-less caller cannot erase an authored mapping.

    Given: a paper instrument mapped to kraken,
    When: ``ensure_instrument`` runs with ``source_exchange=None``
        (the trader path, which doesn't know the source),
    Then: the mapping stays intact and the row is not versioned.
    """
    repo, spid = await _repo_with_symbol(tmp_path, "src_keep.db")
    now = datetime.now(UTC)
    first_id, pid = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="paper",
        session_id="s1",
        sequence_id=1,
        timestamp=now,
        source_exchange="kraken",
    )
    second_id, _ = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="paper",
        session_id="s1",
        sequence_id=2,
        timestamp=now,
    )
    assert second_id == first_id
    row = await _active_instrument(repo, pid)
    assert row.source_exchange == "kraken"


@pytest.mark.asyncio
async def test_ensure_instrument_revises_on_source_change(tmp_path: Path) -> None:
    """A CHANGED source revises the row via SCD2, preserving identity.

    Given: a paper instrument mapped to kraken whose active row has
        ``requires_ai_review=True``,
    When: ``ensure_instrument`` runs with ``source_exchange='walutomat'``,
    Then: the active row carries the new mapping under the SAME
        public_id with ``requires_ai_review`` preserved, and the old
        version is closed (two total versions).
    """
    repo, spid = await _repo_with_symbol(tmp_path, "src_revise.db")
    now = datetime.now(UTC)
    _, pid = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="paper",
        session_id="s1",
        sequence_id=1,
        timestamp=now,
        source_exchange="kraken",
    )
    async with repo.session() as s:
        await s.execute(
            update(Instrument)
            .where(Instrument.public_id == pid, Instrument.known_to == KNOWN_TO_MAX)
            .values(requires_ai_review=True)
        )
        await s.commit()
    await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="paper",
        session_id="s1",
        sequence_id=2,
        timestamp=datetime.now(UTC),
        source_exchange="walutomat",
    )
    row = await _active_instrument(repo, pid)
    assert row.source_exchange == "walutomat"
    assert row.requires_ai_review is True
    async with repo.session() as s:
        versions = (
            (await s.execute(select(Instrument).where(Instrument.public_id == pid))).scalars().all()
        )
    assert len(versions) == 2


@pytest.mark.asyncio
async def test_resolver_maps_paper_to_source_instrument(tmp_path: Path) -> None:
    """A mapped paper instrument resolves to its source instrument.

    Given: a kraken instrument and a paper instrument sharing the
        symbol, the paper row mapped via ``source_exchange='kraken'``,
    When: the resolver runs with the PAPER public_id,
    Then: the KRAKEN instrument's public_id returns.
    """
    repo, spid = await _repo_with_symbol(tmp_path, "src_resolve.db")
    now = datetime.now(UTC)
    _, kraken_pid = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="kraken",
        session_id="s1",
        sequence_id=1,
        timestamp=now,
    )
    _, paper_pid = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="paper",
        session_id="s1",
        sequence_id=2,
        timestamp=now,
        source_exchange="kraken",
    )
    resolved = await repo.resolve_source_instrument_public_id(paper_pid)
    assert resolved == {
        "valuation_public_id": kraken_pid,
        "is_paper": True,
        "mapped": True,
    }


@pytest.mark.asyncio
async def test_resolver_echoes_non_paper_and_unmapped(tmp_path: Path) -> None:
    """Non-paper, unmapped-paper, and unknown identities echo back.

    Given: a kraken instrument, an unmapped paper instrument, and a
        fabricated public_id,
    When: the resolver runs on each,
    Then: every call returns its input unchanged.
    """
    repo, spid = await _repo_with_symbol(tmp_path, "src_echo.db")
    now = datetime.now(UTC)
    _, kraken_pid = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="kraken",
        session_id="s1",
        sequence_id=1,
        timestamp=now,
    )
    _, paper_pid = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="paper",
        session_id="s1",
        sequence_id=2,
        timestamp=now,
    )
    kraken_res = await repo.resolve_source_instrument_public_id(kraken_pid)
    assert kraken_res == {
        "valuation_public_id": kraken_pid,
        "is_paper": False,
        "mapped": False,
    }
    paper_res = await repo.resolve_source_instrument_public_id(paper_pid)
    assert paper_res == {
        "valuation_public_id": paper_pid,
        "is_paper": True,
        "mapped": False,
    }
    ghost = "00000000-0000-7000-8000-00000000beef"
    ghost_res = await repo.resolve_source_instrument_public_id(ghost)
    assert ghost_res == {
        "valuation_public_id": ghost,
        "is_paper": False,
        "mapped": False,
    }


@pytest.mark.asyncio
async def test_resolver_echoes_when_source_instrument_missing(tmp_path: Path) -> None:
    """A mapping to a source venue with NO instrument row echoes back.

    Given: a paper instrument mapped to walutomat but no walutomat
        instrument for the symbol,
    When: the resolver runs,
    Then: the paper public_id echoes back (the caller's oracle then
        fails closed on pricing, never mis-keys).
    """
    repo, spid = await _repo_with_symbol(tmp_path, "src_ghost.db")
    now = datetime.now(UTC)
    _, paper_pid = await repo.ensure_instrument(
        symbol_public_id=spid,
        exchange="paper",
        session_id="s1",
        sequence_id=1,
        timestamp=now,
        source_exchange="walutomat",
    )
    ghost_mapped = await repo.resolve_source_instrument_public_id(paper_pid)
    assert ghost_mapped == {
        "valuation_public_id": paper_pid,
        "is_paper": True,
        "mapped": False,
    }


@pytest.mark.asyncio
async def test_fill_events_for_order_identity_scopes_strictly(tmp_path: Path) -> None:
    """Durable fill rows come back only for the full stable identity.

    Given: fill_observed rows for one order plus foreign rows sharing
        the cid under a different mode, wallet, and exchange, and one
        row carrying a DIFFERENT venue order id,
    When: the identity-scoped read runs with a known venue order id,
    Then: only the matching rows return (id-ordered), with null-xoid
        rows still counted and every foreign row excluded.
    """
    repo, _spid = await _repo_with_symbol(tmp_path, "identity_scope.db")
    now = datetime.now(UTC)

    async def _insert_event(
        seq: int,
        fill_size: float,
        exec_id: str | None,
        *,
        mode: str = "live",
        wallet: str = "wal-1",
        exchange: str = "kraken",
        exchange_order_id: str | None = None,
    ) -> None:
        """Insert one fill_observed venue event row.

        Args:
            seq: Bus sequence uniquifier.
            fill_size: Additive delta recorded on the row.
            exec_id: Venue execution identity.
            mode: Execution mode (identity scope).
            wallet: Owning wallet (identity scope).
            exchange: Venue discriminator (identity scope).
            exchange_order_id: Venue order id on the row.
        """
        await repo.insert_venue_event(
            {
                "event_type": "fill_observed",
                "shard_key": f"{exchange}.BTC-USD.{mode}",
                "exchange": exchange,
                "instrument": "BTC-USD",
                "mode": mode,
                "client_order_id": "cid-add-1",
                "fill_size": fill_size,
                "exec_id": exec_id,
                "exchange_order_id": exchange_order_id,
                "wallet_public_id": wallet,
                "received_at": now,
                "session_id": "s1",
                "sequence_id": seq,
                "timestamp": now,
            }
        )

    await _insert_event(1, 0.3, "ex-a", exchange_order_id="xo-1")
    await _insert_event(2, 0.2, None, exchange_order_id=None)
    await _insert_event(3, 0.9, "ex-f", mode="paper")
    await _insert_event(4, 0.8, "ex-g", wallet="wal-2")
    await _insert_event(5, 0.7, "ex-h", exchange="walutomat")
    await _insert_event(6, 0.6, "ex-i", exchange_order_id="xo-OTHER")
    rows = await repo.get_fill_venue_events_for_order_identity(
        "cid-add-1", "wal-1", "live", "kraken", "xo-1"
    )
    assert [row["fill_size"] for row in rows] == [0.3, 0.2]


@pytest.mark.asyncio
async def test_fill_events_for_order_identity_empty_is_empty(tmp_path: Path) -> None:
    """No durable fill rows return an empty list.

    Given: no fill_observed rows for the order,
    When: the identity-scoped read runs,
    Then: an empty list returns.
    """
    repo, _spid = await _repo_with_symbol(tmp_path, "identity_empty.db")
    rows = await repo.get_fill_venue_events_for_order_identity(
        "cid-none", "wal-1", "live", "kraken", None
    )
    assert rows == []


@pytest.mark.asyncio
async def test_ensure_instrument_race_reconciles_requested_mapping(tmp_path: Path) -> None:
    """An insert-race winner lacking the mapping is revised in place.

    Given: the initial current-row lookup misses, the INSERT trips the
        active-unique race, and the re-read winner carries NO source
        mapping while the caller requested one,
    When: ``ensure_instrument`` reconciles,
    Then: the winner is SCD2-revised to carry the requested mapping —
        a concurrent source-less writer can never silently drop it.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'race.db'}")
    now = datetime.now(UTC)
    winner = MagicMock()
    winner.id = 7
    winner.public_id = "winner-pid"
    winner.symbol_public_id = "sym-1"
    winner.exchange = "paper"
    winner.requires_ai_review = False
    winner.source_exchange = None
    winner.timestamp = now

    miss = MagicMock()
    miss.scalar_one_or_none = MagicMock(return_value=None)
    hit = MagicMock()
    hit.scalar_one_or_none = MagicMock(return_value=winner)
    close_result = MagicMock()

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=[miss, hit, close_result])
    session.add = MagicMock(side_effect=lambda obj: setattr(obj, "id", 99))
    session.flush = AsyncMock()
    session.commit = AsyncMock(side_effect=[IntegrityError("dup", "p", Exception()), None])
    session.rollback = AsyncMock()

    with patch.object(repo, "session") as ctx:
        ctx.return_value.__aenter__.return_value = session
        ctx.return_value.__aexit__.return_value = None
        result = await repo.ensure_instrument(
            symbol_public_id="sym-1",
            exchange="paper",
            session_id="s1",
            sequence_id=5,
            timestamp=now,
            source_exchange="kraken",
        )
    assert result == (99, "winner-pid")
    revised = session.add.call_args_list[-1].args[0]
    assert revised.source_exchange == "kraken"
    assert revised.public_id == "winner-pid"
