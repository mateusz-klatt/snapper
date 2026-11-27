"""Unit tests for :class:`USDConverter`.

Covers:
    - USD quote: happy path returns ``quantity × last_price``.
    - Non-USD quote without FX evidence: raises :class:`PriceUnavailableError`
      with ``fx_snapshot_missing``.
    - Missing instrument: ``instrument_not_found``.
    - Missing snapshot: ``snapshot_missing``.
    - Null last_price: ``last_price_null``.
    - Stale snapshot (> 300s): ``price_stale`` — both via cached
      re-check AND on fresh load.
    - Cache behavior: repeated calls within 60s TTL skip DB; after
      TTL expiry, DB is consulted again; test hook ``_invalidate``
      evicts a single entry.

Uses in-memory aiosqlite DB + seeded Symbol/Instrument/
MarketSnapshot fixtures so the behavior is exercised end-to-end
without mocking the SQL layer.
"""

import asyncio as _asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from snapper.application.pricing.usd_converter import CACHE_TTL_SECONDS
from snapper.application.pricing.usd_converter import MAX_FUTURE_SKEW_SECONDS
from snapper.application.pricing.usd_converter import STALENESS_THRESHOLD_SECONDS
from snapper.application.pricing.usd_converter import PriceUnavailableError
from snapper.application.pricing.usd_converter import USDConverter
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import MarketSnapshot
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ActiveCandlePriceRow


@pytest.fixture
async def repo() -> SQLAlchemyRepository:
    """In-memory aiosqlite Repository with schema created."""
    r = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await r.create_all()
    return r


def _seed_time() -> datetime:
    """Return a fixed UTC wall-clock for deterministic tests."""
    return datetime(2026, 4, 18, 12, 0, 0, tzinfo=UTC)


async def _seed_instrument(
    repo: SQLAlchemyRepository,
    *,
    instrument_public_id: str,
    quote: str,
) -> None:
    """Insert active Symbol + Instrument rows with the requested quote."""
    now = _seed_time()
    async with repo.session() as s:
        symbol = Symbol(
            public_id=f"sym-{instrument_public_id}",
            timestamp=now,
            known_to=KNOWN_TO_MAX,
            session_id="seed",
            sequence_id=1,
            native_symbol=f"NS-{instrument_public_id}",
            base="BTC",
            quote=quote,
            asset_type="crypto",
            created_at=now,
        )
        s.add(symbol)
        await s.flush()
        instr = Instrument(
            public_id=instrument_public_id,
            timestamp=now,
            known_to=KNOWN_TO_MAX,
            session_id="seed",
            sequence_id=1,
            symbol_public_id=symbol.public_id,
            exchange="kraken",
        )
        s.add(instr)
        await s.commit()


async def _seed_snapshot(
    repo: SQLAlchemyRepository,
    *,
    instrument_public_id: str,
    last_price: float | None,
    snapshot_time: datetime | None = None,
) -> None:
    """Insert active MarketSnapshot row for the instrument."""
    async with repo.session() as s:
        snap = MarketSnapshot(
            public_id=f"snap-{instrument_public_id}",
            timestamp=snapshot_time or _seed_time(),
            known_to=KNOWN_TO_MAX,
            session_id="seed",
            sequence_id=1,
            instrument_public_id=instrument_public_id,
            last_price=last_price,
        )
        s.add(snap)
        await s.commit()


async def _seed_candle(
    repo: SQLAlchemyRepository,
    *,
    instrument_public_id: str,
    close: float,
    open_at: datetime,
    complete: bool = True,
) -> None:
    """Insert an active 1m calculated candle row for the instrument."""
    async with repo.session() as s:
        candle = Candle(
            public_id=f"candle-{instrument_public_id}-{int(open_at.timestamp())}",
            instrument_public_id=instrument_public_id,
            open_at=open_at,
            timeframe="1m",
            open=close,
            high=close,
            low=close,
            close=close,
            volume=1.0,
            vwap=close,
            trades=1,
            source="calculated",
            complete=complete,
            timestamp=open_at,
            known_to=KNOWN_TO_MAX,
            session_id="seed",
            sequence_id=1,
        )
        s.add(candle)
        await s.commit()


@pytest.mark.asyncio
async def test_usd_quote_happy_path(repo: SQLAlchemyRepository) -> None:
    """USD-quoted instrument returns quantity × last_price.

    Given: an Instrument with a USD-quoted Symbol and a fresh
        MarketSnapshot with ``last_price = 50_000``,
    When: ``to_usd(instrument_public_id, Decimal('0.5'))`` runs,
    Then: the Decimal result equals ``0.5 × 50000 = 25000``.
    """
    inst_id = "inst-btc-usd"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    await _seed_snapshot(repo, instrument_public_id=inst_id, last_price=50_000.0)
    conv = USDConverter(repo, now=_seed_time)
    result = await conv.to_usd(inst_id, Decimal("0.5"))
    assert result == Decimal("25000")


@pytest.mark.asyncio
async def test_non_usd_quote_without_fx_snapshot_is_unavailable(
    repo: SQLAlchemyRepository,
) -> None:
    """Non-USD quote without independently evidenced FX fails closed.

    Given: an EUR-quoted Symbol + Instrument + fresh snapshot,
    When: ``to_usd`` runs,
    Then: :class:`PriceUnavailableError` with
        ``reason='fx_snapshot_missing'`` is raised.
    """
    inst_id = "inst-btc-eur"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="EUR")
    await _seed_snapshot(repo, instrument_public_id=inst_id, last_price=47_000.0)
    conv = USDConverter(repo, now=_seed_time)
    one_unit = Decimal("1")
    with pytest.raises(PriceUnavailableError) as exc:
        await conv.to_usd(inst_id, one_unit)
    assert exc.value.reason_code == "fx_snapshot_missing"


@pytest.mark.asyncio
async def test_missing_instrument_raises_instrument_not_found(
    repo: SQLAlchemyRepository,
) -> None:
    """Unknown instrument raises ``instrument_not_found``.

    Given: a Repository with no Instrument rows,
    When: ``to_usd('ghost', ...)`` runs,
    Then: :class:`PriceUnavailableError` with
        ``reason='instrument_not_found'`` is raised.
    """
    conv = USDConverter(repo, now=_seed_time)
    one_unit = Decimal("1")
    with pytest.raises(PriceUnavailableError) as exc:
        await conv.to_usd("ghost-instrument", one_unit)
    assert exc.value.reason_code == "instrument_not_found"


@pytest.mark.asyncio
async def test_missing_snapshot_raises_snapshot_missing(
    repo: SQLAlchemyRepository,
) -> None:
    """Instrument without MarketSnapshot raises ``snapshot_missing``.

    Given: an Instrument seeded with USD quote but no snapshot row,
    When: ``to_usd`` runs,
    Then: :class:`PriceUnavailableError` with
        ``reason='snapshot_missing'`` is raised.
    """
    inst_id = "inst-no-snap"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    conv = USDConverter(repo, now=_seed_time)
    one_unit = Decimal("1")
    with pytest.raises(PriceUnavailableError) as exc:
        await conv.to_usd(inst_id, one_unit)
    assert exc.value.reason_code == "snapshot_missing"


@pytest.mark.asyncio
async def test_null_last_price_raises_last_price_null(
    repo: SQLAlchemyRepository,
) -> None:
    """Snapshot with last_price=None raises ``last_price_null``.

    Given: an Instrument with a MarketSnapshot whose
        ``last_price IS NULL`` (venue never reported a trade),
    When: ``to_usd`` runs,
    Then: :class:`PriceUnavailableError` with
        ``reason='last_price_null'`` is raised.
    """
    inst_id = "inst-null-price"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    await _seed_snapshot(repo, instrument_public_id=inst_id, last_price=None)
    conv = USDConverter(repo, now=_seed_time)
    one_unit = Decimal("1")
    with pytest.raises(PriceUnavailableError) as exc:
        await conv.to_usd(inst_id, one_unit)
    assert exc.value.reason_code == "last_price_null"


@pytest.mark.asyncio
async def test_stale_snapshot_fresh_load_raises_price_stale(
    repo: SQLAlchemyRepository,
) -> None:
    """Snapshot older than 300s at first load raises ``price_stale``.

    Given: a snapshot with ``timestamp`` set 10 minutes before
        the converter's clock,
    When: ``to_usd`` runs (fresh load path),
    Then: :class:`PriceUnavailableError` with
        ``reason='price_stale'`` is raised.
    """
    inst_id = "inst-stale"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    old = _seed_time() - timedelta(seconds=STALENESS_THRESHOLD_SECONDS + 100)
    await _seed_snapshot(repo, instrument_public_id=inst_id, last_price=1.0, snapshot_time=old)
    conv = USDConverter(repo, now=_seed_time)
    one_unit = Decimal("1")
    with pytest.raises(PriceUnavailableError) as exc:
        await conv.to_usd(inst_id, one_unit)
    assert exc.value.reason_code == "price_stale"


@pytest.mark.asyncio
async def test_stale_snapshot_via_cache_re_check_raises(
    repo: SQLAlchemyRepository,
) -> None:
    """Cached entry re-checked as stale on subsequent call raises.

    Given: an initial fresh call populates the cache; wall-clock
        then advances past the staleness threshold while still
        within the 60s TTL,
    When: a second ``to_usd`` call runs,
    Then: the cached entry is re-validated and raises
        ``price_stale`` — guards against serving an aged price
        from cache beyond the staleness window.
    """
    inst_id = "inst-cache-stale"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    snap_time = _seed_time() - timedelta(seconds=STALENESS_THRESHOLD_SECONDS - 10)
    await _seed_snapshot(
        repo,
        instrument_public_id=inst_id,
        last_price=100.0,
        snapshot_time=snap_time,
    )
    clock = {"t": _seed_time()}
    conv = USDConverter(repo, now=lambda: clock["t"])
    first = await conv.to_usd(inst_id, Decimal("2"))
    assert first == Decimal("200")
    clock["t"] = _seed_time() + timedelta(seconds=15)
    one_unit = Decimal("1")
    with pytest.raises(PriceUnavailableError) as exc:
        await conv.to_usd(inst_id, one_unit)
    assert exc.value.reason_code == "price_stale"


@pytest.mark.asyncio
async def test_cache_hit_skips_db(repo: SQLAlchemyRepository) -> None:
    """Second call within TTL returns from cache without DB hit.

    Given: an initial call populates the cache; the underlying
        MarketSnapshot row is then deleted,
    When: a second call runs within the 60s TTL,
    Then: the converter returns the cached price (DB miss would
        raise ``snapshot_missing``) — proves the cache short-circuit.
    """
    inst_id = "inst-cache-hit"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    await _seed_snapshot(repo, instrument_public_id=inst_id, last_price=10.0)
    conv = USDConverter(repo, now=_seed_time)
    first = await conv.to_usd(inst_id, Decimal("1"))
    assert first == Decimal("10")

    async with repo.session() as s:
        await s.execute(MarketSnapshot.__table__.delete())
        await s.commit()

    second = await conv.to_usd(inst_id, Decimal("2"))
    assert second == Decimal("20")


@pytest.mark.asyncio
async def test_cache_ttl_expiry_forces_reload(
    repo: SQLAlchemyRepository,
) -> None:
    """Call after 60s TTL re-reads from DB and sees updated price.

    Given: an initial call caches last_price=10; the MarketSnapshot
        is then updated to last_price=20 and the clock advances
        past the 60s TTL,
    When: a third call runs,
    Then: the new value (20) is used — the TTL expiry path is
        exercised.
    """
    inst_id = "inst-ttl-expiry"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    await _seed_snapshot(repo, instrument_public_id=inst_id, last_price=10.0)
    clock = {"t": _seed_time()}
    conv = USDConverter(repo, now=lambda: clock["t"])
    first = await conv.to_usd(inst_id, Decimal("1"))
    assert first == Decimal("10")

    async with repo.session() as s:
        await s.execute(MarketSnapshot.__table__.delete())
        await s.commit()
    new_time = _seed_time() + timedelta(seconds=CACHE_TTL_SECONDS + 5)
    await _seed_snapshot(
        repo, instrument_public_id=inst_id, last_price=20.0, snapshot_time=new_time
    )
    clock["t"] = new_time
    result = await conv.to_usd(inst_id, Decimal("1"))
    assert result == Decimal("20")


@pytest.mark.asyncio
async def test_invalidate_single_entry_forces_db_reload(
    repo: SQLAlchemyRepository,
) -> None:
    """``_invalidate`` evicts one cache entry without affecting others.

    Given: two instruments both cached,
    When: ``_invalidate(inst_a)`` runs followed by a lookup for
        each,
    Then: ``inst_a`` re-reads from DB (raises on missing snapshot)
        while ``inst_b`` still returns from cache — proves the
        test hook operates at single-key granularity.
    """
    inst_a = "inst-a"
    inst_b = "inst-b"
    await _seed_instrument(repo, instrument_public_id=inst_a, quote="USD")
    await _seed_instrument(repo, instrument_public_id=inst_b, quote="USD")
    await _seed_snapshot(repo, instrument_public_id=inst_a, last_price=1.0)
    await _seed_snapshot(repo, instrument_public_id=inst_b, last_price=2.0)
    conv = USDConverter(repo, now=_seed_time)
    await conv.to_usd(inst_a, Decimal("1"))
    await conv.to_usd(inst_b, Decimal("1"))

    async with repo.session() as s:
        await s.execute(MarketSnapshot.__table__.delete())
        await s.commit()

    conv._invalidate(inst_a)
    one_unit = Decimal("1")
    with pytest.raises(PriceUnavailableError):
        await conv.to_usd(inst_a, one_unit)
    b_result = await conv.to_usd(inst_b, Decimal("1"))
    assert b_result == Decimal("2")


@pytest.mark.asyncio
async def test_concurrent_first_loads_share_one_db_round_trip(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two simultaneous ``to_usd`` calls race on the cache-lock.

    Given: a fresh converter with an empty cache, and two callers
        launched via ``asyncio.gather`` for the same instrument,
    When: both enter the cold-cache branch, contend on
        ``_cache_lock``, and the second one finds the cache
        already populated by the first inside the lock,
    Then: both get the correct value AND only one DB-level load
        happens. Exercises the double-check-after-lock re-read
        branch that prevents a thundering-herd DB hit.
    """
    inst_id = "inst-concurrent"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    await _seed_snapshot(repo, instrument_public_id=inst_id, last_price=42.0)
    conv = USDConverter(repo, now=_seed_time)

    load_calls = {"n": 0}
    orig_load = conv._load_and_cache

    async def counting_load(pid: str, now: datetime) -> object:
        load_calls["n"] += 1
        await _asyncio.sleep(0.02)
        return await orig_load(pid, now)

    monkeypatch.setattr(conv, "_load_and_cache", counting_load)

    a, b = await _asyncio.gather(
        conv.to_usd(inst_id, Decimal("1")),
        conv.to_usd(inst_id, Decimal("1")),
    )
    assert a == Decimal("42")
    assert b == Decimal("42")
    assert load_calls["n"] == 1


@pytest.mark.asyncio
async def test_price_unavailable_error_carries_context(
    repo: SQLAlchemyRepository,
) -> None:
    """Error exposes ``reason``, ``instrument_public_id``, ``detail``.

    Given: an EUR-quoted instrument,
    When: ``to_usd`` raises,
    Then: the exception carries a meaningful ``str()`` plus the
        structured fields the HTTP layer uses to build the
        ``caps_price_unavailable`` response.
    """
    inst_id = "inst-error-context"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="EUR")
    await _seed_snapshot(repo, instrument_public_id=inst_id, last_price=100.0)
    conv = USDConverter(repo, now=_seed_time)
    one_unit = Decimal("1")
    with pytest.raises(PriceUnavailableError) as exc:
        await conv.to_usd(inst_id, one_unit)
    assert exc.value.reason_code == "fx_snapshot_missing"
    assert exc.value.instrument_public_id == inst_id
    assert "EUR" in exc.value.detail
    assert inst_id in str(exc.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("age_from_open,complete", [(360, True), (15, False)])
async def test_candle_only_fresh_price_succeeds(
    repo: SQLAlchemyRepository,
    age_from_open: int,
    complete: bool,
) -> None:
    """Active 1m candle close within close-time freshness prices without a snapshot.

    Given: an Instrument with no MarketSnapshot row and an active 1m
        calculated candle (at exact 300s close-time boundary or in-progress
        provisional minute),
    When: ``to_usd`` runs,
    Then: the candle's close prices the order without ``snapshot_missing``.
    """
    inst_id = "inst-candle-only"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    await _seed_candle(
        repo,
        instrument_public_id=inst_id,
        close=62_000.0,
        open_at=_seed_time() - timedelta(seconds=age_from_open),
        complete=complete,
    )
    conv = USDConverter(repo, now=_seed_time)
    assert await conv.to_usd(inst_id, Decimal("0.25")) == Decimal("15500")


@pytest.mark.asyncio
async def test_snapshot_only_fallback_when_candle_stale(
    repo: SQLAlchemyRepository,
) -> None:
    """Fresh snapshot serves as fallback when the newest 1m candle is stale.

    Given: an Instrument with a stale 1m candle (10 minutes old) and a
        fresh MarketSnapshot,
    When: ``to_usd`` runs,
    Then: the fresh MarketSnapshot price is selected.
    """
    inst_id = "inst-snapshot-fallback"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    await _seed_candle(
        repo,
        instrument_public_id=inst_id,
        close=40_000.0,
        open_at=_seed_time() - timedelta(seconds=600),
    )
    await _seed_snapshot(repo, instrument_public_id=inst_id, last_price=55_000.0)
    conv = USDConverter(repo, now=_seed_time)
    assert await conv.to_usd(inst_id, Decimal("2")) == Decimal("110000")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "snapshot_age,candle_open_age,expected_price",
    [
        (57_600, 90, "50000"),
        (120, 90, "50000"),
        (10, 180, "48000"),
        (30, 90, "50000"),
    ],
)
async def test_both_present_fresher_source_wins(
    repo: SQLAlchemyRepository,
    snapshot_age: int,
    candle_open_age: int,
    expected_price: str,
) -> None:
    """When both snapshot and 1m candle exist, the fresher evidence wins.

    Given: an Instrument with both a MarketSnapshot (price 48000) and a
        1m candle (price 50000) at varying ages, including equal effective
        timestamps where the live candle takes precedence,
    When: ``to_usd`` runs,
    Then: the fresher source determines the USD notional.
    """
    inst_id = f"inst-both-{snapshot_age}-{candle_open_age}"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    await _seed_snapshot(
        repo,
        instrument_public_id=inst_id,
        last_price=48_000.0,
        snapshot_time=_seed_time() - timedelta(seconds=snapshot_age),
    )
    await _seed_candle(
        repo,
        instrument_public_id=inst_id,
        close=50_000.0,
        open_at=_seed_time() - timedelta(seconds=candle_open_age),
    )
    conv = USDConverter(repo, now=_seed_time)
    assert await conv.to_usd(inst_id, Decimal("1")) == Decimal(expected_price)


@pytest.mark.asyncio
async def test_candle_stale_and_snapshot_stale_raises_price_stale(
    repo: SQLAlchemyRepository,
) -> None:
    """Both candle and snapshot beyond 300s refuse with ``price_stale``.

    Given: a 1m candle whose close-time is 301s old and a MarketSnapshot
        that is 400s old,
    When: ``to_usd`` runs,
    Then: :class:`PriceUnavailableError` with ``price_stale`` is raised.
    """
    inst_id = "inst-both-stale"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    await _seed_candle(
        repo,
        instrument_public_id=inst_id,
        close=50_000.0,
        open_at=_seed_time() - timedelta(seconds=STALENESS_THRESHOLD_SECONDS + 61),
    )
    await _seed_snapshot(
        repo,
        instrument_public_id=inst_id,
        last_price=49_000.0,
        snapshot_time=_seed_time() - timedelta(seconds=STALENESS_THRESHOLD_SECONDS + 100),
    )
    conv = USDConverter(repo, now=_seed_time)
    with pytest.raises(PriceUnavailableError) as exc:
        await conv.to_usd(inst_id, Decimal("1"))
    assert exc.value.reason_code == "price_stale"


@pytest.mark.asyncio
async def test_candle_cache_uses_candle_evidence_timestamp(
    repo: SQLAlchemyRepository,
) -> None:
    """Cached candle entry stores the candle close-time and enforces staleness.

    Given: a candle-only price at 290s close-time age cached on initial load
        and then deleted from the DB,
    When: a second call runs 5s later (fresh cache hit) and a third call
        runs 15s later (still within 60s TTL, now 305s old),
    Then: the second call uses the cached candle price and the third raises
        ``price_stale``.
    """
    inst_id = "inst-candle-cache"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    await _seed_candle(
        repo,
        instrument_public_id=inst_id,
        close=100.0,
        open_at=_seed_time() - timedelta(seconds=STALENESS_THRESHOLD_SECONDS + 50),
    )
    clock = {"t": _seed_time()}
    conv = USDConverter(repo, now=lambda: clock["t"])
    assert await conv.to_usd(inst_id, Decimal("2")) == Decimal("200")
    async with repo.session() as s:
        await s.execute(Candle.__table__.delete())
        await s.commit()
    clock["t"] = _seed_time() + timedelta(seconds=5)
    assert await conv.to_usd(inst_id, Decimal("3")) == Decimal("300")
    clock["t"] = _seed_time() + timedelta(seconds=15)
    with pytest.raises(PriceUnavailableError) as exc:
        await conv.to_usd(inst_id, Decimal("1"))
    assert exc.value.reason_code == "price_stale"


@pytest.mark.asyncio
async def test_candle_future_skew_naive_and_invalid_price_fail_closed(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Future-skewed, naive, and nonpositive candle rows obey fail-closed guards.

    Given: a USD-quoted instrument with candle prices at the skew boundary,
        beyond future skew, with a naive open_at, or with a zero close,
    When: ``to_usd`` runs,
    Then: exact skew is accepted while excessive skew, naive timestamp, and
        invalid close fail closed; a valid candle also overrides a NULL snapshot.
    """
    inst_id = "inst-candle-guards"
    await _seed_instrument(repo, instrument_public_id=inst_id, quote="USD")
    await _seed_snapshot(repo, instrument_public_id=inst_id, last_price=None)
    await _seed_candle(
        repo,
        instrument_public_id=inst_id,
        close=250.0,
        open_at=_seed_time() + timedelta(seconds=MAX_FUTURE_SKEW_SECONDS),
    )
    assert await USDConverter(repo, now=_seed_time).to_usd(inst_id, Decimal("2")) == Decimal("500")

    bad_future: ActiveCandlePriceRow = {
        "candle_public_id": "c-future",
        "instrument_public_id": inst_id,
        "close": 250.0,
        "open_at": _seed_time() + timedelta(seconds=MAX_FUTURE_SKEW_SECONDS + 1),
    }
    monkeypatch.setattr(
        repo, "get_active_latest_1m_candle_price", AsyncMock(return_value=bad_future)
    )
    with pytest.raises(PriceUnavailableError, match="price_stale"):
        await USDConverter(repo, now=_seed_time).to_usd(inst_id, Decimal("1"))

    naive_row: ActiveCandlePriceRow = {
        "candle_public_id": "c-naive",
        "instrument_public_id": inst_id,
        "close": 250.0,
        "open_at": _seed_time().replace(tzinfo=None),
    }
    monkeypatch.setattr(
        repo, "get_active_latest_1m_candle_price", AsyncMock(return_value=naive_row)
    )
    with pytest.raises(PriceUnavailableError, match="price_stale"):
        await USDConverter(repo, now=_seed_time).to_usd(inst_id, Decimal("1"))

    invalid_row: ActiveCandlePriceRow = {
        "candle_public_id": "c-zero",
        "instrument_public_id": inst_id,
        "close": 0.0,
        "open_at": _seed_time(),
    }
    monkeypatch.setattr(
        repo, "get_active_latest_1m_candle_price", AsyncMock(return_value=invalid_row)
    )
    with pytest.raises(PriceUnavailableError, match="last_price_null"):
        await USDConverter(repo, now=_seed_time).to_usd(inst_id, Decimal("1"))

    monkeypatch.setattr(repo, "get_active_market_snapshot_price", AsyncMock(return_value=None))
    with pytest.raises(PriceUnavailableError, match="last_price_invalid"):
        await USDConverter(repo, now=_seed_time).to_usd(inst_id, Decimal("1"))
