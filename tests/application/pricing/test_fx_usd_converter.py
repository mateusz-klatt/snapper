"""Exercise current FX evidence, temporal joins and final-clock cap valuation."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import Overflow
from decimal import localcontext
from unittest.mock import AsyncMock

import pytest

from snapper.application.pricing.usd_converter import MAX_FUTURE_SKEW_SECONDS
from snapper.application.pricing.usd_converter import STALENESS_THRESHOLD_SECONDS
from snapper.application.pricing.usd_converter import PriceUnavailableError
from snapper.application.pricing.usd_converter import USDConverter
from snapper.application.pricing.usd_converter import _normalized_fx_rate
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import MarketSnapshot
from snapper.data.models import Symbol
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import TradeCommand
from snapper.data.models import UserTradingCaps
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import InstrumentValuationMetadataRow
from snapper.data.repository_types import UsdFxCandleCandidateRow
from snapper.data.repository_types import UsdFxSnapshotCandidateRow

_NOW = datetime(2026, 11, 16, 12, tzinfo=UTC)


@dataclass(frozen=True)
class _Market:
    """One isolated catalog plane with independently controllable evidence."""

    identity: str
    base: str = "EUR"
    quote: str = "USD"
    price: float | None = 1.25
    timestamp: datetime = _NOW
    exchange: str = "kraken"
    asset_type: str = "forex"
    can_market_data: bool = True
    snapshot: bool = True
    closed: str = ""
    capability_exchange: str | None = None


async def _seed(repo: SQLAlchemyRepository, market: _Market) -> None:
    """Persist explicit currency, capability and snapshot evidence for one plane."""
    common = {"timestamp": _NOW, "session_id": "test", "sequence_id": 1}
    symbol = Symbol(
        public_id=f"sym-{market.identity}",
        native_symbol=market.identity,
        base=market.base,
        quote=market.quote,
        asset_type=market.asset_type,
        created_at=_NOW,
        **common,
    )
    instrument = Instrument(
        public_id=market.identity,
        symbol_public_id=symbol.public_id,
        exchange=market.exchange,
        **common,
    )
    capability = SymbolExchangeCapability(
        public_id=f"cap-{market.identity}",
        symbol_public_id=symbol.public_id,
        exchange=market.capability_exchange or market.exchange,
        can_market_data=market.can_market_data,
        created_at=_NOW,
        **common,
    )
    snapshot = MarketSnapshot(
        public_id=f"snap-{market.identity}",
        instrument_public_id=instrument.public_id,
        last_price=market.price,
        timestamp=market.timestamp,
        session_id="test",
        sequence_id=1,
    )
    rows: dict[str, Symbol | Instrument | SymbolExchangeCapability | MarketSnapshot] = {
        "symbol": symbol,
        "instrument": instrument,
        "capability": capability,
    }
    if market.snapshot:
        rows["snapshot"] = snapshot
    for name, row in rows.items():
        row.known_to = _NOW if name == market.closed else KNOWN_TO_MAX
    async with repo.session() as session:
        session.add_all(rows.values())
        await session.commit()


@pytest.fixture
async def repo() -> AsyncIterator[SQLAlchemyRepository]:
    """Provide an isolated repository with a fresh EUR-quoted spot instrument."""
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repository.create_all()
    await _seed(
        repository, _Market("main", base="BTC", quote="EUR", price=100, asset_type="crypto")
    )
    yield repository
    await repository.engine.dispose()


@pytest.mark.parametrize(
    "change",
    [
        {"closed": "symbol"},
        {"closed": "instrument"},
        {"closed": "capability"},
        {"closed": "snapshot"},
        {"snapshot": False},
        {"exchange": "paper", "capability_exchange": "paper"},
        {"asset_type": "crypto"},
        {"can_market_data": False},
        {"base": "GBP"},
        {"capability_exchange": "walutomat"},
    ],
)
async def test_repository_fx_read_requires_every_current_evidence_plane(
    repo: SQLAlchemyRepository, change: dict[str, object]
) -> None:
    """Exclude an ineligible plane while retaining both current USD orientations.

    Given: two usable forex planes and a third with one invalid eligibility fact.
    When: current EUR conversion candidates are read from real SQLite joins.
    Then: only direct and inverse current market-data evidence enters the oracle.
    """
    await _seed(repo, _Market("direct"))
    await _seed(repo, _Market("inverse", base="USD", quote="EUR", price=0.8))
    await _seed(repo, replace(_Market("excluded"), **change))
    rows = await repo.get_active_usd_fx_snapshot_candidates("EUR")
    assert {row["instrument_public_id"] for row in rows} == {"direct", "inverse"}
    assert {row["snapshot_public_id"] for row in rows} == {"snap-direct", "snap-inverse"}
    assert all(row["timestamp"] == _NOW for row in rows)


@pytest.mark.parametrize("inverse", [False, True])
@pytest.mark.parametrize("quantity", [Decimal(0), Decimal("2")])
async def test_direct_and_inverse_fx_produce_the_same_usd_notional(
    repo: SQLAlchemyRepository, inverse: bool, quantity: Decimal
) -> None:
    """Apply normalized Decimal FX after the submitted instrument's quote exposure.

    Given: BTC-EUR at 100 and either EUR-USD at 1.25 or USD-EUR at 0.8.
    When: zero or two units are valued.
    Then: both orientations yield identical USD notional without a zero shortcut.
    """
    market = _Market("fx", base="USD", quote="EUR", price=0.8) if inverse else _Market("fx")
    await _seed(repo, market)
    assert await USDConverter(repo, now=lambda: _NOW).to_usd("main", quantity) == quantity * 125


@pytest.mark.parametrize(
    "direct,inverse,expected",
    [(1.25, 0.8, "250"), (1.25, 1.25, None)],
)
async def test_equal_time_planes_compare_normalized_rates(
    repo: SQLAlchemyRepository, direct: float, inverse: float, expected: str | None
) -> None:
    """Deduplicate reciprocal rates and refuse apparently equal raw inverse prices.

    Given: equally recent direct and inverse planes.
    When: the oracle compares their USD-per-EUR values.
    Then: reciprocal prices agree while equal raw prices conflict.
    """
    await _seed(repo, _Market("direct", price=direct))
    await _seed(repo, _Market("inverse", base="USD", quote="EUR", price=inverse))
    converter = USDConverter(repo, now=lambda: _NOW)
    if expected is None:
        with pytest.raises(PriceUnavailableError, match="fx_plane_conflict"):
            await converter.to_usd("main", Decimal(2))
    else:
        assert await converter.to_usd("main", Decimal(2)) == Decimal(expected)


async def test_newest_usable_fx_wins_and_main_cache_does_not_cache_fx(
    repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Read independent current FX on every call while coalescing the main price load.

    Given: older and newer usable FX rates and a 60-second main-price cache.
    When: two orders are valued and the newer rate changes between calls.
    Then: the main snapshot is loaded once and FX evidence is read twice.
    """
    await _seed(repo, _Market("older", price=1.1, timestamp=_NOW - timedelta(seconds=1)))
    await _seed(repo, _Market("newer", price=1.25))
    main = AsyncMock(wraps=repo.get_active_market_snapshot_price)
    fx = AsyncMock(wraps=repo.get_active_usd_fx_snapshot_candidates)
    monkeypatch.setattr(repo, "get_active_market_snapshot_price", main)
    monkeypatch.setattr(repo, "get_active_usd_fx_snapshot_candidates", fx)
    converter = USDConverter(repo, now=lambda: _NOW)
    assert await converter.to_usd("main", Decimal(2)) == 250
    async with repo.session() as session:
        snapshot = await session.get(MarketSnapshot, 3)
        assert snapshot is not None
        snapshot.last_price = 1.5
        await session.commit()
    assert await converter.to_usd("main", Decimal(2)) == 300
    assert main.await_count == 1
    assert fx.await_count == 2


@pytest.mark.parametrize("quantity", [Decimal(0), Decimal(1)])
async def test_missing_fx_refuses_even_zero_quantity(
    repo: SQLAlchemyRepository, quantity: Decimal
) -> None:
    """Never bypass the independent FX leg merely because the requested size is zero.

    Given: a fresh main price with no eligible FX snapshot.
    When: any supported quantity is converted.
    Then: the unavailable leg is identified without producing USD exposure.
    """
    with pytest.raises(PriceUnavailableError, match="fx_snapshot_missing"):
        await USDConverter(repo, now=lambda: _NOW).to_usd("main", quantity)


@pytest.mark.parametrize("price", [None, 0, -1, float("inf"), float("-inf"), float("nan")])
async def test_bad_fx_prices_are_unavailable(
    repo: SQLAlchemyRepository, price: float | None
) -> None:
    """Refuse nonpositive, missing and nonfinite FX marks before inversion.

    Given: a current eligible forex plane with an unusable raw price.
    When: a positive order requests USD exposure.
    Then: fx_price_invalid identifies the failed FX evidence.
    """
    await _seed(repo, _Market("fx", price=price))
    with pytest.raises(PriceUnavailableError, match="fx_price_invalid"):
        await USDConverter(repo, now=lambda: _NOW).to_usd("main", Decimal(1))


@pytest.mark.parametrize("leg", ["main", "fx"])
@pytest.mark.parametrize("age,accepted", [(300, True), (301, False), (-5, True), (-6, False)])
async def test_both_legs_obey_inclusive_age_and_future_skew_boundaries(
    repo: SQLAlchemyRepository, leg: str, age: int, accepted: bool
) -> None:
    """Pin the shared five-second skew and 300-second freshness boundaries.

    Given: one leg exactly on or just outside either freshness boundary.
    When: a two-hop order is converted using the final clock.
    Then: both legs use the same inclusive limits and report their own refusal.
    """
    assert MAX_FUTURE_SKEW_SECONDS == 5
    timestamp = _NOW - timedelta(seconds=age)
    await _seed(repo, _Market("fx", timestamp=timestamp if leg == "fx" else _NOW))
    if leg == "main":
        async with repo.session() as session:
            snapshot = await session.get(MarketSnapshot, 1)
            assert snapshot is not None
            snapshot.timestamp = timestamp
            await session.commit()
    converter = USDConverter(repo, now=lambda: _NOW)
    if accepted:
        assert await converter.to_usd("main", Decimal(1)) == 125
    else:
        reason = "price_stale" if leg == "main" else "fx_price_stale"
        with pytest.raises(PriceUnavailableError, match=reason):
            await converter.to_usd("main", Decimal(1))


@pytest.mark.parametrize("cached", [False, True])
async def test_fx_read_crossing_main_freshness_boundary_refuses(
    repo: SQLAlchemyRepository, cached: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Check main-price age after the FX await even when the main cache hits.

    Given: a main mark aged 299 seconds and an FX query that advances the clock.
    When: the query completes two seconds later on a cold or cached path.
    Then: the now-301-second main mark is refused.
    """
    await _seed(repo, _Market("fx"))
    async with repo.session() as session:
        main = await session.get(MarketSnapshot, 1)
        assert main is not None
        main.timestamp = _NOW - timedelta(seconds=299)
        await session.commit()
    clock = [_NOW]
    converter = USDConverter(repo, now=lambda: clock[0])
    if cached:
        assert await converter.to_usd("main", Decimal(1)) == 125
    original = repo.get_active_usd_fx_snapshot_candidates

    async def delayed(currency: str) -> list[UsdFxSnapshotCandidateRow]:
        rows = await original(currency)
        clock[0] += timedelta(seconds=2)
        return rows

    monkeypatch.setattr(repo, "get_active_usd_fx_snapshot_candidates", delayed)
    with pytest.raises(PriceUnavailableError, match="price_stale"):
        await converter.to_usd("main", Decimal(1))


async def test_fx_query_does_not_hold_the_global_main_cache_lock(
    repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Permit a different instrument's main load while the first FX read is pending.

    Given: an EUR call blocked on its independent FX query and an uncached USD market.
    When: the USD request runs before the FX query is released.
    Then: the global cache lock cannot serialize the requests behind the FX await.
    """
    await _seed(repo, _Market("usd-main", base="BTC", price=50, asset_type="crypto"))
    started = asyncio.Event()
    release = asyncio.Event()
    original = repo.get_active_usd_fx_snapshot_candidates

    async def blocked(currency: str) -> list[UsdFxSnapshotCandidateRow]:
        started.set()
        await release.wait()
        return await original(currency)

    monkeypatch.setattr(repo, "get_active_usd_fx_snapshot_candidates", blocked)
    converter = USDConverter(repo, now=lambda: _NOW)
    fx_task = asyncio.create_task(converter.to_usd("main", Decimal(1)))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert await asyncio.wait_for(converter.to_usd("usd-main", Decimal(1)), timeout=1) == 50
    finally:
        release.set()
    with pytest.raises(PriceUnavailableError, match="fx_snapshot_missing"):
        await fx_task


@pytest.mark.parametrize("trap", [False, True])
async def test_nonfinite_final_usd_is_refused(repo: SQLAlchemyRepository, trap: bool) -> None:
    """Contain trapped overflow and nonfinite results from the final FX multiplication.

    Given: a finite quote notional whose conversion exceeds the Decimal exponent range.
    When: the final FX arithmetic runs with overflow trapping on or off.
    Then: no nonfinite USD result escapes the public oracle.
    """
    await _seed(repo, _Market("fx", price=1000))
    with localcontext() as context:
        context.Emax = 4
        context.traps[Overflow] = trap
        with pytest.raises(PriceUnavailableError, match="fx_price_invalid"):
            await USDConverter(repo, now=lambda: _NOW).to_usd("main", Decimal(1))


@pytest.mark.parametrize("leg", ["main", "fx"])
async def test_naive_snapshot_timestamps_fail_closed(
    repo: SQLAlchemyRepository, leg: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject naive boundary timestamps rather than comparing incompatible clocks.

    Given: either current snapshot read returns a naive timestamp at its boundary.
    When: the public converter tries to value an EUR-quoted order.
    Then: a structured refusal identifies the unsupported evidence leg.
    """
    await _seed(repo, _Market("fx"))
    if leg == "main":
        monkeypatch.setattr(
            repo,
            "get_active_market_snapshot_price",
            AsyncMock(return_value=(100, _NOW.replace(tzinfo=None))),
        )
    else:
        rows = await repo.get_active_usd_fx_snapshot_candidates("EUR")
        rows[0]["timestamp"] = _NOW.replace(tzinfo=None)
        monkeypatch.setattr(
            repo, "get_active_usd_fx_snapshot_candidates", AsyncMock(return_value=rows)
        )
    reason = "price_stale" if leg == "main" else "fx_price_stale"
    with pytest.raises(PriceUnavailableError, match=reason):
        await USDConverter(repo, now=lambda: _NOW).to_usd("main", Decimal(1))


@pytest.mark.parametrize("cached", [False, True])
async def test_usd_only_metadata_await_cannot_extend_main_freshness(
    repo: SQLAlchemyRepository, cached: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Validate USD-only returns with the final clock while never fetching FX.

    Given: a USD main price aged 299 seconds and a metadata await lasting two seconds.
    When: the order is valued on a cold or cached main path.
    Then: the now-stale main mark refuses and no FX query has occurred.
    """
    await _seed(
        repo,
        _Market(
            "usd",
            base="BTC",
            asset_type="crypto",
            price=100,
            timestamp=_NOW - timedelta(seconds=299),
        ),
    )
    clock = [_NOW]
    converter = USDConverter(repo, now=lambda: clock[0])
    fx = AsyncMock()
    monkeypatch.setattr(repo, "get_active_usd_fx_snapshot_candidates", fx)
    if cached:
        assert await converter.to_usd("usd", Decimal(1)) == 100
    original = repo.get_active_instrument_valuation_metadata

    async def delayed(identity: str) -> InstrumentValuationMetadataRow | None:
        metadata = await original(identity)
        clock[0] += timedelta(seconds=2)
        return metadata

    monkeypatch.setattr(repo, "get_active_instrument_valuation_metadata", delayed)
    with pytest.raises(PriceUnavailableError, match="price_stale"):
        await converter.to_usd("usd", Decimal(1))
    fx.assert_not_awaited()


async def test_main_lock_wait_uses_a_fresh_clock_before_return(
    repo: SQLAlchemyRepository,
) -> None:
    """A cold request cannot reuse the clock captured before waiting for another load.

    Given: a main mark aged 299 seconds and an already-held cache lock.
    When: a pending conversion resumes two seconds later.
    Then: the final main mark is refused rather than extending freshness across contention.
    """
    await _seed(
        repo,
        _Market(
            "usd",
            base="BTC",
            asset_type="crypto",
            price=100,
            timestamp=_NOW - timedelta(seconds=299),
        ),
    )
    clock = [_NOW]
    converter = USDConverter(repo, now=lambda: clock[0])
    await converter._cache_lock.acquire()
    task = asyncio.create_task(converter.to_usd("usd", Decimal(1)))
    try:
        await asyncio.sleep(0)
        assert not task.done()
        clock[0] += timedelta(seconds=2)
    finally:
        converter._cache_lock.release()
    with pytest.raises(PriceUnavailableError, match="price_stale"):
        await task


@pytest.mark.parametrize("cached", [False, True])
async def test_cache_does_not_admit_future_main_price(
    repo: SQLAlchemyRepository, cached: bool
) -> None:
    """Recheck main future skew on both cold and cached USD-only returns.

    Given: a USD main mark initially at the allowed five-second future boundary.
    When: the reader clock moves backward by one second before a subsequent lookup.
    Then: the six-second-future evidence refuses even inside the cache TTL.
    """
    await _seed(
        repo,
        _Market(
            "usd", base="BTC", asset_type="crypto", price=100, timestamp=_NOW + timedelta(seconds=5)
        ),
    )
    clock = [_NOW]
    converter = USDConverter(repo, now=lambda: clock[0])
    assert await converter.to_usd("usd", Decimal(1)) == 100
    if not cached:
        converter._invalidate("usd")
    clock[0] -= timedelta(seconds=1)
    with pytest.raises(PriceUnavailableError, match="price_stale"):
        await converter.to_usd("usd", Decimal(1))


async def test_admission_stores_cent_usd_and_rolling_cap_never_revalues_it(
    repo: SQLAlchemyRepository,
) -> None:
    """Keep real admission persistence and rolling notional in the same USD units.

    Given: one EUR order converted at 1.25001, then a new FX rate of 0.5.
    When: the first rounded-up admission value is persisted and a second order is checked.
    Then: the cap sums stored 1.26 USD plus new 0.50 USD, not historical EUR or new FX.
    """
    await _seed(repo, _Market("fx", price=1.25001))
    async with repo.session() as session:
        session.add(
            UserTradingCaps(
                public_id="caps",
                user_public_id="user",
                max_daily_notional_usd=Decimal("1.70"),
                timestamp=_NOW,
                session_id="test",
                sequence_id=1,
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()
    converter = USDConverter(repo, now=lambda: _NOW)
    enforcer = TradingCapsEnforcer(repo, converter, now=lambda: _NOW)
    submission = TradeCommandSubmission(
        user_public_id="user",
        operator_public_id="operator",
        wallet_public_id="wallet",
        instrument_public_id="main",
        command_type="create",
        side="buy",
        order_type="limit",
        quantity=Decimal("0.01"),
        price=Decimal(100),
        source_surface="rest",
        idempotency_key=None,
    )
    async with enforcer.guard(submission) as guard:
        assert guard.submitted_notional_usd == 1.26
        async with repo.session() as session:
            session.add(
                TradeCommand(
                    public_id=guard.assigned_public_id,
                    timestamp=_NOW,
                    known_to=KNOWN_TO_MAX,
                    session_id="test",
                    sequence_id=1,
                    command_type="create",
                    shard_key="kraken.BTC-EUR.live",
                    wallet_public_id="wallet",
                    user_public_id="user",
                    exchange="kraken",
                    instrument="BTC-EUR",
                    mode="live",
                    strategy_id="manual",
                    client_order_id="client",
                    venue_client_id="venue",
                    side="buy",
                    order_type="limit",
                    quantity=0.01,
                    price=100,
                    submitted_notional_usd=Decimal(str(guard.submitted_notional_usd)),
                    status="cancelled",
                    created_at=_NOW,
                    correlation_id="correlation",
                )
            )
            await session.commit()
    history = await repo.get_user_recent_submits("user", _NOW - timedelta(hours=24))
    assert [row["submitted_notional_usd"] for row in history] == [1.26]
    async with repo.session() as session:
        snapshot = await session.get(MarketSnapshot, 2)
        assert snapshot is not None
        snapshot.last_price = 0.5
        await session.commit()
    with pytest.raises(CapsViolationError, match="max_daily_notional_usd"):
        async with enforcer.guard(submission):
            pytest.fail("Stored USD history must consume the configured cap")


@pytest.mark.parametrize("price", [1e-308, 1e308])
async def test_inverse_normalization_refuses_nonfinite_or_underflowed_rate(
    repo: SQLAlchemyRepository, price: float
) -> None:
    """Reject an unusable reciprocal even when the raw inverse rate is positive finite.

    Given: an extreme inverse raw rate and a constrained Decimal context.
    When: normalization overflows or underflows without trapping.
    Then: the unusable normalized rate cannot value an order.
    """
    await _seed(repo, _Market("inverse", base="USD", quote="EUR", price=price))
    with localcontext() as context:
        context.prec = 4
        context.Emax = 4
        context.Emin = -2
        context.traps[Overflow] = False
        with pytest.raises(PriceUnavailableError, match="fx_price_invalid"):
            await USDConverter(repo, now=lambda: _NOW).to_usd("main", Decimal(1))


@pytest.mark.parametrize("removed", [False, True])
async def test_metadata_change_during_cold_load_refuses_mixed_quote_evidence(
    repo: SQLAlchemyRepository, removed: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject a main definition removed or relabeled between its separate reads.

    Given: a valid main quote and snapshot whose subsequent current metadata differs.
    When: the converter assembles a cold main-price entry.
    Then: no mixed-currency identity enters the cache or produces USD exposure.
    """
    metadata = await repo.get_active_instrument_valuation_metadata("main")
    assert metadata is not None
    metadata["quote"] = "GBP"
    monkeypatch.setattr(
        repo,
        "get_active_instrument_valuation_metadata",
        AsyncMock(return_value=None if removed else metadata),
    )
    with pytest.raises(PriceUnavailableError, match="instrument_not_found"):
        await USDConverter(repo, now=lambda: _NOW).to_usd("main", Decimal(1))


async def _seed_fx_candle(
    repo: SQLAlchemyRepository,
    market: _Market,
    *,
    open_at: datetime,
    complete: bool = True,
    snapshot_price: float | None = None,
) -> None:
    """Persist an FX plane with a candle and optionally a same-plane snapshot."""
    await _seed(repo, replace(market, snapshot=snapshot_price is not None, price=snapshot_price))
    close_price = market.price if market.price is not None else 1.25
    async with repo.session() as session:
        session.add(
            Candle(
                public_id=f"candle-{market.identity}-{int(open_at.timestamp())}",
                instrument_public_id=market.identity,
                open_at=open_at,
                timeframe="1m",
                open=close_price,
                high=close_price,
                low=close_price,
                close=close_price,
                volume=1.0,
                vwap=close_price,
                trades=1,
                source="calculated",
                complete=complete,
                timestamp=open_at,
                known_to=_NOW if market.closed == "candle" else KNOWN_TO_MAX,
                session_id="test",
                sequence_id=1,
            )
        )
        await session.commit()


@pytest.mark.parametrize(
    "inverse,quote,price,expected", [(False, "EUR", 1.25, "250"), (True, "PLN", 4.0, "50")]
)
async def test_fx_leg_from_candles_in_both_orientations(
    repo: SQLAlchemyRepository,
    inverse: bool,
    quote: str,
    price: float,
    expected: str,
) -> None:
    """Resolve direct EUR-USD and inverse USD-PLN FX planes from 1m candle closes.

    Given: a spot instrument quoted in EUR or PLN with no FX snapshot and a
        fresh active 1m candle on EUR-USD or USD-PLN,
    When: two units are converted to USD,
    Then: both direct and inverse 1m candle orientations produce exact USD notional.
    """
    main_id = f"main-{quote.lower()}"
    await _seed(repo, _Market(main_id, base="BTC", quote=quote, price=100, asset_type="crypto"))
    fx_market = (
        _Market("fx-pln", base="USD", quote="PLN", price=price, exchange="walutomat")
        if inverse
        else _Market("fx-eur", base="EUR", quote="USD", price=price, exchange="kraken")
    )
    await _seed_fx_candle(repo, fx_market, open_at=_NOW - timedelta(seconds=60))
    assert await USDConverter(repo, now=lambda: _NOW).to_usd(main_id, Decimal(2)) == Decimal(
        expected
    )


async def test_fx_plane_conflict_between_candle_and_snapshot_at_equal_timestamps(
    repo: SQLAlchemyRepository,
) -> None:
    """Refuse equal-freshness FX candle and snapshot rates that disagree after normalization.

    Given: an active 1m candle closed at _NOW (1.25) and a snapshot on a
        different venue at _NOW (1.20),
    When: the oracle selects the FX rate,
    Then: ``fx_plane_conflict`` refuses admission.
    """
    await _seed_fx_candle(
        repo,
        _Market("fx-candle", base="EUR", quote="USD", price=1.25, exchange="kraken"),
        open_at=_NOW - timedelta(seconds=60),
    )
    await _seed(
        repo,
        _Market(
            "fx-snap", base="EUR", quote="USD", price=1.20, timestamp=_NOW, exchange="walutomat"
        ),
    )
    with pytest.raises(PriceUnavailableError, match="fx_plane_conflict"):
        await USDConverter(repo, now=lambda: _NOW).to_usd("main", Decimal(2))


@pytest.mark.parametrize("candle_open_age", [60, 120, STALENESS_THRESHOLD_SECONDS + 61])
async def test_fx_candle_displaces_same_plane_snapshot_only(
    repo: SQLAlchemyRepository,
    candle_open_age: int,
) -> None:
    """Same-plane snapshots are ignored even when the candle is stale.

    Given: an FX candle and a fresh snapshot for one plane, followed by
        another plane with only a usable snapshot,
    When: EUR exposure is converted to USD,
    Then: the first plane returns the candle rate or raises ``fx_price_stale``.
        The second plane's snapshot is eligible once it is added.
    """
    await _seed_fx_candle(
        repo,
        _Market("fx-pair", price=1.25, timestamp=_NOW - timedelta(seconds=5)),
        open_at=_NOW - timedelta(seconds=candle_open_age),
        snapshot_price=1.10,
    )
    converter = USDConverter(repo, now=lambda: _NOW)
    if candle_open_age > STALENESS_THRESHOLD_SECONDS + 60:
        with pytest.raises(PriceUnavailableError, match="fx_price_stale"):
            await converter.to_usd("main", Decimal(2))
    else:
        assert await converter.to_usd("main", Decimal(2)) == Decimal("250")
    await _seed(
        repo,
        _Market(
            "fx-snapshot-only",
            price=1.50,
            timestamp=_NOW + timedelta(seconds=1),
            exchange="walutomat",
        ),
    )
    assert await converter.to_usd("main", Decimal(2)) == Decimal("300")


async def test_fx_candle_staleness_invalid_and_naive_guards(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FX 1m candles obey identical age, skew, naive, and positivity guards.

    Given: stale, future-skewed, naive, and invalid FX candle candidates,
    When: EUR exposure is converted to USD,
    Then: stale, skewed, and naive timestamps raise ``fx_price_stale`` while
        nonpositive close raises ``fx_price_invalid``.
    """
    await _seed_fx_candle(
        repo,
        _Market("fx-stale", base="EUR", quote="USD", price=1.25),
        open_at=_NOW - timedelta(seconds=STALENESS_THRESHOLD_SECONDS + 61),
        snapshot_price=1.5,
    )
    with pytest.raises(PriceUnavailableError, match="fx_price_stale"):
        await USDConverter(repo, now=lambda: _NOW).to_usd("main", Decimal(1))

    skewed: UsdFxCandleCandidateRow = {
        "candle_public_id": "c-skew",
        "instrument_public_id": "fx-stale",
        "exchange": "kraken",
        "base": "EUR",
        "quote": "USD",
        "close": 1.25,
        "open_at": _NOW + timedelta(seconds=MAX_FUTURE_SKEW_SECONDS + 1),
    }
    monkeypatch.setattr(
        repo, "get_active_usd_fx_candle_candidates", AsyncMock(return_value=[skewed])
    )
    with pytest.raises(PriceUnavailableError, match="fx_price_stale"):
        await USDConverter(repo, now=lambda: _NOW).to_usd("main", Decimal(1))

    naive: UsdFxCandleCandidateRow = {**skewed, "open_at": _NOW.replace(tzinfo=None)}
    monkeypatch.setattr(
        repo, "get_active_usd_fx_candle_candidates", AsyncMock(return_value=[naive])
    )
    with pytest.raises(PriceUnavailableError, match="fx_price_stale"):
        await USDConverter(repo, now=lambda: _NOW).to_usd("main", Decimal(1))

    zero_price: UsdFxCandleCandidateRow = {**skewed, "open_at": _NOW, "close": 0.0}
    monkeypatch.setattr(
        repo, "get_active_usd_fx_candle_candidates", AsyncMock(return_value=[zero_price])
    )
    with pytest.raises(PriceUnavailableError, match="fx_price_invalid"):
        await USDConverter(repo, now=lambda: _NOW).to_usd("main", Decimal(1))


@pytest.mark.parametrize("inverse_price,expected", [(0.8, "250"), (1.25, None)])
async def test_equal_time_candle_and_snapshot_compare_normalized_fx_rates(
    repo: SQLAlchemyRepository,
    inverse_price: float,
    expected: str | None,
) -> None:
    """Equal-time inverse FX candle and direct snapshot agree or conflict after normalization.

    Given: a USD-EUR 1m candle closed at _NOW and an EUR-USD snapshot at _NOW,
    When: normalized USD-per-EUR rates are compared,
    Then: reciprocal rates succeed deterministically while equal raw rates raise
        ``fx_plane_conflict``.
    """
    await _seed_fx_candle(
        repo,
        _Market("fx-inv-candle", base="USD", quote="EUR", price=inverse_price, exchange="kraken"),
        open_at=_NOW - timedelta(seconds=60),
    )
    await _seed(
        repo,
        _Market(
            "fx-dir-snap", base="EUR", quote="USD", price=1.25, timestamp=_NOW, exchange="walutomat"
        ),
    )
    converter = USDConverter(repo, now=lambda: _NOW)
    if expected is None:
        with pytest.raises(PriceUnavailableError, match="fx_plane_conflict"):
            await converter.to_usd("main", Decimal(2))
    else:
        assert await converter.to_usd("main", Decimal(2)) == Decimal(expected)


async def test_fx_candles_are_reread_per_call_and_skipped_for_usd(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FX 1m candle evidence is reread on every conversion and skipped for USD quotes.

    Given: a USD-quoted main instrument and an EUR-quoted main instrument,
    When: ``to_usd`` runs once for USD and twice for EUR inside the main cache TTL,
    Then: ``get_active_usd_fx_candle_candidates`` is not awaited for USD and is
        awaited twice for EUR.
    """
    await _seed(repo, _Market("usd-only", base="BTC", quote="USD", price=50, asset_type="crypto"))
    await _seed_fx_candle(
        repo,
        _Market("fx-eur", base="EUR", quote="USD", price=1.25, exchange="kraken"),
        open_at=_NOW - timedelta(seconds=60),
    )
    fx_candles = AsyncMock(wraps=repo.get_active_usd_fx_candle_candidates)
    monkeypatch.setattr(repo, "get_active_usd_fx_candle_candidates", fx_candles)
    converter = USDConverter(repo, now=lambda: _NOW)

    assert await converter.to_usd("usd-only", Decimal(2)) == Decimal("100")
    fx_candles.assert_not_awaited()

    assert await converter.to_usd("main", Decimal(2)) == Decimal("250")
    assert await converter.to_usd("main", Decimal(2)) == Decimal("250")
    assert fx_candles.await_count == 2


def test_inverse_fx_normalization_overflow_is_refused() -> None:
    """Treat a trapped overflow while inverting a USD-based quote as unusable evidence.

    Given: a positive finite USD-based FX price whose inverse exceeds the Decimal exponent range.
    When: the rate is normalized with overflow trapping enabled.
    Then: no rate is produced instead of the exception escaping.
    """
    candidate: UsdFxSnapshotCandidateRow = {
        "snapshot_public_id": "snap-overflow",
        "instrument_public_id": "usd-pln",
        "exchange": "walutomat",
        "base": "USD",
        "quote": "PLN",
        "last_price": 1e-300,
        "timestamp": _NOW,
    }
    with localcontext() as context:
        context.Emax = 4
        context.traps[Overflow] = True
        assert _normalized_fx_rate(candidate) is None
