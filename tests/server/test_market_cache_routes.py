"""Route tests for :mod:`snapper.server.market_cache_routes`.

Direct-call style mirroring ``tests/server/test_scope_grant_routes.py``:
each test invokes the handler coroutine with mocked dependencies + a
mocked :class:`Request` so the FastAPI plumbing stays out of scope.

Coverage targets: timeframe routing (1m cache, 5/15/30m derived,
1h/4h/1d DB fallback), is_warm flag flips, source discriminator,
exchange + timeframe validation, stats placeholder + 404, health
snapshot, cache-unavailable 503 paths.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.application.services.market_cache import CandleSnap
from snapper.application.services.market_cache import PairStats
from snapper.application.services.market_stats import PairSpec
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.types import ExchangeEnum
from snapper.data.repository_types import CandleRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.market_cache_routes import _derive_candles
from snapper.server.market_cache_routes import get_cache_health
from snapper.server.market_cache_routes import get_cached_candles
from snapper.server.market_cache_routes import get_cached_pair_stats


def _principal() -> AuthPrincipal:
    """Build an ADMIN principal for the RBAC-gated handlers."""
    return AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id="00000000-0000-7000-8000-000000000099",
    )


def _make_request(
    *,
    cache: Any = None,
    stats_worker: Any = None,
    policy: Any = None,
) -> Request:
    """Build a mock :class:`Request` with the FastAPI state attributes."""
    request = MagicMock(spec=Request)
    request.app.state.rest_tracker = SequenceTracker()
    request.app.state.market_cache = cache
    request.app.state.market_stats_worker = stats_worker
    request.app.state.market_persist_policy = policy
    return request


def _snap(open_at_ms: int, close: float) -> CandleSnap:
    """Tiny :class:`CandleSnap` builder for the derived-candle tests."""
    return CandleSnap(
        open_at_ms=open_at_ms,
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=10.0,
    )


def _row(open_at_ms: int, close: float, timeframe: str = "1h") -> CandleRow:
    """Stub :class:`CandleRow` for the DB-fallback path."""
    return CandleRow(
        open_at=datetime.fromtimestamp(open_at_ms / 1000, tz=UTC),
        timeframe=timeframe,
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=10.0,
        vwap=None,
        trades=None,
        public_id="00000000-0000-7000-8000-0000000000aa",
        timestamp=datetime.fromtimestamp(open_at_ms / 1000, tz=UTC),
        session_id="seed",
        sequence_id=1,
    )


def _stub_cache(snaps: list[CandleSnap]) -> MagicMock:
    """Mock :class:`MarketCacheService` returning the given snaps for any read."""
    cache = MagicMock()
    cache.get_1m_candles = AsyncMock(return_value=snaps)
    cache.cache_capacity_per_instrument = MagicMock(return_value=100)
    cache.instruments_cached = AsyncMock(return_value=len(snaps))
    cache.pair_stats_keys = AsyncMock(return_value=[])
    cache.get_pair_stats = AsyncMock(return_value=None)
    return cache


def _stub_repo(rows: list[CandleRow]) -> MagicMock:
    """Mock :class:`Repository` returning the given rows for ``get_candles``."""
    repo = MagicMock()
    repo.get_candles = AsyncMock(return_value=rows)
    return repo


class TestDeriveCandlesHelper:
    """``_derive_candles`` aggregates fixed-size windows + drops partial tails."""

    def test_aggregates_full_windows(self) -> None:
        """Five 1m bars aggregate into one 5m bar with correct OHLCV."""
        snaps = [
            _snap(0 * 60_000, 1.0),
            _snap(1 * 60_000, 2.0),
            _snap(2 * 60_000, 1.5),
            _snap(3 * 60_000, 0.5),
            _snap(4 * 60_000, 3.0),
        ]
        derived = _derive_candles(snaps, minutes_per_bar=5)
        assert len(derived) == 1
        bar = derived[0]
        assert bar.open == 1.0
        assert bar.close == 3.0
        assert bar.high == pytest.approx(4.0)
        assert bar.low == pytest.approx(-0.5)
        assert bar.volume == pytest.approx(50.0)

    def test_drops_partial_trailing_bucket(self) -> None:
        """A 7-snap input with bucket size 5 yields one bar (5 used, 2 dropped)."""
        snaps = [_snap(i * 60_000, float(i)) for i in range(7)]
        derived = _derive_candles(snaps, minutes_per_bar=5)
        assert len(derived) == 1

    def test_empty_input_returns_empty(self) -> None:
        """No snaps means no derived bars."""
        assert _derive_candles([], minutes_per_bar=5) == []

    def test_minutes_per_bar_one_is_identity(self) -> None:
        """``minutes_per_bar=1`` passes input through unchanged."""
        snaps = [_snap(0, 1.0), _snap(60_000, 2.0)]
        assert _derive_candles(snaps, minutes_per_bar=1) == snaps


class TestCandlesRoute:
    """``get_cached_candles`` routes through 1m / derived / DB paths."""

    @pytest.mark.asyncio
    async def test_one_minute_path_returns_cache_source(self) -> None:
        """``timeframe=1m`` reads the cache deque + flags ``source=cache``."""
        snaps = [_snap(i * 60_000, float(i)) for i in range(100)]
        cache = _stub_cache(snaps)
        result = await get_cached_candles(
            request=_make_request(cache=cache),
            _principal=_principal(),
            exchange="kraken",
            native_symbol="BTC-USD",
            repo=cast(Any, _stub_repo([])),
            timeframe="1m",
            limit=100,
        )
        assert result.payload.source == "cache"
        assert result.payload.sample_count == 100
        assert result.payload.is_warm is True
        assert all(c.timeframe == "1m" for c in result.payload.candles)

    @pytest.mark.asyncio
    async def test_cold_one_minute_returns_is_warm_false(self) -> None:
        """A short cache slice flags ``is_warm=False``."""
        snaps = [_snap(0, 1.0)]
        cache = _stub_cache(snaps)
        result = await get_cached_candles(
            request=_make_request(cache=cache),
            _principal=_principal(),
            exchange="kraken",
            native_symbol="BTC-USD",
            repo=cast(Any, _stub_repo([])),
            timeframe="1m",
            limit=100,
        )
        assert result.payload.is_warm is False

    @pytest.mark.asyncio
    async def test_derived_five_minute_aggregates_from_one_minute(self) -> None:
        """A 5m fetch aggregates the 1m deque + flags ``source=derived``."""
        snaps = [_snap(i * 60_000, float(i)) for i in range(100)]
        cache = _stub_cache(snaps)
        result = await get_cached_candles(
            request=_make_request(cache=cache),
            _principal=_principal(),
            exchange="kraken",
            native_symbol="BTC-USD",
            repo=cast(Any, _stub_repo([])),
            timeframe="5m",
            limit=20,
        )
        assert result.payload.source == "derived"
        assert result.payload.sample_count == 20
        assert all(c.timeframe == "5m" for c in result.payload.candles)

    @pytest.mark.asyncio
    async def test_db_fallback_for_one_hour_timeframe(self) -> None:
        """``timeframe=1h`` falls through to the repository + flags ``source=db``."""
        rows = [_row(i * 60_000 * 60, float(i), timeframe="1h") for i in range(24)]
        repo = _stub_repo(rows)
        result = await get_cached_candles(
            request=_make_request(cache=_stub_cache([])),
            _principal=_principal(),
            exchange="kraken",
            native_symbol="BTC-USD",
            repo=cast(Any, repo),
            timeframe="1h",
            limit=24,
        )
        assert result.payload.source == "db"
        assert result.payload.sample_count == 24
        repo.get_candles.assert_awaited()

    @pytest.mark.asyncio
    async def test_invalid_timeframe_raises_400(self) -> None:
        """An unsupported timeframe raises HTTP 400 before any read."""
        with pytest.raises(HTTPException) as exc:
            await get_cached_candles(
                request=_make_request(cache=_stub_cache([])),
                _principal=_principal(),
                exchange="kraken",
                native_symbol="BTC-USD",
                repo=cast(Any, _stub_repo([])),
                timeframe="2m",
                limit=100,
            )
        assert exc.value.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.asyncio
    async def test_invalid_exchange_raises_400(self) -> None:
        """An exchange outside :data:`AllExchange` raises HTTP 400."""
        with pytest.raises(HTTPException) as exc:
            await get_cached_candles(
                request=_make_request(cache=_stub_cache([])),
                _principal=_principal(),
                exchange="bogus",
                native_symbol="BTC-USD",
                repo=cast(Any, _stub_repo([])),
                timeframe="1m",
                limit=100,
            )
        assert exc.value.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.asyncio
    async def test_one_minute_without_cache_returns_503(self) -> None:
        """Missing cache state on a 1m request raises HTTP 503."""
        with pytest.raises(HTTPException) as exc:
            await get_cached_candles(
                request=_make_request(cache=None),
                _principal=_principal(),
                exchange="kraken",
                native_symbol="BTC-USD",
                repo=cast(Any, _stub_repo([])),
                timeframe="1m",
                limit=100,
            )
        assert exc.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    @pytest.mark.asyncio
    async def test_derived_without_cache_returns_503(self) -> None:
        """Missing cache state on a 5m derived request raises HTTP 503."""
        with pytest.raises(HTTPException) as exc:
            await get_cached_candles(
                request=_make_request(cache=None),
                _principal=_principal(),
                exchange="kraken",
                native_symbol="BTC-USD",
                repo=cast(Any, _stub_repo([])),
                timeframe="5m",
                limit=100,
            )
        assert exc.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE


class TestStatsRoute:
    """``get_cached_pair_stats`` routes through configured / unconfigured / placeholder."""

    def _worker_with_pair(self, left: str, right: str) -> MagicMock:
        worker = MagicMock()
        worker.configured_pairs.return_value = [
            PairSpec(
                left=(ExchangeEnum.KRAKEN, "BTC-USD"),
                right=(ExchangeEnum.KRAKEN, "ETH-USD"),
                left_str=left,
                right_str=right,
            )
        ]
        return worker

    @pytest.mark.asyncio
    async def test_configured_pair_with_stats_returns_payload(self) -> None:
        """A configured pair with computed stats returns a populated envelope."""
        cache = _stub_cache([])
        cache.get_pair_stats = AsyncMock(
            return_value=PairStats(
                pearson_r=0.95,
                pearson_n=42,
                coint_t=-3.5,
                coint_pvalue=0.01,
                coint_critical_values=(-3.9, -3.3, -3.0),
                computed_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC),
                sample_count=42,
                is_warm=True,
            )
        )
        worker = self._worker_with_pair("kraken:BTC-USD", "kraken:ETH-USD")
        result = await get_cached_pair_stats(
            request=_make_request(cache=cache, stats_worker=worker),
            _principal=_principal(),
            exchange_a="kraken",
            symbol_a="BTC-USD",
            exchange_b="kraken",
            symbol_b="ETH-USD",
        )
        assert result.payload.pearson_r == pytest.approx(0.95)
        assert result.payload.is_warm is True

    @pytest.mark.asyncio
    async def test_configured_pair_without_stats_returns_placeholder(self) -> None:
        """A configured pair with no computed stats returns ``is_warm=False``."""
        cache = _stub_cache([])
        cache.get_pair_stats = AsyncMock(return_value=None)
        worker = self._worker_with_pair("kraken:BTC-USD", "kraken:ETH-USD")
        result = await get_cached_pair_stats(
            request=_make_request(cache=cache, stats_worker=worker),
            _principal=_principal(),
            exchange_a="kraken",
            symbol_a="BTC-USD",
            exchange_b="kraken",
            symbol_b="ETH-USD",
        )
        assert result.payload.is_warm is False
        assert result.payload.pearson_r is None

    @pytest.mark.asyncio
    async def test_unconfigured_pair_returns_404(self) -> None:
        """A pair outside ``market_stats_pairs`` raises HTTP 404."""
        cache = _stub_cache([])
        worker = self._worker_with_pair("kraken:BTC-USD", "kraken:ETH-USD")
        with pytest.raises(HTTPException) as exc:
            await get_cached_pair_stats(
                request=_make_request(cache=cache, stats_worker=worker),
                _principal=_principal(),
                exchange_a="kraken",
                symbol_a="OTHER",
                exchange_b="kraken",
                symbol_b="MORE",
            )
        assert exc.value.status_code == status.HTTP_404_NOT_FOUND

    @pytest.mark.asyncio
    async def test_missing_worker_returns_503(self) -> None:
        """No stats worker on app.state raises HTTP 503."""
        with pytest.raises(HTTPException) as exc:
            await get_cached_pair_stats(
                request=_make_request(cache=_stub_cache([]), stats_worker=None),
                _principal=_principal(),
                exchange_a="kraken",
                symbol_a="BTC-USD",
                exchange_b="kraken",
                symbol_b="ETH-USD",
            )
        assert exc.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    @pytest.mark.asyncio
    async def test_invalid_exchange_raises_400(self) -> None:
        """An invalid exchange path parameter raises HTTP 400."""
        with pytest.raises(HTTPException) as exc:
            await get_cached_pair_stats(
                request=_make_request(),
                _principal=_principal(),
                exchange_a="bogus",
                symbol_a="BTC-USD",
                exchange_b="kraken",
                symbol_b="ETH-USD",
            )
        assert exc.value.status_code == status.HTTP_400_BAD_REQUEST


class TestHealthRoute:
    """``get_cache_health`` snapshot of cache + policy."""

    @pytest.mark.asyncio
    async def test_health_returns_snapshot(self) -> None:
        """The health route reports cache + persist universe counts."""
        cache = _stub_cache([])
        cache.instruments_cached = AsyncMock(return_value=7)
        cache.pair_stats_keys = AsyncMock(return_value=[("a", "b"), ("c", "d")])
        policy = MagicMock()
        policy.iter_persisted_instruments.return_value = iter(
            [
                (ExchangeEnum.KRAKEN, "BTC-USD"),
                (ExchangeEnum.KRAKEN, "ETH-USD"),
            ]
        )
        result = await get_cache_health(
            request=_make_request(cache=cache, policy=policy),
            _principal=_principal(),
        )
        assert result.payload.instruments_cached == 7
        assert result.payload.pairs_cached == 2
        assert result.payload.persist_universe_size == 2

    @pytest.mark.asyncio
    async def test_health_without_cache_returns_503(self) -> None:
        """Missing cache state on health raises HTTP 503."""
        with pytest.raises(HTTPException) as exc:
            await get_cache_health(
                request=_make_request(cache=None),
                _principal=_principal(),
            )
        assert exc.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE

    @pytest.mark.asyncio
    async def test_health_handles_policy_failure(self) -> None:
        """A policy iter exception logs + returns zero persist universe size."""
        cache = _stub_cache([])
        cache.instruments_cached = AsyncMock(return_value=3)
        policy = MagicMock()
        policy.iter_persisted_instruments.side_effect = RuntimeError("policy fail")
        result = await get_cache_health(
            request=_make_request(cache=cache, policy=policy),
            _principal=_principal(),
        )
        assert result.payload.persist_universe_size == 0
        assert result.payload.instruments_cached == 3

    @pytest.mark.asyncio
    async def test_health_without_policy_returns_zero_universe(self) -> None:
        """A missing policy on app.state yields ``persist_universe_size=0``."""
        cache = _stub_cache([])
        cache.instruments_cached = AsyncMock(return_value=2)
        result = await get_cache_health(
            request=_make_request(cache=cache, policy=None),
            _principal=_principal(),
        )
        assert result.payload.persist_universe_size == 0
