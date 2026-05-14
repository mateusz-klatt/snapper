"""Route tests for :mod:`snapper.server.market_cache_routes`.

The cache candle read used to live here at ``/candles/{exchange}/
{native_symbol}`` but was migrated 2026-05-14 into the candles router
at ``/api/candles/cache?...``; see
:mod:`tests.application.services.test_candle_query` for the smart-
routing service tests and :mod:`tests.server.test_server_app` for the
new route-level integration tests.

This module covers the surviving diagnostic endpoints — the stats
route and the health route.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.application.services.market_cache import PairStats
from snapper.application.services.market_stats import PairSpec
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.types import ExchangeEnum
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.market_cache_routes import get_cache_health
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


def _stub_cache() -> MagicMock:
    """Mock :class:`MarketCacheService` for stats + health probes."""
    cache = MagicMock()
    cache.instruments_cached = AsyncMock(return_value=0)
    cache.pair_stats_keys = AsyncMock(return_value=[])
    cache.get_pair_stats = AsyncMock(return_value=None)
    return cache


class TestStatsRoute:
    """``get_cached_pair_stats`` routes through configured / unconfigured / placeholder."""

    def _worker_with_pair(self, left: str, right: str) -> MagicMock:
        """Build a mock worker that exposes a single configured pair."""
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
        cache = _stub_cache()
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
        cache = _stub_cache()
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
        cache = _stub_cache()
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
                request=_make_request(cache=_stub_cache(), stats_worker=None),
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
        cache = _stub_cache()
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
        cache = _stub_cache()
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
        cache = _stub_cache()
        cache.instruments_cached = AsyncMock(return_value=2)
        result = await get_cache_health(
            request=_make_request(cache=cache, policy=None),
            _principal=_principal(),
        )
        assert result.payload.persist_universe_size == 0
