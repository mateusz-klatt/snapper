"""Tests for the P2 DB-warmup generalization.

Covers plan §P2 (``plan_2026_07_03_strategy_runtime_split_and_mcp_wake.md``):
timeframe-generic DB warmup, per-instrument mode for single-leg
strategies, fail-closed aligned mode for multi-leg, the pre-callback
warm-up floor (warmed bars are context, never triggers, and never
mutate strategy-local signal state), and the RSI/MACD lookback
declarations that make indicator strategies signal-ready on the first
live bar.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from snapper.application.services.signals.service import signal_service
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.topics.builders import parse_market_topic
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import StrategyConfig
from snapper.strategies.base import StrategySignal
from snapper.strategies.cointegration import CointegrationPairs
from snapper.strategies.macd import MACDCrossover
from snapper.strategies.rsi import RSIReversion

_BASE_OPEN_AT = datetime(2026, 7, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _mock_signal_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace signal persistence with a no-op for these unit tests."""
    monkeypatch.setattr(signal_service, "store_signal", AsyncMock(return_value=""))


class _RecordingStrategy(BaseStrategy):
    """Single-leg strategy recording callback invocations and emitting a signal."""

    def __init__(self, config: StrategyConfig) -> None:
        """Initialize with an empty callback log."""
        super().__init__(config)
        self.seen: list[datetime] = []

    def required_candle_history(self) -> int:
        """Return the configured warm-up bar count (0 when unset)."""
        return int(self.params.get("warmup_n", 0))

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Record the invocation and emit a plain buy signal."""
        self.seen.append(candle.open_at)
        return StrategySignal(
            instrument=instrument,
            side="buy",
            strength=0.5,
            reason="recorded",
            price=candle.close,
        )

    async def reset(self) -> None:
        """Reset the callback log for replay."""
        self.seen.clear()


def _config(
    *,
    inputs: list[str] | None = None,
    params: dict[str, Any] | None = None,
    strategy_class: str = "_RecordingStrategy",
    outputs: list[str] | None = None,
) -> StrategyConfig:
    """Build a kraken-input paper config for warmup tests."""
    return StrategyConfig(
        name="warmup_gen",
        strategy_class=strategy_class,
        inputs=inputs or ["market.kraken.BTC-USD.candles.1h"],
        outputs=outputs or ["BTC-USD"],
        exchange="paper",
        params=params or {"warmup_n": 3},
    )


def _db_row(index: int, *, timeframe: str = "1h", complete: bool = True) -> dict[str, Any]:
    """Build a persisted CandleRow dict at hour-offset ``index``."""
    open_at = _BASE_OPEN_AT + timedelta(hours=index)
    return {
        "open_at": open_at,
        "timeframe": timeframe,
        "open": 100.0 + index,
        "high": 101.0 + index,
        "low": 99.0 + index,
        "close": 100.5 + index,
        "volume": 5.0,
        "vwap": 100.4 + index,
        "trades": 3,
        "source": "synthesized",
        "complete": complete,
        "public_id": f"00000000-0000-7000-8000-0000000000{index:02x}",
        "timestamp": open_at,
        "session_id": "seed",
        "sequence_id": index,
    }


class _StubRepo:
    """Repository stub honoring the complete predicate per instrument."""

    def __init__(self, rows_by_symbol: dict[str, list[dict[str, Any]]]) -> None:
        """Bind the configured rows."""
        self._rows = rows_by_symbol
        self.calls: list[dict[str, Any]] = []

    async def get_candles(
        self,
        *,
        instrument: str,
        timeframe: str,
        start: Any,
        end: Any,
        exchange: Any,
        as_of: Any,
        limit: int | None = None,
        order: str = "asc",
        complete: bool | None = None,
    ) -> list[dict[str, Any]]:
        """Return configured rows filtered like the SQL implementation."""
        self.calls.append({"instrument": instrument, "timeframe": timeframe, "complete": complete})
        rows = [r for r in self._rows.get(instrument, []) if r["timeframe"] == timeframe]
        if complete is not None:
            rows = [r for r in rows if bool(r["complete"]) is complete]
        rows = sorted(rows, key=lambda r: r["open_at"], reverse=order == "desc")
        return rows[:limit] if limit is not None else rows


def _wire_repo(monkeypatch: pytest.MonkeyPatch, repo: _StubRepo) -> None:
    """Point the warmup's repository accessor at the stub."""
    monkeypatch.setattr("snapper.strategies.base.get_repository", lambda url: repo)


def _candle_payload(open_at: datetime, *, close: float = 200.0) -> str:
    """Build a live 1h candle JSON payload at the given window."""
    return CandleData(
        session_id="live",
        sequence_id=1,
        public_id="live-1",
        timestamp=open_at,
        instrument="BTC-USD",
        exchange="kraken",
        timeframe="1h",
        open_at=open_at,
        open=close - 1,
        high=close + 1,
        low=close - 2,
        close=close,
        volume=7.0,
    ).model_dump_json()


class TestPerInstrumentMode:
    """Single-candle-leg strategies warm per-instrument from the DB."""

    @pytest.mark.asyncio
    async def test_single_leg_warms_1h_from_db_without_crypto_optin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a 1h leg warms via the DB with correct timeframe stamping.

        Given: Three complete persisted 1h rows and no warmup_market_type,
        When: Warmup runs,
        Then: The buffer holds the bars stamped 1h, the read used the
            leg's timeframe with complete=True, and high-water is set.
        """
        repo = _StubRepo({"BTC-USD": [_db_row(i) for i in range(3)]})
        _wire_repo(monkeypatch, repo)
        strategy = _RecordingStrategy(_config())
        await strategy._warmup_candle_buffer()
        buffer = strategy.candle_buffer["BTC-USD"]
        assert [c.open_at for c in buffer] == [_BASE_OPEN_AT + timedelta(hours=i) for i in range(3)]
        assert all(c.timeframe == "1h" for c in buffer)
        assert repo.calls == [{"instrument": "BTC-USD", "timeframe": "1h", "complete": True}]
        assert strategy._warmup_high_water["BTC-USD"] == _BASE_OPEN_AT + timedelta(hours=2)

    @pytest.mark.asyncio
    async def test_single_leg_short_plane_installs_partial(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a short plane still installs what exists.

        Given: Only two complete rows against a lookback of three,
        When: Warmup runs,
        Then: Both rows install and high-water tracks the newest one.
        """
        repo = _StubRepo({"BTC-USD": [_db_row(i) for i in range(2)]})
        _wire_repo(monkeypatch, repo)
        strategy = _RecordingStrategy(_config())
        await strategy._warmup_candle_buffer()
        assert len(strategy.candle_buffer["BTC-USD"]) == 2
        assert strategy._warmup_high_water["BTC-USD"] == _BASE_OPEN_AT + timedelta(hours=1)

    @pytest.mark.asyncio
    async def test_single_leg_empty_plane_stays_live_only(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify an empty plane leaves the strategy live-only.

        Given: No persisted rows,
        When: Warmup runs,
        Then: No buffer and no high-water are installed.
        """
        repo = _StubRepo({})
        _wire_repo(monkeypatch, repo)
        strategy = _RecordingStrategy(_config())
        await strategy._warmup_candle_buffer()
        assert "BTC-USD" not in strategy.candle_buffer
        assert strategy._warmup_high_water == {}

    @pytest.mark.asyncio
    async def test_provisional_rows_excluded_from_warmup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify provisional rows never enter the warmed buffer.

        Given: Two complete rows and a provisional newest row,
        When: Warmup runs,
        Then: Only the complete rows install and high-water ignores the
            provisional window.
        """
        rows = [_db_row(0), _db_row(1), _db_row(2, complete=False)]
        repo = _StubRepo({"BTC-USD": rows})
        _wire_repo(monkeypatch, repo)
        strategy = _RecordingStrategy(_config())
        await strategy._warmup_candle_buffer()
        assert [c.open_at for c in strategy.candle_buffer["BTC-USD"]] == [
            _BASE_OPEN_AT,
            _BASE_OPEN_AT + timedelta(hours=1),
        ]
        assert strategy._warmup_high_water["BTC-USD"] == _BASE_OPEN_AT + timedelta(hours=1)

    @pytest.mark.asyncio
    async def test_candles_topic_without_timeframe_is_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a timeframe-less candles topic never becomes a warmup leg.

        Given: A candles input topic missing its timeframe segment,
        When: Warmup runs,
        Then: No leg qualifies and the repository is never queried.
        """
        repo = _StubRepo({"BTC-USD": [_db_row(0)]})
        _wire_repo(monkeypatch, repo)
        strategy = _RecordingStrategy(_config(inputs=["market.kraken.BTC-USD.candles"]))
        await strategy._warmup_candle_buffer()
        assert repo.calls == []
        assert strategy.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_zero_lookback_skips_warmup(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify the default zero lookback never touches the repository.

        Given: A strategy with required_candle_history()==0,
        When: Warmup runs,
        Then: The repository is never queried.
        """
        repo = _StubRepo({"BTC-USD": [_db_row(0)]})
        _wire_repo(monkeypatch, repo)
        strategy = _RecordingStrategy(_config(params={"warmup_n": 0}))
        await strategy._warmup_candle_buffer()
        assert repo.calls == []


class TestCacheLegLoader:
    """Direct coverage of the Polygon-cache leg loader edge branches."""

    def test_non_paper_leg_without_crypto_ticker_returns_empty(self, tmp_path: Path) -> None:
        """Verify a non-paper, non-BASE-QUOTE leg yields no cache rows.

        Given: A kraken (non-paper) candles leg whose symbol has no dash,
        When: _load_warmup_leg runs against any cache root,
        Then: The topic exchange is used and the loader returns [] (no
            Polygon crypto ticker can be derived).
        """
        parsed = parse_market_topic("market.kraken.BTCUSD.candles.1d")
        assert parsed is not None
        strategy = _RecordingStrategy(_config(inputs=["market.kraken.BTC-USD.candles.1h"]))
        assert strategy._load_warmup_leg(parsed, tmp_path, 3, datetime.now(UTC).date()) == []


class TestAlignedFailClosed:
    """Multi-leg strategies always warm aligned ALL-OR-NOTHING."""

    @pytest.mark.asyncio
    async def test_two_legs_one_short_installs_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a short leg blocks the whole multi-leg warmup.

        Given: One full leg and one short leg without a crypto cache,
        When: Warmup runs on a two-leg strategy,
        Then: NOTHING installs (fail-closed aligned mode).
        """
        repo = _StubRepo(
            {
                "BTC-USD": [_db_row(i) for i in range(3)],
                "ETH-USD": [_db_row(0)],
            }
        )
        _wire_repo(monkeypatch, repo)
        strategy = _RecordingStrategy(
            _config(
                inputs=[
                    "market.kraken.BTC-USD.candles.1h",
                    "market.kraken.ETH-USD.candles.1h",
                ],
                outputs=["BTC-USD", "ETH-USD"],
            )
        )
        await strategy._warmup_candle_buffer()
        assert strategy.candle_buffer == {}
        assert strategy._warmup_high_water == {}

    @pytest.mark.asyncio
    async def test_mixed_timeframes_install_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify aligned mode rejects a mixed-timeframe leg set.

        Given: A 1h leg and a 1d leg on one strategy,
        When: Warmup runs,
        Then: NOTHING installs and the repository is never queried.
        """
        repo = _StubRepo({"BTC-USD": [_db_row(i) for i in range(3)]})
        _wire_repo(monkeypatch, repo)
        strategy = _RecordingStrategy(
            _config(
                inputs=[
                    "market.kraken.BTC-USD.candles.1h",
                    "market.kraken.ETH-USD.candles.1d",
                ],
                outputs=["BTC-USD", "ETH-USD"],
            )
        )
        await strategy._warmup_candle_buffer()
        assert strategy.candle_buffer == {}
        assert repo.calls == []

    def test_alignment_hooks(self) -> None:
        """Verify the alignment contract: base False, cointegration True.

        Given: The base default and the cointegration override,
        When: requires_aligned_warmup is read,
        Then: Base strategies default to per-instrument and the pair
            strategy pins aligned mode.
        """
        assert _RecordingStrategy(_config()).requires_aligned_warmup() is False
        pair = CointegrationPairs(
            StrategyConfig(
                name="pair",
                strategy_class="CointegrationPairs",
                inputs=[
                    "market.kraken.BTC-USD.candles.1d",
                    "market.kraken.ETH-USD.candles.1d",
                ],
                outputs=["BTC-USD", "ETH-USD"],
                exchange="paper",
                params={"lookback_window": 5},
            )
        )
        assert pair.requires_aligned_warmup() is True


class TestWarmupFloor:
    """Warmed bars are context: buffered, never dispatched to the callback."""

    @pytest.mark.asyncio
    async def test_warmed_window_bar_buffers_without_callback(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a bar at the high-water is buffered but suppressed.

        Given: A warmed strategy whose high-water is the latest warmed bar,
        When: A live re-publish of that window arrives,
        Then: The buffer upserts it, the callback never runs, and no
            signal group returns.
        """
        repo = _StubRepo({"BTC-USD": [_db_row(i) for i in range(3)]})
        _wire_repo(monkeypatch, repo)
        strategy = _RecordingStrategy(_config())
        await strategy._warmup_candle_buffer()
        high_water = strategy._warmup_high_water["BTC-USD"]
        group = await strategy._handle_candle_data(
            "BTC-USD", _candle_payload(high_water, close=250.0)
        )
        assert group == []
        assert strategy.seen == []
        assert strategy.candle_buffer["BTC-USD"][-1].close == 250.0

    @pytest.mark.asyncio
    async def test_first_post_warmup_bar_reaches_callback(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify the first post-warm-up bar dispatches normally.

        Given: A warmed strategy,
        When: A bar one window past the high-water arrives,
        Then: The callback runs and its signal group returns.
        """
        repo = _StubRepo({"BTC-USD": [_db_row(i) for i in range(3)]})
        _wire_repo(monkeypatch, repo)
        strategy = _RecordingStrategy(_config())
        await strategy._warmup_candle_buffer()
        next_open = strategy._warmup_high_water["BTC-USD"] + timedelta(hours=1)
        group = await strategy._handle_candle_data("BTC-USD", _candle_payload(next_open))
        assert strategy.seen == [next_open]
        assert len(group) == 1
        assert group[0].side == "buy"

    @pytest.mark.asyncio
    async def test_suppressed_bar_does_not_mutate_rsi_cooldown(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify warmed-window bars cannot touch RSI cooldown state.

        Given: A warmed RSI strategy with a cooldown configured,
        When: A warmed-window bar arrives,
        Then: The cooldown ledger stays untouched (callback suppressed).
        """
        repo = _StubRepo({"BTC-USD": [_db_row(i) for i in range(15)]})
        _wire_repo(monkeypatch, repo)
        strategy = RSIReversion(
            _config(
                strategy_class="RSIReversion",
                params={"period": 14, "cooldown": 3, "buffer_size": 100},
            )
        )
        await strategy._warmup_candle_buffer()
        high_water = strategy._warmup_high_water["BTC-USD"]
        await strategy._handle_candle_data("BTC-USD", _candle_payload(high_water))
        assert strategy._cool == {}


class TestIndicatorLookbacks:
    """RSI/MACD declare their lookbacks and are first-bar ready."""

    def test_rsi_and_macd_lookbacks(self) -> None:
        """Verify the declared warm-up bar counts.

        Given: Default RSI and MACD params,
        When: required_candle_history is read,
        Then: RSI needs period+1 and MACD needs slow+signal bars.
        """
        rsi = RSIReversion(_config(strategy_class="RSIReversion", params={"period": 14}))
        assert rsi.required_candle_history() == 15
        macd = MACDCrossover(
            _config(
                strategy_class="MACDCrossover",
                params={"fast": 12, "slow": 26, "signal_period": 9},
            )
        )
        assert macd.required_candle_history() == 35

    def test_previous_hist_short_or_nan_series_returns_none(self) -> None:
        """Verify the bootstrap declines short or NaN-headed series.

        Given: An empty cache and a one-point series, then a NaN prior,
        When: _previous_hist runs,
        Then: Both return None (no cross can be judged yet).
        """
        strategy = MACDCrossover(
            _config(
                strategy_class="MACDCrossover",
                params={"fast": 12, "slow": 26, "signal_period": 9},
            )
        )
        short = pd.Series([0.4], dtype=float)
        assert strategy._previous_hist("BTC-USD", short) is None
        nan_head = pd.Series([float("nan"), 0.4], dtype=float)
        assert strategy._previous_hist("BTC-USD", nan_head) is None

    def test_previous_hist_prefers_live_cache_over_series(self) -> None:
        """Verify live continuity: the cached point wins over the series.

        Given: A cached prior from the last live callback and a recomputed
            series whose iloc[-2] sits on the other side of zero,
        When: _previous_hist runs,
        Then: The cached value returns (pruned-buffer recomputation must
            not miss or double-fire a live cross).
        """
        strategy = MACDCrossover(
            _config(
                strategy_class="MACDCrossover",
                params={"fast": 12, "slow": 26, "signal_period": 9},
            )
        )
        strategy._last_hist["BTC-USD"] = 0.25
        recomputed = pd.Series([-0.1, -0.05], dtype=float)
        assert strategy._previous_hist("BTC-USD", recomputed) == 0.25

    @pytest.mark.asyncio
    async def test_macd_crosses_on_first_post_warmup_bar(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a warmed MACD emits on its very first live bar.

        Given: A warmed buffer and a histogram series whose tail crosses
            from negative to positive on the first callback,
        When: The first post-warm-up bar arrives,
        Then: A buy signal emits without any prior live callback.
        """
        repo = _StubRepo({"BTC-USD": [_db_row(i) for i in range(35)]})
        _wire_repo(monkeypatch, repo)
        strategy = MACDCrossover(
            _config(
                strategy_class="MACDCrossover",
                params={"fast": 12, "slow": 26, "signal_period": 9, "buffer_size": 100},
            )
        )
        await strategy._warmup_candle_buffer()

        def fake_macd(
            series: pd.Series, fast: int, slow: int, signal_period: int
        ) -> tuple[pd.Series, pd.Series, pd.Series]:
            index = pd.RangeIndex(len(series))
            zero = pd.Series([0.0] * len(series), index=index, dtype=float)
            data = [-0.4] * len(series)
            data[-1] = 0.4
            return zero, zero.copy(), pd.Series(data, index=index, dtype=float)

        monkeypatch.setattr("snapper.strategies.macd.macd", fake_macd)
        next_open = strategy._warmup_high_water["BTC-USD"] + timedelta(hours=1)
        group = await strategy._handle_candle_data("BTC-USD", _candle_payload(next_open))
        assert len(group) == 1
        assert group[0].side == "buy"
        assert "MACD bull cross" in group[0].reason
