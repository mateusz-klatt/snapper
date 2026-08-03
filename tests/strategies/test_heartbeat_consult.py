"""Tests for the HeartbeatConsult wake-path strategy.

Fast-smoke coverage of plan P1
(``plan_2026_07_03_strategy_runtime_split_and_mcp_wake.md``): construction
fail-fast validation (paper-only, scoped config, UUID7 identity params,
deadline bounds), one-consult-per-window dedup, DETACHED consult rounds
(``on_candle`` returns immediately; ``stop``/``reset`` cancel the
in-flight round), approved-outcome emission with AI-review attribution,
the self-contained traded-market and CME macro snapshots in the consult
envelope, honest open/closed/halted freshness semantics, and fail-soft
behavior for every consult error mode. The AI-review service and
repository are mocked — the end-to-end DB path lives in the ai_review
test suites.
"""

import asyncio
import contextlib
import math
import statistics
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import tzinfo
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid7

import pytest

from snapper.application.ai_review.service import MAX_SIGNAL_ENVELOPE_BYTES
from snapper.application.ai_review.service import AiReviewCreateRequest
from snapper.application.ai_review.service import AiReviewDecisionOutcome
from snapper.application.ai_review.service import DelegateBusyError
from snapper.application.ai_review.service import NoLiveDelegateError
from snapper.application.ai_review.service import _serialize_signal_envelope_canonical
from snapper.application.services.signals.service import signal_service
from snapper.core.types import AiReviewStatusEnum
from snapper.core.types import AllExchange
from snapper.core.types import ExchangeEnum
from snapper.core.types import TradeSideEnum
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import MarketViewRow
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import StrategyConfig
from snapper.strategies.heartbeat_consult import CONSULT_SEQUENCE_STREAM
from snapper.strategies.heartbeat_consult import DAY_BARS
from snapper.strategies.heartbeat_consult import DEFAULT_CONSULT_DEADLINE_SECONDS
from snapper.strategies.heartbeat_consult import DEFAULT_MACRO_CONTRACT_SYMBOL
from snapper.strategies.heartbeat_consult import DEFAULT_MACRO_STALE_AFTER_MINUTES
from snapper.strategies.heartbeat_consult import MACRO_SNAPSHOT_BARS
from snapper.strategies.heartbeat_consult import MACRO_SNAPSHOT_TIMEFRAME
from snapper.strategies.heartbeat_consult import SNAPSHOT_BARS
from snapper.strategies.heartbeat_consult import SNAPSHOT_RANGE_START
from snapper.strategies.heartbeat_consult import HeartbeatConsult
from snapper.strategies.heartbeat_consult import _build_macro_snapshot
from snapper.strategies.heartbeat_consult import _build_market_snapshot
from snapper.strategies.heartbeat_consult import _finite_or_none
from snapper.strategies.heartbeat_consult import _pct_change
from snapper.strategies.heartbeat_consult import _realized_vol_pct
from snapper.strategies.heartbeat_consult import _rsi
from snapper.strategies.heartbeat_consult import _sma

_OPEN_AT = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)
_CONSULT_AS_OF = datetime(2026, 7, 21, 15, 2, tzinfo=UTC)


class _FixedConsultClock:
    """Deterministic wall clock patched into consult request tests."""

    @staticmethod
    def now(timezone: tzinfo | None = None) -> datetime:
        """Return the fixed consult anchor in the requested timezone."""
        if timezone is None:
            return _CONSULT_AS_OF.replace(tzinfo=None)
        return _CONSULT_AS_OF.astimezone(timezone)


@pytest.fixture(autouse=True)
def _mock_signal_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace signal persistence with a no-op for these unit tests."""
    monkeypatch.setattr(signal_service, "store_signal", AsyncMock(return_value=""))


def _consult_params(**overrides: Any) -> dict[str, Any]:
    """Build valid consult params, overridable per test."""
    params: dict[str, Any] = {
        "ai_review_user_public_id": str(uuid7()),
        "ai_review_strategy_public_id": str(uuid7()),
        "ai_review_deadline_seconds": DEFAULT_CONSULT_DEADLINE_SECONDS,
    }
    params.update(overrides)
    return params


def _config(**overrides: Any) -> StrategyConfig:
    """Build a valid scoped paper config, overridable per test."""
    base: dict[str, Any] = {
        "name": "heartbeat_consult_test",
        "strategy_class": "HeartbeatConsult",
        "inputs": ["market.kraken.BTC-USD.candles.1h"],
        "outputs": ["BTC-USD"],
        "exchange": "paper",
        "params": _consult_params(),
        "wallet_public_id": str(uuid7()),
        "operator_public_id": str(uuid7()),
    }
    base.update(overrides)
    return StrategyConfig(**base)


def _candle(
    open_at: datetime = _OPEN_AT,
    close: float = 50000.0,
    high: float | None = None,
    low: float | None = None,
) -> CandleData:
    """Build a 1h kraken candle at the given window."""
    return CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        instrument="BTC-USD",
        timeframe="1h",
        open=close - 100,
        high=high if high is not None else close + 100,
        low=low if low is not None else close - 200,
        close=close,
        volume=10.0,
        exchange="kraken",
        timestamp=open_at,
        open_at=open_at,
    )


def _approved_outcome() -> AiReviewDecisionOutcome:
    """Build a terminal approved outcome."""
    return AiReviewDecisionOutcome(
        review_public_id=str(uuid7()),
        status=AiReviewStatusEnum.RESOLVED_APPROVED,
        resolution_mode=None,
        decision=None,
        rationale=None,
        dispatch_version=1,
        responding_delegate_public_id=str(uuid7()),
    )


def _rejected_outcome() -> AiReviewDecisionOutcome:
    """Build a terminal rejected outcome."""
    return AiReviewDecisionOutcome(
        review_public_id=str(uuid7()),
        status=AiReviewStatusEnum.RESOLVED_REJECTED,
        resolution_mode=None,
        decision=None,
        rationale=None,
        dispatch_version=1,
        responding_delegate_public_id=str(uuid7()),
    )


async def _drain_round(strategy: HeartbeatConsult) -> None:
    """Await the strategy's detached consult round to completion."""
    task = strategy._consult_task
    if task is not None:
        await task


def _desc_rows(closes: list[float], last_open_at: datetime) -> list[CandleRow]:
    """Build repository candle rows NEWEST-FIRST (the ``order='desc'`` contract).

    The newest row carries ``last_open_at``; each older row steps back
    one hour. Highs sit one above the close, lows one below.
    """
    ordered: list[CandleRow] = [
        {
            "open_at": last_open_at - timedelta(hours=len(closes) - 1 - index),
            "timeframe": "1h",
            "open": close - 0.5,
            "close": close,
            "high": close + 1.0,
            "low": close - 1.0,
            "volume": 1.0,
            "vwap": close,
            "trades": 1,
            "source": "native",
            "complete": True,
            "public_id": f"market-{index}",
            "timestamp": last_open_at - timedelta(hours=len(closes) - 2 - index),
            "session_id": "market-session",
            "sequence_id": index,
        }
        for index, close in enumerate(closes)
    ]
    return list(reversed(ordered))


def _macro_desc_rows(
    candles: list[tuple[datetime, float, float]], latest_timestamp: datetime
) -> list[CandleRow]:
    """Build complete macro rows newest-first with an explicit latest bus time."""
    ordered: list[CandleRow] = [
        {
            "open_at": open_at,
            "timeframe": MACRO_SNAPSHOT_TIMEFRAME,
            "open": open_price,
            "high": max(open_price, close) + 1.0,
            "low": min(open_price, close) - 1.0,
            "close": close,
            "volume": 1.0,
            "vwap": close,
            "trades": 1,
            "source": "native",
            "complete": True,
            "public_id": f"macro-{index}",
            "timestamp": (
                latest_timestamp if index == len(candles) - 1 else open_at + timedelta(minutes=1)
            ),
            "session_id": "macro-session",
            "sequence_id": index,
        }
        for index, (open_at, open_price, close) in enumerate(candles)
    ]
    return list(reversed(ordered))


def _fresh_macro_rows() -> list[CandleRow]:
    """Build deterministic fresh macro history for consult-envelope tests."""
    candles = [
        (datetime(2026, 7, 20, 14, 59, tzinfo=UTC), 97.0, 98.0),
        (datetime(2026, 7, 20, 22, 0, tzinfo=UTC), 99.0, 100.0),
        (datetime(2026, 7, 21, 14, 0, tzinfo=UTC), 101.0, 102.0),
        (datetime(2026, 7, 21, 14, 59, tzinfo=UTC), 102.0, 103.0),
        (datetime(2026, 7, 21, 15, 0, tzinfo=UTC), 104.0, 105.0),
    ]
    return _macro_desc_rows(
        candles,
        latest_timestamp=datetime(2026, 7, 21, 15, 1, tzinfo=UTC),
    )


def _market_view() -> MarketViewRow:
    """Build a current research artifact with more events than the digest cap."""
    view_as_of = _CONSULT_AS_OF - timedelta(minutes=40)
    return {
        "public_id": "market-view-1",
        "research_round_public_id": "research-round-1",
        "trigger": "periodic",
        "status": "completed",
        "as_of": view_as_of,
        "submitted_at": _CONSULT_AS_OF - timedelta(minutes=17, seconds=20),
        "valid_until": _CONSULT_AS_OF + timedelta(hours=4),
        "regime": "risk_off",
        "bias": "avoid_new_longs",
        "confidence": 0.82,
        "horizon_hours": 4,
        "key_risks": ["Inflation surprise"],
        "next_events": [
            {
                "when_utc": _CONSULT_AS_OF + timedelta(minutes=30),
                "name": "CPI release",
                "severity": "high",
            },
            {
                "when_utc": _CONSULT_AS_OF + timedelta(hours=1),
                "name": "Fed speaker",
                "severity": "medium",
            },
            {
                "when_utc": _CONSULT_AS_OF + timedelta(hours=2),
                "name": "Treasury auction",
                "severity": "medium",
            },
            {
                "when_utc": _CONSULT_AS_OF + timedelta(hours=3),
                "name": "Earnings release",
                "severity": "low",
            },
        ],
        "rationale": "Risk appetite is constrained ahead of scheduled events.",
        "sources": [
            {
                "public_id": "market-view-source-1",
                "market_view_public_id": "market-view-1",
                "ordinal": 0,
                "url": "https://example.com/research",
                "title": "Market briefing",
                "retrieved_at": view_as_of - timedelta(minutes=5),
            }
        ],
    }


class _BlockedConsult:
    """A ``_consult`` stand-in that blocks until released, counting calls."""

    def __init__(self) -> None:
        """Initialize the coordination events and call counter."""
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def __call__(self, instrument: str, candle: CandleData) -> AiReviewDecisionOutcome | None:
        """Record the call, block until released, then fall through."""
        self.calls += 1
        self.started.set()
        await self.release.wait()
        return None


class _StubbornConsult:
    """A ``_consult`` stand-in whose cancellation blocks until released.

    Models a round that takes extra event-loop turns to die (e.g. one
    cancelled inside ``emit_signal``), pinning the stop/reset drain gap
    the dispatch gate must cover.
    """

    def __init__(self) -> None:
        """Initialize the coordination events and call counter."""
        self.started = asyncio.Event()
        self.cancel_seen = asyncio.Event()
        self.release_cancel = asyncio.Event()
        self.calls = 0

    async def __call__(self, instrument: str, candle: CandleData) -> AiReviewDecisionOutcome | None:
        """Record the call, then absorb cancellation until released."""
        self.calls += 1
        self.started.set()
        blocker: asyncio.Event = asyncio.Event()
        try:
            await blocker.wait()
        except asyncio.CancelledError:
            self.cancel_seen.set()
            await self.release_cancel.wait()
            raise
        return None


class TestIndicatorHelpers:
    """Exact-value coverage of the pure snapshot indicator helpers."""

    def test_finite_passes_non_finite_filtered(self) -> None:
        """Verify the finiteness filter passes numbers and drops inf.

        Given: A finite float and a non-finite float,
        When: _finite_or_none filters both,
        Then: The finite value survives and inf becomes None.
        """
        assert _finite_or_none(1.5) == 1.5
        assert _finite_or_none(math.inf) is None

    def test_sma_exact_and_short_series(self) -> None:
        """Verify SMA math on a known window and the short-series guard.

        Given: Four closes and a window of two,
        When: _sma computes the trailing average,
        Then: The last two closes average; a window longer than the
            series yields None.
        """
        assert _sma([1.0, 2.0, 3.0, 4.0], 2) == 3.5
        assert _sma([1.0, 2.0, 3.0], 4) is None

    def test_rsi_gains_only_is_100(self) -> None:
        """Verify a monotonic ramp saturates RSI at 100.

        Given: Nineteen strictly rising closes (Wilder smoothing runs),
        When: _rsi computes over the series,
        Then: The zero-loss branch returns 100.0.
        """
        assert _rsi([float(value) for value in range(1, 20)]) == 100.0

    def test_rsi_losses_only_is_0(self) -> None:
        """Verify a monotonic decline pins RSI at 0.

        Given: Nineteen strictly falling closes,
        When: _rsi computes over the series,
        Then: The general branch evaluates to 0.0.
        """
        assert _rsi([float(value) for value in range(19, 0, -1)]) == 0.0

    def test_rsi_flat_series_is_neutral(self) -> None:
        """Verify a flat series reads as neutral rather than saturated.

        Given: Twenty identical closes (no gains, no losses),
        When: _rsi computes over the series,
        Then: The neutral 50.0 sentinel is returned.
        """
        assert _rsi([5.0] * 20) == 50.0

    def test_rsi_alternating_is_50(self) -> None:
        """Verify balanced gains/losses compute a true 50 via the formula.

        Given: Fifteen closes alternating 1, 2, 1, 2, ... (seven +1
            deltas and seven -1 deltas in the seed window),
        When: _rsi computes over the series,
        Then: RS is 1 and RSI is exactly 50.0.
        """
        closes = [1.0 if index % 2 == 0 else 2.0 for index in range(15)]
        assert _rsi(closes) == 50.0

    def test_rsi_short_series_is_none(self) -> None:
        """Verify fewer closes than period+1 yields None.

        Given: Fourteen closes for a 14-period RSI,
        When: _rsi computes over the series,
        Then: None marks the warm-up state.
        """
        assert _rsi([float(value) for value in range(14)]) is None

    def test_pct_change_exact_and_zero_reference(self) -> None:
        """Verify percent-change math and the non-positive-reference guard.

        Given: A 100 -> 110 move and a zero reference,
        When: _pct_change computes both,
        Then: The move reads 10.0 percent and the zero reference is None.
        """
        assert _pct_change(110.0, 100.0) == 10.0
        assert _pct_change(5.0, 0.0) is None

    def test_realized_vol_flat_is_zero(self) -> None:
        """Verify a flat series has zero realized volatility.

        Given: Thirty identical closes,
        When: _realized_vol_pct computes the trailing window,
        Then: The volatility is exactly 0.0.
        """
        assert _realized_vol_pct([5.0] * 30) == 0.0

    def test_realized_vol_short_series_is_none(self) -> None:
        """Verify fewer closes than bars+1 yields None.

        Given: Exactly DAY_BARS closes,
        When: _realized_vol_pct computes,
        Then: None marks the warm-up state.
        """
        assert _realized_vol_pct([5.0] * DAY_BARS) is None

    def test_realized_vol_non_positive_close_is_none(self) -> None:
        """Verify a non-positive close inside the window poisons the calc.

        Given: A window containing a zero close,
        When: _realized_vol_pct computes,
        Then: None is returned instead of a degenerate return series.
        """
        closes = [1.0] * 10 + [0.0] + [1.0] * 20
        assert _realized_vol_pct(closes) is None

    def test_realized_vol_non_positive_final_close_is_none(self) -> None:
        """Verify a non-positive FINAL close also poisons the calc.

        Given: Windows whose last close is zero or negative (the final
            close is never used as a return denominator, so it needs
            its own guard),
        When: _realized_vol_pct computes,
        Then: None is returned instead of a finite nonsense volatility.
        """
        assert _realized_vol_pct([1.0] * 24 + [0.0]) is None
        assert _realized_vol_pct([1.0] * 24 + [-1.0]) is None


class TestBuildMarketSnapshot:
    """Snapshot assembly from repository rows plus the triggering bar."""

    @pytest.mark.asyncio
    async def test_full_history_computes_all_fields(self) -> None:
        """Verify every snapshot field on a fully warmed ramp series.

        Given: Sixty persisted ramp closes (1..60) and a newer trigger
            bar closing at 61,
        When: _build_market_snapshot assembles the envelope section,
        Then: Every field matches the hand-computed indicator values and
            the trigger bar is appended exactly once.
        """
        last_db_open_at = _OPEN_AT - timedelta(hours=1)
        repo = AsyncMock()
        repo.get_candles = AsyncMock(
            return_value=_desc_rows([float(value) for value in range(1, 61)], last_db_open_at)
        )
        candle = _candle(open_at=_OPEN_AT, close=61.0, high=62.0, low=59.0)
        snapshot = await _build_market_snapshot(repo, "BTC-USD", candle, _OPEN_AT)
        expected_vol = round(statistics.pstdev([1.0 / value for value in range(37, 61)]) * 100.0, 4)
        assert snapshot == {
            "timeframe": "1h",
            "bars": 61,
            "last_close": 61.0,
            "change_1_bar_pct": 1.6667,
            "change_24_bar_pct": 64.8649,
            "sma_20": 51.5,
            "sma_50": 36.5,
            "rsi_14": 100.0,
            "high_24_bar": 62.0,
            "low_24_bar": 37.0,
            "realized_vol_24_bar_pct": expected_vol,
        }
        repo.get_candles.assert_awaited_once_with(
            "BTC-USD",
            "1h",
            SNAPSHOT_RANGE_START,
            _OPEN_AT,
            "kraken",
            _OPEN_AT,
            limit=SNAPSHOT_BARS,
            order="desc",
            complete=True,
        )

    @pytest.mark.asyncio
    async def test_already_persisted_trigger_not_double_counted(self) -> None:
        """Verify a trigger bar the store already holds is not appended.

        Given: Thirty persisted closes whose newest row shares the
            trigger's open_at,
        When: _build_market_snapshot assembles the envelope section,
        Then: The bar count equals the persisted depth.
        """
        repo = AsyncMock()
        repo.get_candles = AsyncMock(
            return_value=_desc_rows([float(value) for value in range(1, 31)], _OPEN_AT)
        )
        snapshot = await _build_market_snapshot(
            repo, "BTC-USD", _candle(open_at=_OPEN_AT, close=30.0), _OPEN_AT
        )
        assert snapshot["bars"] == 30
        assert snapshot["last_close"] == 30.0

    @pytest.mark.asyncio
    async def test_empty_store_warms_up_from_trigger_only(self) -> None:
        """Verify an empty candle store yields a single-bar warm-up snapshot.

        Given: No persisted candles,
        When: _build_market_snapshot assembles the envelope section,
        Then: Only the trigger bar counts and every indicator is None.
        """
        repo = AsyncMock()
        repo.get_candles = AsyncMock(return_value=[])
        snapshot = await _build_market_snapshot(repo, "BTC-USD", _candle(), _OPEN_AT)
        assert snapshot == {
            "timeframe": "1h",
            "bars": 1,
            "last_close": 50000.0,
            "change_1_bar_pct": None,
            "change_24_bar_pct": None,
            "sma_20": None,
            "sma_50": None,
            "rsi_14": None,
            "high_24_bar": None,
            "low_24_bar": None,
            "realized_vol_24_bar_pct": None,
        }

    @pytest.mark.asyncio
    async def test_rows_newer_than_trigger_are_dropped(self) -> None:
        """Verify the defensive filter drops nonconforming newer rows.

        Given: A store returning thirty closes (1..30) whose newest
            five bars open AFTER the trigger window (the range query
            forbids this — the mock simulates a misbehaving store),
        When: _build_market_snapshot assembles the envelope section,
        Then: Only the 25 bars up to the trigger count — the trigger is
            recognized as already persisted (close 25) and the newer
            closes never leak into the snapshot.
        """
        repo = AsyncMock()
        repo.get_candles = AsyncMock(
            return_value=_desc_rows(
                [float(value) for value in range(1, 31)], _OPEN_AT + timedelta(hours=5)
            )
        )
        snapshot = await _build_market_snapshot(
            repo, "BTC-USD", _candle(open_at=_OPEN_AT, close=25.0), _OPEN_AT
        )
        assert snapshot["bars"] == 25
        assert snapshot["last_close"] == 25.0

    @pytest.mark.asyncio
    async def test_only_newer_rows_falls_back_to_trigger(self) -> None:
        """Verify an all-nonconforming store degrades to a trigger-only warm-up.

        Given: A misbehaving store returning only closes that open
            after the trigger window (the range query forbids this),
        When: _build_market_snapshot assembles the envelope section,
        Then: The snapshot counts only the appended trigger bar.
        """
        repo = AsyncMock()
        repo.get_candles = AsyncMock(
            return_value=_desc_rows([10.0, 11.0], _OPEN_AT + timedelta(hours=2))
        )
        snapshot = await _build_market_snapshot(
            repo, "BTC-USD", _candle(open_at=_OPEN_AT), _OPEN_AT
        )
        assert snapshot["bars"] == 1
        assert snapshot["last_close"] == 50000.0


class TestBuildMacroSnapshot:
    """Cross-asset snapshot assembly and CME session semantics."""

    @pytest.mark.asyncio
    async def test_fresh_open_session_computes_requested_fields(self) -> None:
        """A fresh open-session print produces the complete macro context.

        Given: Complete one-minute Nasdaq futures candles spanning the
            session open, one-hour reference, and 24-hour boundary,
        When: The macro snapshot is built during an open CME session,
        Then: It reports server-computed age and all requested changes
            without adding an inactive stale flag.
        """
        as_of = datetime(2026, 7, 21, 15, 2, tzinfo=UTC)
        candles = [
            (datetime(2026, 7, 20, 14, 59, tzinfo=UTC), 97.0, 98.0),
            (datetime(2026, 7, 20, 22, 0, tzinfo=UTC), 99.0, 100.0),
            (datetime(2026, 7, 21, 14, 0, tzinfo=UTC), 101.0, 102.0),
            (datetime(2026, 7, 21, 14, 59, tzinfo=UTC), 102.0, 103.0),
            (datetime(2026, 7, 21, 15, 0, tzinfo=UTC), 104.0, 105.0),
        ]
        repo = AsyncMock()
        repo.get_candles = AsyncMock(
            return_value=_macro_desc_rows(
                candles,
                latest_timestamp=datetime(2026, 7, 21, 15, 1, tzinfo=UTC),
            )
        )
        snapshot = await _build_macro_snapshot(
            repo,
            DEFAULT_MACRO_CONTRACT_SYMBOL,
            as_of,
            DEFAULT_MACRO_STALE_AFTER_MINUTES,
        )
        expected_returns = [2.0 / 98.0, 2.0 / 100.0, 1.0 / 102.0, 2.0 / 103.0]
        expected_vol = round(statistics.pstdev(expected_returns) * 100.0, 4)
        assert snapshot == {
            "symbol": DEFAULT_MACRO_CONTRACT_SYMBOL,
            "as_of": as_of.isoformat(),
            "age_minutes": 1.0,
            "session": "open",
            "change_1h_pct": 2.9412,
            "change_since_session_open_pct": 6.0606,
            "realized_vol_24h_pct": expected_vol,
        }
        repo.get_candles.assert_awaited_once_with(
            DEFAULT_MACRO_CONTRACT_SYMBOL,
            MACRO_SNAPSHOT_TIMEFRAME,
            SNAPSHOT_RANGE_START,
            as_of,
            ExchangeEnum.KRAKEN_EQUITIES,
            as_of,
            limit=MACRO_SNAPSHOT_BARS,
            order="desc",
            complete=True,
        )

    @pytest.mark.asyncio
    async def test_weekend_closure_preserves_old_print_and_values(self) -> None:
        """A structural weekend gap remains useful and is never stale.

        Given: Saturday consult time and Friday's legitimate last print,
        When: The shared CME calendar classifies the venue closed,
        Then: The large age and derived values remain present with no
            stale flag or proxy substitution.
        """
        as_of = datetime(2026, 7, 18, 12, 0, tzinfo=UTC)
        candles = [
            (datetime(2026, 7, 16, 22, 0, tzinfo=UTC), 99.0, 100.0),
            (datetime(2026, 7, 17, 19, 59, tzinfo=UTC), 101.0, 102.0),
            (datetime(2026, 7, 17, 20, 59, tzinfo=UTC), 103.0, 104.0),
        ]
        repo = AsyncMock()
        repo.get_candles = AsyncMock(
            return_value=_macro_desc_rows(
                candles,
                latest_timestamp=datetime(2026, 7, 17, 21, 0, tzinfo=UTC),
            )
        )
        snapshot = await _build_macro_snapshot(
            repo,
            DEFAULT_MACRO_CONTRACT_SYMBOL,
            as_of,
            DEFAULT_MACRO_STALE_AFTER_MINUTES,
        )
        expected_vol = round(statistics.pstdev([2.0 / 100.0, 2.0 / 102.0]) * 100.0, 4)
        assert snapshot == {
            "symbol": DEFAULT_MACRO_CONTRACT_SYMBOL,
            "as_of": as_of.isoformat(),
            "age_minutes": 900.0,
            "session": "closed",
            "change_1h_pct": 1.9608,
            "change_since_session_open_pct": 5.0505,
            "realized_vol_24h_pct": expected_vol,
        }

    @pytest.mark.asyncio
    async def test_open_session_old_print_is_halted_and_values_are_nulled(self) -> None:
        """An old print while the calendar is open is a feed incident.

        Given: An open CME session whose latest persisted bus timestamp
            is eleven minutes old,
        When: The ten-minute threshold is evaluated,
        Then: Session is halted, stale is present and true, and every
            decision value is null even though references are available.
        """
        as_of = datetime(2026, 7, 21, 15, 2, tzinfo=UTC)
        candles = [
            (datetime(2026, 7, 20, 22, 0, tzinfo=UTC), 99.0, 100.0),
            (datetime(2026, 7, 21, 13, 50, tzinfo=UTC), 101.0, 102.0),
            (datetime(2026, 7, 21, 14, 50, tzinfo=UTC), 103.0, 104.0),
        ]
        repo = AsyncMock()
        repo.get_candles = AsyncMock(
            return_value=_macro_desc_rows(
                candles,
                latest_timestamp=datetime(2026, 7, 21, 14, 51, tzinfo=UTC),
            )
        )
        snapshot = await _build_macro_snapshot(
            repo,
            DEFAULT_MACRO_CONTRACT_SYMBOL,
            as_of,
            DEFAULT_MACRO_STALE_AFTER_MINUTES,
        )
        assert snapshot == {
            "symbol": DEFAULT_MACRO_CONTRACT_SYMBOL,
            "as_of": as_of.isoformat(),
            "age_minutes": 11.0,
            "session": "halted",
            "change_1h_pct": None,
            "change_since_session_open_pct": None,
            "realized_vol_24h_pct": None,
            "stale": True,
        }

    @pytest.mark.asyncio
    async def test_threshold_is_strict_and_missing_references_are_null(self) -> None:
        """Exactly-threshold age stays open without inventing references.

        Given: A recently versioned pre-weekend print exactly ten minutes
            old just after the Sunday reopen,
        When: No current-session or one-hour reference candle exists,
        Then: Age derives from bus timestamp rather than old event time,
            session stays open, and all unavailable values are null.
        """
        as_of = datetime(2026, 7, 19, 22, 5, tzinfo=UTC)
        repo = AsyncMock()
        repo.get_candles = AsyncMock(
            return_value=_macro_desc_rows(
                [(datetime(2026, 7, 17, 20, 59, tzinfo=UTC), 103.0, 104.0)],
                latest_timestamp=datetime(2026, 7, 19, 21, 55, tzinfo=UTC),
            )
        )
        snapshot = await _build_macro_snapshot(
            repo,
            DEFAULT_MACRO_CONTRACT_SYMBOL,
            as_of,
            DEFAULT_MACRO_STALE_AFTER_MINUTES,
        )
        assert snapshot == {
            "symbol": DEFAULT_MACRO_CONTRACT_SYMBOL,
            "as_of": as_of.isoformat(),
            "age_minutes": 10.0,
            "session": "open",
            "change_1h_pct": None,
            "change_since_session_open_pct": None,
            "realized_vol_24h_pct": None,
        }

    @pytest.mark.asyncio
    async def test_no_qualifying_complete_candle_raises(self) -> None:
        """Defensive future-row filtering turns missing history into failure.

        Given: Rows whose event time or bus timestamp is after the anchor,
        When: The snapshot defensively filters repository output,
        Then: ValueError lets the consult's fail-soft boundary omit macro.
        """
        as_of = datetime(2026, 7, 21, 15, 2, tzinfo=UTC)
        future_event = _macro_desc_rows(
            [(as_of + timedelta(minutes=1), 100.0, 101.0)],
            latest_timestamp=as_of,
        )
        future_bus = _macro_desc_rows(
            [(as_of - timedelta(minutes=1), 100.0, 101.0)],
            latest_timestamp=as_of + timedelta(minutes=1),
        )
        repo = AsyncMock()
        repo.get_candles = AsyncMock(return_value=future_event + future_bus)
        with pytest.raises(ValueError, match="No complete macro candles"):
            await _build_macro_snapshot(
                repo,
                DEFAULT_MACRO_CONTRACT_SYMBOL,
                as_of,
                DEFAULT_MACRO_STALE_AFTER_MINUTES,
            )


class TestHeartbeatConsultConstruction:
    """Fail-fast validation at construction time."""

    def test_valid_config_constructs(self) -> None:
        """Verify a scoped paper config with UUID7 params constructs.

        Given: A paper config with wallet/operator scope and UUID7 params,
        When: HeartbeatConsult is instantiated,
        Then: Consult identity attributes are bound.
        """
        config = _config()
        strategy = HeartbeatConsult(config)
        assert strategy.consult_user_public_id == config.params["ai_review_user_public_id"]
        assert strategy.consult_strategy_public_id == config.params["ai_review_strategy_public_id"]
        assert strategy.consult_deadline_seconds == DEFAULT_CONSULT_DEADLINE_SECONDS
        assert strategy.consult_signal_strength == 0.0
        assert strategy.macro_contract_symbol == DEFAULT_MACRO_CONTRACT_SYMBOL
        assert strategy.macro_stale_after_minutes == DEFAULT_MACRO_STALE_AFTER_MINUTES
        assert strategy._consult_task is None

    def test_macro_parameters_override_defaults(self) -> None:
        """Verify quarterly symbol rotation and threshold are config-driven.

        Given: Params naming a different CME contract and threshold,
        When: HeartbeatConsult is instantiated,
        Then: Both macro settings bind without a code change.
        """
        params = _consult_params(
            macro_contract_symbol="MESZ6-CME",
            macro_stale_after_minutes=12.5,
        )
        strategy = HeartbeatConsult(_config(params=params))
        assert strategy.macro_contract_symbol == "MESZ6-CME"
        assert strategy.macro_stale_after_minutes == 12.5

    def test_empty_macro_contract_rejected(self) -> None:
        """Verify a blank macro symbol cannot silently disable context.

        Given: A whitespace-only macro contract parameter,
        When: HeartbeatConsult is instantiated,
        Then: ValueError names the invalid parameter.
        """
        params = _consult_params(macro_contract_symbol="   ")
        blank_macro_symbol_config = _config(params=params)
        with pytest.raises(ValueError, match="macro_contract_symbol"):
            HeartbeatConsult(blank_macro_symbol_config)

    @pytest.mark.parametrize("threshold", [0.0, math.inf])
    def test_invalid_macro_stale_threshold_rejected(self, threshold: float) -> None:
        """Verify the incident threshold must be finite and positive.

        Given: A zero or infinite macro staleness threshold,
        When: HeartbeatConsult is instantiated,
        Then: ValueError names the invalid parameter.
        """
        params = _consult_params(macro_stale_after_minutes=threshold)
        invalid_threshold_config = _config(params=params)
        with pytest.raises(ValueError, match="macro_stale_after_minutes"):
            HeartbeatConsult(invalid_threshold_config)

    def test_non_paper_exchange_rejected(self) -> None:
        """Verify a live exchange is rejected.

        Given: A config targeting kraken,
        When: HeartbeatConsult is instantiated,
        Then: ValueError marks the strategy paper-only.
        """
        live_exchange_config = _config(exchange="kraken")
        with pytest.raises(ValueError, match="paper-only"):
            HeartbeatConsult(live_exchange_config)

    def test_unscoped_config_rejected(self) -> None:
        """Verify missing wallet/operator scope is rejected.

        Given: A config without wallet and operator identities,
        When: HeartbeatConsult is instantiated,
        Then: ValueError demands a scoped config.
        """
        unscoped_config = _config(wallet_public_id="", operator_public_id="")
        with pytest.raises(ValueError, match="scoped config"):
            HeartbeatConsult(unscoped_config)

    def test_non_uuid7_user_rejected(self) -> None:
        """Verify a non-UUID7 user identity is rejected.

        Given: Params with a plain-string user id,
        When: HeartbeatConsult is instantiated,
        Then: ValueError names 'ai_review_user_public_id'.
        """
        params = _consult_params(ai_review_user_public_id="not-a-uuid")
        invalid_user_config = _config(params=params)
        with pytest.raises(ValueError, match="ai_review_user_public_id"):
            HeartbeatConsult(invalid_user_config)

    def test_non_uuid7_strategy_rejected(self) -> None:
        """Verify a non-UUID7 strategy identity is rejected.

        Given: Params with an empty strategy id,
        When: HeartbeatConsult is instantiated,
        Then: ValueError names 'ai_review_strategy_public_id'.
        """
        params = _consult_params(ai_review_strategy_public_id="")
        invalid_strategy_config = _config(params=params)
        with pytest.raises(ValueError, match="ai_review_strategy_public_id"):
            HeartbeatConsult(invalid_strategy_config)

    def test_deadline_out_of_range_rejected(self) -> None:
        """Verify an out-of-range deadline is rejected.

        Given: Params with a 301s deadline,
        When: HeartbeatConsult is instantiated,
        Then: ValueError names 'ai_review_deadline_seconds'.
        """
        params = _consult_params(ai_review_deadline_seconds=301)
        out_of_range_deadline_config = _config(params=params)
        with pytest.raises(ValueError, match="ai_review_deadline_seconds"):
            HeartbeatConsult(out_of_range_deadline_config)

    def test_missing_deadline_rejected(self) -> None:
        """Verify an absent deadline param is rejected.

        Given: Params without 'ai_review_deadline_seconds',
        When: HeartbeatConsult is instantiated,
        Then: ValueError names the param (0 falls outside the range).
        """
        params = _consult_params()
        del params["ai_review_deadline_seconds"]
        missing_deadline_config = _config(params=params)
        with pytest.raises(ValueError, match="ai_review_deadline_seconds"):
            HeartbeatConsult(missing_deadline_config)

    def test_signal_strength_out_of_range_rejected(self) -> None:
        """Verify an out-of-range signal strength is rejected.

        Given: Params with heartbeat_signal_strength=1.5 (above the cap),
        When: HeartbeatConsult is instantiated,
        Then: ValueError names 'heartbeat_signal_strength'.
        """
        params = _consult_params(heartbeat_signal_strength=1.5)
        above_cap_strength_config = _config(params=params)
        with pytest.raises(ValueError, match="heartbeat_signal_strength"):
            HeartbeatConsult(above_cap_strength_config)

    def test_signal_strength_negative_rejected(self) -> None:
        """Verify a negative signal strength is rejected (not coerced to default).

        Given: Params with heartbeat_signal_strength=-0.1 (stays truthy, so the
            `or DEFAULT` guard does not mask it),
        When: HeartbeatConsult is instantiated,
        Then: ValueError names 'heartbeat_signal_strength'.
        """
        params = _consult_params(heartbeat_signal_strength=-0.1)
        negative_strength_config = _config(params=params)
        with pytest.raises(ValueError, match="heartbeat_signal_strength"):
            HeartbeatConsult(negative_strength_config)

    def test_signal_strength_upper_boundary_accepted(self) -> None:
        """Verify the inclusive 1.0 upper boundary constructs.

        Given: Params with heartbeat_signal_strength=1.0 (the cap),
        When: HeartbeatConsult is instantiated,
        Then: consult_signal_strength binds to 1.0.
        """
        strategy = HeartbeatConsult(_config(params=_consult_params(heartbeat_signal_strength=1.0)))
        assert strategy.consult_signal_strength == 1.0

    def test_absent_signal_strength_defaults_flat(self) -> None:
        """Verify an omitted signal-strength param defaults to target-flat.

        Given: Params without 'heartbeat_signal_strength',
        When: HeartbeatConsult is instantiated,
        Then: consult_signal_strength is the 0.0 target-flat default.
        """
        params = _consult_params()
        assert "heartbeat_signal_strength" not in params
        strategy = HeartbeatConsult(_config(params=params))
        assert strategy.consult_signal_strength == 0.0


class TestHeartbeatConsultOnCandle:
    """Per-window consult dispatch and emission semantics."""

    @pytest.mark.asyncio
    async def test_approved_outcome_emits_target_flat(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify an approved consult emits the target-flat heartbeat.

        Given: A consult resolving RESOLVED_APPROVED,
        When: A new 1h candle arrives and the detached round drains,
        Then: emit_signal publishes strength 0.0 with the outcome stamped.
        """
        strategy = HeartbeatConsult(_config())
        outcome = _approved_outcome()
        monkeypatch.setattr(strategy, "_consult", AsyncMock(return_value=outcome))
        emit = AsyncMock()
        monkeypatch.setattr(strategy, "emit_signal", emit)
        result = await strategy.on_candle("BTC-USD", _candle())
        await _drain_round(strategy)
        assert result is None
        emit.assert_awaited_once()
        assert emit.await_args is not None
        signal = emit.await_args.args[0]
        assert signal.strength == 0.0
        assert signal.instrument == "BTC-USD"
        assert emit.await_args.kwargs["outcome"] is outcome

    @pytest.mark.asyncio
    async def test_configured_strength_emits_actionable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a configured strength>0 emits an actionable paper long.

        Given: A config with heartbeat_signal_strength=0.5 and an approved consult,
        When: A new candle arrives and the detached round drains,
        Then: emit_signal publishes the configured strength on the BUY side.
        """
        strategy = HeartbeatConsult(_config(params=_consult_params(heartbeat_signal_strength=0.5)))
        outcome = _approved_outcome()
        monkeypatch.setattr(strategy, "_consult", AsyncMock(return_value=outcome))
        emit = AsyncMock()
        monkeypatch.setattr(strategy, "emit_signal", emit)
        result = await strategy.on_candle("BTC-USD", _candle())
        await _drain_round(strategy)
        assert result is None
        emit.assert_awaited_once()
        assert emit.await_args is not None
        signal = emit.await_args.args[0]
        assert signal.strength == 0.5
        assert signal.side == TradeSideEnum.BUY

    @pytest.mark.asyncio
    async def test_emit_failure_is_fail_soft(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify an emit-path failure never escapes the detached round.

        Given: An approved consult and emit_signal raising,
        When: on_candle runs and the round drains,
        Then: Nothing propagates (the strategy keeps probing next windows).
        """
        strategy = HeartbeatConsult(_config())
        monkeypatch.setattr(strategy, "_consult", AsyncMock(return_value=_approved_outcome()))
        monkeypatch.setattr(
            strategy, "emit_signal", AsyncMock(side_effect=RuntimeError("publisher down"))
        )
        assert await strategy.on_candle("BTC-USD", _candle()) is None
        await _drain_round(strategy)

    @pytest.mark.asyncio
    async def test_rejected_outcome_does_not_emit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a rejected consult never emits.

        Given: A consult resolving RESOLVED_REJECTED,
        When: A new 1h candle arrives and the round drains,
        Then: No signal is emitted.
        """
        strategy = HeartbeatConsult(_config())
        monkeypatch.setattr(strategy, "_consult", AsyncMock(return_value=_rejected_outcome()))
        emit = AsyncMock()
        monkeypatch.setattr(strategy, "emit_signal", emit)
        assert await strategy.on_candle("BTC-USD", _candle()) is None
        await _drain_round(strategy)
        emit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_none_outcome_does_not_emit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a fallen-through consult never emits.

        Given: A consult returning None (no delegate / error),
        When: A new 1h candle arrives and the round drains,
        Then: No signal is emitted.
        """
        strategy = HeartbeatConsult(_config())
        monkeypatch.setattr(strategy, "_consult", AsyncMock(return_value=None))
        emit = AsyncMock()
        monkeypatch.setattr(strategy, "emit_signal", emit)
        assert await strategy.on_candle("BTC-USD", _candle()) is None
        await _drain_round(strategy)
        emit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_same_window_consults_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify revised bars of an already-consulted window are skipped.

        Given: Two candles sharing the same open_at,
        When: Both flow through on_candle with the first round drained,
        Then: The consult runs exactly once.
        """
        strategy = HeartbeatConsult(_config())
        consult = AsyncMock(return_value=None)
        monkeypatch.setattr(strategy, "_consult", consult)
        await strategy.on_candle("BTC-USD", _candle())
        await _drain_round(strategy)
        await strategy.on_candle("BTC-USD", _candle(close=51000.0))
        await _drain_round(strategy)
        consult.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_older_window_skipped_newer_consults(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify ordering: older bars skip, a newer window consults again.

        Given: A consulted window, then an older bar, then a newer bar,
        When: All flow through on_candle with rounds drained,
        Then: Only the two distinct forward windows consult.
        """
        strategy = HeartbeatConsult(_config())
        consult = AsyncMock(return_value=None)
        monkeypatch.setattr(strategy, "_consult", consult)
        await strategy.on_candle("BTC-USD", _candle())
        await _drain_round(strategy)
        await strategy.on_candle("BTC-USD", _candle(open_at=_OPEN_AT - timedelta(hours=1)))
        await _drain_round(strategy)
        await strategy.on_candle("BTC-USD", _candle(open_at=_OPEN_AT + timedelta(hours=1)))
        await _drain_round(strategy)
        assert consult.await_count == 2

    @pytest.mark.asyncio
    async def test_reset_clears_consult_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify replay reset re-arms the per-window dedup guard.

        Given: A consulted window followed by reset(),
        When: The same window's candle arrives again,
        Then: The consult runs a second time.
        """
        strategy = HeartbeatConsult(_config())
        consult = AsyncMock(return_value=None)
        monkeypatch.setattr(strategy, "_consult", consult)
        await strategy.on_candle("BTC-USD", _candle())
        await _drain_round(strategy)
        await strategy.reset()
        await strategy.on_candle("BTC-USD", _candle())
        await _drain_round(strategy)
        assert consult.await_count == 2


class TestHeartbeatConsultDetachedRound:
    """The consult round must never block the listen loop."""

    @pytest.mark.asyncio
    async def test_on_candle_returns_while_round_in_flight(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify on_candle returns immediately while the consult deliberates.

        Given: A consult blocked on an external event,
        When: on_candle dispatches the round,
        Then: The callback has already returned with the round still
            pending, and the round completes only after release.
        """
        strategy = HeartbeatConsult(_config())
        blocked = _BlockedConsult()
        monkeypatch.setattr(strategy, "_consult", blocked)
        result = await strategy.on_candle("BTC-USD", _candle())
        assert result is None
        await blocked.started.wait()
        task = strategy._consult_task
        assert task is not None
        assert not task.done()
        blocked.release.set()
        await task
        assert blocked.calls == 1

    @pytest.mark.asyncio
    async def test_inflight_round_skips_window_without_consuming(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a mid-round window is skipped but stays consultable.

        Given: Window A's round blocked in flight and window B arriving,
        When: B flows through on_candle during the round, then again
            after the round resolves,
        Then: B is skipped without spawning a second task, and consults
            successfully on the republish.
        """
        strategy = HeartbeatConsult(_config())
        blocked = _BlockedConsult()
        monkeypatch.setattr(strategy, "_consult", blocked)
        await strategy.on_candle("BTC-USD", _candle())
        await blocked.started.wait()
        task_a = strategy._consult_task
        window_b = _candle(open_at=_OPEN_AT + timedelta(hours=1))
        assert await strategy.on_candle("BTC-USD", window_b) is None
        assert strategy._consult_task is task_a
        assert blocked.calls == 1
        blocked.release.set()
        await _drain_round(strategy)
        assert await strategy.on_candle("BTC-USD", window_b) is None
        await _drain_round(strategy)
        assert blocked.calls == 2

    @pytest.mark.asyncio
    async def test_stop_cancels_inflight_round(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify stop() cancels and drains the in-flight round.

        Given: A consult blocked in flight,
        When: stop() runs,
        Then: The round task is cancelled and the slot cleared.
        """
        strategy = HeartbeatConsult(_config())
        blocked = _BlockedConsult()
        monkeypatch.setattr(strategy, "_consult", blocked)
        await strategy.on_candle("BTC-USD", _candle())
        await blocked.started.wait()
        task = strategy._consult_task
        assert task is not None
        await strategy.stop()
        assert task.cancelled()
        assert strategy._consult_task is None

    @pytest.mark.asyncio
    async def test_reset_cancels_inflight_round_and_rearms(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify reset() cancels the round and re-arms the window guard.

        Given: A consult blocked in flight,
        When: reset() runs and the same window's candle arrives again,
        Then: The old task is cancelled and a fresh round runs.
        """
        strategy = HeartbeatConsult(_config())
        blocked = _BlockedConsult()
        monkeypatch.setattr(strategy, "_consult", blocked)
        await strategy.on_candle("BTC-USD", _candle())
        await blocked.started.wait()
        task = strategy._consult_task
        assert task is not None
        await strategy.reset()
        assert task.cancelled()
        assert strategy._consult_task is None
        blocked.release.set()
        await strategy.on_candle("BTC-USD", _candle())
        await _drain_round(strategy)
        assert blocked.calls == 2

    @pytest.mark.asyncio
    async def test_cancel_inflight_noop_on_done_round(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify cancelling with a completed round is a no-op clear.

        Given: A drained (done) consult round,
        When: _cancel_inflight_round runs,
        Then: The slot clears without cancelling anything.
        """
        strategy = HeartbeatConsult(_config())
        monkeypatch.setattr(strategy, "_consult", AsyncMock(return_value=None))
        await strategy.on_candle("BTC-USD", _candle())
        await _drain_round(strategy)
        task = strategy._consult_task
        assert task is not None
        await strategy._cancel_inflight_round()
        assert strategy._consult_task is None
        assert not task.cancelled()

    @pytest.mark.asyncio
    async def test_stop_gate_blocks_dispatch_during_drain(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a candle landing mid-stop cannot spawn a fresh round.

        Given: An in-flight round whose cancellation blocks in flight,
        When: stop() drains it while a new window's candle arrives, and
            another candle arrives after the stop completes,
        Then: No replacement round ever spawns — the gate stays closed
            permanently.
        """
        strategy = HeartbeatConsult(_config())
        stubborn = _StubbornConsult()
        monkeypatch.setattr(strategy, "_consult", stubborn)
        await strategy.on_candle("BTC-USD", _candle())
        await stubborn.started.wait()
        stop_task = asyncio.create_task(strategy.stop())
        await stubborn.cancel_seen.wait()
        mid_stop = _candle(open_at=_OPEN_AT + timedelta(hours=1))
        assert await strategy.on_candle("BTC-USD", mid_stop) is None
        assert strategy._consult_task is None
        assert stubborn.calls == 1
        stubborn.release_cancel.set()
        await stop_task
        post_stop = _candle(open_at=_OPEN_AT + timedelta(hours=2))
        assert await strategy.on_candle("BTC-USD", post_stop) is None
        assert strategy._consult_task is None
        assert stubborn.calls == 1

    @pytest.mark.asyncio
    async def test_reset_gate_blocks_dispatch_during_drain_then_reopens(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a candle landing mid-reset is skipped, not lost forever.

        Given: An in-flight round whose cancellation blocks in flight,
        When: reset() drains it while a new window's candle arrives,
        Then: The mid-reset window is skipped without consuming, and the
            same window consults normally once the reset completes.
        """
        strategy = HeartbeatConsult(_config())
        stubborn = _StubbornConsult()
        monkeypatch.setattr(strategy, "_consult", stubborn)
        await strategy.on_candle("BTC-USD", _candle())
        await stubborn.started.wait()
        reset_task = asyncio.create_task(strategy.reset())
        await stubborn.cancel_seen.wait()
        window_b = _candle(open_at=_OPEN_AT + timedelta(hours=1))
        assert await strategy.on_candle("BTC-USD", window_b) is None
        assert strategy._consult_task is None
        assert stubborn.calls == 1
        stubborn.release_cancel.set()
        await reset_task
        quick = AsyncMock(return_value=None)
        monkeypatch.setattr(strategy, "_consult", quick)
        await strategy.on_candle("BTC-USD", window_b)
        await _drain_round(strategy)
        quick.assert_awaited_once()
        assert stubborn.calls == 1

    @pytest.mark.asyncio
    async def test_stop_during_reset_keeps_gate_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a reset unwound by a concurrent stop cannot reopen the gate.

        Given: reset() blocked draining a stubborn in-flight round,
        When: stop() completes while the reset is still draining, and
            the reset then finishes (its finally clears only the
            reset-scoped flag),
        Then: The permanent stop gate survives and no candle can spawn
            a fresh round afterwards.
        """
        strategy = HeartbeatConsult(_config())
        stubborn = _StubbornConsult()
        monkeypatch.setattr(strategy, "_consult", stubborn)
        await strategy.on_candle("BTC-USD", _candle())
        await stubborn.started.wait()
        reset_task = asyncio.create_task(strategy.reset())
        await stubborn.cancel_seen.wait()
        await strategy.stop()
        stubborn.release_cancel.set()
        with contextlib.suppress(asyncio.CancelledError):
            await reset_task
        post = _candle(open_at=_OPEN_AT + timedelta(hours=1))
        assert await strategy.on_candle("BTC-USD", post) is None
        assert strategy._consult_task is None
        assert stubborn.calls == 1


class TestHeartbeatConsultConsult:
    """Request construction and fail-soft error handling."""

    def _wire(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        instrument_public_id: str | None,
        primitive: AsyncMock,
        candle_rows: list[CandleRow] | None = None,
        macro_rows: list[CandleRow] | None = None,
        candles_error: Exception | None = None,
        macro_error: Exception | None = None,
        market_view: MarketViewRow | None = None,
        research_error: Exception | None = None,
    ) -> AsyncMock:
        """Patch repository + primitive for a consult round, return the repo mock."""
        repo = AsyncMock()
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=instrument_public_id)

        async def read_candles(
            instrument: str,
            timeframe: str,
            start: datetime | None,
            end: datetime | None,
            exchange: AllExchange,
            as_of: datetime,
            limit: int | None = None,
            order: str = "asc",
            complete: bool | None = None,
        ) -> list[CandleRow]:
            """Return market and macro fixtures through the repository signature."""
            if exchange == ExchangeEnum.KRAKEN_EQUITIES:
                if macro_error is not None:
                    raise macro_error
                return macro_rows or []
            if candles_error is not None:
                raise candles_error
            return candle_rows or []

        repo.get_candles = AsyncMock(side_effect=read_candles)
        repo.get_latest_market_view = AsyncMock(
            return_value=market_view,
            side_effect=research_error,
        )
        monkeypatch.setattr(
            "snapper.strategies.heartbeat_consult.get_repository", lambda db_url: repo
        )
        monkeypatch.setattr(
            "snapper.strategies.heartbeat_consult.create_ai_review_and_await", primitive
        )
        monkeypatch.setattr("snapper.strategies.heartbeat_consult.datetime", _FixedConsultClock)
        return repo

    @pytest.mark.asyncio
    async def test_builds_request_from_validated_identity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify the consult request carries every validated identity field.

        Given: A resolvable instrument, an empty candle store, and a
            succeeding primitive,
        When: _consult runs for a candle,
        Then: The AiReviewCreateRequest carries the config identities,
            the consult sequence stream, the proposed action, and the
            warm-up market snapshot.
        """
        config = _config()
        strategy = HeartbeatConsult(config)
        outcome = _approved_outcome()
        primitive = AsyncMock(return_value=outcome)
        instrument_public_id = str(uuid7())
        market_view = _market_view()
        repo = self._wire(
            monkeypatch,
            instrument_public_id=instrument_public_id,
            primitive=primitive,
            macro_rows=_fresh_macro_rows(),
            market_view=market_view,
        )
        candle = _candle()
        result = await strategy._consult("BTC-USD", candle)
        assert result is outcome
        primitive.assert_awaited_once()
        assert primitive.await_args is not None
        request = primitive.await_args.args[0]
        assert isinstance(request, AiReviewCreateRequest)
        assert request.user_public_id == config.params["ai_review_user_public_id"]
        assert request.strategy_public_id == config.params["ai_review_strategy_public_id"]
        assert request.operator_public_id == config.operator_public_id
        assert request.wallet_public_id == config.wallet_public_id
        assert request.instrument_public_id == instrument_public_id
        assert request.deadline_seconds == DEFAULT_CONSULT_DEADLINE_SECONDS
        assert request.session_id == strategy._tracker.session_id
        expected_macro_vol = round(
            statistics.pstdev([2.0 / 98.0, 2.0 / 100.0, 1.0 / 102.0, 2.0 / 103.0]) * 100.0,
            4,
        )
        assert request.signal_envelope == {
            "kind": "heartbeat",
            "open_at": candle.open_at.isoformat(),
            "close": candle.close,
            "proposed_side": "buy",
            "proposed_strength": 0.0,
            "market": {
                "timeframe": "1h",
                "bars": 1,
                "last_close": candle.close,
                "change_1_bar_pct": None,
                "change_24_bar_pct": None,
                "sma_20": None,
                "sma_50": None,
                "rsi_14": None,
                "high_24_bar": None,
                "low_24_bar": None,
                "realized_vol_24_bar_pct": None,
            },
            "macro": {
                "symbol": DEFAULT_MACRO_CONTRACT_SYMBOL,
                "as_of": _CONSULT_AS_OF.isoformat(),
                "age_minutes": 1.0,
                "session": "open",
                "change_1h_pct": 2.9412,
                "change_since_session_open_pct": 6.0606,
                "realized_vol_24h_pct": expected_macro_vol,
            },
            "research": {
                "market_view_public_id": "market-view-1",
                "regime": "risk_off",
                "bias": "avoid_new_longs",
                "confidence": 0.82,
                "age_minutes": 17.33,
                "next_events": [
                    {
                        "when_utc": (_CONSULT_AS_OF + timedelta(minutes=30)).isoformat(),
                        "name": "CPI release",
                        "severity": "high",
                    },
                    {
                        "when_utc": (_CONSULT_AS_OF + timedelta(hours=1)).isoformat(),
                        "name": "Fed speaker",
                        "severity": "medium",
                    },
                    {
                        "when_utc": (_CONSULT_AS_OF + timedelta(hours=2)).isoformat(),
                        "name": "Treasury auction",
                        "severity": "medium",
                    },
                ],
            },
        }
        repo.get_latest_market_view.assert_awaited_once_with(replay_at=_CONSULT_AS_OF)
        assert request.instrument_metadata == {"last_price": candle.close}
        assert primitive.await_args.kwargs["deadline_seconds"] == DEFAULT_CONSULT_DEADLINE_SECONDS
        assert (
            len(_serialize_signal_envelope_canonical(request.signal_envelope))
            < MAX_SIGNAL_ENVELOPE_BYTES
        )

    @pytest.mark.asyncio
    async def test_configured_macro_contract_drives_repository_read(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify quarterly contract rotation reaches the candle query.

        Given: A strategy configured with the December S&P micro contract,
        When: A consult builds its cross-asset snapshot,
        Then: The repository read and resulting envelope use that symbol.
        """
        params = _consult_params(macro_contract_symbol="MESZ6-CME")
        strategy = HeartbeatConsult(_config(params=params))
        primitive = AsyncMock(return_value=_approved_outcome())
        repo = self._wire(
            monkeypatch,
            instrument_public_id=str(uuid7()),
            primitive=primitive,
            macro_rows=_fresh_macro_rows(),
        )
        await strategy._consult("BTC-USD", _candle())
        assert repo.get_candles.await_args_list[1].args[0] == "MESZ6-CME"
        assert primitive.await_args is not None
        envelope = primitive.await_args.args[0].signal_envelope
        assert envelope["macro"]["symbol"] == "MESZ6-CME"

    @pytest.mark.asyncio
    async def test_market_snapshot_failure_keeps_macro_snapshot(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a traded-market snapshot failure cannot erase macro.

        Given: The traded candle read fails while macro candles are healthy,
        When: _consult runs,
        Then: The request still goes out without market and retains macro.
        """
        strategy = HeartbeatConsult(_config())
        outcome = _approved_outcome()
        primitive = AsyncMock(return_value=outcome)
        self._wire(
            monkeypatch,
            instrument_public_id=str(uuid7()),
            primitive=primitive,
            candles_error=RuntimeError("candle store down"),
            macro_rows=_fresh_macro_rows(),
        )
        candle = _candle()
        assert await strategy._consult("BTC-USD", candle) is outcome
        assert primitive.await_args is not None
        envelope = primitive.await_args.args[0].signal_envelope
        assert "market" not in envelope
        assert envelope["macro"]["symbol"] == DEFAULT_MACRO_CONTRACT_SYMBOL
        assert envelope["kind"] == "heartbeat"
        assert envelope["proposed_side"] == "buy"

    @pytest.mark.asyncio
    async def test_macro_snapshot_failure_keeps_market_snapshot(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify macro failure omits only macro and never breaks consult.

        Given: A healthy traded-market read and failing macro read,
        When: _consult runs,
        Then: The request is created with market and without macro.
        """
        strategy = HeartbeatConsult(_config())
        outcome = _approved_outcome()
        primitive = AsyncMock(return_value=outcome)
        self._wire(
            monkeypatch,
            instrument_public_id=str(uuid7()),
            primitive=primitive,
            macro_error=RuntimeError("macro candle store down"),
        )
        assert await strategy._consult("BTC-USD", _candle()) is outcome
        assert primitive.await_args is not None
        envelope = primitive.await_args.args[0].signal_envelope
        assert "market" in envelope
        assert "macro" not in envelope

    @pytest.mark.asyncio
    async def test_no_current_research_view_omits_digest(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify the normal absence of research omits only that section.

        Given: No causally eligible current market view,
        When: _consult builds the delegate request,
        Then: The consult succeeds without a research envelope key.
        """
        strategy = HeartbeatConsult(_config())
        outcome = _approved_outcome()
        primitive = AsyncMock(return_value=outcome)
        repo = self._wire(
            monkeypatch,
            instrument_public_id=str(uuid7()),
            primitive=primitive,
            macro_rows=_fresh_macro_rows(),
        )
        assert await strategy._consult("BTC-USD", _candle()) is outcome
        assert primitive.await_args is not None
        envelope = primitive.await_args.args[0].signal_envelope
        assert "research" not in envelope
        repo.get_latest_market_view.assert_awaited_once_with(replay_at=_CONSULT_AS_OF)

    @pytest.mark.asyncio
    async def test_research_read_failure_is_fail_soft_and_warns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a research read error cannot block the consult.

        Given: The latest-market-view read raises an unexpected error,
        When: _consult builds the delegate request,
        Then: The consult succeeds without research and logs a warning.
        """
        strategy = HeartbeatConsult(_config())
        outcome = _approved_outcome()
        primitive = AsyncMock(return_value=outcome)
        warning = MagicMock()
        monkeypatch.setattr("snapper.strategies.heartbeat_consult.logger.warning", warning)
        self._wire(
            monkeypatch,
            instrument_public_id=str(uuid7()),
            primitive=primitive,
            macro_rows=_fresh_macro_rows(),
            research_error=RuntimeError("research store down"),
        )
        assert await strategy._consult("BTC-USD", _candle()) is outcome
        assert primitive.await_args is not None
        envelope = primitive.await_args.args[0].signal_envelope
        assert "research" not in envelope
        warning.assert_called_once_with(
            f"Strategy {strategy.name}: research digest unavailable — research store down"
        )

    @pytest.mark.asyncio
    async def test_consult_sequence_uses_named_stream(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify consult provenance advances the dedicated stream.

        Given: Two consecutive consult rounds,
        When: Both build requests,
        Then: sequence_id advances within the consult stream.
        """
        strategy = HeartbeatConsult(_config())
        primitive = AsyncMock(return_value=None)
        self._wire(monkeypatch, instrument_public_id=str(uuid7()), primitive=primitive)
        expected_first = strategy._tracker.next_sequence(CONSULT_SEQUENCE_STREAM) + 1
        await strategy._consult("BTC-USD", _candle())
        assert primitive.await_args is not None
        first = primitive.await_args.args[0].sequence_id
        await strategy._consult("BTC-USD", _candle())
        assert primitive.await_args is not None
        second = primitive.await_args.args[0].sequence_id
        assert first == expected_first
        assert second == first + 1

    @pytest.mark.asyncio
    async def test_unresolvable_instrument_skips_round(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a missing instrument row skips the round without a create.

        Given: No active instrument row for the symbol,
        When: _consult runs,
        Then: None is returned and the primitive is never awaited.
        """
        strategy = HeartbeatConsult(_config())
        primitive = AsyncMock(return_value=_approved_outcome())
        self._wire(monkeypatch, instrument_public_id=None, primitive=primitive)
        assert await strategy._consult("BTC-USD", _candle()) is None
        primitive.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "admission_error",
        [NoLiveDelegateError("no delegate"), DelegateBusyError("busy")],
    )
    async def test_admission_errors_fall_through(
        self, monkeypatch: pytest.MonkeyPatch, admission_error: Exception
    ) -> None:
        """Verify admission-control errors fall through to None.

        Given: The primitive raising an admission error,
        When: _consult runs,
        Then: None is returned and nothing propagates.
        """
        strategy = HeartbeatConsult(_config())
        primitive = AsyncMock(side_effect=admission_error)
        self._wire(monkeypatch, instrument_public_id=str(uuid7()), primitive=primitive)
        assert await strategy._consult("BTC-USD", _candle()) is None

    @pytest.mark.asyncio
    async def test_unexpected_error_is_fail_soft(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify an unexpected error never escapes the consult round.

        Given: The primitive raising RuntimeError,
        When: _consult runs,
        Then: None is returned and nothing propagates.
        """
        strategy = HeartbeatConsult(_config())
        primitive = AsyncMock(side_effect=RuntimeError("boom"))
        self._wire(monkeypatch, instrument_public_id=str(uuid7()), primitive=primitive)
        assert await strategy._consult("BTC-USD", _candle()) is None
