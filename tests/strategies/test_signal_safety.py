"""Tests for the P6 signal-safety convention.

Covers plan §P6 (``plan_2026_07_03_strategy_runtime_split_and_mcp_wake.md``):
exits expressed as flat targets (``strength=0.0``) instead of opposite-
side ``strength>0`` (so a dropped entry followed by an exit no-ops
instead of opening a reversed position), the bar-driven standing-target
re-assert layer (in-session drop repair, idempotent on the trade side),
same-bar duplicate suppression, warm-up-floor suppression, RSI/MACD
``long_only`` clamping, and cointegration's paired re-assert + floor
gating.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest

from snapper.application.services.signals.service import signal_service
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import ReplayStartData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import StrategyConfig
from snapper.strategies.base import StrategySignal
from snapper.strategies.cointegration import CointegrationPairs
from snapper.strategies.macd import MACDCrossover
from snapper.strategies.process_wrapper import create_strategy_process
from snapper.strategies.rsi import RSIReversion

_OPEN_AT = datetime(2026, 7, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _mock_signal_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace signal persistence with a no-op for these unit tests."""
    monkeypatch.setattr(signal_service, "store_signal", AsyncMock(return_value=""))


class _ScriptStrategy(BaseStrategy):
    """Single-leg strategy that returns pre-scripted signals per bar.

    ``reassert`` toggles the re-assert opt-in so the standing-target
    layer can be exercised deterministically.
    """

    def __init__(self, config: StrategyConfig, script: list[StrategySignal | None]) -> None:
        """Bind the per-bar script and the re-assert opt-in flag."""
        super().__init__(config)
        self._script = list(script)
        self._reassert = bool(config.params.get("reassert", False))

    def reasserts_targets(self) -> bool:
        """Return the configured re-assert opt-in."""
        return self._reassert

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Pop the next scripted signal (or None when the script is empty)."""
        del instrument, candle
        return self._script.pop(0) if self._script else None

    async def reset(self) -> None:
        """No-op reset for replay."""


def _config(**params: Any) -> StrategyConfig:
    """Build a single-leg BTC-USD paper config for the script strategy."""
    return StrategyConfig(
        name="signal_safety",
        strategy_class="_ScriptStrategy",
        inputs=["market.kraken.BTC-USD.candles.1h"],
        outputs=["BTC-USD"],
        exchange="paper",
        params=params,
    )


def _candle(open_at: datetime, close: float = 100.0) -> CandleData:
    """Build a 1h kraken candle at the given window."""
    return CandleData(
        session_id="live",
        sequence_id=1,
        public_id="c",
        timestamp=open_at,
        instrument="BTC-USD",
        exchange="kraken",
        timeframe="1h",
        open_at=open_at,
        open=close,
        high=close + 1,
        low=close - 1,
        close=close,
        volume=1.0,
    )


def _flat_exit() -> StrategySignal:
    """Return a flat exit signal used by the missed-entry test scripts."""
    return StrategySignal(
        instrument="BTC-USD", side="buy", strength=0.0, reason="exit", price=100.0
    )


def _buy(strength: float = 1.0) -> StrategySignal:
    """Build a BUY signal at the given strength."""
    return StrategySignal(
        instrument="BTC-USD", side="buy", strength=strength, reason="buy", price=100.0
    )


async def _feed(strategy: BaseStrategy, open_at: datetime, close: float = 100.0) -> None:
    """Drive one candle through the handler then emit its group (as the loop does).

    Mirrors ``_listen_loop``: ``_handle_candle_data`` runs the callback +
    the re-assert layer, and the caller emits the returned callback group
    (which is what populates the standing-target map via
    ``_emit_signal_group``).
    """
    group = await strategy._handle_candle_data("BTC-USD", _candle(open_at, close).model_dump_json())
    await strategy._emit_signal_group(group)


class TestEmitFlat:
    """emit_flat is a flat-target signal builder."""

    def test_emit_flat_builds_zero_strength(self) -> None:
        """Verify emit_flat returns a strength-0.0 signal (a flat target).

        Given: A strategy and an instrument to flatten,
        When: emit_flat is called,
        Then: The returned signal has strength 0.0 (never a naked short).
        """
        strategy = _ScriptStrategy(_config(), [])
        flat = strategy.emit_flat("BTC-USD", "exit", 100.0)
        assert flat.strength == 0.0
        assert flat.instrument == "BTC-USD"

    @pytest.mark.asyncio
    async def test_missed_entry_then_flat_exit_is_no_reversal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a dropped entry then flat-exit emits only a flat target.

        Given: The entry bar's emission is dropped (at-most-once), then an
            exit bar,
        When: Both bars flow through the handler,
        Then: The only emitted signal is the flat exit (strength 0.0) — the
            coordinator delta engine no-ops it against a flat engine, so no
            reversed position can open. (Contrast: an opposite-side
            strength>0 exit would be an absolute reversed target.)
        """
        strategy = _ScriptStrategy(_config(), [_buy(), _flat_exit()])
        emitted: list[StrategySignal] = []
        monkeypatch.setattr(
            strategy, "emit_signal", AsyncMock(side_effect=lambda s, **k: emitted.append(s))
        )
        await _feed(strategy, _OPEN_AT)
        emitted.clear()
        await _feed(strategy, _OPEN_AT + timedelta(hours=1))
        assert [s.strength for s in emitted] == [0.0]


class TestReassertLayer:
    """Bar-driven standing-target re-assert (opt-in)."""

    @pytest.mark.asyncio
    async def test_no_reassert_when_opted_out(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify the default (opt-out) strategy never re-asserts.

        Given: A strategy with reassert=False that emitted a BUY,
        When: A subsequent idle bar arrives,
        Then: No re-assertion is emitted (only the original BUY).
        """
        strategy = _ScriptStrategy(_config(reassert=False), [_buy(), None])
        emitted: list[StrategySignal] = []
        monkeypatch.setattr(
            strategy, "emit_signal", AsyncMock(side_effect=lambda s, **k: emitted.append(s))
        )
        await _feed(strategy, _OPEN_AT)
        emitted.clear()
        await _feed(strategy, _OPEN_AT + timedelta(hours=1))
        assert emitted == []

    @pytest.mark.asyncio
    async def test_reassert_standing_target_on_idle_bar(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify an idle bar re-asserts the standing target when opted in.

        Given: An opted-in strategy that emitted a BUY, then an idle bar,
        When: The idle bar flows through,
        Then: The standing BUY target is re-emitted (in-session self-heal).
        """
        strategy = _ScriptStrategy(_config(reassert=True), [_buy(), None])
        emitted: list[StrategySignal] = []
        monkeypatch.setattr(
            strategy, "emit_signal", AsyncMock(side_effect=lambda s, **k: emitted.append(s))
        )
        await _feed(strategy, _OPEN_AT)
        emitted.clear()
        await _feed(strategy, _OPEN_AT + timedelta(hours=1))
        assert [s.side for s in emitted] == ["buy"]
        assert [s.strength for s in emitted] == [1.0]

    @pytest.mark.asyncio
    async def test_fresh_callback_signal_not_duplicated_by_reassert(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a bar's own callback signal is not also re-asserted.

        Given: An opted-in strategy that emitted a BUY, then a bar whose
            callback returns a fresh flat exit for the same instrument,
        When: The exit bar flows through,
        Then: Only the fresh flat exit is emitted — the prior BUY is NOT
            re-asserted on the same bar (no same-bar duplicate).
        """
        strategy = _ScriptStrategy(_config(reassert=True), [_buy(), _flat_exit()])
        emitted: list[StrategySignal] = []
        monkeypatch.setattr(
            strategy, "emit_signal", AsyncMock(side_effect=lambda s, **k: emitted.append(s))
        )
        await _feed(strategy, _OPEN_AT)
        emitted.clear()
        await _feed(strategy, _OPEN_AT + timedelta(hours=1))
        assert [s.strength for s in emitted] == [0.0]

    @pytest.mark.asyncio
    async def test_no_reassert_on_warmup_floor_bar(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a warm-up-floor bar suppresses the callback AND re-assert.

        Given: An opted-in strategy with a standing BUY and a warm-up
            high-water,
        When: A bar at/below the high-water arrives,
        Then: Nothing is emitted (the bar returns before the callback and
            the re-assert layer).
        """
        strategy = _ScriptStrategy(_config(reassert=True), [_buy()])
        emitted: list[StrategySignal] = []
        monkeypatch.setattr(
            strategy, "emit_signal", AsyncMock(side_effect=lambda s, **k: emitted.append(s))
        )
        await _feed(strategy, _OPEN_AT)
        emitted.clear()
        strategy._warmup_high_water["BTC-USD"] = _OPEN_AT + timedelta(hours=5)
        await _feed(strategy, _OPEN_AT + timedelta(hours=5))
        assert emitted == []


class TestLongOnlyClamp:
    """RSI/MACD long_only clamps a would-be short to a flat target."""

    def _rsi(self, long_only: bool) -> RSIReversion:
        """Build an RSI strategy with the given long_only flag."""
        return RSIReversion(
            StrategyConfig(
                name="rsi",
                strategy_class="RSIReversion",
                inputs=["market.kraken.BTC-USD.candles.1h"],
                outputs=["BTC-USD"],
                exchange="paper",
                params={"period": 3, "upper": 70.0, "lower": 30.0, "long_only": long_only},
            )
        )

    @pytest.mark.asyncio
    async def test_rsi_overbought_flattens_when_long_only(self) -> None:
        """Verify long_only RSI flattens on overbought instead of shorting.

        Given: A long_only RSI and a rising overbought series,
        When: on_candle fires on the overbought bar,
        Then: The signal is flat (strength 0.0), never a naked short.
        """
        strategy = self._rsi(long_only=True)
        for i, close in enumerate((100.0, 101.0, 102.0, 103.0, 130.0)):
            strategy._buffer_candle("BTC-USD", _candle(_OPEN_AT + timedelta(hours=i), close))
        result = await strategy.on_candle("BTC-USD", _candle(_OPEN_AT + timedelta(hours=4), 130.0))
        assert result is not None
        assert result.strength == 0.0

    @pytest.mark.asyncio
    async def test_rsi_overbought_shorts_when_not_long_only(self) -> None:
        """Verify non-long_only RSI still emits a genuine short.

        Given: A long_only=False RSI and an overbought series,
        When: on_candle fires on the overbought bar,
        Then: A SELL strength>0 is emitted (the deliberate reversal path).
        """
        strategy = self._rsi(long_only=False)
        for i, close in enumerate((100.0, 101.0, 102.0, 103.0, 130.0)):
            strategy._buffer_candle("BTC-USD", _candle(_OPEN_AT + timedelta(hours=i), close))
        result = await strategy.on_candle("BTC-USD", _candle(_OPEN_AT + timedelta(hours=4), 130.0))
        assert result is not None
        assert result.side == "sell"
        assert result.strength > 0.0

    def test_macd_long_only_default_true(self) -> None:
        """Verify MACD defaults to long_only=True (conservative first arming)."""
        strategy = MACDCrossover(
            StrategyConfig(
                name="macd",
                strategy_class="MACDCrossover",
                inputs=["market.kraken.BTC-USD.candles.1h"],
                outputs=["BTC-USD"],
                exchange="paper",
                params={},
            )
        )
        assert strategy.long_only is True


class TestCointegrationReassert:
    """Cointegration re-asserts both legs as a paired group, floor-gated."""

    def _pair(self) -> CointegrationPairs:
        """Build a cointegration pair over BTC-USD / ETH-USD (paper)."""
        return CointegrationPairs(
            StrategyConfig(
                name="coint",
                strategy_class="CointegrationPairs",
                inputs=[
                    "market.paper.kraken.BTC-USD.candles.1h",
                    "market.paper.kraken.ETH-USD.candles.1h",
                ],
                outputs=["BTC-USD", "ETH-USD"],
                exchange="paper",
                params={
                    "beta": 0.05,
                    "entry_threshold": 2.0,
                    "exit_threshold": 0.5,
                    "lookback_window": 5,
                    "min_data_points": 3,
                },
            )
        )

    def _buffer_both(self, pair: CointegrationPairs, days: int) -> datetime:
        """Buffer ``days`` aligned daily bars for both legs; return the last open_at."""
        last = _OPEN_AT
        for i in range(days):
            last = _OPEN_AT + timedelta(days=i)
            for inst in (pair.instrument1, pair.instrument2):
                pair.candle_buffer.setdefault(inst, []).append(_candle(last))
        return last

    def test_pair_opts_into_reassert(self) -> None:
        """Verify the pair opts into the re-assert layer."""
        assert self._pair().reasserts_targets() is True

    def test_reassert_group_returns_both_legs_in_order(self) -> None:
        """Verify the pair re-asserts both standing legs, primary then hedge.

        Given: Standing targets for both legs,
        When: _reassert_target_group runs,
        Then: It returns [instrument1_leg, instrument2_leg] (never a single
            naked leg).
        """
        pair = self._pair()
        leg1 = StrategySignal(
            instrument=pair.instrument1, side="buy", strength=1.0, reason="e", price=1.0
        )
        leg2 = StrategySignal(
            instrument=pair.instrument2, side="sell", strength=0.05, reason="e", price=1.0
        )
        pair._target[pair.instrument1] = leg1
        pair._target[pair.instrument2] = leg2
        group = pair._reassert_target_group(pair.instrument1, leg1)
        assert [s.instrument for s in group] == [pair.instrument1, pair.instrument2]

    def test_reassert_group_empty_when_a_leg_missing(self) -> None:
        """Verify a missing standing leg yields no re-assertion (never naked)."""
        pair = self._pair()
        pair._target[pair.instrument1] = StrategySignal(
            instrument=pair.instrument1, side="buy", strength=1.0, reason="e", price=1.0
        )
        assert pair._reassert_target_group(pair.instrument1, pair._target[pair.instrument1]) == []

    def test_should_reassert_only_primary_on_aligned_bar(self) -> None:
        """Verify re-assert fires only for the primary leg on a valid bar.

        Given: Aligned buffers past any floor,
        When: should_reassert_targets is checked for each leg,
        Then: True for the primary leg, False for the hedge leg (single
            deterministic trigger).
        """
        pair = self._pair()
        last = self._buffer_both(pair, 4)
        bar = _candle(last)
        assert pair.should_reassert_targets(pair.instrument1, bar) is True
        assert pair.should_reassert_targets(pair.instrument2, bar) is False

    def test_should_reassert_false_below_signal_floor(self) -> None:
        """Verify a bar at/under the signal floor never re-asserts.

        Given: A signal floor set past the latest aligned bar,
        When: should_reassert_targets is checked,
        Then: It is False (warmed/stale bars never re-assert a tradeable
            target).
        """
        pair = self._pair()
        last = self._buffer_both(pair, 4)
        pair._last_signal_open_at = last
        assert pair.should_reassert_targets(pair.instrument1, _candle(last)) is False


class TestReassertCodeReviewFixes:
    """Regression tests for the P6 code-review fixes."""

    @pytest.mark.asyncio
    async def test_reassertion_reprices_to_current_market(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a re-assertion is repriced to the current buffered close.

        Given: A BUY at price 100 dropped, then an idle bar at price 150,
        When: The idle bar re-asserts,
        Then: The re-emitted signal carries price 150 (current market),
            not the stale 100 — the coordinator prices/sizes against it.
        """
        strategy = _ScriptStrategy(_config(reassert=True), [_buy(), None])
        emitted: list[StrategySignal] = []
        monkeypatch.setattr(
            strategy, "emit_signal", AsyncMock(side_effect=lambda s, **k: emitted.append(s))
        )
        await _feed(strategy, _OPEN_AT, close=100.0)
        emitted.clear()
        await _feed(strategy, _OPEN_AT + timedelta(hours=1), close=150.0)
        assert [s.price for s in emitted] == [150.0]
        assert emitted[0].timestamp is None or emitted[0].strength == 1.0

    @pytest.mark.asyncio
    async def test_flat_standing_target_reasserted_to_self_heal_exit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a dropped flat exit is re-asserted to self-heal the close.

        Given: A flat exit emitted (standing target strength 0.0), then an
            idle bar,
        When: The idle bar flows through,
        Then: The flat target is re-asserted (strength 0.0) so a dropped
            exit still flattens on the next bar (idempotent on an
            already-flat engine).
        """
        strategy = _ScriptStrategy(_config(reassert=True), [_flat_exit(), None])
        emitted: list[StrategySignal] = []
        monkeypatch.setattr(
            strategy, "emit_signal", AsyncMock(side_effect=lambda s, **k: emitted.append(s))
        )
        await _feed(strategy, _OPEN_AT)
        emitted.clear()
        await _feed(strategy, _OPEN_AT + timedelta(hours=1))
        assert [s.strength for s in emitted] == [0.0]

    @pytest.mark.asyncio
    async def test_reassert_reason_does_not_stack(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify repeated re-asserts never stack the reason suffix.

        Given: A standing BUY re-asserted over three idle bars,
        When: Each idle bar re-asserts,
        Then: The reason carries at most one ' (re-assert)' suffix (the
            standing target is never overwritten by a re-assertion, so it
            cannot drift or stack).
        """
        strategy = _ScriptStrategy(_config(reassert=True), [_buy(), None, None, None])
        emitted: list[StrategySignal] = []
        monkeypatch.setattr(
            strategy, "emit_signal", AsyncMock(side_effect=lambda s, **k: emitted.append(s))
        )
        await _feed(strategy, _OPEN_AT)
        for i in range(1, 4):
            await _feed(strategy, _OPEN_AT + timedelta(hours=i))
        reasserts = [s for s in emitted if "re-assert" in s.reason]
        assert reasserts
        assert all(s.reason.count("(re-assert)") == 1 for s in reasserts)

    @pytest.mark.asyncio
    async def test_reassert_failure_is_fail_soft(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a re-assertion emit failure never drops the bar's callback.

        Given: A standing BUY and a bar whose callback returns a fresh BUY
            while re-assertion of a SECOND instrument raises,
        When: The bar flows through,
        Then: _handle_candle_data does not raise (the caller still emits the
            callback group).
        """
        strategy = _ScriptStrategy(_config(reassert=True), [_buy(), _buy()])
        monkeypatch.setattr(strategy, "emit_signal", AsyncMock())
        await _feed(strategy, _OPEN_AT)
        strategy._target["ETH-USD"] = StrategySignal(
            instrument="ETH-USD", side="buy", strength=1.0, reason="e", price=1.0
        )
        monkeypatch.setattr(
            strategy, "_emit_signal_group", AsyncMock(side_effect=RuntimeError("publisher down"))
        )
        group = await strategy._handle_candle_data(
            "BTC-USD", _candle(_OPEN_AT + timedelta(hours=1)).model_dump_json()
        )
        assert len(group) == 1

    @pytest.mark.asyncio
    async def test_replay_reset_clears_standing_targets(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify replay reset clears the standing-target map.

        Given: A strategy with a standing target,
        When: The replay-start system message is handled,
        Then: _target is cleared so a post-reset bar cannot re-assert a
            stale pre-reset target.
        """
        strategy = _ScriptStrategy(_config(reassert=True), [_buy()])
        monkeypatch.setattr(strategy, "emit_signal", AsyncMock())
        await _feed(strategy, _OPEN_AT)
        assert strategy._target != {}
        payload = ReplayStartData(
            session_id="s",
            sequence_id=1,
            public_id="r",
            timestamp=datetime(2026, 7, 1, tzinfo=UTC),
            started_at=datetime(2026, 7, 1, tzinfo=UTC),
        ).model_dump_json()
        await strategy._system_router.handle_replay_start(payload)
        assert strategy._target == {}


class TestCoverageBranches:
    """Remaining P6 branch coverage (gate requires 100%)."""

    @pytest.mark.asyncio
    async def test_should_reassert_false_skips_instrument(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a False should_reassert_targets gate skips the instrument.

        Given: An opted-in strategy with a standing BUY whose gate returns
            False, then an idle bar,
        When: The idle bar flows through,
        Then: No re-assertion is emitted (the per-instrument gate holds).
        """
        strategy = _ScriptStrategy(_config(reassert=True), [_buy(), None])
        emitted: list[StrategySignal] = []
        monkeypatch.setattr(
            strategy, "emit_signal", AsyncMock(side_effect=lambda s, **k: emitted.append(s))
        )
        await _feed(strategy, _OPEN_AT)
        emitted.clear()
        monkeypatch.setattr(strategy, "should_reassert_targets", lambda inst, c: False)
        await _feed(strategy, _OPEN_AT + timedelta(hours=1))
        assert emitted == []

    def test_rsi_opts_into_reassert(self) -> None:
        """Verify RSIReversion opts into the re-assert layer."""
        strategy = RSIReversion(
            StrategyConfig(
                name="rsi",
                strategy_class="RSIReversion",
                inputs=["market.kraken.BTC-USD.candles.1h"],
                outputs=["BTC-USD"],
                exchange="paper",
                params={},
            )
        )
        assert strategy.reasserts_targets() is True

    @pytest.mark.asyncio
    async def test_macd_bear_cross_flattens_when_long_only(self) -> None:
        """Verify long_only MACD flattens on a bear cross instead of shorting.

        Given: A long_only MACD with a cached positive histogram and a
            buffer whose recomputed histogram is negative,
        When: on_candle fires,
        Then: The signal is flat (strength 0.0), never a short.
        """
        strategy = MACDCrossover(
            StrategyConfig(
                name="macd",
                strategy_class="MACDCrossover",
                inputs=["market.kraken.BTC-USD.candles.1h"],
                outputs=["BTC-USD"],
                exchange="paper",
                params={"fast": 3, "slow": 5, "signal_period": 2},
            )
        )
        closes = [100.0, 104.0, 108.0, 112.0, 116.0, 112.0, 106.0, 100.0, 94.0, 88.0]
        for i, close in enumerate(closes):
            strategy.candle_buffer.setdefault("BTC-USD", []).append(
                _candle(_OPEN_AT + timedelta(hours=i), close)
            )
        strategy._last_hist["BTC-USD"] = 0.5
        result = await strategy.on_candle("BTC-USD", _candle(_OPEN_AT + timedelta(hours=9), 88.0))
        assert result is not None
        assert result.strength == 0.0

    def test_coint_should_reassert_false_without_common_bars(self) -> None:
        """Verify no common aligned bars means no re-assert (pair unaligned)."""
        pair = TestCointegrationReassert()._pair()
        pair.candle_buffer[pair.instrument1] = [_candle(_OPEN_AT)]
        pair.candle_buffer[pair.instrument2] = []
        assert pair.should_reassert_targets(pair.instrument1, _candle(_OPEN_AT)) is False

    def test_coint_should_reassert_false_on_stale_bar(self) -> None:
        """Verify a bar older than the latest common day never re-asserts."""
        helper = TestCointegrationReassert()
        pair = helper._pair()
        helper._buffer_both(pair, 4)
        assert pair.should_reassert_targets(pair.instrument1, _candle(_OPEN_AT)) is False


class TestBusFrameIsolation:
    """P7 soak findings: bad frames must not kill or zombify a strategy."""

    @pytest.mark.asyncio
    async def test_malformed_payload_skipped_loop_survives(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a malformed candle payload is skipped, not fatal.

        Given: A frame whose payload fails CandleData validation, then a
            valid bar,
        When: Both frames dispatch through _handle_bus_frame,
        Then: No exception propagates and the valid bar still reaches the
            strategy (buffer grows) — one bad frame from any publisher on
            the shared bus cannot kill the listen loop.
        """
        strategy = _ScriptStrategy(_config(), [None])
        monkeypatch.setattr(strategy, "emit_signal", AsyncMock())
        await strategy._handle_bus_frame(
            "market.kraken.BTC-USD.candles.1h", '{"type": "not_a_candle"}'
        )
        await strategy._handle_bus_frame(
            "market.kraken.BTC-USD.candles.1h", _candle(_OPEN_AT).model_dump_json()
        )
        assert len(strategy.candle_buffer.get("BTC-USD", [])) == 1

    @pytest.mark.asyncio
    async def test_system_frame_error_is_isolated(self) -> None:
        """Verify a failing system-message handler cannot kill the loop.

        Given: A system frame whose payload is invalid JSON,
        When: _handle_bus_frame dispatches it,
        Then: No exception propagates (frame logged and skipped).
        """
        strategy = _ScriptStrategy(_config(), [])
        await strategy._handle_bus_frame("system.replay.start", "{not json")

    @pytest.mark.asyncio
    async def test_wrapper_raises_when_listen_loop_dies(self) -> None:
        """Verify the process wrapper converts a dead listen loop to an error.

        Given: A started strategy process whose listen task ends while the
            stop event is unset,
        When: The wrapper's wait guard observes the death,
        Then: RuntimeError raises (so the launcher watchdog restarts the
            process instead of leaving a running=True zombie).
        """
        wrapper_cls = create_strategy_process(
            "p7_zombie_guard_test",
            "_ScriptStrategy",
            {
                "name": "zombie_guard",
                "inputs": ["market.kraken.BTC-USD.candles.1h"],
                "outputs": ["BTC-USD"],
                "exchange": "paper",
                "params": {},
            },
        )
        wrapper = wrapper_cls(
            name="zombie_guard",
            inputs=["market.kraken.BTC-USD.candles.1h"],
            outputs=["BTC-USD"],
        )
        wrapper._stop_event = asyncio.Event()

        class _FakeStrategy:
            """Carrier for a pre-failed listen task."""

        fake = _FakeStrategy()

        async def _dead_loop() -> None:
            raise ValueError("socket died")

        fake._listen_task = asyncio.ensure_future(_dead_loop())
        await asyncio.sleep(0)
        wrapper.strategy = fake
        with pytest.raises(RuntimeError, match="listen loop exited unexpectedly"):
            await wrapper._wait_stop_or_listen_death()

    @pytest.mark.asyncio
    async def test_wrapper_returns_on_stop_event(self) -> None:
        """Verify a normal stop returns without raising.

        Given: A wrapper whose stop event fires while the listen loop
            keeps running,
        When: The wait guard runs,
        Then: It returns normally (no false-positive zombie error).
        """
        wrapper_cls = create_strategy_process(
            "p7_stop_guard_test",
            "_ScriptStrategy",
            {
                "name": "stop_guard",
                "inputs": ["market.kraken.BTC-USD.candles.1h"],
                "outputs": ["BTC-USD"],
                "exchange": "paper",
                "params": {},
            },
        )
        wrapper = wrapper_cls(
            name="stop_guard",
            inputs=["market.kraken.BTC-USD.candles.1h"],
            outputs=["BTC-USD"],
        )
        wrapper._stop_event = asyncio.Event()

        class _FakeStrategy:
            """Carrier for a live listen task."""

        fake = _FakeStrategy()

        async def _live_loop() -> None:
            await asyncio.Event().wait()

        fake._listen_task = asyncio.ensure_future(_live_loop())
        wrapper.strategy = fake
        wrapper._stop_event.set()
        await wrapper._wait_stop_or_listen_death()
        fake._listen_task.cancel()


class TestEmitFailureFatal:
    """Emit failures must NOT be swallowed as skipped frames (round-2 fix)."""

    @pytest.mark.asyncio
    async def test_emit_failure_propagates_from_bus_frame(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a broken publisher path fails the frame handler loudly.

        Given: A valid bar whose callback yields a signal while
            _emit_signal_group raises (publisher socket broken),
        When: _handle_bus_frame dispatches it,
        Then: The exception propagates (so the listen loop dies and the
            watchdog restarts the strategy) instead of being logged as a
            skipped frame while the decision is silently lost.
        """
        strategy = _ScriptStrategy(_config(), [_buy()])
        monkeypatch.setattr(
            strategy,
            "_emit_signal_group",
            AsyncMock(side_effect=RuntimeError("publisher down")),
        )
        with pytest.raises(RuntimeError, match="publisher down"):
            await strategy._handle_bus_frame(
                "market.kraken.BTC-USD.candles.1h", _candle(_OPEN_AT).model_dump_json()
            )

    @pytest.mark.asyncio
    async def test_stop_cancels_listen_task_before_socket_close(self) -> None:
        """Verify stop() cancels the listen task before closing the socket.

        Given: A started-shaped strategy with a live listen task and a
            recorded unsubscribe,
        When: stop() runs,
        Then: The listen task is already cancelled by the time the
            subscriber closes (an intentional stop can never surface a
            recv error as a crash).
        """
        strategy = _ScriptStrategy(_config(), [])
        order: list[str] = []

        async def _live_loop() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                order.append("listen_cancelled")
                raise

        strategy._listen_task = asyncio.ensure_future(_live_loop())
        await asyncio.sleep(0)

        async def _record_unsub() -> None:
            order.append("subscriber_closed")

        strategy._unsubscribe_inputs = _record_unsub
        await strategy.stop()
        assert order == ["listen_cancelled", "subscriber_closed"]


class TestIsolationCoverageBranches:
    """Remaining branch coverage for the P7 isolation fixes."""

    @pytest.mark.asyncio
    async def test_cancellation_propagates_through_frame_handler(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify CancelledError is never treated as a skippable frame error.

        Given: A dispatch that raises CancelledError (shutdown during
            handling),
        When: _handle_bus_frame processes a market frame,
        Then: The cancellation propagates (isolation must not eat it).
        """
        strategy = _ScriptStrategy(_config(), [])
        monkeypatch.setattr(
            strategy,
            "_dispatch_market_data",
            AsyncMock(side_effect=asyncio.CancelledError()),
        )
        with pytest.raises(asyncio.CancelledError):
            await strategy._handle_bus_frame(
                "market.kraken.BTC-USD.candles.1h", _candle(_OPEN_AT).model_dump_json()
            )

    @pytest.mark.asyncio
    async def test_wrapper_waits_stop_only_without_listen_task(self) -> None:
        """Verify a strategy without a listen task waits on stop alone.

        Given: A wrapper whose strategy exposes no _listen_task,
        When: The wait guard runs with the stop event already set,
        Then: It returns normally (direct-emit strategies keep the plain
            stop-event contract).
        """
        wrapper_cls = create_strategy_process(
            "p7_no_listen_guard_test",
            "_ScriptStrategy",
            {
                "name": "no_listen_guard",
                "inputs": ["market.kraken.BTC-USD.candles.1h"],
                "outputs": ["BTC-USD"],
                "exchange": "paper",
                "params": {},
            },
        )
        wrapper = wrapper_cls(
            name="no_listen_guard",
            inputs=["market.kraken.BTC-USD.candles.1h"],
            outputs=["BTC-USD"],
        )
        wrapper._stop_event = asyncio.Event()

        class _NoListenStrategy:
            """Strategy stand-in without a _listen_task attribute."""

        wrapper.strategy = _NoListenStrategy()
        wrapper._stop_event.set()
        await wrapper._wait_stop_or_listen_death()
