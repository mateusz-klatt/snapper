"""Unit tests for the N-leg paired-signal helpers.

Covers :mod:`snapper.strategies.multi_leg` — ``resolve_legs`` plus the
``MultiLegSpreadMixin`` partner-iteration helpers. Behavioural
integration with :class:`CointegrationPairs` is covered separately by
the existing strategy tests; this module isolates the helper layer.
"""

from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import MagicMock

import pytest

from snapper.core.types import MarketDataExchange
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.models import StrategySignal
from snapper.strategies.multi_leg import MultiLegSpreadMixin
from snapper.strategies.multi_leg import _extract_instrument_from_topic
from snapper.strategies.multi_leg import resolve_legs

NOW = datetime(2026, 5, 8, 12, 0, 0, tzinfo=UTC)


@dataclass
class _StubConfig:
    """Minimal duck-typed config object for resolve_legs unit tests.

    Using StrategyConfig directly would trigger output-instrument
    tradeability validation, which is irrelevant to the helper's
    contract — resolve_legs reads only ``inputs`` and ``outputs``.
    """

    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)


def _config(inputs: list[str], outputs: list[str]) -> _StubConfig:
    return _StubConfig(inputs=inputs, outputs=outputs)


def _candle(ts: datetime, close: float) -> CandleData:
    return CandleData(
        sequence_id=1,
        public_id="01000000-0000-7000-8000-000000000000",
        timestamp=ts,
        session_id="test-session",
        instrument="X-USD",
        exchange=cast("MarketDataExchange", "kraken"),
        timeframe="1h",
        open_at=ts,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1.0,
        vwap=close,
    )


class TestExtractInstrumentFromTopic:
    """Tests for the topic-extraction helper."""

    def test_canonical_topic_returns_instrument(self) -> None:
        """Canonical market topic parses to its instrument."""
        out = _extract_instrument_from_topic("market.paper.kraken.BTC-USD.candles.1h")
        assert out == "BTC-USD"

    def test_unparseable_topic_falls_back_to_raw(self) -> None:
        """Unparseable topic returns the raw string."""
        out = _extract_instrument_from_topic("not-a-topic")
        assert out == "not-a-topic"


class TestResolveLegs:
    """Tests for ``resolve_legs`` invocation-shape resolution."""

    def test_two_inputs_live(self) -> None:
        """Two ZMQ topics resolve to a 2-tuple of leg instruments."""
        config = _config(
            inputs=[
                "market.paper.kraken.BTC-USD.candles.1h",
                "market.paper.kraken.ETH-USD.candles.1h",
            ],
            outputs=["BTC-USD", "ETH-USD"],
        )
        assert resolve_legs(config) == ("BTC-USD", "ETH-USD")

    def test_three_inputs_live(self) -> None:
        """Three ZMQ topics resolve to a 3-tuple of leg instruments."""
        config = _config(
            inputs=[
                "market.paper.kraken.BTC-USD.candles.1h",
                "market.paper.kraken.ETH-USD.candles.1h",
                "market.paper.kraken.SOL-USD.candles.1h",
            ],
            outputs=[],
        )
        assert resolve_legs(config) == ("BTC-USD", "ETH-USD", "SOL-USD")

    def test_synthetic_with_two_outputs(self) -> None:
        """Synthetic input + 2 outputs resolves to 2 legs from outputs."""
        config = _config(
            inputs=["candles.kraken.synthetic.1h"],
            outputs=["BTC-USD", "ETH-USD"],
        )
        assert resolve_legs(config) == ("BTC-USD", "ETH-USD")

    def test_synthetic_with_three_outputs(self) -> None:
        """Synthetic input + 3 outputs resolves to 3 legs from outputs."""
        config = _config(
            inputs=["candles.kraken.synthetic.1d"],
            outputs=["BTC-USD", "ETH-USD", "SOL-USD"],
        )
        assert resolve_legs(config) == ("BTC-USD", "ETH-USD", "SOL-USD")

    def test_expected_count_matches(self) -> None:
        """Matching expected_count returns the resolved tuple."""
        config = _config(
            inputs=["candles.kraken.synthetic.1h"],
            outputs=["BTC-USD", "ETH-USD", "SOL-USD"],
        )
        assert resolve_legs(config, expected_count=3) == ("BTC-USD", "ETH-USD", "SOL-USD")

    def test_expected_count_too_few(self) -> None:
        """Too-few legs vs expected_count raises ValueError."""
        config = _config(
            inputs=[
                "market.paper.kraken.BTC-USD.candles.1h",
                "market.paper.kraken.ETH-USD.candles.1h",
            ],
            outputs=["BTC-USD", "ETH-USD"],
        )
        with pytest.raises(ValueError, match="exactly 3"):
            resolve_legs(config, expected_count=3)

    def test_expected_count_too_many_live_form(self) -> None:
        """Too-many live-form legs raises with the live-form message."""
        config = _config(
            inputs=[
                "market.paper.kraken.BTC-USD.candles.1h",
                "market.paper.kraken.ETH-USD.candles.1h",
                "market.paper.kraken.SOL-USD.candles.1h",
            ],
            outputs=[],
        )
        with pytest.raises(ValueError, match="exactly 2 inputs"):
            resolve_legs(config, expected_count=2)

    def test_expected_count_mismatch_synthetic(self) -> None:
        """Synthetic-form mismatch raises with the generic message."""
        config = _config(
            inputs=["candles.kraken.synthetic.1h"],
            outputs=["BTC-USD", "ETH-USD", "SOL-USD"],
        )
        with pytest.raises(ValueError, match="Expected exactly 2"):
            resolve_legs(config, expected_count=2)

    def test_invalid_shape_one_input_no_synthetic(self) -> None:
        """One non-synthetic input with insufficient outputs raises."""
        config = _config(
            inputs=["market.paper.kraken.BTC-USD.candles.1h"],
            outputs=["BTC-USD"],
        )
        with pytest.raises(ValueError, match="requires either"):
            resolve_legs(config)

    def test_invalid_shape_synthetic_one_output(self) -> None:
        """Synthetic input + only 1 output raises."""
        config = _config(
            inputs=["candles.kraken.synthetic.1h"],
            outputs=["BTC-USD"],
        )
        with pytest.raises(ValueError, match="requires either"):
            resolve_legs(config)

    def test_unparseable_topic_falls_back_to_raw_string(self) -> None:
        """Live-form with unparseable topics returns raw strings."""
        config = _config(
            inputs=["not.a.topic.shape", "another.weird.topic"],
            outputs=[],
        )
        assert resolve_legs(config) == ("not.a.topic.shape", "another.weird.topic")


class _TestStrategy(MultiLegSpreadMixin):
    """Minimal mixin host for partner-iteration tests."""

    def __init__(self, config: _StubConfig) -> None:
        """Docstring for __init__."""
        self.config = config
        self.candle_buffer: dict[str, list[CandleData]] = {}
        self._pending: list[StrategySignal] = []

    def emit_paired_signal(self, signal: StrategySignal) -> None:
        """Capture queued signals in a list for assertion."""
        self._pending.append(signal)


class TestMultiLegSpreadMixin:
    """Tests for the partner-leg iteration helpers."""

    def _build(self, n_legs: int) -> _TestStrategy:
        """Construct a stub strategy with N legs."""
        if n_legs == 2:
            outputs = ["BTC-USD", "ETH-USD"]
        elif n_legs == 3:
            outputs = ["BTC-USD", "ETH-USD", "SOL-USD"]
        else:
            outputs = ["A", "B", "C", "D"][:n_legs]
        cfg = _config(inputs=["candles.kraken.synthetic.1h"], outputs=outputs)
        s = _TestStrategy(cfg)
        s._init_legs(expected_count=n_legs)
        return s

    def test_init_legs_populates_legs_tuple(self) -> None:
        """_init_legs sets self.legs from the resolved tuple."""
        s = self._build(3)
        assert s.legs == ("BTC-USD", "ETH-USD", "SOL-USD")

    def test_partner_legs_excludes_current(self) -> None:
        """_partner_legs filters out the current instrument."""
        s = self._build(4)
        partners = s._partner_legs("B")
        assert partners == ("A", "C", "D")

    def test_partner_legs_unknown_current_returns_all(self) -> None:
        """Unknown current instrument leaves the partner list intact."""
        s = self._build(3)
        partners = s._partner_legs("UNKNOWN")
        assert partners == ("BTC-USD", "ETH-USD", "SOL-USD")

    def test_partner_prices_skips_unbuffered(self) -> None:
        """Partners with empty candle_buffer are omitted from the dict."""
        s = self._build(3)
        s.candle_buffer["ETH-USD"] = [_candle(NOW, 200.0)]
        prices = s._partner_prices("BTC-USD")
        assert prices == {"ETH-USD": 200.0}

    def test_partner_prices_returns_last_close(self) -> None:
        """Last-buffered close is returned per partner."""
        s = self._build(2)
        s.candle_buffer["ETH-USD"] = [_candle(NOW, 200.0), _candle(NOW, 250.0)]
        prices = s._partner_prices("BTC-USD")
        assert prices == {"ETH-USD": 250.0}

    def test_emit_partner_signals_drains_one_per_buffered_partner(self) -> None:
        """One paired signal queued per buffered partner."""
        s = self._build(3)
        s.candle_buffer["ETH-USD"] = [_candle(NOW, 200.0)]
        s.candle_buffer["SOL-USD"] = [_candle(NOW, 50.0)]

        def builder(leg: str, price: float) -> StrategySignal:
            """Build a stub signal for one partner leg."""
            return StrategySignal(
                instrument=leg,
                side="buy",
                strength=1.0,
                reason="t",
                price=price,
            )

        s._emit_partner_signals("BTC-USD", builder)
        assert len(s._pending) == 2
        legs = sorted(sig.instrument for sig in s._pending)
        assert legs == ["ETH-USD", "SOL-USD"]

    def test_emit_partner_signals_skips_unbuffered_partners(self) -> None:
        """Unbuffered partners are skipped at emission time."""
        s = self._build(3)
        s.candle_buffer["ETH-USD"] = [_candle(NOW, 200.0)]

        def builder(leg: str, price: float) -> StrategySignal:
            """Build a stub signal for one partner leg."""
            return StrategySignal(
                instrument=leg,
                side="buy",
                strength=1.0,
                reason="t",
                price=price,
            )

        s._emit_partner_signals("BTC-USD", builder)
        assert len(s._pending) == 1
        assert s._pending[0].instrument == "ETH-USD"


class TestMultiLegHostProtocol:
    """Sanity that BaseStrategy-shaped MagicMocks satisfy the protocol."""

    def test_magicmock_with_required_attrs_works(self) -> None:
        """BaseStrategy-shaped MagicMocks satisfy the host protocol."""
        host = MagicMock()
        host.config = _config(
            inputs=["candles.kraken.synthetic.1h"],
            outputs=["A", "B"],
        )
        host.candle_buffer = {}
        legs = resolve_legs(host.config, expected_count=2)
        assert legs == ("A", "B")

    def test_mixin_emit_paired_signal_stub_raises_without_host(self) -> None:
        """Mixin's emit_paired_signal stub raises if no BaseStrategy host overrides it.

        The mixin declares ``emit_paired_signal`` only as a type-checker
        contract — the concrete implementation comes from ``BaseStrategy``
        via the MRO. Calling the bare mixin method (i.e. without a
        ``BaseStrategy`` host overriding it) must raise loudly so misuse
        is caught at runtime instead of silently dropping signals.
        """

        class _BareHost(MultiLegSpreadMixin):
            def __init__(self) -> None:
                """Construct a bare mixin host without a BaseStrategy override."""

        host = _BareHost()
        sig = StrategySignal(
            instrument="X",
            side="buy",
            strength=1.0,
            reason="r",
            price=1.0,
        )
        with pytest.raises(NotImplementedError, match="BaseStrategy host"):
            MultiLegSpreadMixin.emit_paired_signal(host, sig)
