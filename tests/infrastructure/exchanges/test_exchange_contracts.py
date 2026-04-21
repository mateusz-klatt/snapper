"""Unit tests for Kraken exchange OHLC schemas and exchange contracts."""

from datetime import UTC
from datetime import datetime

import pytest

from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import FundingRateSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import to_fill_status
from snapper.infrastructure.exchanges.schemas.kraken import KrakenCandleSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenOhlcEventEnvelope
from snapper.infrastructure.exchanges.schemas.kraken import KrakenOhlcSubscribeParamsSchema


def test_ohlc_subscribe_params_as_params() -> None:
    """Verify OHLC subscription params serialize correctly.

    Given KrakenOhlcSubscribeParamsSchema with symbol, interval, snapshot,
    When as_params() is called,
    Then returns dict with channel='ohlc' and all fields.
    """
    params = KrakenOhlcSubscribeParamsSchema(symbol=["BTC-USD"], interval=5, snapshot=False)
    rendered = params.as_params()
    assert rendered == {
        "channel": "ohlc",
        "symbol": ["BTC-USD"],
        "interval": 5,
        "snapshot": False,
    }


def test_ohlc_candle_as_dict_returns_floats() -> None:
    """Verify candle schema exports all fields as proper types.

    Given a KrakenCandleSchema with OHLCV data,
    When as_dict() is called,
    Then returns dict with float values for numeric fields.
    """
    candle = KrakenCandleSchema(
        symbol="BTC-USD",
        open=50000.0,
        high=50100.0,
        low=49900.0,
        close=50050.0,
        volume=12.5,
        trades=42,
        interval=1,
        timestamp="1700000000",
    )
    payload = candle.as_dict()
    assert payload["open"] == pytest.approx(50000.0)
    assert payload["volume"] == pytest.approx(12.5)
    assert payload["trades"] == 42
    assert payload["symbol"] == "BTC-USD"


def test_ohlc_message_helpers_return_primary_symbol_and_dicts() -> None:
    """Verify envelope helpers extract symbol and normalize candles.

    Given a KrakenOhlcEventEnvelope with two candles for ETH-USD,
    When primary_symbol() and as_dicts() are called,
    Then primary_symbol returns 'ETH-USD' and as_dicts returns 2 dicts.
    """
    candle_1 = KrakenCandleSchema(
        symbol="ETH-USD",
        open=3000.0,
        high=3050.0,
        low=2950.0,
        close=3025.0,
    )
    candle_2 = KrakenCandleSchema(
        symbol="ETH-USD",
        open=3025.0,
        high=3060.0,
        low=3010.0,
        close=3055.0,
    )
    message = KrakenOhlcEventEnvelope(channel="ohlc", type="snapshot", data=[candle_1, candle_2])
    normalized = message.as_dicts()
    assert message.primary_symbol() == "ETH-USD"
    assert len(normalized) == 2
    assert normalized[0]["close"] == pytest.approx(3025.0)


class TestToFillStatus:
    """Tests for to_fill_status helper function."""

    def _make_execution(
        self, status: ExchangeOrderStatusEnum, cum_qty: float | None = None
    ) -> ExecutionUpdate:
        """Build a minimal ExecutionUpdate for fill-status testing.

        Args:
            status: Order status to set on the execution.
            cum_qty: Cumulative filled quantity (optional).

        Returns:
            ExecutionUpdate with the specified status and cum_qty.
        """
        return ExecutionUpdate(
            order_id="test-order",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=status,
            timestamp=datetime.now(UTC),
            cum_qty=cum_qty,
        )

    def test_partial_when_open_with_fills(self) -> None:
        """Return 'partial' for an open order with cumulative fills.

        Given: ExecutionUpdate with OPEN status and cum_qty > 0,
        When: to_fill_status is called,
        Then: Returns 'partial'.
        """
        execution = self._make_execution(ExchangeOrderStatusEnum.OPEN, cum_qty=5.0)
        assert to_fill_status(execution) == "partial"

    def test_filled_when_closed(self) -> None:
        """Return 'filled' for a closed order.

        Given: ExecutionUpdate with CLOSED status,
        When: to_fill_status is called,
        Then: Returns 'filled'.
        """
        execution = self._make_execution(ExchangeOrderStatusEnum.CLOSED)
        assert to_fill_status(execution) == "filled"

    def test_filled_when_open_with_zero_cum_qty(self) -> None:
        """Return 'filled' for an open order with zero cum_qty.

        Given: ExecutionUpdate with OPEN status and cum_qty=0,
        When: to_fill_status is called,
        Then: Returns 'filled' (no partial fills yet).
        """
        execution = self._make_execution(ExchangeOrderStatusEnum.OPEN, cum_qty=0.0)
        assert to_fill_status(execution) == "filled"

    def test_filled_when_open_with_none_cum_qty(self) -> None:
        """Return 'filled' for an open order with None cum_qty.

        Given: ExecutionUpdate with OPEN status and cum_qty=None,
        When: to_fill_status is called,
        Then: Returns 'filled' (None treated as zero).
        """
        execution = self._make_execution(ExchangeOrderStatusEnum.OPEN, cum_qty=None)
        assert to_fill_status(execution) == "filled"


class TestFundingRateSnapshot:
    """Tests for FundingRateSnapshot frozen dataclass."""

    def test_creation_and_fields(self) -> None:
        """FundingRateSnapshot stores all fields correctly.

        Given: All required constructor arguments,
        When: FundingRateSnapshot is created,
        Then: All fields are accessible and correct.
        """
        effective = datetime(2026, 3, 1, 16, tzinfo=UTC)
        snap = FundingRateSnapshot(
            symbol="BTC-USD-PERP",
            exchange="kraken_futures",
            rate_type="perpetual_funding",
            direction="both",
            rate=7.182e-05,
            effective_from=effective,
            notional_asset="USD",
            source="exchange_api",
        )
        assert snap.symbol == "BTC-USD-PERP"
        assert snap.exchange == "kraken_futures"
        assert snap.rate_type == "perpetual_funding"
        assert snap.direction == "both"
        assert snap.rate == pytest.approx(7.182e-05)
        assert snap.effective_from == effective
        assert snap.notional_asset == "USD"
        assert snap.source == "exchange_api"

    def test_frozen_raises_on_assignment(self) -> None:
        """FundingRateSnapshot is frozen (immutable).

        Given: An existing FundingRateSnapshot,
        When: Attempting to change a field,
        Then: Raises FrozenInstanceError.
        """
        snap = FundingRateSnapshot(
            symbol="BTC-USD-PERP",
            exchange="kraken_futures",
            rate_type="perpetual_funding",
            direction="both",
            rate=7.182e-05,
            effective_from=datetime(2026, 3, 1, 16, tzinfo=UTC),
            notional_asset="USD",
            source="exchange_api",
        )
        with pytest.raises(AttributeError):
            snap.rate = 0.0


def test_ticker_update_default_is_delayed_is_false() -> None:
    """``TickerUpdate`` defaults ``is_delayed`` to False + extended-hours to None.

    Given: a TickerUpdate constructed without the new delay flags,
    When: its fields are inspected,
    Then: ``is_delayed`` is ``False`` and ``is_extended_hours`` is ``None``
        (ensures existing adapters that construct TickerUpdate positionally
        without the new kwargs stay no-op).
    """
    update = TickerUpdate(
        symbol="BTC-USD",
        bid=100.0,
        bid_qty=1.0,
        ask=101.0,
        ask_qty=1.0,
        last=100.5,
        volume=10.0,
        vwap=100.3,
        low=99.0,
        high=102.0,
        change=0.5,
        change_pct=0.5,
    )
    assert update.is_delayed is False
    assert update.is_extended_hours is None


def test_ticker_update_accepts_delay_overrides() -> None:
    """``TickerUpdate`` accepts explicit delay/session overrides.

    Given: a TickerUpdate constructed with ``is_delayed=True`` and
        ``is_extended_hours=True``,
    When: its fields are inspected,
    Then: both overrides are preserved.
    """
    update = TickerUpdate(
        symbol="MNQM6-CME",
        bid=0.0,
        bid_qty=0.0,
        ask=0.0,
        ask_qty=0.0,
        last=0.0,
        volume=0.0,
        vwap=0.0,
        low=0.0,
        high=0.0,
        change=0.0,
        change_pct=0.0,
        is_delayed=True,
        is_extended_hours=True,
    )
    assert update.is_delayed is True
    assert update.is_extended_hours is True
