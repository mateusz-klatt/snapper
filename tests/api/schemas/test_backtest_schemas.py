"""Tests for backtest API schemas validation."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.api.schemas.backtest import BacktestCreateBody

NOW = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)
_MOCK_STRATEGIES: dict[str, Any] = {"sma_cross": MagicMock()}


@patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", _MOCK_STRATEGIES)
class TestBacktestCreateBody:
    """Tests for BacktestCreateBody validators."""

    def test_valid_body(self) -> None:
        """Valid body passes all validators."""
        body = BacktestCreateBody(
            strategy_class="sma_cross",
            instrument_public_id="BTC-USD",
            exchange="kraken",
            start_date=NOW,
            end_date=NOW + timedelta(days=30),
        )
        assert body.strategy_class == "sma_cross"

    def test_unknown_strategy_raises(self) -> None:
        """Unknown strategy_class raises ValueError."""
        with pytest.raises(ValueError, match="Unknown strategy class"):
            BacktestCreateBody(
                strategy_class="nonexistent",
                instrument_public_id="BTC-USD",
                exchange="kraken",
                start_date=NOW,
                end_date=NOW + timedelta(days=30),
            )

    def test_negative_initial_cash_raises(self) -> None:
        """Negative initial_cash raises ValueError."""
        with pytest.raises(ValueError, match="initial_cash must be positive"):
            BacktestCreateBody(
                strategy_class="sma_cross",
                instrument_public_id="BTC-USD",
                exchange="kraken",
                start_date=NOW,
                end_date=NOW + timedelta(days=30),
                initial_cash=-100.0,
            )

    def test_end_before_start_raises(self) -> None:
        """end_date before start_date raises ValueError."""
        with pytest.raises(ValueError, match="end_date must be after start_date"):
            BacktestCreateBody(
                strategy_class="sma_cross",
                instrument_public_id="BTC-USD",
                exchange="kraken",
                start_date=NOW + timedelta(days=30),
                end_date=NOW,
            )
