"""Tests for strategy process registration and wrapping."""

import asyncio
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import snapper.strategies.macd as macd_module
import snapper.strategies.rsi as rsi_module
from snapper.application.process_manager.registry import get_registered_processes
from snapper.strategies.base import StrategyConfig
from snapper.strategies.process_wrapper import create_strategy_process

assert macd_module
assert rsi_module


@pytest.fixture
def mock_settings() -> MagicMock:
    """Provide a mock settings object for testing."""
    return MagicMock()


@pytest.fixture
def mock_strategy() -> MagicMock:
    """Provide a mock strategy with default StrategyConfig for testing."""
    strategy = MagicMock()
    strategy.config = StrategyConfig(
        name="test_strategy",
        strategy_class="MACDCrossover",
        inputs=["market.kraken.BTC-USD.candles.1m"],
        outputs=["BTC-USD"],
        params={"fast": 12},
    )
    strategy._running = False
    strategy.is_running = False
    strategy.start = AsyncMock()
    strategy.stop = AsyncMock()
    return strategy


@pytest.mark.asyncio
async def test_strategy_process_creation() -> None:
    """Verify create_strategy_process creates properly registered class.

    Given: Strategy configuration with name and parameters,
    When: create_strategy_process is called,
    Then: Process class is created with correct metadata.
    """
    strategy_process_cls = create_strategy_process(
        process_name="test_macd",
        strategy_class="MACDCrossover",
        default_config={
            "name": "macd_test",
            "inputs": ["market.kraken.BTC-USD.candles.1m"],
            "outputs": ["macd_test"],
            "exchange": "paper",
            "params": {"fast": 12, "slow": 26, "signal_period": 9},
        },
    )
    metadata = get_registered_processes().get("test_macd")
    assert metadata is not None
    assert metadata.enabled is False
    assert metadata.mode == "thread"
    assert metadata.args == []
    assert metadata.role == "strategy"
    assert "strategy" in metadata.tags
    mock_settings = MagicMock()
    kwargs = strategy_process_cls.get_default_kwargs(mock_settings)
    assert kwargs["name"] == "macd_test"
    assert kwargs["inputs"] == ["market.kraken.BTC-USD.candles.1m"]
    assert kwargs["outputs"] == ["macd_test"]
    assert kwargs["params"]["fast"] == 12


@pytest.mark.asyncio
async def test_strategy_process_initialization() -> None:
    """Verify strategy process initializes with correct config.

    Given: Strategy process class with default configuration,
    When: Instance is created with custom parameters,
    Then: StrategyConfig contains merged configuration.
    """
    strategy_process_cls = create_strategy_process(
        process_name="test_strategy",
        strategy_class="MACDCrossover",
        default_config={
            "name": "macd_test",
            "inputs": ["BTC-USD:1h"],
            "outputs": ["macd_test"],
            "exchange": "paper",
            "params": {},
        },
    )
    process = strategy_process_cls(
        name="my_strategy",
        inputs=["market.kraken.BTC-USD.candles.1m"],
        outputs=["BTC-USD"],
        params={"fast": 12},
    )
    assert process.process_name == "test_strategy"
    assert process.config.name == "my_strategy"
    assert process.config.strategy_class == "MACDCrossover"
    assert process.config.inputs == ["market.kraken.BTC-USD.candles.1m"]
    assert process.config.outputs == ["BTC-USD"]
    assert process.config.exchange == "paper"
    assert process.config.params == {"fast": 12}
    assert process.strategy is None


@pytest.mark.asyncio
async def test_strategy_process_start(mock_strategy: MagicMock) -> None:
    """Verify start creates strategy via factory and delegates start.

    Given: Strategy process with mock factory,
    When: start() is called,
    Then: Factory creates and starts strategy instance.
    """
    strategy_process_cls = create_strategy_process(
        process_name="test_strategy",
        strategy_class="MACDCrossover",
        default_config={
            "name": "macd_test",
            "inputs": ["BTC-USD:1h"],
            "outputs": ["macd_test"],
            "exchange": "paper",
            "params": {},
        },
    )
    process = strategy_process_cls(
        name="my_strategy",
        inputs=["market.kraken.BTC-USD.candles.1m"],
        outputs=["BTC-USD"],
        params={},
    )
    with patch.object(
        process.factory, "create_strategy", return_value=mock_strategy
    ) as mock_create, patch.object(
        process.factory, "start_strategy", new_callable=AsyncMock
    ) as mock_start, patch.object(
        process.factory, "stop_strategy", new_callable=AsyncMock
    ) as mock_stop:
        start_task = asyncio.create_task(process.start())
        await asyncio.sleep(0)
        mock_create.assert_called_once_with(process.config)
        mock_start.assert_called_once_with("my_strategy")
        assert process.strategy == mock_strategy
        assert not start_task.done()
        await process.stop()
        await asyncio.wait_for(start_task, timeout=0.1)
        mock_stop.assert_called_once_with("my_strategy")


@pytest.mark.asyncio
async def test_strategy_process_start_create_failure(mock_strategy: MagicMock) -> None:
    """Verify start propagates factory create_strategy errors.

    Given: Factory that raises ValueError on create,
    When: start() is called,
    Then: ValueError propagates and strategy remains None.
    """
    strategy_process_cls = create_strategy_process(
        process_name="test_strategy",
        strategy_class="MACDCrossover",
        default_config={
            "name": "macd_test",
            "inputs": ["BTC-USD:1h"],
            "outputs": ["macd_test"],
            "exchange": "paper",
            "params": {},
        },
    )
    process = strategy_process_cls(
        name="my_strategy",
        inputs=["market.kraken.BTC-USD.candles.1m"],
        outputs=["BTC-USD"],
        params={},
    )
    with patch.object(
        process.factory, "create_strategy", side_effect=ValueError("Invalid strategy config")
    ) as mock_create:
        with pytest.raises(ValueError, match="Invalid strategy config"):
            await process.start()
        mock_create.assert_called_once_with(process.config)
        assert process.strategy is None


@pytest.mark.asyncio
async def test_strategy_process_start_start_failure(mock_strategy: MagicMock) -> None:
    """Verify start propagates factory start_strategy errors.

    Given: Factory where start_strategy raises RuntimeError,
    When: start() is called,
    Then: RuntimeError propagates but strategy is assigned.
    """
    strategy_process_cls = create_strategy_process(
        process_name="test_strategy",
        strategy_class="MACDCrossover",
        default_config={
            "name": "macd_test",
            "inputs": ["BTC-USD:1h"],
            "outputs": ["macd_test"],
            "exchange": "paper",
            "params": {},
        },
    )
    process = strategy_process_cls(
        name="my_strategy",
        inputs=["market.kraken.BTC-USD.candles.1m"],
        outputs=["BTC-USD"],
        params={},
    )
    with patch.object(
        process.factory, "create_strategy", return_value=mock_strategy
    ) as mock_create, patch.object(
        process.factory, "start_strategy", side_effect=RuntimeError("Start failed")
    ) as mock_start:
        with pytest.raises(RuntimeError, match="Start failed"):
            await process.start()
        mock_create.assert_called_once_with(process.config)
        mock_start.assert_called_once_with("my_strategy")
        assert process.strategy == mock_strategy


@pytest.mark.asyncio
async def test_strategy_process_stop(mock_strategy: MagicMock) -> None:
    """Verify stop delegates to factory and clears strategy.

    Given: Running strategy process with mock strategy,
    When: stop() is called,
    Then: Factory stop_strategy is called and strategy cleared.
    """
    strategy_process_cls = create_strategy_process(
        process_name="test_strategy",
        strategy_class="MACDCrossover",
        default_config={
            "name": "macd_test",
            "inputs": ["BTC-USD:1h"],
            "outputs": ["macd_test"],
            "exchange": "paper",
            "params": {},
        },
    )
    process = strategy_process_cls(
        name="my_strategy",
        inputs=["market.kraken.BTC-USD.candles.1m"],
        outputs=["BTC-USD"],
        params={},
    )
    process.strategy = mock_strategy
    process._stop_event = asyncio.Event()
    with patch.object(process.factory, "stop_strategy", new_callable=AsyncMock) as mock_stop:
        await process.stop()
        mock_stop.assert_called_once_with("my_strategy")
        assert process.strategy is None
        assert process._stop_event is not None and process._stop_event.is_set()


@pytest.mark.asyncio
async def test_strategy_process_get_status_running(mock_strategy: MagicMock) -> None:
    """Verify get_status returns correct info for running strategy.

    Given: Strategy process with running strategy,
    When: get_status() is called,
    Then: Status includes all config details and running=True.
    """
    strategy_process_cls = create_strategy_process(
        process_name="test_strategy",
        strategy_class="MACDCrossover",
        default_config={
            "name": "macd_test",
            "inputs": ["BTC-USD:1h"],
            "outputs": ["macd_test"],
            "exchange": "paper",
            "params": {},
        },
    )
    process = strategy_process_cls(
        name="my_strategy",
        inputs=["market.kraken.BTC-USD.candles.1m"],
        outputs=["BTC-USD"],
        params={"fast": 12},
    )
    mock_strategy.is_running = True
    process.strategy = mock_strategy
    status = process.get_status()
    assert status["process_name"] == "test_strategy"
    assert status["strategy_name"] == "my_strategy"
    assert status["strategy_class"] == "MACDCrossover"
    assert status["inputs"] == ["market.kraken.BTC-USD.candles.1m"]
    assert status["outputs"] == ["BTC-USD"]
    assert status["exchange"] == "paper"
    assert status["params"] == {"fast": 12}
    assert status["running"] is True


@pytest.mark.asyncio
async def test_strategy_process_get_status_stopped() -> None:
    """Verify get_status returns running=False when not started.

    Given: Strategy process that was never started,
    When: get_status() is called,
    Then: Status shows running=False.
    """
    strategy_process_cls = create_strategy_process(
        process_name="test_strategy",
        strategy_class="MACDCrossover",
        default_config={
            "name": "macd_test",
            "inputs": ["BTC-USD:1h"],
            "outputs": ["macd_test"],
            "exchange": "paper",
            "params": {},
        },
    )
    process = strategy_process_cls(
        name="my_strategy",
        inputs=["market.kraken.BTC-USD.candles.1m"],
        outputs=["BTC-USD"],
        params={},
    )
    status = process.get_status()
    assert status["process_name"] == "test_strategy"
    assert status["running"] is False


@pytest.mark.asyncio
async def test_strategy_process_start_cancelled(mock_strategy: MagicMock) -> None:
    """Verify start handles task cancellation gracefully.

    Given: Strategy process start task,
    When: Task is cancelled,
    Then: CancelledError propagates and stop_event remains None.
    """
    strategy_process_cls = create_strategy_process(
        process_name="test_strategy_cancel",
        strategy_class="MACDCrossover",
        default_config={
            "name": "macd_test",
            "inputs": ["BTC-USD:1h"],
            "outputs": ["macd_test"],
            "exchange": "paper",
            "params": {},
        },
    )
    process = strategy_process_cls(
        name="my_strategy",
        inputs=["market.kraken.BTC-USD.candles.1m"],
        outputs=["BTC-USD"],
        params={},
    )
    with patch.object(process.factory, "create_strategy", return_value=mock_strategy), patch.object(
        process.factory, "start_strategy", new_callable=AsyncMock
    ):
        task = asyncio.create_task(process.start())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert process._stop_event is None


@pytest.mark.asyncio
async def test_strategy_process_stop_without_event() -> None:
    """Test stopping a strategy process without an initialized event.

    Given a strategy process with _stop_event set to None,
    When stop() is called,
    Then it completes gracefully without raising errors.
    """
    strategy_process_cls = create_strategy_process(
        process_name="test_strategy_stop",
        strategy_class="MACDCrossover",
        default_config={
            "name": "macd_default",
            "inputs": ["market.kraken.BTC-USD.candles.1m"],
            "outputs": ["BTC-USD"],
        },
    )
    process = strategy_process_cls(
        name="s",
        inputs=["market.kraken.BTC-USD.candles.1m"],
        outputs=["BTC-USD"],
        params={},
    )
    process._stop_event = None
    process.strategy = None
    await process.stop()
    assert process._stop_event is None
    assert process.strategy is None


@pytest.mark.asyncio
async def test_predefined_macd_strategy() -> None:
    """Test that MACD strategy is registered with correct metadata.

    Given the predefined MACD strategy 'strategy_macd_btc_1h',
    When fetching its process metadata,
    Then it returns valid configuration with disabled state, thread mode, and strategy role.
    """
    metadata = get_registered_processes().get("strategy_macd_btc_1h")
    assert metadata is not None
    assert metadata.enabled is False
    assert metadata.mode == "thread"
    assert metadata.role == "strategy"
    assert "strategy" in metadata.tags


@pytest.mark.asyncio
async def test_predefined_rsi_strategy() -> None:
    """Test that RSI strategy is registered with correct metadata.

    Given the predefined RSI strategy 'strategy_rsi_eth_1h',
    When fetching its process metadata,
    Then it returns valid configuration with disabled state, thread mode, and strategy role.
    """
    metadata = get_registered_processes().get("strategy_rsi_eth_1h")
    assert metadata is not None
    assert metadata.enabled is False
    assert metadata.mode == "thread"
    assert metadata.role == "strategy"
    assert "strategy" in metadata.tags
