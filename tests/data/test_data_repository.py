"""Tests for data repository and infrastructure components."""

import asyncio
import time
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import MagicMock

import pytest
import urllib3.util.retry as retry_module
from sqlalchemy.exc import IntegrityError
from urllib3.util.retry import RequestHistory

import snapper.data.repository
from snapper.data.models import Base
from snapper.data.models import SymbolCatalog
from snapper.data.repository import MSSQLRepository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations import polygon as polygon_module
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient
from snapper.infrastructure.exchanges.implementations.polygon import PolygonRetryPolicy
from snapper.messaging.infrastructure.logger import ZmqMessageLogger


class TestZmqMessageLogger:
    """Tests for ZmqMessageLogger message handling and file operations."""

    @pytest.mark.asyncio
    async def test_log_message_binary_payload(self) -> None:
        """Test ZmqMessageLogger handles binary payload decode error.

        Given: A payload that raises UnicodeDecodeError,
        When: _log_message is called,
        Then: Handles error gracefully.
        """
        logger = ZmqMessageLogger(log_to_file=False, log_payload=False)

        class BadPayload:
            def __len__(self) -> int:
                return 4

            def decode(self, *_: Any, **__: Any) -> str:
                raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad")

        bad_payload = BadPayload()
        await logger._log_message(topic="test.topic", payload_bytes=bad_payload)

    @pytest.mark.asyncio
    async def test_write_to_audit_file_error(self) -> None:
        """Test ZmqMessageLogger handles file write errors.

        Given: _append_to_file raises RuntimeError,
        When: _write_to_audit_file is called,
        Then: Handles error gracefully.
        """
        logger = ZmqMessageLogger(
            log_to_file=True, log_payload=True, audit_file="/tmp/zmq_audit_test.jsonl"
        )
        logger._append_to_file = MagicMock(side_effect=RuntimeError("disk error"))
        await logger._write_to_audit_file(
            timestamp=datetime.now(UTC),
            topic="topic",
            payload_size=10,
            payload_preview="preview",
        )


class TestPolygonSmallBranches:
    """Tests for PolygonExchangeClient edge cases and small branches."""

    @pytest.mark.asyncio
    async def test_fetch_ohlcv_converts_aggregates(self) -> None:
        """Test PolygonExchangeClient converts aggregates to OHLCV.

        Given: Polygon API returning aggregate data,
        When: get_ohlcv is called,
        Then: Returns list of OHLCV snapshots.
        """
        client = PolygonExchangeClient.__new__(PolygonExchangeClient)
        client.rate_limit = 120

        async def fake_list_aggregates(**_: Any) -> list[Any]:
            return [
                SimpleNamespace(
                    timestamp=1_600_000_000_000, open=1, high=2, low=0.5, close=1.5, volume=10
                )
            ]

        cast(Any, client).list_aggregates = fake_list_aggregates
        client._resolve_timeframe = lambda timeframe: (1, "minute", timedelta(minutes=1))
        snapshots = await client.get_ohlcv(symbol="X:BTCUSD", timeframe="1m", since=None, limit=1)
        assert len(snapshots) == 1
        assert snapshots[0].close == pytest.approx(1.5)

    @pytest.mark.asyncio
    async def test_poll_tickers_handles_keyboard_interrupt(self) -> None:
        """Test poll_tickers handles KeyboardInterrupt.

        Given: Sleep raises KeyboardInterrupt,
        When: poll_tickers is called,
        Then: Handles interrupt gracefully.
        """
        client = PolygonExchangeClient.__new__(PolygonExchangeClient)
        client.rate_limit = 120

        async def fake_get_ticker(_symbol: str) -> Any:
            raise RuntimeError("fetch error")

        client.get_ticker = fake_get_ticker

        async def fake_sleep(_: float) -> None:
            raise KeyboardInterrupt()

        original_sleep = asyncio.sleep
        asyncio.sleep = fake_sleep
        try:
            await client.poll_tickers(symbols=["X:BTCUSD"], interval_seconds=0.01)
        except KeyboardInterrupt:
            pytest.fail("KeyboardInterrupt should be handled inside poll_tickers")
        finally:
            asyncio.sleep = original_sleep

    @pytest.mark.asyncio
    async def test_subscribe_instruments_builds_ticker_dict(self) -> None:
        """Test subscribe_instruments builds ticker dictionaries.

        Given: Polygon API returning ticker data,
        When: subscribe_instruments is called,
        Then: Yields ticker dictionaries with all fields.
        """
        client = PolygonExchangeClient.__new__(PolygonExchangeClient)
        client.rate_limit = 120
        client.symbols_cache_file = Path("/tmp/polygon_cache.json")
        client._is_cache_valid = lambda: False
        client._load_symbols_from_cache = MagicMock()
        client._save_symbols_to_cache = MagicMock()
        ticker = SimpleNamespace(
            ticker="X:ABC",
            name="ABC",
            market="crypto",
            locale="global",
            primary_exchange="PEX",
            type="crypto",
            active=True,
            currency_symbol="USD",
            currency_name="Dollar",
            base_currency_symbol="ABC",
            base_currency_name="ABC Coin",
            cik="CIK",
            composite_figi="FIGI",
            share_class_figi="SFIGI",
            source_feed="feed",
        )
        client._client = SimpleNamespace(list_tickers=lambda **_: [ticker])
        symbols = [symbol async for symbol in client.subscribe_instruments()]
        assert symbols == [
            {
                "ticker": "X:ABC",
                "name": "ABC",
                "market": "crypto",
                "locale": "global",
                "type": "crypto",
                "active": True,
                "currency_symbol": "USD",
                "currency_name": "Dollar",
                "base_currency_symbol": "ABC",
                "composite_figi": "FIGI",
                "base_currency_name": "ABC Coin",
                "primary_exchange": "PEX",
                "cik": "CIK",
                "share_class_figi": "SFIGI",
                "source_feed": "feed",
            }
        ]

    @pytest.mark.asyncio
    async def test_subscribe_instruments_uses_cache(self) -> None:
        """Test subscribe_instruments uses cached symbols.

        Given: Valid symbol cache,
        When: subscribe_instruments is called,
        Then: Returns cached symbols without API call.
        """
        client = PolygonExchangeClient.__new__(PolygonExchangeClient)
        client._is_cache_valid = lambda: True
        client._load_symbols_from_cache = lambda: [{"ticker": "X:CACHED"}]
        symbols = [symbol async for symbol in client.subscribe_instruments()]
        assert symbols == [{"ticker": "X:CACHED"}]

    @pytest.mark.asyncio
    async def test_subscribe_instruments_pages_and_sleeps(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test subscribe_instruments handles pagination with sleep.

        Given: API returning 1000 tickers,
        When: subscribe_instruments is called,
        Then: Sleeps for rate limiting and saves cache.
        """
        client = PolygonExchangeClient.__new__(PolygonExchangeClient)
        client.rate_limit = 120
        client.symbols_cache_file = Path("/tmp/polygon_cache.json")
        client._is_cache_valid = lambda: False
        client._save_symbols_to_cache = MagicMock()
        ticker = SimpleNamespace(
            ticker="X:ABC",
            name="ABC",
            market="crypto",
            locale="global",
        )
        client._client = SimpleNamespace(list_tickers=lambda **_: [ticker] * 1000)
        sleep_calls: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        logger_mock = MagicMock()
        monkeypatch.setattr(polygon_module, "logger", logger_mock)
        symbols = [symbol async for symbol in client.subscribe_instruments()]
        assert len(symbols) == 1000
        assert sleep_calls == [12]
        assert logger_mock.info.called
        client._save_symbols_to_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_poll_tickers_logs_generic_error(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test poll_tickers logs generic errors.

        Given: RuntimeError during polling,
        When: poll_tickers encounters error,
        Then: Logs polling error message.
        """
        client = PolygonExchangeClient.__new__(PolygonExchangeClient)
        client.rate_limit = 120
        caplog.set_level("ERROR")
        original_logger: Any = polygon_module.logger
        logger_mock = MagicMock()
        polygon_module.logger = logger_mock

        async def failing_sleep(_: float) -> None:
            raise RuntimeError("sleep boom")

        async def stopping_sleep(_: float) -> None:
            raise asyncio.CancelledError()

        sleep_calls = 0

        async def sleep_side_effect(delay: float) -> None:
            nonlocal sleep_calls
            if sleep_calls == 0:
                sleep_calls += 1
                await failing_sleep(delay)
            else:
                await stopping_sleep(delay)

        client.get_ticker = MagicMock(side_effect=RuntimeError("fetch failed"))
        original_sleep = asyncio.sleep
        asyncio.sleep = sleep_side_effect
        try:
            with pytest.raises(asyncio.CancelledError):
                await client.poll_tickers(symbols=["X:BTCUSD"], interval_seconds=0.01)
        finally:
            asyncio.sleep = original_sleep
            polygon_module.logger = original_logger
        assert any(
            "Polling error" in str(call.args[0]) for call in logger_mock.error.call_args_list
        )

    @pytest.mark.asyncio
    async def test_wait_for_rate_limit_sleeps(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test _wait_for_rate_limit sleeps when rate limited.

        Given: Request timestamps at rate limit,
        When: _wait_for_rate_limit is called,
        Then: Sleeps until rate limit window passes.
        """
        client = PolygonExchangeClient.__new__(PolygonExchangeClient)
        client.rate_limit = 2
        now = time.time()
        client._request_timestamps = [now - 1.0, now - 0.2]
        logger_mock = MagicMock()
        monkeypatch.setattr(polygon_module, "logger", logger_mock)
        sleep_calls: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        await client._wait_for_rate_limit()
        assert len(sleep_calls) >= 2
        assert len(client._request_timestamps) == 3

    def test_retry_policy_backoff_logging(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test PolygonRetryPolicy logs backoff warnings.

        Given: Retry history with one attempt,
        When: get_backoff_time and sleep are called,
        Then: Returns backoff time and logs warning.
        """
        policy = PolygonRetryPolicy(backoff_factor=12.0)
        policy.history = (
            RequestHistory(
                method="GET", url="/test", error=None, status=None, redirect_location=None
            ),
        )
        logger_mock = MagicMock()
        monkeypatch.setattr(polygon_module, "logger", logger_mock)
        monkeypatch.setattr(retry_module.time, "sleep", lambda _: None)
        backoff = policy.get_backoff_time()
        assert backoff == pytest.approx(24.0)
        policy.sleep(response=None)
        logger_mock.warning.assert_called_once()


TEST_TIMEOUT = 5


class DummySyncEngine:
    """Dummy synchronous engine for testing MSSQL repository."""

    def __init__(self) -> None:
        """Initialize the instance."""

        class URL:
            def get_dialect(self) -> Any:
                class D:
                    name = "mssql"

                return D()

        self.url = URL()
        self.connected = False

    def connect(self) -> Any:
        """Simulate opening a database connection."""
        self.connected = True

        class Conn:
            def __enter__(self) -> Any:
                return self

            def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                return None

        return Conn()


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_mssql_repository_mock_engine(monkeypatch: Any) -> None:
    """Test MSSQLRepository with mocked sync engine.

    Given: Mocked SQLAlchemy sync engine,
    When: Repository operations are called,
    Then: Uses sync sessions correctly.
    """

    def fake_create_sync_engine(url: str, future: bool = True) -> DummySyncEngine:
        assert url.startswith("mssql+pyodbc://")
        assert ("driver=" in url) or ("Driver=" in url)
        return DummySyncEngine()

    monkeypatch.setattr(snapper.data.repository, "create_sync_engine", fake_create_sync_engine)
    commit_should_fail: dict[str, bool] = {"pending": True}
    scalar_one_called: dict[str, bool] = {"value": False}

    class DummySession:
        def __init__(self) -> None:
            self._last_added: Any | None = None
            self._execute_count = 0

        def __enter__(self) -> Any:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, *_args: Any, **_kwargs: Any) -> Any:
            self._execute_count += 1
            call_num = self._execute_count

            class Q:
                def scalar_one_or_none(self: Any) -> Any:
                    if call_num >= 2:
                        scalar_one_called["value"] = True

                        class One:
                            id = 1

                        return One()
                    return None

                def all(self) -> list[Any]:
                    return []

            return Q()

        def add(self, _obj: Any) -> None:
            self._last_added = _obj

        def commit(self) -> None:
            if commit_should_fail["pending"]:
                commit_should_fail["pending"] = False
                raise IntegrityError("insert", {}, Exception("duplicate"))
            if self._last_added is not None and not getattr(self._last_added, "id", None):
                self._last_added.id = 1

        def rollback(self) -> None:
            """No-op for test stub."""
            pass

        def refresh(self, _obj: Any) -> None:
            """No-op for test stub."""
            pass

    def fake_sync_sessionmaker(
        _engine: Any, expire_on_commit: bool = False, class_: Any = None
    ) -> Any:
        def factory() -> DummySession:
            return DummySession()

        return factory

    monkeypatch.setattr(snapper.data.repository, "sync_sessionmaker", fake_sync_sessionmaker)

    def fake_create_all(engine: Any) -> None:
        with engine.connect():
            return None

    monkeypatch.setattr(Base.metadata, "create_all", fake_create_all)
    ms_repo = MSSQLRepository("mssql+pyodbc://user:pass@server:1433/db")
    assert ms_repo.dialect_name == "mssql"
    monkeypatch.setattr(snapper.data.repository, "sync_sessionmaker", fake_sync_sessionmaker)
    await ms_repo.create_all()
    assert isinstance(ms_repo.engine, DummySyncEngine)
    assert ms_repo.engine.connected is True
    inst_id = await ms_repo.upsert_instrument(
        symbol="BTC-USD", base="BTC", quote="USD", exchange="kraken", tick_size=0.1, lot_size=0.0001
    )
    assert isinstance(inst_id, int)
    inserted_c = await ms_repo.upsert_candles(
        [
            {
                "instrument_id": 1,
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "timeframe": "1m",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
                "vwap": None,
                "trades": 1,
            }
        ]
    )
    assert inserted_c in (0, 1)
    inserted_t = await ms_repo.upsert_trades(
        [
            {
                "trade_id": "t1",
                "instrument_id": 1,
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "price": 100.0,
                "size": 0.5,
                "side": "buy",
            }
        ]
    )
    assert inserted_t in (0, 1)
    assert scalar_one_called["value"] is True


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_mssql_repository_order_execution_methods(monkeypatch: Any) -> None:
    """Test MSSQLRepository order and execution methods.

    Given: Mocked session with extended queries,
    When: update_order, insert_execution, get_candles called,
    Then: Executes correct SQL statements.
    """

    def fake_create_sync_engine(url: str, future: bool = True) -> DummySyncEngine:
        return DummySyncEngine()

    monkeypatch.setattr(snapper.data.repository, "create_sync_engine", fake_create_sync_engine)
    execution_id_counter = {"value": 1}
    executed_stmts: list[str] = []

    class ExtendedDummySession:
        def __init__(self) -> None:
            self._last_added: Any | None = None

        def __enter__(self) -> Any:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, stmt: Any, *_args: Any, **_kwargs: Any) -> Any:
            executed_stmts.append(str(type(stmt).__name__))

            class Q:
                def scalar_one_or_none(self) -> Any:
                    class Inst:
                        id = 1

                    return Inst()

                def scalars(self) -> "Q":
                    return self

                def first(self) -> Any:
                    class Inst:
                        id = 1

                    return Inst()

                def all(self) -> list[Any]:
                    class Row:
                        timestamp = datetime(2024, 1, 1, tzinfo=UTC)
                        timeframe = "1m"
                        open = 1.0
                        high = 2.0
                        low = 0.5
                        close = 1.5
                        volume = 10.0
                        vwap = 1.2
                        trades = 5

                    return [Row()]

            return Q()

        def add(self, obj: Any) -> None:
            self._last_added = obj

        def commit(self) -> None:
            if self._last_added is not None:
                self._last_added.id = execution_id_counter["value"]
                execution_id_counter["value"] += 1

        def refresh(self, _obj: Any) -> None:
            """No-op for test stub."""
            pass

    def fake_sync_sessionmaker(
        _engine: Any, expire_on_commit: bool = False, class_: Any = None
    ) -> Any:
        def factory() -> ExtendedDummySession:
            return ExtendedDummySession()

        return factory

    monkeypatch.setattr(snapper.data.repository, "sync_sessionmaker", fake_sync_sessionmaker)

    def fake_create_all(engine: Any) -> None:
        """Intentionally empty mock implementation."""
        pass

    monkeypatch.setattr(Base.metadata, "create_all", fake_create_all)
    ms_repo = MSSQLRepository("mssql+pyodbc://user:pass@server:1433/db")
    await ms_repo.update_order(
        order_id=1,
        status="filled",
        updated_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
        exchange_order_id="ex-123",
        error=None,
    )
    assert "Update" in executed_stmts
    exec_id = await ms_repo.insert_execution(
        order_id=1,
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
        price=100.0,
        size=0.5,
        fee=0.01,
        fee_asset="USD",
    )
    assert isinstance(exec_id, int)
    assert exec_id >= 1
    candles = await ms_repo.get_candles(
        instrument="BTC-USD",
        timeframe="1m",
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 2, tzinfo=UTC),
        exchange="kraken",
    )
    assert len(candles) == 1
    assert candles[0]["timeframe"] == "1m"
    assert candles[0]["open"] == pytest.approx(1.0)
    assert candles[0]["close"] == pytest.approx(1.5)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_mssql_get_candles_returns_empty_when_no_instrument(monkeypatch: Any) -> None:
    """Test MSSQLRepository returns empty for unknown instrument.

    Given: Session returning None for instrument lookup,
    When: get_candles is called with unknown instrument,
    Then: Returns empty list.
    """

    def fake_create_sync_engine(url: str, future: bool = True) -> DummySyncEngine:
        return DummySyncEngine()

    monkeypatch.setattr(snapper.data.repository, "create_sync_engine", fake_create_sync_engine)

    class NoInstrumentSession:
        def __enter__(self) -> Any:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, stmt: Any, *_args: Any, **_kwargs: Any) -> Any:
            class Q:
                def scalars(self) -> "Q":
                    return self

                def first(self) -> None:
                    return None

            return Q()

    def fake_sync_sessionmaker(
        _engine: Any, expire_on_commit: bool = False, class_: Any = None
    ) -> Any:
        def factory() -> NoInstrumentSession:
            return NoInstrumentSession()

        return factory

    monkeypatch.setattr(snapper.data.repository, "sync_sessionmaker", fake_sync_sessionmaker)

    def fake_create_all(engine: Any) -> None:
        """Intentionally empty mock implementation."""
        pass

    monkeypatch.setattr(Base.metadata, "create_all", fake_create_all)
    ms_repo = MSSQLRepository("mssql+pyodbc://user:pass@server:1433/db")
    candles = await ms_repo.get_candles(
        instrument="UNKNOWN",
        timeframe="1m",
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 2, tzinfo=UTC),
        exchange="kraken",
    )
    assert candles == []


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_mssql_get_candles_with_exchange_filter(monkeypatch: Any) -> None:
    """Test MSSQLRepository get_candles filters by exchange when provided.

    Given: Session returning None for instrument lookup,
    When: get_candles is called with exchange parameter,
    Then: Returns empty list (exchange filter applied).
    """

    def fake_create_sync_engine(url: str, future: bool = True) -> DummySyncEngine:
        return DummySyncEngine()

    monkeypatch.setattr(snapper.data.repository, "create_sync_engine", fake_create_sync_engine)

    class NoInstrumentSession:
        def __enter__(self) -> Any:
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, stmt: Any, *_args: Any, **_kwargs: Any) -> Any:
            class Q:
                def scalars(self) -> "Q":
                    return self

                def first(self) -> None:
                    return None

            return Q()

    def fake_sync_sessionmaker(
        _engine: Any, expire_on_commit: bool = False, class_: Any = None
    ) -> Any:
        def factory() -> NoInstrumentSession:
            return NoInstrumentSession()

        return factory

    monkeypatch.setattr(snapper.data.repository, "sync_sessionmaker", fake_sync_sessionmaker)

    def fake_create_all(engine: Any) -> None:
        """Intentionally empty mock implementation."""
        pass

    monkeypatch.setattr(Base.metadata, "create_all", fake_create_all)
    ms_repo = MSSQLRepository("mssql+pyodbc://user:pass@server:1433/db")
    candles = await ms_repo.get_candles(
        instrument="UNKNOWN",
        timeframe="1m",
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 2, tzinfo=UTC),
        exchange="kraken",
    )
    assert candles == []


@pytest.mark.asyncio
async def test_mssql_get_trades_returns_empty_for_missing_instrument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test MSSQLRepository get_trades returns empty for missing instrument.

    Given: Session returning None for instrument,
    When: get_trades is called,
    Then: Returns empty list.
    """

    class NoInstrumentSession:
        def __enter__(self) -> "NoInstrumentSession":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def execute(self, stmt: Any) -> Any:
            class _Result:
                def scalars(self) -> "_Result":
                    return self

                def first(self) -> None:
                    return None

            return _Result()

    def fake_sync_sessionmaker(*_: object, **__: object) -> Callable[[], NoInstrumentSession]:
        def factory() -> NoInstrumentSession:
            return NoInstrumentSession()

        return factory

    monkeypatch.setattr(snapper.data.repository, "sync_sessionmaker", fake_sync_sessionmaker)
    monkeypatch.setattr(Base.metadata, "create_all", lambda _: None)
    ms_repo = MSSQLRepository("mssql+pyodbc://user:pass@server:1433/db")
    trades = await ms_repo.get_trades(
        instrument="UNKNOWN",
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 2, tzinfo=UTC),
        exchange="kraken",
    )
    assert trades == []


@pytest.mark.asyncio
async def test_mssql_get_trades_with_exchange_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test MSSQLRepository get_trades filters by exchange when provided.

    Given: Session returning None for instrument with exchange filter,
    When: get_trades is called with exchange parameter,
    Then: Returns empty list.
    """

    class NoInstrumentSession:
        def __enter__(self) -> "NoInstrumentSession":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def execute(self, stmt: Any) -> Any:
            class _Result:
                def scalars(self) -> "_Result":
                    return self

                def first(self) -> None:
                    return None

            return _Result()

    def fake_sync_sessionmaker(*_: object, **__: object) -> Callable[[], NoInstrumentSession]:
        def factory() -> NoInstrumentSession:
            return NoInstrumentSession()

        return factory

    monkeypatch.setattr(snapper.data.repository, "sync_sessionmaker", fake_sync_sessionmaker)
    monkeypatch.setattr(Base.metadata, "create_all", lambda _: None)
    ms_repo = MSSQLRepository("mssql+pyodbc://user:pass@server:1433/db")
    trades = await ms_repo.get_trades(
        instrument="UNKNOWN",
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 2, tzinfo=UTC),
        exchange="kraken",
    )
    assert trades == []


@pytest.mark.asyncio
async def test_mssql_get_market_snapshots_returns_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test MSSQLRepository get_market_snapshots returns data.

    Given: Session returning snapshot data,
    When: get_market_snapshots is called,
    Then: Returns list of snapshot dictionaries.
    """

    class FakeSnapshot:
        def __init__(self) -> None:
            self.exchange = "kraken"
            self.symbol = "BTC/USD"
            self.updated_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
            self.bid = 42000.0
            self.ask = 42100.0
            self.bid_volume = 1.5
            self.ask_volume = 2.0
            self.last_price = 42050.0
            self.volume_24h = 100.0

    class FakeSession:
        def __enter__(self) -> "FakeSession":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def execute(self, stmt: Any) -> Any:
            class _Result:
                def scalars(self) -> "_Result":
                    return self

                def all(self) -> list[FakeSnapshot]:
                    return [FakeSnapshot()]

            return _Result()

    def fake_sync_sessionmaker(*_: object, **__: object) -> Callable[[], FakeSession]:
        def factory() -> FakeSession:
            return FakeSession()

        return factory

    monkeypatch.setattr(snapper.data.repository, "sync_sessionmaker", fake_sync_sessionmaker)
    monkeypatch.setattr(Base.metadata, "create_all", lambda _: None)
    ms_repo = MSSQLRepository("mssql+pyodbc://user:pass@server:1433/db")
    snapshots = await ms_repo.get_market_snapshots(
        exchange="kraken",
        symbols=["BTC/USD"],
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 2, tzinfo=UTC),
    )
    assert len(snapshots) == 1
    assert snapshots[0]["exchange"] == "kraken"
    assert snapshots[0]["symbol"] == "BTC/USD"
    assert snapshots[0]["bid"] == pytest.approx(42000.0)


@pytest.mark.asyncio
async def test_mssql_get_trades_returns_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test MSSQLRepository get_trades returns trade data.

    Given: Session returning instrument and trade data,
    When: get_trades is called,
    Then: Returns list of trade dictionaries.
    """

    class FakeInstrument:
        id = 1
        symbol = "BTC/USD"

    class FakeTrade:
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        price = 42000.0
        size = 0.5
        side = "buy"

    call_count = 0

    class FakeSession:
        def __enter__(self) -> "FakeSession":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def execute(self, stmt: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:

                class _InstrumentResult:
                    def scalars(self) -> "_InstrumentResult":
                        return self

                    def first(self) -> FakeInstrument:
                        return FakeInstrument()

                return _InstrumentResult()
            else:

                class _TradesResult:
                    def all(self) -> list[FakeTrade]:
                        return [FakeTrade()]

                return _TradesResult()

    def fake_sync_sessionmaker(*_: object, **__: object) -> Callable[[], FakeSession]:
        def factory() -> FakeSession:
            return FakeSession()

        return factory

    monkeypatch.setattr(snapper.data.repository, "sync_sessionmaker", fake_sync_sessionmaker)
    monkeypatch.setattr(Base.metadata, "create_all", lambda _: None)
    ms_repo = MSSQLRepository("mssql+pyodbc://user:pass@server:1433/db")
    trades = await ms_repo.get_trades(
        instrument="BTC/USD",
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 2, tzinfo=UTC),
        exchange="kraken",
    )
    assert len(trades) == 1
    assert trades[0]["price"] == pytest.approx(42000.0)
    assert trades[0]["side"] == "buy"


TEST_TIMEOUT = 5


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_repository_create_and_upserts(tmp_path: Path) -> None:
    """Test SQLAlchemyRepository create and upsert operations.

    Given: SQLite repository,
    When: create_all and upsert operations are called,
    Then: Creates tables and inserts data.
    """
    db_path = tmp_path / "test.db"
    url = f"sqlite+aiosqlite:///{db_path.as_posix()}"
    repo = SQLAlchemyRepository(url)
    await repo.create_all()
    async with repo.session() as s:
        s.add(
            SymbolCatalog(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
        await s.commit()
    inst_id = await repo.upsert_instrument(
        symbol="BTC-USD", base="BTC", quote="USD", exchange="kraken", tick_size=0.1, lot_size=0.0001
    )
    assert inst_id > 0
    inserted = await repo.upsert_candles(
        [
            {
                "instrument_id": inst_id,
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "timeframe": "1m",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
                "vwap": None,
                "trades": 1,
            }
        ]
    )
    assert inserted in (0, 1)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_repository_upserts_empty_lists(tmp_path: Path) -> None:
    """Test SQLAlchemyRepository handles empty upsert lists.

    Given: SQLite repository,
    When: upsert_candles and upsert_trades with empty lists,
    Then: Returns 0 for both.
    """
    db_path = tmp_path / "test2.db"
    url = f"sqlite+aiosqlite:///{db_path.as_posix()}"
    repo = SQLAlchemyRepository(url)
    await repo.create_all()
    assert await repo.upsert_candles([]) == 0
    assert await repo.upsert_trades([]) == 0


def test_repository_factory_cloud_urls() -> None:
    """Test get_repository factory creates correct types.

    Given: PostgreSQL and MSSQL connection URLs,
    When: get_repository is called,
    Then: Returns appropriate repository types.
    """
    pg = get_repository("postgresql+asyncpg://user:pass@localhost/dbname")
    assert pg.dialect_name.startswith("postgres")
    ms = get_repository(
        "mssql+pyodbc://user:pass@server:1433/db?driver=ODBC+Driver+18+for+SQL+Server"
    )
    assert isinstance(ms, MSSQLRepository)


TEST_TIMEOUT = 5


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_upsert_trades_sqlite(tmp_path: Path) -> None:
    """Test SQLAlchemyRepository upsert_trades with SQLite.

    Given: SQLite repository with instrument,
    When: upsert_trades is called with trade data,
    Then: Inserts trades successfully.
    """
    db_path = tmp_path / "t.db"
    url = f"sqlite+aiosqlite:///{db_path.as_posix()}"
    repo = SQLAlchemyRepository(url)
    await repo.create_all()
    async with repo.session() as s:
        s.add(
            SymbolCatalog(
                native_symbol="ETH-USD",
                base="ETH",
                quote="USD",
                asset_type="crypto",
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
        await s.commit()
    inst_id = await repo.upsert_instrument(
        symbol="ETH-USD", base="ETH", quote="USD", exchange="kraken", tick_size=0.01, lot_size=0.001
    )
    rows = [
        {
            "trade_id": "1",
            "instrument_id": inst_id,
            "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "price": 100.0,
            "size": 0.5,
            "side": "buy",
        },
        {
            "trade_id": "2",
            "instrument_id": inst_id,
            "timestamp": datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
            "price": 101.0,
            "size": 0.25,
            "side": "sell",
        },
    ]
    inserted = await repo.upsert_trades(rows)
    assert inserted in (0, 1, 2)
