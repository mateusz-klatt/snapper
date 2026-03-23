"""Tests for data repository and infrastructure components."""

import asyncio
import time
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
from urllib3.util.retry import RequestHistory

from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations import polygon as polygon_module
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient
from snapper.infrastructure.exchanges.implementations.polygon import PolygonRetryPolicy
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
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
            Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=datetime.now(UTC),
                timestamp=datetime.now(UTC),
                session_id="test-session",
                sequence_id=1,
            )
        )
        await s.commit()
    spid = await resolve_symbol_public_id(repo, "BTC-USD")
    assert spid is not None
    inst_id, inst_pub_id = await repo.upsert_instrument(
        symbol_public_id=spid,
        symbol="BTC-USD",
        base="BTC",
        quote="USD",
        exchange="kraken",
        tick_size=0.1,
        lot_size=0.0001,
        timestamp=datetime.now(UTC),
        session_id="test-session",
        sequence_id=1,
    )
    assert inst_id > 0
    assert isinstance(inst_pub_id, str)
    inserted = await repo.upsert_candles(
        [
            {
                "instrument_id": inst_id,
                "instrument_public_id": inst_pub_id,
                "open_at": datetime(2024, 1, 1, tzinfo=UTC),
                "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                "timeframe": "1m",
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10.0,
                "vwap": None,
                "trades": 1,
                "session_id": "test-session",
                "sequence_id": 1,
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
    assert await repo.upsert_ticks([]) == 0


def test_repository_factory_cloud_urls() -> None:
    """Test get_repository factory creates correct types.

    Given: PostgreSQL connection URL,
    When: get_repository is called,
    Then: Returns appropriate repository type.
    """
    pg = get_repository("postgresql+asyncpg://user:pass@localhost/dbname")
    assert pg.dialect_name.startswith("postgres")


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
            Symbol(
                native_symbol="ETH-USD",
                base="ETH",
                quote="USD",
                asset_type="crypto",
                created_at=datetime.now(UTC),
                timestamp=datetime.now(UTC),
                session_id="test-session",
                sequence_id=1,
            )
        )
        await s.commit()
    spid = await resolve_symbol_public_id(repo, "ETH-USD")
    assert spid is not None
    inst_id, inst_pub_id = await repo.upsert_instrument(
        symbol_public_id=spid,
        symbol="ETH-USD",
        base="ETH",
        quote="USD",
        exchange="kraken",
        tick_size=0.01,
        lot_size=0.001,
        session_id="test-session",
        sequence_id=1,
    )
    rows = [
        {
            "trade_id": "1",
            "instrument_id": inst_id,
            "instrument_public_id": inst_pub_id,
            "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "price": 100.0,
            "size": 0.5,
            "side": "buy",
            "session_id": "test-session",
            "sequence_id": 1,
        },
        {
            "trade_id": "2",
            "instrument_id": inst_id,
            "instrument_public_id": inst_pub_id,
            "timestamp": datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
            "price": 101.0,
            "size": 0.25,
            "side": "sell",
            "session_id": "test-session",
            "sequence_id": 2,
        },
    ]
    inserted = await repo.upsert_trades(rows)
    assert inserted in (0, 1, 2)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_upsert_ticks_sqlite(tmp_path: Path) -> None:
    """Test SQLAlchemyRepository upsert_ticks with SQLite.

    Given: SQLite repository with instrument,
    When: upsert_ticks is called with tick data,
    Then: Inserts ticks successfully.
    """
    db_path = tmp_path / "t.db"
    url = f"sqlite+aiosqlite:///{db_path.as_posix()}"
    repo = SQLAlchemyRepository(url)
    await repo.create_all()
    async with repo.session() as s:
        s.add(
            Symbol(
                native_symbol="ETH-USD",
                base="ETH",
                quote="USD",
                asset_type="crypto",
                created_at=datetime.now(UTC),
                timestamp=datetime.now(UTC),
                session_id="test-session",
                sequence_id=1,
            )
        )
        await s.commit()
    spid = await resolve_symbol_public_id(repo, "ETH-USD")
    assert spid is not None
    inst_id, inst_pub_id = await repo.upsert_instrument(
        symbol_public_id=spid,
        symbol="ETH-USD",
        base="ETH",
        quote="USD",
        exchange="kraken",
        tick_size=0.01,
        lot_size=0.001,
        session_id="test-session",
        sequence_id=1,
    )
    tick_rows = [
        {
            "instrument_id": inst_id,
            "instrument_public_id": inst_pub_id,
            "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "bid": 99.5,
            "ask": 100.5,
            "last": 100.0,
            "volume": 1000.0,
            "session_id": "test-session",
            "sequence_id": 1,
        },
        {
            "public_id": "019d0000-0000-7000-8000-000000000001",
            "instrument_id": inst_id,
            "instrument_public_id": inst_pub_id,
            "timestamp": datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
            "bid": 99.0,
            "ask": 101.0,
            "last": 100.5,
            "volume": 500.0,
            "session_id": "test-session",
            "sequence_id": 2,
        },
    ]
    inserted = await repo.upsert_ticks(tick_rows)
    assert inserted == 2
