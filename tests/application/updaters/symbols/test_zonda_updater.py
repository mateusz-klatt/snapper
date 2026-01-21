"""Tests for Zonda symbol mapping updater service."""

from collections.abc import AsyncIterator
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import MagicMock
from unittest.mock import Mock

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from snapper.application.updaters.symbols.zonda import ZondaSymbolMappingUpdaterService
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import WebSocketTokenRotator
from snapper.data.models import Base
from snapper.data.models import SymbolMapping
from snapper.data.repository import DatabaseRepository
from snapper.indicators.ta_lib_adapter import macd
from snapper.indicators.ta_lib_adapter import rsi
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient


class TestTokensWebSocketRotatorSingleton:
    """Test cases for WebSocket rotator singleton behavior."""

    def test_singleton_reinit_skipped(self) -> None:
        """Verify WebSocket rotator singleton prevents reinitialization.

        Given: Existing rotator instance with token manager,
        When: New rotator created with different token manager,
        Then: Original instance returned with original token manager.
        """
        WebSocketTokenRotator._instance = None
        mock_token_manager = MagicMock(spec=TokenManager)
        rotator1 = WebSocketTokenRotator(token_manager=mock_token_manager)
        assert rotator1.token_manager is mock_token_manager
        another_mock = MagicMock(spec=TokenManager)
        rotator2 = WebSocketTokenRotator(token_manager=another_mock)
        assert rotator1 is rotator2
        assert rotator2.token_manager is mock_token_manager
        WebSocketTokenRotator._instance = None


class TestZondaSymbolUpdaterMethods:
    """Test cases for ZondaSymbolMappingUpdaterService methods."""

    @pytest.mark.asyncio
    async def test_create_exchange_client(self) -> None:
        """Verify _create_exchange_client returns ZondaExchangeClient.

        Given: ZondaSymbolMappingUpdaterService instance,
        When: _create_exchange_client called,
        Then: ZondaExchangeClient instance returned.
        """
        service = ZondaSymbolMappingUpdaterService(update_threshold_hours=6, force=False)
        client = service._create_exchange_client()
        assert isinstance(client, ZondaExchangeClient)

    @pytest.mark.asyncio
    async def test_fetch_symbols_delegates_to_load_zonda_markets(self) -> None:
        """Verify _fetch_symbols delegates to load_zonda_markets.

        Given: Service with mocked load_zonda_markets,
        When: _fetch_symbols called,
        Then: Returns expected symbols from load_zonda_markets.
        """
        service = ZondaSymbolMappingUpdaterService(update_threshold_hours=6, force=False)
        mock_client = MagicMock()
        expected_symbols: list[dict[str, Any]] = [
            {
                "native_symbol": "BTC-PLN",
                "base_currency": "BTC",
                "quote_currency": "PLN",
                "zonda_symbol": "BTC-PLN",
            }
        ]

        async def mock_load_zonda_markets(client: Any) -> list[dict[str, Any]]:
            return expected_symbols

        service.load_zonda_markets = mock_load_zonda_markets
        result = await service._fetch_symbols(mock_client)
        assert result == expected_symbols


class TestTaLibAdapterImportBranch:
    """Test cases for TA-Lib adapter fallback implementations."""

    def test_talib_fallback_when_not_available(self) -> None:
        """Verify RSI fallback implementation works without TA-Lib.

        Given: Price series with 8 values,
        When: RSI calculated with period=5,
        Then: Valid pandas Series returned with same length.
        """
        prices = pd.Series([44.0, 44.5, 43.5, 44.0, 44.5, 45.0, 45.5, 45.0])
        result = rsi(prices, period=5)
        assert isinstance(result, pd.Series)
        assert len(result) == len(prices)

    def test_macd_fallback_when_not_available(self) -> None:
        """Verify MACD fallback implementation works without TA-Lib.

        Given: Extended price series,
        When: MACD calculated,
        Then: Three valid pandas Series returned.
        """
        prices = pd.Series([44.0, 44.5, 43.5, 44.0, 44.5, 45.0, 45.5, 45.0] * 5)
        macd_line, signal_line, histogram = macd(prices)
        assert isinstance(macd_line, pd.Series)
        assert isinstance(signal_line, pd.Series)
        assert isinstance(histogram, pd.Series)


class StubZondaClient:
    """Mock Zonda client for testing market subscription."""

    def __init__(self, markets: list[dict[str, Any]]) -> None:
        """Initialize the instance."""
        self._markets = markets

    async def subscribe_instruments(self) -> AsyncIterator[dict[str, Any]]:
        """Yield configured market data."""
        for market in self._markets:
            yield market


class ExposedZondaSymbolMappingUpdater(ZondaSymbolMappingUpdaterService):
    """Exposed updater for testing protected methods."""

    async def load_zonda_markets_public(self, client: StubZondaClient) -> list[dict[str, Any]]:
        """Expose load_zonda_markets for testing."""
        return await self.load_zonda_markets(cast(ZondaExchangeClient, client))

    async def update_database_public(self, symbols: list[dict[str, Any]]) -> None:
        """Expose _update_database for testing."""
        await super()._update_database(symbols)


@pytest.fixture()
def updater_with_repository(
    tmp_path: Path,
) -> Iterator[tuple[ExposedZondaSymbolMappingUpdater, DatabaseRepository]]:
    """Provide Zonda updater instance with test database."""
    db_path = tmp_path / "zonda_symbols.sqlite"
    repository = DatabaseRepository(f"sqlite:///{db_path}")
    repository.create_all()
    updater = ExposedZondaSymbolMappingUpdater(update_threshold_hours=1, force=True)
    updater.repository = repository
    yield updater, repository
    repository.engine.dispose()


@pytest.mark.asyncio()
async def test_load_zonda_markets_filters_invalid_entries() -> None:
    """Verify load_zonda_markets filters entries with invalid data.

    Given: Markets with valid, missing id, wrong type, and empty fields,
    When: load_zonda_markets called,
    Then: Only valid market entry returned.
    """
    markets: list[dict[str, Any]] = [
        {
            "id": "BTC-USD",
            "symbol": "BTC-USD",
            "base": "btc",
            "quote": "usd",
        },
        {
            "id": "ETH-USD",
            "symbol": "ETH-USD",
            "quote": "USD",
        },
        {
            "id": "DOGE-PLN",
            "symbol": 123,
            "base": "DOGE",
            "quote": "PLN",
        },
        {
            "id": "LTC-PLN",
            "symbol": "LTC/PLN",
            "base": " ",
            "quote": "",
        },
    ]
    client = StubZondaClient(markets)
    updater = ExposedZondaSymbolMappingUpdater(update_threshold_hours=1, force=True)
    parsed = await updater.load_zonda_markets_public(client)
    assert parsed == [
        {
            "zonda_symbol": "BTC-USD",
            "native_symbol": "BTC-USD",
            "ccxt_symbol": "BTC-USD",
            "base": "btc",
            "quote": "usd",
        }
    ]


@pytest.mark.asyncio()
async def test_update_database_handles_inserts_and_updates(
    updater_with_repository: tuple[ExposedZondaSymbolMappingUpdater, DatabaseRepository],
) -> None:
    """Verify update_database handles both inserts and updates.

    Given: Existing BTC-USD mapping with old zonda_symbol,
    When: Update database called with updated BTC and new ETH,
    Then: BTC updated, ETH inserted with correct timestamps.
    """
    updater, repository = updater_with_repository
    original_timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolMapping(
                native_symbol="BTC-USD",
                zonda_symbol="BTC-USD-OLD",
                base_currency="BTC",
                quote_currency="USD",
                created_at=original_timestamp,
                updated_at=original_timestamp,
            )
        )
        session.commit()
    payload = [
        {
            "native_symbol": "BTC-USD",
            "zonda_symbol": "BTC-USD",
            "base": "BTC",
            "quote": "USD",
        },
        {
            "native_symbol": "ETH-USD",
            "zonda_symbol": "ETH-USD",
            "base": "ETH",
            "quote": "USD",
        },
    ]
    await updater.update_database_public(payload)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        btc_mapping = session.execute(
            select(SymbolMapping).where(SymbolMapping.native_symbol == "BTC-USD")
        ).scalar_one()
        eth_mapping = session.execute(
            select(SymbolMapping).where(SymbolMapping.native_symbol == "ETH-USD")
        ).scalar_one()
    assert btc_mapping.zonda_symbol == "BTC-USD"
    assert btc_mapping.updated_at.replace(tzinfo=None) > original_timestamp.replace(tzinfo=None)
    assert eth_mapping.zonda_symbol == "ETH-USD"
    assert eth_mapping.base_currency == "ETH"
    assert eth_mapping.quote_currency == "USD"
    assert eth_mapping.created_at == eth_mapping.updated_at


@pytest.mark.asyncio()
async def test_update_database_skips_unchanged_mapping(
    updater_with_repository: tuple[ExposedZondaSymbolMappingUpdater, DatabaseRepository],
) -> None:
    """Verify update_database skips update when mapping unchanged.

    Given: Existing mapping with same values as payload,
    When: Update database called,
    Then: updated_at timestamp preserved unchanged.
    """
    updater, repository = updater_with_repository
    original_timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolMapping(
                native_symbol="BTC-USD",
                zonda_symbol="BTC-USD",
                ccxt_symbol="BTC/USD",
                base_currency="BTC",
                quote_currency="USD",
                created_at=original_timestamp,
                updated_at=original_timestamp,
            )
        )
        session.commit()
    payload = [
        {
            "native_symbol": "BTC-USD",
            "zonda_symbol": "BTC-USD",
            "ccxt_symbol": "BTC/USD",
            "base": "BTC",
            "quote": "USD",
        },
    ]
    await updater.update_database_public(payload)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        btc_mapping = session.execute(
            select(SymbolMapping).where(SymbolMapping.native_symbol == "BTC-USD")
        ).scalar_one()
    assert btc_mapping.updated_at.replace(tzinfo=None) == original_timestamp.replace(tzinfo=None)
    assert btc_mapping.zonda_symbol == "BTC-USD"
    assert btc_mapping.ccxt_symbol == "BTC/USD"


class DummyClient(SimpleNamespace):
    """Mock client for testing market iteration."""

    def __init__(self, markets: list[dict[str, Any]]) -> None:
        """Initialize the instance."""
        super().__init__()
        self._markets = markets

    async def subscribe_instruments(self) -> AsyncIterator[dict[str, Any]]:
        """Yield configured markets."""
        for m in self._markets:
            yield m


@pytest.mark.asyncio
async def test_load_zonda_markets_filters_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify load_zonda_markets filters entries with null/missing fields.

    Given: Markets with valid, null id, and null symbol,
    When: load_zonda_markets called,
    Then: Only valid entry with proper fields returned.
    """
    markets: list[dict[str, Any]] = [
        {"id": "BTC-USD", "symbol": "BTC/USD", "base": "BTC", "quote": "USD"},
        {"id": None, "symbol": "ETH/USD", "base": "ETH", "quote": "USD"},
        {"id": "BAD", "symbol": None, "base": "X", "quote": "Y"},
    ]
    svc = ZondaSymbolMappingUpdaterService(update_threshold_hours=24, force=True)
    result = await svc.load_zonda_markets(cast(Any, DummyClient(markets)))
    assert len(result) == 1
    assert result[0]["zonda_symbol"] == "BTC-USD"


@pytest.mark.asyncio
async def test_update_database_creates_and_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify update_database creates new mappings.

    Given: Session mock with no existing mapping,
    When: Update database called with symbols,
    Then: Session add called for new mapping.
    """
    svc = ZondaSymbolMappingUpdaterService(update_threshold_hours=24, force=True)
    fake_session = SimpleNamespace(
        execute=Mock(return_value=SimpleNamespace(scalar_one_or_none=lambda: None)),
        add=Mock(),
        commit=Mock(),
    )

    class DummyRepo(SimpleNamespace):
        def get_session(self) -> "DummyRepo":
            return self

        def __enter__(self) -> Any:
            return fake_session

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            pass

    repo: Any = DummyRepo()
    svc.repository = repo
    symbols = [
        {
            "zonda_symbol": "BTC-USD",
            "native_symbol": "BTC-USD",
            "ccxt_symbol": "BTC/USD",
            "base": "BTC",
            "quote": "USD",
        }
    ]
    await svc._update_database(symbols)
    fake_session.add.assert_called_once()


def test_get_default_kwargs_and_setting_key() -> None:
    """Verify get_default_kwargs and _get_setting_key return expected values.

    Given: ZondaSymbolMappingUpdaterService instance,
    When: get_default_kwargs and _get_setting_key called,
    Then: Expected threshold hours and setting key returned.
    """
    svc = ZondaSymbolMappingUpdaterService(update_threshold_hours=1, force=False)
    defaults = svc.get_default_kwargs(object())
    assert defaults["update_threshold_hours"] == 24
    assert svc._get_setting_key() == "zonda_symbol_mapping_last_update"


@pytest.mark.asyncio
async def test_load_zonda_markets_raises_on_error() -> None:
    """Verify load_zonda_markets propagates client errors.

    Given: Client that raises RuntimeError on subscribe,
    When: load_zonda_markets called,
    Then: RuntimeError propagated.
    """
    svc = ZondaSymbolMappingUpdaterService(update_threshold_hours=24, force=True)

    class FailingClient:
        async def subscribe_instruments(self) -> AsyncIterator[dict[str, Any]]:
            raise RuntimeError("boom")
            yield {}

    with pytest.raises(RuntimeError):
        await svc.load_zonda_markets(cast(Any, FailingClient()))


@pytest.mark.asyncio
async def test_update_database_updates_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify update_database updates existing and inserts new mappings.

    Given: Database with existing BTC-USD mapping,
    When: Update database called with updated BTC and new ETH,
    Then: BTC zonda_symbol updated, ETH inserted.
    """
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_local = sessionmaker(bind=engine)
    svc = ZondaSymbolMappingUpdaterService(update_threshold_hours=24, force=True)
    with session_local() as session:
        mapping = SymbolMapping(
            native_symbol="BTC-USD",
            zonda_symbol="OLD",
            base_currency="BTC",
            quote_currency="USD",
            ccxt_symbol="OLD/USDT",
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        session.add(mapping)
        session.commit()

    class Repo:
        def get_session(self) -> Session:
            return session_local()

    svc.repository = Repo()
    symbols = [
        {
            "zonda_symbol": "BTC-USD",
            "native_symbol": "BTC-USD",
            "ccxt_symbol": "BTC/USD",
            "base": "BTC",
            "quote": "USD",
        },
        {
            "zonda_symbol": "ETH-USD",
            "native_symbol": "ETH-USD",
            "ccxt_symbol": "ETH/USD",
            "base": "ETH",
            "quote": "USD",
        },
    ]
    await svc._update_database(symbols)
    with session_local() as session:
        btc = session.query(SymbolMapping).filter_by(native_symbol="BTC-USD").one()
        eth = session.query(SymbolMapping).filter_by(native_symbol="ETH-USD").one()
        assert btc.zonda_symbol == "BTC-USD"
        assert btc.ccxt_symbol == "BTC/USD"
        assert eth.zonda_symbol == "ETH-USD"


@pytest.mark.asyncio
async def test_update_database_handles_commit_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify update_database propagates commit errors.

    Given: Session that raises RuntimeError on commit,
    When: Update database called,
    Then: RuntimeError propagated.
    """
    svc = ZondaSymbolMappingUpdaterService(update_threshold_hours=24, force=True)

    class FaultySession:
        def __enter__(self) -> "FaultySession":
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, _stmt: Any) -> Any:
            return SimpleNamespace(scalar_one_or_none=lambda: None)

        def add(self, _obj: Any) -> None:
            return None

        def commit(self) -> None:
            raise RuntimeError("fail")

    class Repo:
        def get_session(self) -> FaultySession:
            return FaultySession()

    svc.repository = Repo()
    symbols = [
        {
            "zonda_symbol": "BTC-USD",
            "native_symbol": "BTC-USD",
            "ccxt_symbol": "BTC/USD",
            "base": "BTC",
            "quote": "USD",
        }
    ]
    with pytest.raises(RuntimeError):
        await svc._update_database(symbols)
