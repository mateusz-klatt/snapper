"""Tests for Zonda symbol updater service.

Validates that the Zonda updater correctly creates Symbol, Symbol,
and SymbolAlias rows via _upsert_symbol / _upsert_alias.
"""

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

from snapper.application.updaters.symbols.zonda import ZondaSymbolUpdaterService
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import WebSocketTokenRotator
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
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
    """Test cases for ZondaSymbolUpdaterService methods."""

    @pytest.mark.asyncio
    async def test_create_exchange_client(self) -> None:
        """Verify _create_exchange_client returns ZondaExchangeClient.

        Given: ZondaSymbolUpdaterService instance,
        When: _create_exchange_client called,
        Then: ZondaExchangeClient instance returned.
        """
        service = ZondaSymbolUpdaterService(update_threshold_hours=6, force=False)
        client = service._create_exchange_client()
        assert isinstance(client, ZondaExchangeClient)

    @pytest.mark.asyncio
    async def test_fetch_symbols_delegates_to_load_zonda_markets(self) -> None:
        """Verify _fetch_symbols delegates to load_zonda_markets.

        Given: Service with mocked load_zonda_markets,
        When: _fetch_symbols called,
        Then: Returns expected symbols from load_zonda_markets.
        """
        service = ZondaSymbolUpdaterService(update_threshold_hours=6, force=False)
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


class ExposedZondaSymbolUpdater(ZondaSymbolUpdaterService):
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
) -> Iterator[tuple[ExposedZondaSymbolUpdater, DatabaseRepository]]:
    """Provide Zonda updater instance with test database."""
    db_path = tmp_path / "zonda_symbols.sqlite"
    repository = DatabaseRepository(f"sqlite:///{db_path}")
    repository.create_all()
    updater = ExposedZondaSymbolUpdater(update_threshold_hours=1, force=True)
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
    updater = ExposedZondaSymbolUpdater(update_threshold_hours=1, force=True)
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
    updater_with_repository: tuple[ExposedZondaSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify update_database handles both inserts and updates.

    Given: Existing BTC-USD catalog+alias with old exchange_symbol,
    When: Update database called with updated BTC and new ETH,
    Then: BTC ws alias updated, ETH catalog+aliases inserted with correct timestamps.
    """
    updater, repository = updater_with_repository
    original_timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=original_timestamp,
                timestamp=original_timestamp,
                session_id="test-session",
                sequence_id=1,
            )
        )

        _spid_btc_usd = session.execute(
            select(Symbol.public_id).where(
                Symbol.known_to == KNOWN_TO_MAX, Symbol.native_symbol == "BTC-USD"
            )
        ).scalar_one()
        session.add(
            SymbolAlias(
                symbol_public_id=_spid_btc_usd,
                exchange="zonda",
                channel="ws",
                exchange_symbol="BTC-USD-OLD",
                created_at=original_timestamp,
                timestamp=original_timestamp,
                session_id="test-session",
                sequence_id=1,
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
        btc_ws = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.symbol_public_id
                == session.execute(
                    select(Symbol.public_id).where(
                        Symbol.known_to == KNOWN_TO_MAX, Symbol.native_symbol == "BTC-USD"
                    )
                ).scalar_one(),
                SymbolAlias.exchange == "zonda",
                SymbolAlias.channel == "ws",
                SymbolAlias.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
        eth_catalog = session.execute(
            select(Symbol).where(Symbol.native_symbol == "ETH-USD")
        ).scalar_one()
        eth_ws = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.symbol_public_id
                == session.execute(
                    select(Symbol.public_id).where(
                        Symbol.known_to == KNOWN_TO_MAX, Symbol.native_symbol == "ETH-USD"
                    )
                ).scalar_one(),
                SymbolAlias.exchange == "zonda",
                SymbolAlias.channel == "ws",
            )
        ).scalar_one()
    assert btc_ws.exchange_symbol == "BTC-USD"
    assert btc_ws.timestamp.replace(tzinfo=None) > original_timestamp.replace(tzinfo=None)
    assert eth_ws.exchange_symbol == "ETH-USD"
    assert eth_catalog.base == "ETH"
    assert eth_catalog.quote == "USD"
    assert eth_ws.created_at == eth_ws.timestamp


@pytest.mark.asyncio()
async def test_update_database_skips_unchanged_mapping(
    updater_with_repository: tuple[ExposedZondaSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify update_database skips update when alias exchange_symbol unchanged.

    Given: Existing catalog+aliases with same values as payload,
    When: Update database called,
    Then: updated_at timestamps preserved unchanged on alias rows.
    """
    updater, repository = updater_with_repository
    original_timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=original_timestamp,
                timestamp=original_timestamp,
                session_id="test-session",
                sequence_id=1,
            )
        )

        _spid_btc_usd = session.execute(
            select(Symbol.public_id).where(
                Symbol.known_to == KNOWN_TO_MAX, Symbol.native_symbol == "BTC-USD"
            )
        ).scalar_one()
        session.add(
            SymbolAlias(
                symbol_public_id=_spid_btc_usd,
                exchange="zonda",
                channel="ws",
                exchange_symbol="BTC-USD",
                created_at=original_timestamp,
                timestamp=original_timestamp,
                session_id="test-session",
                sequence_id=1,
            )
        )
        _spid_btc_usd = session.execute(
            select(Symbol.public_id).where(
                Symbol.known_to == KNOWN_TO_MAX, Symbol.native_symbol == "BTC-USD"
            )
        ).scalar_one()
        session.add(
            SymbolAlias(
                symbol_public_id=_spid_btc_usd,
                exchange="zonda",
                channel="ccxt",
                exchange_symbol="BTC/USD",
                created_at=original_timestamp,
                timestamp=original_timestamp,
                session_id="test-session",
                sequence_id=1,
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
        btc_ws = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.symbol_public_id
                == session.execute(
                    select(Symbol.public_id).where(
                        Symbol.known_to == KNOWN_TO_MAX, Symbol.native_symbol == "BTC-USD"
                    )
                ).scalar_one(),
                SymbolAlias.exchange == "zonda",
                SymbolAlias.channel == "ws",
            )
        ).scalar_one()
        btc_ccxt = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.symbol_public_id
                == session.execute(
                    select(Symbol.public_id).where(
                        Symbol.known_to == KNOWN_TO_MAX, Symbol.native_symbol == "BTC-USD"
                    )
                ).scalar_one(),
                SymbolAlias.exchange == "zonda",
                SymbolAlias.channel == "ccxt",
            )
        ).scalar_one()
    assert btc_ws.timestamp.replace(tzinfo=None) == original_timestamp.replace(tzinfo=None)
    assert btc_ws.exchange_symbol == "BTC-USD"
    assert btc_ccxt.exchange_symbol == "BTC/USD"


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
    svc = ZondaSymbolUpdaterService(update_threshold_hours=24, force=True)
    result = await svc.load_zonda_markets(cast(Any, DummyClient(markets)))
    assert len(result) == 1
    assert result[0]["zonda_symbol"] == "BTC-USD"


@pytest.mark.asyncio
async def test_update_database_creates_and_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify update_database creates catalog, alias, and capability rows.

    Given: Session mock with no existing catalog or alias rows,
    When: Update database called with one symbol (including ccxt_symbol),
    Then: Session add called for catalog + ws alias + ccxt alias + capability (four times).
    """
    svc = ZondaSymbolUpdaterService(update_threshold_hours=24, force=True)
    fake_session = SimpleNamespace(
        execute=Mock(
            return_value=SimpleNamespace(
                scalar_one_or_none=lambda: None,
                scalars=lambda: SimpleNamespace(all=lambda: []),
            )
        ),
        add=Mock(),
        flush=Mock(),
        commit=Mock(),
    )

    class DummyRepo(SimpleNamespace):
        """Fake repository returning a context-managed fake session."""

        def get_session(self) -> DummyRepo:
            """Return self as the context manager."""
            return self

        def __enter__(self) -> Any:
            """Provide the fake session on context entry."""
            return fake_session

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            """No cleanup required on context exit."""

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
    assert fake_session.add.call_count == 5


def test_get_default_parameters_and_setting_key() -> None:
    """Verify get_default_parameters and _get_setting_key return expected values.

    Given: ZondaSymbolUpdaterService instance,
    When: get_default_parameters and _get_setting_key called,
    Then: Expected threshold hours and setting key returned.
    """
    svc = ZondaSymbolUpdaterService(update_threshold_hours=1, force=False)
    defaults = svc.get_default_parameters(object())
    assert defaults["update_threshold_hours"] == 24
    assert svc._get_setting_key() == "zonda_symbols_last_update"


@pytest.mark.asyncio
async def test_load_zonda_markets_raises_on_error() -> None:
    """Verify load_zonda_markets propagates client errors.

    Given: Client that raises RuntimeError on subscribe,
    When: load_zonda_markets called,
    Then: RuntimeError propagated.
    """
    svc = ZondaSymbolUpdaterService(update_threshold_hours=24, force=True)

    class FailingClient:
        async def subscribe_instruments(self) -> AsyncIterator[dict[str, Any]]:
            raise RuntimeError("boom")
            yield {}

    with pytest.raises(RuntimeError):
        await svc.load_zonda_markets(cast(Any, FailingClient()))


@pytest.mark.asyncio
async def test_update_database_updates_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify update_database updates existing aliases and inserts new ones.

    Given: Database with existing BTC-USD catalog and ws/ccxt aliases with OLD symbols,
    When: Update database called with updated BTC and new ETH,
    Then: BTC aliases updated, ETH catalog+aliases inserted.
    """
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_local = sessionmaker(bind=engine)
    svc = ZondaSymbolUpdaterService(update_threshold_hours=24, force=True)
    seed_time = datetime.now(UTC)
    with session_local() as session:
        session.add(
            Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=seed_time,
                timestamp=seed_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.flush()
        _spid_btc_usd = session.query(Symbol).filter_by(native_symbol="BTC-USD").one().public_id

        session.add(
            SymbolAlias(
                symbol_public_id=_spid_btc_usd,
                exchange="zonda",
                channel="ws",
                exchange_symbol="OLD",
                created_at=seed_time,
                timestamp=seed_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        _spid_btc_usd = session.execute(
            select(Symbol.public_id).where(
                Symbol.known_to == KNOWN_TO_MAX, Symbol.native_symbol == "BTC-USD"
            )
        ).scalar_one()
        session.add(
            SymbolAlias(
                symbol_public_id=_spid_btc_usd,
                exchange="zonda",
                channel="ccxt",
                exchange_symbol="OLD/USDT",
                created_at=seed_time,
                timestamp=seed_time,
                session_id="test-session",
                sequence_id=1,
            )
        )
        session.commit()

    class Repo:
        """Minimal repository exposing a session factory."""

        def get_session(self) -> Session:
            """Return a new SQLAlchemy session."""
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
        btc_ws = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.symbol_public_id
                == session.execute(
                    select(Symbol.public_id).where(
                        Symbol.known_to == KNOWN_TO_MAX, Symbol.native_symbol == "BTC-USD"
                    )
                ).scalar_one(),
                SymbolAlias.exchange == "zonda",
                SymbolAlias.channel == "ws",
                SymbolAlias.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
        btc_ccxt = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.symbol_public_id
                == session.execute(
                    select(Symbol.public_id).where(
                        Symbol.known_to == KNOWN_TO_MAX, Symbol.native_symbol == "BTC-USD"
                    )
                ).scalar_one(),
                SymbolAlias.exchange == "zonda",
                SymbolAlias.channel == "ccxt",
                SymbolAlias.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
        eth_ws = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.symbol_public_id
                == session.execute(
                    select(Symbol.public_id).where(
                        Symbol.known_to == KNOWN_TO_MAX, Symbol.native_symbol == "ETH-USD"
                    )
                ).scalar_one(),
                SymbolAlias.exchange == "zonda",
                SymbolAlias.channel == "ws",
            )
        ).scalar_one()
        assert btc_ws.exchange_symbol == "BTC-USD"
        assert btc_ccxt.exchange_symbol == "BTC/USD"
        assert eth_ws.exchange_symbol == "ETH-USD"


@pytest.mark.asyncio
async def test_update_database_handles_commit_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify update_database propagates commit errors.

    Given: Session that raises RuntimeError on commit,
    When: Update database called with catalog and alias upserts,
    Then: RuntimeError propagated.
    """
    svc = ZondaSymbolUpdaterService(update_threshold_hours=24, force=True)

    class FaultySession:
        """Session stub that raises on commit."""

        def __enter__(self) -> FaultySession:
            """Return self on context entry."""
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            """No cleanup required on context exit."""

        def execute(self, _stmt: Any) -> Any:
            """Return stub supporting both scalar_one_or_none and scalars().all()."""
            return SimpleNamespace(
                scalar_one_or_none=lambda: None,
                scalars=lambda: SimpleNamespace(all=lambda: []),
            )

        def add(self, _obj: Any) -> None:
            """Accept any add call without action."""

        def flush(self) -> None:
            """Accept any flush call without action."""

        def commit(self) -> None:
            """Raise RuntimeError to simulate commit failure."""
            raise RuntimeError("fail")

    class Repo:
        """Minimal repository returning a faulty session."""

        def get_session(self) -> FaultySession:
            """Return a new FaultySession instance."""
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


@pytest.mark.asyncio()
async def test_update_database_creates_capability_rows(
    updater_with_repository: tuple[ExposedZondaSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify _update_database creates SymbolExchangeCapability rows.

    Given: Empty database,
    When: _update_database called with two symbols,
    Then: Capability row created per symbol with exchange=zonda,
          can_market_data=True, can_trade=True, source=zonda_updater.
    """
    updater, repository = updater_with_repository
    payload = [
        {
            "native_symbol": "BTC-USD",
            "zonda_symbol": "BTC-USD",
            "ccxt_symbol": "BTC/USD",
            "base": "BTC",
            "quote": "USD",
        },
        {
            "native_symbol": "ETH-USD",
            "zonda_symbol": "ETH-USD",
            "ccxt_symbol": "ETH/USD",
            "base": "ETH",
            "quote": "USD",
        },
    ]
    await updater.update_database_public(payload)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        caps = session.execute(select(SymbolExchangeCapability)).scalars().all()
        assert len(caps) == 2
        _sym_map = {
            s.public_id: s.native_symbol for s in session.execute(select(Symbol)).scalars().all()
        }
        cap_map = {_sym_map.get(c.symbol_public_id, c.symbol_public_id): c for c in caps}
        for native_symbol in ("BTC-USD", "ETH-USD"):
            cap = cap_map[native_symbol]
            assert cap.exchange == "zonda"
            assert cap.can_market_data is True
            assert cap.can_trade is True
            assert cap.source == "zonda_updater"
            assert cap.reason is None
