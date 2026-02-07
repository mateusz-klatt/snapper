"""Tests for Kraken symbol updater service."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from snapper.application.updaters.symbols.kraken import KrakenSymbolUpdaterService
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.data.models import Base
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolCatalog
from snapper.data.models import SymbolExchangeCapability
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient


class TestKrakenSymbolUpdater:
    """Test cases for KrakenSymbolUpdaterService basic functionality."""

    @pytest.fixture
    def mock_settings(self) -> MagicMock:
        """Create mock settings for Kraken updater."""
        settings = MagicMock()
        settings.db_url = "sqlite:///test.db"
        settings.zmq_broker_xsub = "tcp://localhost:5555"
        return settings

    @pytest.fixture
    def updater(self, mock_settings: MagicMock) -> KrakenSymbolUpdaterService:
        """Create KrakenSymbolUpdaterService with mock settings."""
        with patch(
            "snapper.config.settings.get_settings",
            return_value=mock_settings,
        ):
            updater = KrakenSymbolUpdaterService(update_threshold_hours=6)
            return updater

    def test_init(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify service initializes with correct attributes.

        Given: Mock settings for Kraken service,
        When: Service initialized,
        Then: Threshold set and client is None.
        """
        assert updater.update_threshold_hours == 6
        assert updater._kraken_client is None

    def test_get_setting_key(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify _get_setting_key returns correct identifier.

        Given: Kraken updater instance,
        When: _get_setting_key called,
        Then: Expected setting key returned.
        """
        assert updater._get_setting_key() == "kraken_symbols_last_update"

    def test_create_exchange_client(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify _create_exchange_client returns KrakenExchangeClient.

        Given: Kraken updater instance,
        When: _create_exchange_client called,
        Then: KrakenExchangeClient instance returned.
        """
        client = updater._create_exchange_client()
        assert isinstance(client, KrakenExchangeClient)

    async def test_load_kraken_rest_symbols(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify load_kraken_rest_symbols loads and parses markets.

        Given: Mock CCXT client with market and tokenized data,
        When: load_kraken_rest_symbols called,
        Then: All symbols returned with correct attributes.
        """
        mock_markets = {
            "BTC-USD": {
                "id": "XXBTZUSD",
                "base": "BTC",
                "quote": "USD",
                "symbol": "BTC-USD",
            },
            "ETH/EUR": {
                "id": "XETHZEUR",
                "base": "ETH",
                "quote": "EUR",
                "symbol": "ETH/EUR",
            },
        }
        tokenized_response = {
            "error": [],
            "result": {
                "GSxUSD": {
                    "base": "GSx",
                    "quote": "ZUSD",
                    "wsname": "GSx/USD",
                    "altname": "GSxUSD",
                },
            },
        }
        mock_client = AsyncMock()
        mock_ccxt = MagicMock()
        mock_ccxt.load_markets = MagicMock(return_value=mock_markets)
        mock_ccxt.publicGetAssetPairs = MagicMock(return_value=tokenized_response)
        mock_client._ccxt_client = mock_ccxt
        mock_client.get_ccxt_client = MagicMock(return_value=mock_ccxt)

        async def mock_to_thread(func: Any, *args: Any) -> Any:
            return func(*args)

        with (
            patch.object(updater, "_create_exchange_client", return_value=mock_client),
            patch("asyncio.to_thread", side_effect=mock_to_thread),
        ):
            result = await updater.load_kraken_rest_symbols()
        assert len(result) == 3
        assert "XXBTZUSD" in result
        assert "XETHZEUR" in result
        assert "GSxUSD" in result
        assert result["XXBTZUSD"]["base"] == "BTC"
        assert result["XXBTZUSD"]["quote"] == "USD"
        assert result["XXBTZUSD"]["ccxt_symbol"] == "BTC-USD"
        assert result["XXBTZUSD"]["asset_class"] == "currency"
        assert result["XETHZEUR"]["base"] == "ETH"
        assert result["XETHZEUR"]["quote"] == "EUR"
        assert result["XETHZEUR"]["ccxt_symbol"] == "ETH/EUR"
        assert result["XETHZEUR"]["asset_class"] == "currency"
        assert result["GSxUSD"]["base"] == "GSx"
        assert result["GSxUSD"]["quote"] == "ZUSD"
        assert result["GSxUSD"]["ccxt_symbol"] is None
        assert result["GSxUSD"]["asset_class"] == "tokenized_asset"
        assert mock_ccxt.load_markets.call_count == 1
        assert mock_ccxt.publicGetAssetPairs.call_count == 1

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify verify_websocket_symbols confirms symbols via WebSocket.

        Given: Set of symbols to verify and mock WS responses,
        When: verify_websocket_symbols called,
        Then: Verified symbols returned.
        """
        ws_symbols_to_verify = {"BTC/USD"}
        raw_ws_responses = [
            {"symbol": "BTC/USD", "base": "XBT", "quote": "USD", "status": "online"},
        ]

        async def mock_subscribe(raw: bool = False) -> AsyncIterator[dict[str, Any]]:
            for resp in raw_ws_responses:
                yield resp

        mock_client = AsyncMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = AsyncMock()
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols(ws_symbols_to_verify)
        assert len(verified) == 1
        assert "BTC/USD" in verified
        assert ws_only == []
        mock_client.disconnect_websocket.assert_called_once()

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_partial_verification(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify verify_websocket_symbols returns partial results.

        Given: Two symbols to verify, only one in WS response,
        When: verify_websocket_symbols called,
        Then: Only verified symbol returned.
        """
        ws_symbols_to_verify = {"BTC/USD", "ETH/USD"}
        raw_ws_responses = [
            {"symbol": "BTC/USD", "base": "XBT", "quote": "USD", "status": "online"},
        ]

        async def mock_subscribe(raw: bool = False) -> AsyncIterator[dict[str, Any]]:
            for resp in raw_ws_responses:
                yield resp

        mock_client = AsyncMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = AsyncMock()
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, _ws_only = await updater.verify_websocket_symbols(ws_symbols_to_verify)
        assert len(verified) == 1
        assert "BTC/USD" in verified
        assert "ETH/USD" not in verified

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_all_verified_via_timeout(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify verify_websocket_symbols completes before timeout.

        Given: Single symbol that gets verified immediately,
        When: verify_websocket_symbols called,
        Then: Symbol verified and returned.
        """
        ws_symbols_to_verify = {"BTC/USD"}

        async def mock_subscribe(raw: bool = False) -> AsyncIterator[dict[str, Any]]:
            yield {"symbol": "BTC/USD", "base": "XBT", "quote": "USD", "status": "online"}

        mock_client = AsyncMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = AsyncMock()
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, _ws_only = await updater.verify_websocket_symbols(ws_symbols_to_verify)
        assert len(verified) == 1
        assert "BTC/USD" in verified

    @pytest.mark.asyncio
    async def test_build_verified_mappings(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify build_verified_mappings creates symbol data dicts.

        Given: Mock REST symbols and WS verification,
        When: build_verified_mappings called,
        Then: Dict mappings created with correct attributes.
        """
        mock_rest = {
            "XXBTZUSD": {
                "base": "XXBT",
                "quote": "ZUSD",
                "symbol": "BTC-USD",
            },
        }
        with (
            patch.object(updater, "load_kraken_rest_symbols", return_value=mock_rest),
            patch.object(updater, "verify_websocket_symbols", return_value=({"BTC-USD"}, [])),
        ):
            mappings, success = await updater.build_verified_mappings()
        assert success is True
        assert len(mappings) == 1
        assert "BTC-USD" in mappings
        mapping = mappings["BTC-USD"]
        assert isinstance(mapping, dict)
        assert mapping["kraken_websocket_symbol"] == "BTC/USD"
        assert mapping["kraken_rest_symbol"] == "XXBTZUSD"

    @pytest.mark.asyncio
    async def test_fetch_symbols(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify _fetch_symbols returns list of mapping dictionaries.

        Given: Mock verified mappings,
        When: _fetch_symbols called,
        Then: List of dict representations returned.
        """
        mock_mappings = {
            "BTC-USD": {
                "native_symbol": "BTC-USD",
                "kraken_websocket_symbol": "BTC/USD",
                "kraken_rest_symbol": "XXBTZUSD",
                "ccxt_symbol": "BTC-USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
            },
        }
        mock_client = AsyncMock()
        with patch.object(updater, "build_verified_mappings", return_value=(mock_mappings, True)):
            result = await updater._fetch_symbols(mock_client)
        assert len(result) == 1
        assert isinstance(result[0], dict)
        assert result[0]["kraken_websocket_symbol"] == "BTC/USD"

    @pytest.mark.asyncio
    async def test_update_database(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify _update_database persists catalog, alias, and capability rows.

        Given: Mock session and symbol list,
        When: _update_database called,
        Then: Session add called for catalog, aliases, and capability; commit called.
        """
        symbols = [
            {
                "native_symbol": "BTC-USD",
                "kraken_websocket_symbol": "BTC/USD",
                "kraken_rest_symbol": "XXBTZUSD",
                "ccxt_symbol": "BTC/USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
            },
        ]
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = None
        mock_session.execute.return_value.scalar_one_or_none.return_value = None
        mock_repo = MagicMock()
        mock_repo.get_session.return_value = mock_session
        with patch.object(updater, "repository", mock_repo):
            await updater._update_database(symbols)
        assert mock_session.add.call_count == 5
        mock_session.commit.assert_called_once()


class DummyKrakenClient(SimpleNamespace):
    """Mock Kraken client for testing REST and WebSocket operations."""

    def __init__(
        self, markets: dict[str, dict[str, Any]], tokenized: dict[str, dict[str, Any]]
    ) -> None:
        """Initialize the instance."""
        super().__init__()
        self._markets = markets
        self._tokenized = tokenized
        self._ccxt_client = SimpleNamespace(
            load_markets=lambda: markets,
            publicGetAssetPairs=lambda params=None: {"result": tokenized},
        )

    def get_ccxt_client(self) -> SimpleNamespace:
        """Return mock CCXT client instance."""
        return self._ccxt_client

    async def connect(self) -> None:
        """No-op connect for mock client."""
        ...


def _market(ccxt_symbol: str, rest_id: str, base: str, quote: str) -> dict[str, Any]:
    return {"id": rest_id, "symbol": ccxt_symbol, "info": {"base": base, "quote": quote}}


@pytest.mark.asyncio
async def test_load_kraken_rest_symbols_handles_tokenized(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify load_kraken_rest_symbols includes tokenized assets.

    Given: Markets with regular and tokenized pairs,
    When: load_kraken_rest_symbols called,
    Then: Both types included in result.
    """
    markets = {"BTC/USD": _market("BTC/USD", "XXBTZUSD", "XBT", "ZUSD")}
    tokenized = {"GSUSD": {"base": "GSX", "quote": "ZUSD"}}
    svc = KrakenSymbolUpdaterService()
    client: Any = DummyKrakenClient(markets, tokenized)
    client.connect = AsyncMock()
    monkeypatch.setattr(
        "snapper.application.updaters.symbols.kraken.KrakenSymbolUpdaterService._create_exchange_client",
        lambda self: client,
    )
    symbols = await svc.load_kraken_rest_symbols()
    assert "XXBTZUSD" in symbols
    assert "GSUSD" in symbols


def test_normalize_currency_mapping_and_fallbacks() -> None:
    """Verify _normalize_currency handles various currency formats.

    Given: Kraken updater instance,
    When: _normalize_currency called with various formats,
    Then: Correct normalized currency codes returned.
    """
    svc = KrakenSymbolUpdaterService()
    assert svc._normalize_currency("ZUSD") == "USD"
    assert svc._normalize_currency("XETH") == "ETH"
    assert svc._normalize_currency("ABC") == "ABC"


@pytest.mark.asyncio
async def test_build_verified_mappings_verification_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify build_verified_mappings returns failure below threshold.

    Given: Symbols with low verification rate,
    When: build_verified_mappings called,
    Then: Success flag is False.
    """
    svc = KrakenSymbolUpdaterService()
    fake_symbols = {
        "XXBTZUSD": {
            "base": "XBT",
            "quote": "ZUSD",
            "ccxt_symbol": "BTC/USD",
            "asset_class": "currency",
        },
        "GSXUSD": {
            "base": "GSx",
            "quote": "ZUSD",
            "ccxt_symbol": None,
            "asset_class": "tokenized_asset",
        },
    }
    monkeypatch.setattr(svc, "load_kraken_rest_symbols", AsyncMock(return_value=fake_symbols))
    monkeypatch.setattr(svc, "verify_websocket_symbols", AsyncMock(return_value=({"BTC/USD"}, [])))
    mappings, success = await svc.build_verified_mappings()
    assert not success
    assert "XBT-USD" in mappings


@pytest.mark.asyncio
async def test_build_verified_mappings_skips_invalid_tokenized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify build_verified_mappings skips invalid tokenized assets.

    Given: Tokenized asset with invalid base format,
    When: build_verified_mappings called,
    Then: Empty mappings returned.
    """
    svc = KrakenSymbolUpdaterService()
    fake_symbols = {
        "BAD": {
            "base": "BAD",
            "quote": "ZUSD",
            "ccxt_symbol": None,
            "asset_class": "tokenized_asset",
        },
    }
    monkeypatch.setattr(svc, "load_kraken_rest_symbols", AsyncMock(return_value=fake_symbols))
    monkeypatch.setattr(svc, "verify_websocket_symbols", AsyncMock(return_value=(set(), [])))
    mappings, success = await svc.build_verified_mappings()
    assert mappings == {}
    assert success is False


@pytest.mark.asyncio
async def test_fetch_symbols_requires_verification(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _fetch_symbols raises when verification fails.

    Given: build_verified_mappings returns success=False,
    When: _fetch_symbols called,
    Then: RuntimeError raised.
    """
    svc = KrakenSymbolUpdaterService()
    monkeypatch.setattr(svc, "build_verified_mappings", AsyncMock(return_value=({}, False)))
    with pytest.raises(RuntimeError):
        await svc._fetch_symbols(None)


@pytest.mark.asyncio
async def test_fetch_symbols_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _fetch_symbols returns mapping dictionaries on success.

    Given: Valid verified mappings,
    When: _fetch_symbols called,
    Then: List of mapping dicts returned.
    """
    svc = KrakenSymbolUpdaterService()
    fake_mapping = {
        "native_symbol": "BTC-USD",
        "kraken_websocket_symbol": "BTC/USD",
        "kraken_rest_symbol": "XXBTZUSD",
        "ccxt_symbol": "BTC/USD",
        "base_currency": "BTC",
        "quote_currency": "USD",
    }
    monkeypatch.setattr(
        svc,
        "build_verified_mappings",
        AsyncMock(return_value=({"BTC-USD": fake_mapping}, True)),
    )
    symbols = await svc._fetch_symbols(None)
    assert symbols[0]["kraken_rest_symbol"] == "XXBTZUSD"


@pytest.mark.asyncio
async def test_update_database_inserts_and_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _update_database handles inserts and updates.

    Given: Database with existing BTC catalog and alias rows,
    When: _update_database called with BTC and ETH,
    Then: BTC aliases updated, ETH catalog and aliases inserted.
    """
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_local = sessionmaker(bind=engine)
    now = datetime.now(UTC)
    with session_local() as session:
        session.add(
            SymbolCatalog(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            SymbolAlias(
                native_symbol="BTC-USD",
                exchange="kraken",
                channel="ws",
                exchange_symbol="OLD/WS",
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            SymbolAlias(
                native_symbol="BTC-USD",
                exchange="kraken",
                channel="rest",
                exchange_symbol="OLDREST",
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            SymbolAlias(
                native_symbol="BTC-USD",
                exchange="kraken",
                channel="ccxt",
                exchange_symbol="OLD/CCXT",
                created_at=now,
                updated_at=now,
            )
        )
        session.commit()

    class Repo:
        def get_session(self) -> Session:
            return session_local()

    svc = KrakenSymbolUpdaterService()
    svc.repository = Repo()
    symbols = [
        {
            "native_symbol": "BTC-USD",
            "kraken_websocket_symbol": "BTC/USD",
            "kraken_rest_symbol": "XXBTZUSD",
            "ccxt_symbol": "BTC/USD",
            "base_currency": "BTC",
            "quote_currency": "USD",
        },
        {
            "native_symbol": "ETH-USD",
            "kraken_websocket_symbol": "ETH/USD",
            "kraken_rest_symbol": "XETHZUSD",
            "ccxt_symbol": "ETH/USD",
            "base_currency": "ETH",
            "quote_currency": "USD",
        },
    ]
    await svc._update_database(symbols)
    with session_local() as session:
        btc_catalog = session.query(SymbolCatalog).filter_by(native_symbol="BTC-USD").one()
        eth_catalog = session.query(SymbolCatalog).filter_by(native_symbol="ETH-USD").one()
        assert btc_catalog.base == "BTC"
        assert eth_catalog.base == "ETH"
        btc_rest = (
            session.query(SymbolAlias)
            .filter_by(native_symbol="BTC-USD", exchange="kraken", channel="rest")
            .one()
        )
        eth_ws = (
            session.query(SymbolAlias)
            .filter_by(native_symbol="ETH-USD", exchange="kraken", channel="ws")
            .one()
        )
        assert btc_rest.exchange_symbol == "XXBTZUSD"
        assert eth_ws.exchange_symbol == "ETH/USD"


@pytest.mark.asyncio
async def test_update_database_handles_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _update_database propagates commit errors.

    Given: Session that raises RuntimeError on commit,
    When: _update_database called,
    Then: RuntimeError propagated.
    """
    svc = KrakenSymbolUpdaterService()

    class FaultySession:
        """Session stub that fails on commit."""

        def __enter__(self) -> "FaultySession":
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def execute(self, _stmt: Any) -> Any:
            """Return stub supporting both scalar_one_or_none and scalars().all()."""
            return SimpleNamespace(
                scalar_one_or_none=lambda: None,
                scalars=lambda: SimpleNamespace(all=lambda: []),
            )

        def add(self, _obj: Any) -> None:
            return None

        def commit(self) -> None:
            raise RuntimeError("fail")

    class Repo:
        """Repository stub returning FaultySession."""

        def get_session(self) -> FaultySession:
            return FaultySession()

    svc.repository = Repo()
    symbols = [
        {
            "native_symbol": "BTC-USD",
            "kraken_websocket_symbol": "BTC/USD",
            "kraken_rest_symbol": "XXBTZUSD",
            "ccxt_symbol": "BTC/USD",
            "base_currency": "BTC",
            "quote_currency": "USD",
        }
    ]
    with pytest.raises(RuntimeError):
        await svc._update_database(symbols)


class TestKrakenUpdaterClientCaching:
    """Test cases for Kraken client instance caching."""

    @pytest.fixture
    def mock_settings(self) -> MagicMock:
        """Create mock settings for caching tests."""
        settings = MagicMock()
        settings.db_url = "sqlite:///test.db"
        settings.zmq_broker_xsub = "tcp://localhost:5555"
        return settings

    @pytest.fixture
    def updater(self, mock_settings: MagicMock) -> KrakenSymbolUpdaterService:
        """Create updater instance for caching tests."""
        with patch(
            "snapper.config.settings.get_settings",
            return_value=mock_settings,
        ):
            return KrakenSymbolUpdaterService()

    def test_create_exchange_client_returns_cached(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify _create_exchange_client returns cached instance.

        Given: Kraken updater instance,
        When: _create_exchange_client called twice,
        Then: Same client instance returned.
        """
        client1 = updater._create_exchange_client()
        assert client1 is not None
        client2 = updater._create_exchange_client()
        assert client2 is client1


class TestLoadKrakenRestSymbolsFallback:
    """Test cases for Kraken REST symbol loading fallback scenarios."""

    @pytest.fixture
    def mock_settings(self) -> MagicMock:
        """Create mock settings for fallback tests."""
        settings = MagicMock()
        settings.db_url = "sqlite:///test.db"
        settings.zmq_broker_xsub = "tcp://localhost:5555"
        return settings

    @pytest.fixture
    def updater(self, mock_settings: MagicMock) -> KrakenSymbolUpdaterService:
        """Create updater instance for fallback tests."""
        with patch(
            "snapper.config.settings.get_settings",
            return_value=mock_settings,
        ):
            return KrakenSymbolUpdaterService()

    async def test_load_symbols_missing_info_fallback(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify load_kraken_rest_symbols uses fallback for missing info.

        Given: Market data without info block,
        When: load_kraken_rest_symbols called,
        Then: Base/quote extracted from top-level fields.
        """
        mock_markets = {
            "BTC/USD": {
                "id": "XXBTZUSD",
                "base": "BTC",
                "quote": "USD",
                "symbol": "BTC/USD",
            },
        }
        mock_client = AsyncMock()
        mock_ccxt = MagicMock()
        mock_ccxt.load_markets = MagicMock(return_value=mock_markets)
        mock_ccxt.publicGetAssetPairs = MagicMock(return_value={"error": [], "result": {}})
        mock_client._ccxt_client = mock_ccxt
        mock_client.get_ccxt_client = MagicMock(return_value=mock_ccxt)

        async def mock_to_thread(func: Any, *args: Any) -> Any:
            return func(*args)

        with (
            patch.object(updater, "_create_exchange_client", return_value=mock_client),
            patch("asyncio.to_thread", side_effect=mock_to_thread),
        ):
            result = await updater.load_kraken_rest_symbols()
        assert "XXBTZUSD" in result
        assert result["XXBTZUSD"]["base"] == "BTC"
        assert result["XXBTZUSD"]["quote"] == "USD"

    async def test_load_symbols_empty_info_base_quote(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify load_kraken_rest_symbols handles empty info fields.

        Given: Market with empty base/quote in info block,
        When: load_kraken_rest_symbols called,
        Then: Fallback to top-level base/quote used.
        """
        mock_markets = {
            "ETH/EUR": {
                "id": "XETHZEUR",
                "base": "ETH",
                "quote": "EUR",
                "symbol": "ETH/EUR",
                "info": {"base": "", "quote": ""},
            },
        }
        mock_client = AsyncMock()
        mock_ccxt = MagicMock()
        mock_ccxt.load_markets = MagicMock(return_value=mock_markets)
        mock_ccxt.publicGetAssetPairs = MagicMock(return_value={"error": [], "result": {}})
        mock_client._ccxt_client = mock_ccxt
        mock_client.get_ccxt_client = MagicMock(return_value=mock_ccxt)

        async def mock_to_thread(func: Any, *args: Any) -> Any:
            return func(*args)

        with (
            patch.object(updater, "_create_exchange_client", return_value=mock_client),
            patch("asyncio.to_thread", side_effect=mock_to_thread),
        ):
            result = await updater.load_kraken_rest_symbols()
        assert "XETHZEUR" in result
        assert result["XETHZEUR"]["base"] == "ETH"
        assert result["XETHZEUR"]["quote"] == "EUR"


class TestVerifyWebsocketSymbols:
    """Test cases for WebSocket symbol verification."""

    @pytest.fixture
    def mock_settings(self) -> MagicMock:
        """Create mock settings for WebSocket tests."""
        settings = MagicMock()
        settings.db_url = "sqlite:///test.db"
        settings.zmq_broker_xsub = "tcp://localhost:5555"
        return settings

    @pytest.fixture
    def updater(self, mock_settings: MagicMock) -> KrakenSymbolUpdaterService:
        """Create updater instance for WebSocket tests."""
        with patch(
            "snapper.config.settings.get_settings",
            return_value=mock_settings,
        ):
            return KrakenSymbolUpdaterService()

    @pytest.mark.asyncio
    async def test_verify_all_symbols_found(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify all WebSocket symbols found when available.

        Given: WebSocket returning both requested symbols,
        When: verify_websocket_symbols called,
        Then: All symbols returned in result set.
        """
        ws_symbols = {"BTC/USD", "ETH/EUR"}

        async def mock_subscribe(raw: bool = False) -> AsyncIterator[dict[str, Any]]:
            for symbol in ["BTC/USD", "ETH/EUR"]:
                yield {"symbol": symbol, "status": "online"}

        mock_client = AsyncMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = MagicMock()
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, _ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert verified == ws_symbols

    @pytest.mark.asyncio
    async def test_verify_timeout_partial_verification(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify partial verification on timeout.

        Given: Slow WebSocket that times out,
        When: verify_websocket_symbols called,
        Then: Partial result set returned.
        """
        ws_symbols = {"BTC/USD", "ETH/EUR", "XRP/USD"}

        async def slow_subscribe(raw: bool = False) -> AsyncIterator[dict[str, Any]]:
            yield {"symbol": "BTC/USD", "status": "online"}
            await asyncio.sleep(10)
            yield {"symbol": "ETH/EUR", "status": "online"}

        mock_client = AsyncMock()
        mock_client.subscribe_instruments = slow_subscribe
        mock_client.disconnect_websocket = MagicMock()
        with (
            patch.object(updater, "_create_exchange_client", return_value=mock_client),
            patch(
                "snapper.application.updaters.symbols.kraken.asyncio.timeout",
                side_effect=asyncio.TimeoutError,
            ),
        ):
            verified, _ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert isinstance(verified, set)

    @pytest.mark.asyncio
    async def test_verify_exception_fallback(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify fallback to all symbols on exception.

        Given: WebSocket connection that raises error,
        When: verify_websocket_symbols called,
        Then: All requested symbols returned as fallback with empty WS-only.
        """
        ws_symbols = {"BTC/USD"}

        async def error_subscribe(raw: bool = False) -> AsyncIterator[dict[str, Any]]:
            raise ConnectionError("WebSocket error")
            yield {}

        mock_client = AsyncMock()
        mock_client.subscribe_instruments = error_subscribe
        mock_client.disconnect_websocket = MagicMock()
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert verified == ws_symbols
        assert ws_only == []


class TestTokenizedAssetWarnings:
    """Test cases for tokenized asset warning scenarios."""

    @pytest.fixture
    def mock_settings(self) -> MagicMock:
        """Create mock settings for tokenized asset tests."""
        settings = MagicMock()
        settings.db_url = "sqlite:///test.db"
        settings.zmq_broker_xsub = "tcp://localhost:5555"
        return settings

    @pytest.fixture
    def updater(self, mock_settings: MagicMock) -> KrakenSymbolUpdaterService:
        """Create updater instance for tokenized asset tests."""
        with patch(
            "snapper.config.settings.get_settings",
            return_value=mock_settings,
        ):
            return KrakenSymbolUpdaterService()

    async def test_tokenized_asset_without_x_suffix(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify tokenized assets without x suffix are skipped.

        Given: Tokenized asset with invalid base format,
        When: build_verified_mappings called,
        Then: Asset excluded from mappings.
        """
        kraken_rest_symbols = {
            "INVALIDUSD": {
                "base": "INVALID",
                "quote": "ZUSD",
                "ccxt_symbol": None,
                "asset_class": "tokenized_asset",
            },
        }
        with (
            patch.object(updater, "load_kraken_rest_symbols", return_value=kraken_rest_symbols),
            patch.object(updater, "verify_websocket_symbols", return_value=(set(), [])),
        ):
            result, _changed = await updater.build_verified_mappings()
        assert len(result) == 0

    async def test_tokenized_asset_unexpected_quote(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify tokenized assets with unexpected quote are skipped.

        Given: Tokenized asset with non-USD quote currency,
        When: build_verified_mappings called,
        Then: Asset excluded from mappings.
        """
        kraken_rest_symbols = {
            "GSxBTC": {
                "base": "GSx",
                "quote": "ZBTC",
                "ccxt_symbol": None,
                "asset_class": "tokenized_asset",
            },
        }
        with (
            patch.object(updater, "load_kraken_rest_symbols", return_value=kraken_rest_symbols),
            patch.object(updater, "verify_websocket_symbols", return_value=(set(), [])),
        ):
            result, _changed = await updater.build_verified_mappings()
        assert len(result) == 0


class TestWebSocketDisconnectError:
    """Test cases for WebSocket disconnect error handling."""

    @pytest.fixture
    def mock_settings(self) -> MagicMock:
        """Create mock settings for disconnect error tests."""
        settings = MagicMock()
        settings.db_url = "sqlite:///test.db"
        settings.zmq_broker_xsub = "tcp://localhost:5555"
        return settings

    @pytest.fixture
    def updater(self, mock_settings: MagicMock) -> KrakenSymbolUpdaterService:
        """Create updater instance for disconnect error tests."""
        with patch(
            "snapper.config.settings.get_settings",
            return_value=mock_settings,
        ):
            return KrakenSymbolUpdaterService()

    async def test_disconnect_websocket_error_is_logged(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify disconnect WebSocket error is logged but not raised.

        Given: WebSocket disconnect that raises RuntimeError,
        When: verify_websocket_symbols called,
        Then: Error logged, symbols still returned.
        """

        async def mock_instrument_iterator() -> AsyncIterator[dict[str, Any]]:
            yield {"symbol": "BTC/USD"}

        async def disconnect_with_error() -> None:
            raise RuntimeError("Simulated disconnect error")

        mock_client = MagicMock()
        mock_client.subscribe_instruments = MagicMock(return_value=mock_instrument_iterator())
        mock_client.disconnect_websocket = disconnect_with_error
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols({"BTC/USD"})
        assert "BTC/USD" in verified
        assert ws_only == []


def _create_mock_settings() -> MagicMock:
    settings = MagicMock()
    settings.db_url = "sqlite:///test.db"
    settings.zmq_broker_xsub = "tcp://localhost:5555"
    return settings


class TestKrakenGetDefaultKwargs:
    """Test cases for get_default_kwargs class method."""

    def test_get_default_kwargs_returns_expected_values(self) -> None:
        """Verify get_default_kwargs returns correct defaults.

        Given: Valid app settings,
        When: get_default_kwargs called,
        Then: Expected update threshold and force values returned.
        """
        bootstrap = BootstrapSettingsLoader()
        settings = AppSettings(bootstrap, None)
        kwargs = KrakenSymbolUpdaterService.get_default_kwargs(settings)
        assert kwargs["update_threshold_hours"] == 6
        assert kwargs["force"] is False


class TestKrakenNormalizeCurrency:
    """Test cases for currency normalization logic."""

    @pytest.fixture
    def updater(self) -> KrakenSymbolUpdaterService:
        """Create updater instance for normalization tests."""
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            return KrakenSymbolUpdaterService()

    def test_normalize_fiat_currencies(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify fiat currencies with Z prefix are normalized.

        Given: Kraken updater instance,
        When: _normalize_currency called with Z-prefixed fiats,
        Then: Z prefix removed from currency codes.
        """
        assert updater._normalize_currency("ZUSD") == "USD"
        assert updater._normalize_currency("ZEUR") == "EUR"
        assert updater._normalize_currency("ZGBP") == "GBP"
        assert updater._normalize_currency("ZJPY") == "JPY"
        assert updater._normalize_currency("ZCAD") == "CAD"
        assert updater._normalize_currency("ZAUD") == "AUD"
        assert updater._normalize_currency("ZCHF") == "CHF"

    def test_normalize_single_x_prefix_crypto(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify crypto currencies with single X prefix are normalized.

        Given: Kraken updater instance,
        When: _normalize_currency called with X-prefixed cryptos,
        Then: X prefix removed from currency codes.
        """
        assert updater._normalize_currency("XETH") == "ETH"
        assert updater._normalize_currency("XLTC") == "LTC"
        assert updater._normalize_currency("XETC") == "ETC"
        assert updater._normalize_currency("XREP") == "REP"
        assert updater._normalize_currency("XZEC") == "ZEC"
        assert updater._normalize_currency("XMLN") == "MLN"

    def test_normalize_double_x_prefix_crypto(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify crypto currencies with double X prefix are normalized.

        Given: Kraken updater instance,
        When: _normalize_currency called with XX-prefixed cryptos,
        Then: Correct standard symbols returned (e.g., BTC, DOGE).
        """
        assert updater._normalize_currency("XXBT") == "BTC"
        assert updater._normalize_currency("XXDG") == "DOGE"
        assert updater._normalize_currency("XXRP") == "XRP"
        assert updater._normalize_currency("XXLM") == "XLM"
        assert updater._normalize_currency("XXMR") == "XMR"

    def test_normalize_unknown_currency_passthrough(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify unknown currencies pass through unchanged.

        Given: Kraken updater instance,
        When: _normalize_currency called with unknown codes,
        Then: Original currency codes returned.
        """
        assert updater._normalize_currency("BTC") == "BTC"
        assert updater._normalize_currency("SOL") == "SOL"
        assert updater._normalize_currency("AVAX") == "AVAX"
        assert updater._normalize_currency("UNKNOWN") == "UNKNOWN"


class TestKrakenVerifyWebsocketSymbols:
    """Test cases for WebSocket symbol verification scenarios."""

    @pytest.fixture
    def updater(self) -> KrakenSymbolUpdaterService:
        """Create updater instance for verification tests."""
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            return KrakenSymbolUpdaterService()

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_all_verified(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify all symbols verified via WebSocket.

        Given: WebSocket returning all requested symbols,
        When: verify_websocket_symbols called,
        Then: Complete set of symbols returned.
        """
        ws_symbols = {"BTC/USD", "ETH/EUR"}

        async def mock_subscribe(raw: bool = False) -> Any:
            yield {"symbol": "BTC/USD"}
            yield {"symbol": "ETH/EUR"}

        mock_client = MagicMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = MagicMock()
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert verified == ws_symbols
        assert ws_only == []

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_timeout_partial(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify partial symbols on WebSocket timeout.

        Given: Slow WebSocket with partial symbols before timeout,
        When: verify_websocket_symbols called,
        Then: Only verified symbols returned.
        """
        ws_symbols = {"BTC/USD", "ETH/EUR", "SOL/USD"}

        async def mock_subscribe(raw: bool = False) -> Any:
            yield {"symbol": "BTC/USD"}
            await asyncio.sleep(10)

        mock_client = MagicMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = MagicMock()
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert "BTC/USD" in verified
        assert len(verified) < len(ws_symbols)
        assert ws_only == []

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_cancelled(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify CancelledError is propagated.

        Given: WebSocket raising CancelledError,
        When: verify_websocket_symbols called,
        Then: CancelledError re-raised.
        """
        ws_symbols = {"BTC/USD", "ETH/EUR"}

        async def mock_subscribe(raw: bool = False) -> Any:
            yield {"symbol": "BTC/USD"}
            raise asyncio.CancelledError()

        mock_client = MagicMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = MagicMock()
        with (
            patch.object(updater, "_create_exchange_client", return_value=mock_client),
            pytest.raises(asyncio.CancelledError),
        ):
            await updater.verify_websocket_symbols(ws_symbols)

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_error_fallback(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify fallback to all symbols on connection error.

        Given: WebSocket raising ConnectionError,
        When: verify_websocket_symbols called,
        Then: All requested symbols returned as fallback.
        """
        ws_symbols = {"BTC/USD", "ETH/EUR"}

        async def mock_subscribe(raw: bool = False) -> Any:
            raise ConnectionError("WebSocket connection failed")
            yield

        mock_client = MagicMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = MagicMock()
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert verified == ws_symbols
        assert ws_only == []

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_async_disconnect(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify async disconnect is awaited properly.

        Given: WebSocket with async disconnect method,
        When: verify_websocket_symbols called,
        Then: Symbols verified and disconnect awaited.
        """
        ws_symbols = {"BTC/USD"}

        async def mock_subscribe(raw: bool = False) -> Any:
            yield {"symbol": "BTC/USD"}

        async def mock_async_disconnect() -> None:
            """Intentionally empty mock implementation."""
            pass

        mock_client = MagicMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = mock_async_disconnect
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert verified == ws_symbols
        assert ws_only == []

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_empty_set_branch(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify empty symbol set returns empty result.

        Given: Empty symbol set to verify,
        When: verify_websocket_symbols called,
        Then: Empty set returned immediately.
        """
        ws_symbols: set[str] = set()

        async def mock_subscribe(raw: bool = False) -> Any:
            if False:
                yield

        mock_client = MagicMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = MagicMock()
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert verified == set()
        assert ws_only == []


class TestKrakenBuildVerifiedMappings:
    """Test cases for building verified symbol mappings."""

    @pytest.fixture
    def updater(self) -> KrakenSymbolUpdaterService:
        """Create updater instance for mapping tests."""
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            return KrakenSymbolUpdaterService()

    @pytest.mark.asyncio
    async def test_build_verified_mappings_tokenized_asset_invalid_base(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify tokenized assets with invalid base are skipped.

        Given: Tokenized asset without x suffix in base,
        When: build_verified_mappings called,
        Then: Asset excluded from mappings.
        """
        mock_rest_symbols = {
            "INVALIDUSD": {
                "base": "INVALID",
                "quote": "ZUSD",
                "ccxt_symbol": None,
                "asset_class": "tokenized_asset",
            }
        }
        with (
            patch.object(updater, "load_kraken_rest_symbols", new_callable=AsyncMock) as mock_load,
            patch.object(
                updater, "verify_websocket_symbols", new_callable=AsyncMock
            ) as mock_verify,
        ):
            mock_load.return_value = mock_rest_symbols
            mock_verify.return_value = (set(), [])
            mappings, _success = await updater.build_verified_mappings()
            assert len(mappings) == 0

    @pytest.mark.asyncio
    async def test_build_verified_mappings_tokenized_asset_invalid_quote(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify tokenized assets with invalid quote are skipped.

        Given: Tokenized asset with non-USD quote currency,
        When: build_verified_mappings called,
        Then: Asset excluded from mappings.
        """
        mock_rest_symbols = {
            "AAPLxBTC": {
                "base": "AAPLx",
                "quote": "BTC",
                "ccxt_symbol": None,
                "asset_class": "tokenized_asset",
            }
        }
        with (
            patch.object(updater, "load_kraken_rest_symbols", new_callable=AsyncMock) as mock_load,
            patch.object(
                updater, "verify_websocket_symbols", new_callable=AsyncMock
            ) as mock_verify,
        ):
            mock_load.return_value = mock_rest_symbols
            mock_verify.return_value = (set(), [])
            mappings, _success = await updater.build_verified_mappings()
            assert len(mappings) == 0

    @pytest.mark.asyncio
    async def test_build_verified_mappings_low_verification_rate(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify low verification rate returns success=False.

        Given: Only half of symbols verified via WebSocket,
        When: build_verified_mappings called,
        Then: Mappings returned but success flag is False.
        """
        mock_rest_symbols = {
            "XXBTZUSD": {
                "base": "XXBT",
                "quote": "ZUSD",
                "ccxt_symbol": "BTC/USD",
                "asset_class": "currency",
            },
            "XETHZEUR": {
                "base": "XETH",
                "quote": "ZEUR",
                "ccxt_symbol": "ETH/EUR",
                "asset_class": "currency",
            },
        }
        with (
            patch.object(updater, "load_kraken_rest_symbols", new_callable=AsyncMock) as mock_load,
            patch.object(
                updater, "verify_websocket_symbols", new_callable=AsyncMock
            ) as mock_verify,
        ):
            mock_load.return_value = mock_rest_symbols
            mock_verify.return_value = ({"BTC/USD"}, [])
            mappings, success = await updater.build_verified_mappings()
            assert len(mappings) == 2
            assert success is False

    @pytest.mark.asyncio
    async def test_build_verified_mappings_exception(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify exception returns empty mappings with success=False.

        Given: load_kraken_rest_symbols raises RuntimeError,
        When: build_verified_mappings called,
        Then: Empty dict and False returned.
        """
        with patch.object(updater, "load_kraken_rest_symbols", new_callable=AsyncMock) as mock_load:
            mock_load.side_effect = RuntimeError("API error")
            mappings, success = await updater.build_verified_mappings()
            assert mappings == {}
            assert success is False

    @pytest.mark.asyncio
    async def test_build_verified_mappings_empty_base_or_quote_skipped(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify symbols with empty base or quote are skipped.

        Given: Symbols with empty base or quote fields,
        When: build_verified_mappings called,
        Then: Only valid symbols included in mappings.
        """
        mock_rest_symbols = {
            "EMPTY_BASE": {
                "base": "",
                "quote": "ZUSD",
                "ccxt_symbol": None,
                "asset_class": "currency",
            },
            "EMPTY_QUOTE": {
                "base": "XXBT",
                "quote": "",
                "ccxt_symbol": None,
                "asset_class": "currency",
            },
            "VALID_SYMBOL": {
                "base": "XXBT",
                "quote": "ZUSD",
                "ccxt_symbol": "BTC/USD",
                "asset_class": "currency",
            },
        }
        with (
            patch.object(updater, "load_kraken_rest_symbols", new_callable=AsyncMock) as mock_load,
            patch.object(
                updater, "verify_websocket_symbols", new_callable=AsyncMock
            ) as mock_verify,
        ):
            mock_load.return_value = mock_rest_symbols
            mock_verify.return_value = ({"BTC/USD"}, [])
            mappings, _success = await updater.build_verified_mappings()
            assert len(mappings) == 1
            assert "BTC-USD" in mappings


class TestKrakenFetchSymbols:
    """Test cases for symbol fetching functionality."""

    @pytest.fixture
    def updater(self) -> KrakenSymbolUpdaterService:
        """Create updater instance for fetch tests."""
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            return KrakenSymbolUpdaterService()

    @pytest.mark.asyncio
    async def test_fetch_symbols_verification_failed(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify _fetch_symbols raises on verification failure.

        Given: build_verified_mappings returns success=False,
        When: _fetch_symbols called,
        Then: RuntimeError raised with verification failed message.
        """
        mock_mappings: dict[str, dict[str, str]] = {}
        with patch.object(updater, "build_verified_mappings", new_callable=AsyncMock) as mock_build:
            mock_build.return_value = (mock_mappings, False)
            mock_client = MagicMock()
            with pytest.raises(RuntimeError, match="WebSocket verification failed"):
                await updater._fetch_symbols(mock_client)

    @pytest.mark.asyncio
    async def test_fetch_symbols_no_mappings(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify _fetch_symbols raises when no mappings generated.

        Given: build_verified_mappings returns empty dict,
        When: _fetch_symbols called,
        Then: RuntimeError raised with no mappings message.
        """
        with patch.object(updater, "build_verified_mappings", new_callable=AsyncMock) as mock_build:
            mock_build.return_value = ({}, True)
            mock_client = MagicMock()
            with pytest.raises(RuntimeError, match="No mappings generated"):
                await updater._fetch_symbols(mock_client)

    @pytest.mark.asyncio
    async def test_fetch_symbols_success(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify _fetch_symbols returns mapping dicts on success.

        Given: Valid verified mappings,
        When: _fetch_symbols called,
        Then: List of symbol dicts returned.
        """
        mock_mapping = {
            "native_symbol": "BTC-USD",
            "kraken_websocket_symbol": "BTC/USD",
            "kraken_rest_symbol": "XXBTZUSD",
            "ccxt_symbol": "BTC/USD",
            "base_currency": "BTC",
            "quote_currency": "USD",
        }
        with patch.object(updater, "build_verified_mappings", new_callable=AsyncMock) as mock_build:
            mock_build.return_value = ({"BTC-USD": mock_mapping}, True)
            mock_client = MagicMock()
            symbols = await updater._fetch_symbols(mock_client)
            assert len(symbols) == 1
            assert symbols[0]["native_symbol"] == "BTC-USD"
            assert symbols[0]["kraken_websocket_symbol"] == "BTC/USD"


class TestKrakenUpdateDatabase:
    """Test cases for database update operations."""

    @pytest.fixture
    def updater(self) -> KrakenSymbolUpdaterService:
        """Create updater instance for database tests."""
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            return KrakenSymbolUpdaterService()

    def test_update_database_requires_repository(self) -> None:
        """Verify repository is None before initialization.

        Given: New Kraken updater instance,
        When: repository checked before setup,
        Then: Repository is None.
        """
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            updater = KrakenSymbolUpdaterService()
        assert updater.repository is None


class TestKrakenLoadRestSymbolsEdgeCases:
    """Test cases for REST symbol loading edge cases."""

    @pytest.fixture
    def updater(self) -> KrakenSymbolUpdaterService:
        """Create updater instance for edge case tests."""
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            return KrakenSymbolUpdaterService()

    @pytest.mark.asyncio
    async def test_load_rest_symbols_empty_rest_id(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify symbols with empty REST ID are skipped.

        Given: Market with empty id field,
        When: load_kraken_rest_symbols called,
        Then: Symbol excluded from result.
        """
        mock_markets = {
            "BTC/USD": {
                "id": "",
                "base": "BTC",
                "quote": "USD",
            }
        }
        mock_client = AsyncMock()
        mock_ccxt = MagicMock()
        mock_ccxt.load_markets = MagicMock(return_value=mock_markets)
        mock_ccxt.publicGetAssetPairs = MagicMock(return_value={"result": {}})
        mock_client._ccxt_client = mock_ccxt
        mock_client.get_ccxt_client = MagicMock(return_value=mock_ccxt)

        async def mock_to_thread(func: Any, *args: Any) -> Any:
            return func(*args)

        with (
            patch.object(updater, "_create_exchange_client", return_value=mock_client),
            patch("asyncio.to_thread", side_effect=mock_to_thread),
        ):
            result = await updater.load_kraken_rest_symbols()
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_load_rest_symbols_missing_base_quote(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify symbols without base/quote are skipped.

        Given: Market with empty info block,
        When: load_kraken_rest_symbols called,
        Then: Symbol excluded from result.
        """
        mock_markets = {
            "BTC/USD": {
                "id": "XXBTZUSD",
                "info": {},
            }
        }
        mock_client = AsyncMock()
        mock_ccxt = MagicMock()
        mock_ccxt.load_markets = MagicMock(return_value=mock_markets)
        mock_ccxt.publicGetAssetPairs = MagicMock(return_value={"result": {}})
        mock_client._ccxt_client = mock_ccxt
        mock_client.get_ccxt_client = MagicMock(return_value=mock_ccxt)

        async def mock_to_thread(func: Any, *args: Any) -> Any:
            return func(*args)

        with (
            patch.object(updater, "_create_exchange_client", return_value=mock_client),
            patch("asyncio.to_thread", side_effect=mock_to_thread),
        ):
            result = await updater.load_kraken_rest_symbols()
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_load_rest_symbols_tokenized_invalid_pair_data(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify tokenized assets with invalid pair data are skipped.

        Given: Tokenized response with non-dict pair data,
        When: load_kraken_rest_symbols called,
        Then: Invalid pair skipped.
        """
        mock_markets: dict[str, Any] = {}
        tokenized_response = {
            "result": {
                "GSxUSD": "not_a_dict",
            }
        }
        mock_client = AsyncMock()
        mock_ccxt = MagicMock()
        mock_ccxt.load_markets = MagicMock(return_value=mock_markets)
        mock_ccxt.publicGetAssetPairs = MagicMock(return_value=tokenized_response)
        mock_client._ccxt_client = mock_ccxt
        mock_client.get_ccxt_client = MagicMock(return_value=mock_ccxt)

        async def mock_to_thread(func: Any, *args: Any) -> Any:
            return func(*args)

        with (
            patch.object(updater, "_create_exchange_client", return_value=mock_client),
            patch("asyncio.to_thread", side_effect=mock_to_thread),
        ):
            result = await updater.load_kraken_rest_symbols()
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_load_rest_symbols_tokenized_missing_base_quote(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify tokenized assets with missing base/quote are skipped.

        Given: Tokenized pair with empty base field,
        When: load_kraken_rest_symbols called,
        Then: Pair excluded from result.
        """
        mock_markets: dict[str, Any] = {}
        tokenized_response = {
            "result": {
                "GSxUSD": {
                    "base": "",
                    "quote": "ZUSD",
                }
            }
        }
        mock_client = AsyncMock()
        mock_ccxt = MagicMock()
        mock_ccxt.load_markets = MagicMock(return_value=mock_markets)
        mock_ccxt.publicGetAssetPairs = MagicMock(return_value=tokenized_response)
        mock_client._ccxt_client = mock_ccxt
        mock_client.get_ccxt_client = MagicMock(return_value=mock_ccxt)

        async def mock_to_thread(func: Any, *args: Any) -> Any:
            return func(*args)

        with (
            patch.object(updater, "_create_exchange_client", return_value=mock_client),
            patch("asyncio.to_thread", side_effect=mock_to_thread),
        ):
            result = await updater.load_kraken_rest_symbols()
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_load_rest_symbols_api_error(self, updater: KrakenSymbolUpdaterService) -> None:
        """Verify API error is propagated.

        Given: CCXT client raising ConnectionError,
        When: load_kraken_rest_symbols called,
        Then: ConnectionError raised.
        """
        mock_client = AsyncMock()
        mock_ccxt = MagicMock()
        mock_ccxt.load_markets = MagicMock(side_effect=ConnectionError("API error"))
        mock_client._ccxt_client = mock_ccxt
        mock_client.get_ccxt_client = MagicMock(return_value=mock_ccxt)

        async def mock_to_thread(func: Any, *args: Any) -> Any:
            return func(*args)

        with (
            patch.object(updater, "_create_exchange_client", return_value=mock_client),
            patch("asyncio.to_thread", side_effect=mock_to_thread),
            pytest.raises(ConnectionError, match="API error"),
        ):
            await updater.load_kraken_rest_symbols()

    @pytest.mark.asyncio
    async def test_load_rest_symbols_invalid_tokenized_result_type(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify invalid tokenized result type raises error.

        Given: Tokenized response with non-dict result,
        When: load_kraken_rest_symbols called,
        Then: RuntimeError raised.
        """
        mock_markets: dict[str, Any] = {}
        tokenized_response = {"result": "not_a_dict"}
        mock_client = AsyncMock()
        mock_ccxt = MagicMock()
        mock_ccxt.load_markets = MagicMock(return_value=mock_markets)
        mock_ccxt.publicGetAssetPairs = MagicMock(return_value=tokenized_response)
        mock_client._ccxt_client = mock_ccxt
        mock_client.get_ccxt_client = MagicMock(return_value=mock_ccxt)

        async def mock_to_thread(func: Any, *args: Any) -> Any:
            return func(*args)

        with (
            patch.object(updater, "_create_exchange_client", return_value=mock_client),
            patch("asyncio.to_thread", side_effect=mock_to_thread),
            pytest.raises(RuntimeError, match="Unexpected tokenized result type"),
        ):
            await updater.load_kraken_rest_symbols()


class TestKrakenVerifyWebsocketSymbolsBranches:
    """Test cases for WebSocket symbol verification branch coverage."""

    @pytest.fixture
    def updater(self) -> KrakenSymbolUpdaterService:
        """Create updater instance for branch tests."""
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            return KrakenSymbolUpdaterService()

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_non_string_symbol(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify non-string symbols are ignored.

        Given: WebSocket returning non-string symbol values,
        When: verify_websocket_symbols called,
        Then: Only valid string symbols verified.
        """
        ws_symbols = {"BTC/USD", "ETH/EUR"}

        async def mock_subscribe(raw: bool = False) -> Any:
            yield {"symbol": None}
            yield {"symbol": 12345}
            yield {"symbol": ["BTC/USD"]}
            yield {"symbol": "BTC/USD"}

        mock_client = MagicMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = MagicMock()
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert "BTC/USD" in verified

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_symbol_not_in_set(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify symbols not in request set are ignored.

        Given: WebSocket returning extra symbols,
        When: verify_websocket_symbols called,
        Then: Only requested symbols included in result.
        """
        ws_symbols = {"BTC/USD", "ETH/EUR"}

        async def mock_subscribe(raw: bool = False) -> Any:
            yield {"symbol": "SOL/USD"}
            yield {"symbol": "DOGE/USD"}
            yield {"symbol": "BTC/USD"}

        mock_client = MagicMock()
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = MagicMock()
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert "BTC/USD" in verified
        assert "SOL/USD" not in verified

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_disconnect_none(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify missing disconnect_websocket is handled.

        Given: Client without disconnect_websocket method,
        When: verify_websocket_symbols times out,
        Then: Partial results returned without error.
        """
        ws_symbols = {"BTC/USD", "ETH/EUR"}

        async def mock_subscribe(raw: bool = False) -> Any:
            yield {"symbol": "BTC/USD"}
            await asyncio.sleep(10)
            yield {"symbol": "ETH/EUR"}

        mock_client = MagicMock(spec=["subscribe_instruments"])
        mock_client.subscribe_instruments = mock_subscribe
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert "BTC/USD" in verified
        assert len(verified) < len(ws_symbols)

    @pytest.mark.asyncio
    async def test_verify_websocket_symbols_disconnect_not_callable(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify non-callable disconnect_websocket is handled.

        Given: Client with disconnect_websocket as string,
        When: verify_websocket_symbols times out,
        Then: Partial results returned without error.
        """
        ws_symbols = {"BTC/USD", "ETH/EUR"}

        async def mock_subscribe(raw: bool = False) -> Any:
            yield {"symbol": "BTC/USD"}
            await asyncio.sleep(10)
            yield {"symbol": "ETH/EUR"}

        mock_client = MagicMock(spec=["subscribe_instruments", "disconnect_websocket"])
        mock_client.subscribe_instruments = mock_subscribe
        mock_client.disconnect_websocket = "not_a_callable_string"
        with patch.object(updater, "_create_exchange_client", return_value=mock_client):
            verified, ws_only = await updater.verify_websocket_symbols(ws_symbols)
        assert "BTC/USD" in verified


class TestKrakenUpdateDatabaseBranches:
    """Test cases for database update branch coverage using SymbolCatalog and SymbolAlias."""

    @pytest.fixture
    def updater(self) -> KrakenSymbolUpdaterService:
        """Create updater instance for branch tests."""
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            return KrakenSymbolUpdaterService()

    @pytest.fixture
    def db_session_factory(self) -> sessionmaker:
        """Create in-memory SQLite session factory with schema."""
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        return sessionmaker(bind=engine)

    @pytest.mark.asyncio
    async def test_update_database_only_ws_alias_changed(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify database update when only WS alias changes.

        Given: Existing catalog and aliases with old WebSocket symbol,
        When: _update_database called with new WS symbol,
        Then: WS alias exchange_symbol updated.
        """
        now = datetime.now(UTC)
        with db_session_factory() as session:
            session.add(
                SymbolCatalog(
                    native_symbol="BTC-USD",
                    base="BTC",
                    quote="USD",
                    asset_type="crypto",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="ws",
                    exchange_symbol="OLD/WS",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="rest",
                    exchange_symbol="XXBTZUSD",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="ccxt",
                    exchange_symbol="BTC/USD",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()

        class Repo:
            """Repository stub returning real SQLite sessions."""

            def get_session(self) -> Session:
                """Return a new session."""
                return cast(Session, db_session_factory())

        updater.repository = Repo()
        symbols = [
            {
                "native_symbol": "BTC-USD",
                "kraken_websocket_symbol": "BTC/USD",
                "kraken_rest_symbol": "XXBTZUSD",
                "ccxt_symbol": "BTC/USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
            }
        ]
        await updater._update_database(symbols)
        with db_session_factory() as session:
            ws_alias = (
                session.query(SymbolAlias)
                .filter_by(native_symbol="BTC-USD", exchange="kraken", channel="ws")
                .one()
            )
            assert ws_alias.exchange_symbol == "BTC/USD"

    @pytest.mark.asyncio
    async def test_update_database_only_rest_alias_changed(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify database update when only REST alias changes.

        Given: Existing catalog and aliases with old REST symbol,
        When: _update_database called with new REST symbol,
        Then: REST alias exchange_symbol updated.
        """
        now = datetime.now(UTC)
        with db_session_factory() as session:
            session.add(
                SymbolCatalog(
                    native_symbol="BTC-USD",
                    base="BTC",
                    quote="USD",
                    asset_type="crypto",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="ws",
                    exchange_symbol="BTC/USD",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="rest",
                    exchange_symbol="OLDREST",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="ccxt",
                    exchange_symbol="BTC/USD",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()

        class Repo:
            """Repository stub returning real SQLite sessions."""

            def get_session(self) -> Session:
                """Return a new session."""
                return cast(Session, db_session_factory())

        updater.repository = Repo()
        symbols = [
            {
                "native_symbol": "BTC-USD",
                "kraken_websocket_symbol": "BTC/USD",
                "kraken_rest_symbol": "XXBTZUSD",
                "ccxt_symbol": "BTC/USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
            }
        ]
        await updater._update_database(symbols)
        with db_session_factory() as session:
            rest_alias = (
                session.query(SymbolAlias)
                .filter_by(native_symbol="BTC-USD", exchange="kraken", channel="rest")
                .one()
            )
            assert rest_alias.exchange_symbol == "XXBTZUSD"

    @pytest.mark.asyncio
    async def test_update_database_only_ccxt_alias_changed(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify database update when only CCXT alias changes.

        Given: Existing catalog and aliases with old CCXT symbol,
        When: _update_database called with new CCXT symbol,
        Then: CCXT alias exchange_symbol updated.
        """
        now = datetime.now(UTC)
        with db_session_factory() as session:
            session.add(
                SymbolCatalog(
                    native_symbol="BTC-USD",
                    base="BTC",
                    quote="USD",
                    asset_type="crypto",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="ws",
                    exchange_symbol="BTC/USD",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="rest",
                    exchange_symbol="XXBTZUSD",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="ccxt",
                    exchange_symbol="OLD/CCXT",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()

        class Repo:
            """Repository stub returning real SQLite sessions."""

            def get_session(self) -> Session:
                """Return a new session."""
                return cast(Session, db_session_factory())

        updater.repository = Repo()
        symbols = [
            {
                "native_symbol": "BTC-USD",
                "kraken_websocket_symbol": "BTC/USD",
                "kraken_rest_symbol": "XXBTZUSD",
                "ccxt_symbol": "BTC/USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
            }
        ]
        await updater._update_database(symbols)
        with db_session_factory() as session:
            ccxt_alias = (
                session.query(SymbolAlias)
                .filter_by(native_symbol="BTC-USD", exchange="kraken", channel="ccxt")
                .one()
            )
            assert ccxt_alias.exchange_symbol == "BTC/USD"

    @pytest.mark.asyncio
    async def test_update_database_no_changes_needed(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify no alias updated_at change when values are identical.

        Given: Existing catalog and aliases with identical values,
        When: _update_database called,
        Then: Alias updated_at timestamps unchanged.
        """
        original_updated_at = datetime(2020, 1, 1, tzinfo=UTC)
        with db_session_factory() as session:
            session.add(
                SymbolCatalog(
                    native_symbol="BTC-USD",
                    base="BTC",
                    quote="USD",
                    asset_type="crypto",
                    created_at=original_updated_at,
                    updated_at=original_updated_at,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="ws",
                    exchange_symbol="BTC/USD",
                    created_at=original_updated_at,
                    updated_at=original_updated_at,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="rest",
                    exchange_symbol="XXBTZUSD",
                    created_at=original_updated_at,
                    updated_at=original_updated_at,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTC-USD",
                    exchange="kraken",
                    channel="ccxt",
                    exchange_symbol="BTC/USD",
                    created_at=original_updated_at,
                    updated_at=original_updated_at,
                )
            )
            session.commit()

        class Repo:
            """Repository stub returning real SQLite sessions."""

            def get_session(self) -> Session:
                """Return a new session."""
                return cast(Session, db_session_factory())

        updater.repository = Repo()
        symbols = [
            {
                "native_symbol": "BTC-USD",
                "kraken_websocket_symbol": "BTC/USD",
                "kraken_rest_symbol": "XXBTZUSD",
                "ccxt_symbol": "BTC/USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
            }
        ]
        await updater._update_database(symbols)
        with db_session_factory() as session:
            ws_alias = (
                session.query(SymbolAlias)
                .filter_by(native_symbol="BTC-USD", exchange="kraken", channel="ws")
                .one()
            )
            assert ws_alias.updated_at == original_updated_at

    @pytest.mark.asyncio
    async def test_update_database_skips_empty_ccxt_symbol(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Skip alias creation when exchange_symbol is empty.

        Given: Symbol data with empty ccxt_symbol,
        When: _update_database is called,
        Then: Only ws and rest aliases are created, ccxt alias is skipped.
        """

        class Repo:
            """Repository stub returning real SQLite sessions."""

            def get_session(self) -> Session:
                """Return a new session."""
                return cast(Session, db_session_factory())

        updater.repository = Repo()
        symbols = [
            {
                "native_symbol": "BTC-USD",
                "kraken_websocket_symbol": "BTC/USD",
                "kraken_rest_symbol": "XXBTZUSD",
                "ccxt_symbol": "",
                "base_currency": "BTC",
                "quote_currency": "USD",
            }
        ]
        await updater._update_database(symbols)
        with db_session_factory() as session:
            all_aliases = session.query(SymbolAlias).filter_by(native_symbol="BTC-USD").all()
            channels = {a.channel for a in all_aliases}
            assert channels == {"ws", "rest"}

    @pytest.mark.asyncio
    async def test_update_database_creates_capability_rows(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify _update_database creates SymbolExchangeCapability rows.

        Given: Empty database,
        When: _update_database called with two symbols,
        Then: Capability row created per symbol with exchange=kraken,
              can_market_data=True, can_trade=True, source=kraken_updater.
        """

        class Repo:
            """Repository stub returning real SQLite sessions."""

            def get_session(self) -> Session:
                """Return a new session."""
                return cast(Session, db_session_factory())

        updater.repository = Repo()
        symbols = [
            {
                "native_symbol": "BTC-USD",
                "kraken_websocket_symbol": "BTC/USD",
                "kraken_rest_symbol": "XXBTZUSD",
                "ccxt_symbol": "BTC/USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
            },
            {
                "native_symbol": "ETH-USD",
                "kraken_websocket_symbol": "ETH/USD",
                "kraken_rest_symbol": "XETHZUSD",
                "ccxt_symbol": "ETH/USD",
                "base_currency": "ETH",
                "quote_currency": "USD",
            },
        ]
        await updater._update_database(symbols)
        with db_session_factory() as session:
            caps = session.query(SymbolExchangeCapability).all()
            assert len(caps) == 2
            cap_map = {c.native_symbol: c for c in caps}
            for native_symbol in ("BTC-USD", "ETH-USD"):
                cap = cap_map[native_symbol]
                assert cap.exchange == "kraken"
                assert cap.can_market_data is True
                assert cap.can_trade is True
                assert cap.source == "kraken_updater"
                assert cap.reason is None

    @pytest.mark.asyncio
    async def test_update_database_ws_only_symbol(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify WS-only symbols get one alias and can_trade=False capability.

        Given: Empty database,
        When: _update_database called with a WS-only symbol,
        Then: One WS alias created, capability has can_trade=False and reason set.
        """

        class Repo:
            """Repository stub returning real SQLite sessions."""

            def get_session(self) -> Session:
                """Return a new session."""
                return cast(Session, db_session_factory())

        updater.repository = Repo()
        symbols: list[dict[str, Any]] = [
            {
                "native_symbol": "BTGOX-USD",
                "kraken_websocket_symbol": "BTGOx/USD",
                "kraken_rest_symbol": "",
                "ccxt_symbol": "",
                "base_currency": "BTGOx",
                "quote_currency": "USD",
                "asset_class": "crypto",
                "ws_only": "true",
            }
        ]
        await updater._update_database(symbols)
        with db_session_factory() as session:
            aliases = session.query(SymbolAlias).filter_by(native_symbol="BTGOX-USD").all()
            assert len(aliases) == 1
            assert aliases[0].channel == "ws"
            assert aliases[0].exchange_symbol == "BTGOx/USD"

            cap = session.query(SymbolExchangeCapability).filter_by(native_symbol="BTGOX-USD").one()
            assert cap.exchange == "kraken"
            assert cap.can_market_data is True
            assert cap.can_trade is False
            assert cap.source == "kraken_updater"
            assert cap.reason == "WS-only, not in REST markets"

    @pytest.mark.asyncio
    async def test_update_database_mixed_rest_and_ws_only(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify mixed REST and WS-only symbols are persisted correctly.

        Given: Empty database,
        When: _update_database called with one REST and one WS-only symbol,
        Then: REST symbol gets 3 aliases + can_trade=True,
              WS-only gets 1 alias + can_trade=False.
        """

        class Repo:
            """Repository stub returning real SQLite sessions."""

            def get_session(self) -> Session:
                """Return a new session."""
                return cast(Session, db_session_factory())

        updater.repository = Repo()
        symbols: list[dict[str, Any]] = [
            {
                "native_symbol": "BTC-USD",
                "kraken_websocket_symbol": "BTC/USD",
                "kraken_rest_symbol": "XXBTZUSD",
                "ccxt_symbol": "BTC/USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
            },
            {
                "native_symbol": "BTGOX-USD",
                "kraken_websocket_symbol": "BTGOx/USD",
                "kraken_rest_symbol": "",
                "ccxt_symbol": "",
                "base_currency": "BTGOx",
                "quote_currency": "USD",
                "asset_class": "crypto",
                "ws_only": "true",
            },
        ]
        await updater._update_database(symbols)
        with db_session_factory() as session:
            rest_aliases = session.query(SymbolAlias).filter_by(native_symbol="BTC-USD").all()
            assert len(rest_aliases) == 3
            rest_cap = (
                session.query(SymbolExchangeCapability).filter_by(native_symbol="BTC-USD").one()
            )
            assert rest_cap.can_trade is True

            ws_aliases = session.query(SymbolAlias).filter_by(native_symbol="BTGOX-USD").all()
            assert len(ws_aliases) == 1
            ws_cap = (
                session.query(SymbolExchangeCapability).filter_by(native_symbol="BTGOX-USD").one()
            )
            assert ws_cap.can_trade is False
            assert ws_cap.reason == "WS-only, not in REST markets"


class TestKrakenIsTokenizedBase:
    """Test cases for _is_tokenized_base static method."""

    def test_tokenized_base_mixed_case_trailing_x(self) -> None:
        """Verify mixed-case base ending in lowercase x is detected as tokenized.

        Given: Base currency ``NVDAx`` (xStock naming convention),
        When: _is_tokenized_base called,
        Then: Returns True.
        """
        assert KrakenSymbolUpdaterService._is_tokenized_base("NVDAx") is True

    def test_tokenized_base_btgox(self) -> None:
        """Verify BTGOx is detected as tokenized asset.

        Given: Base currency ``BTGOx``,
        When: _is_tokenized_base called,
        Then: Returns True.
        """
        assert KrakenSymbolUpdaterService._is_tokenized_base("BTGOx") is True

    def test_standard_crypto_all_uppercase(self) -> None:
        """Verify standard all-uppercase crypto is not tokenized.

        Given: Base currency ``BTC`` (standard crypto),
        When: _is_tokenized_base called,
        Then: Returns False.
        """
        assert KrakenSymbolUpdaterService._is_tokenized_base("BTC") is False

    def test_all_uppercase_ending_x(self) -> None:
        """Verify all-uppercase ticker ending in X is not tokenized.

        Given: Base currency ``HEX`` (all uppercase, ends in X),
        When: _is_tokenized_base called,
        Then: Returns False (uppercase X, not lowercase x).
        """
        assert KrakenSymbolUpdaterService._is_tokenized_base("HEX") is False

    def test_single_char_x(self) -> None:
        """Verify single-character ``x`` is not tokenized.

        Given: Single-character base ``x``,
        When: _is_tokenized_base called,
        Then: Returns False (too short).
        """
        assert KrakenSymbolUpdaterService._is_tokenized_base("x") is False


class TestKrakenBuildWsOnlyMapping:
    """Test cases for _build_ws_only_mapping static method."""

    def test_build_ws_only_mapping_tokenized_asset(self) -> None:
        """Verify WS-only tokenized asset gets asset_class=tokenized_asset.

        Given: WS-only instrument with mixed-case base ending in x,
        When: _build_ws_only_mapping called,
        Then: asset_class is tokenized_asset (not crypto).
        """
        instrument = {"symbol": "BTGOx/USD", "base": "BTGOx", "quote": "USD"}
        mapping = KrakenSymbolUpdaterService._build_ws_only_mapping(instrument)
        assert mapping["native_symbol"] == "BTGOX-USD"
        assert mapping["kraken_websocket_symbol"] == "BTGOx/USD"
        assert mapping["kraken_rest_symbol"] == ""
        assert mapping["ccxt_symbol"] == ""
        assert mapping["base_currency"] == "BTGOx"
        assert mapping["quote_currency"] == "USD"
        assert mapping["asset_class"] == "tokenized_asset"
        assert mapping["ws_only"] == "true"

    def test_build_ws_only_mapping_standard_crypto(self) -> None:
        """Verify WS-only standard crypto keeps asset_class=crypto.

        Given: WS-only instrument with all-uppercase base,
        When: _build_ws_only_mapping called,
        Then: asset_class remains crypto.
        """
        instrument = {"symbol": "SPV/EUR", "base": "SPV", "quote": "EUR"}
        mapping = KrakenSymbolUpdaterService._build_ws_only_mapping(instrument)
        assert mapping["native_symbol"] == "SPV-EUR"
        assert mapping["kraken_websocket_symbol"] == "SPV/EUR"
        assert mapping["asset_class"] == "crypto"


class TestKrakenCollectVerifiedSymbolsWsOnly:
    """Test cases for WS-only instrument discovery in _collect_verified_symbols."""

    @pytest.fixture
    def updater(self) -> KrakenSymbolUpdaterService:
        """Create updater instance for WS-only collection tests."""
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            return KrakenSymbolUpdaterService()

    @pytest.mark.asyncio
    async def test_collect_verified_symbols_discovers_ws_only(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify WS-only instruments are collected alongside verified symbols.

        Given: WS feed with both REST-known and WS-only instruments,
        When: _collect_verified_symbols called,
        Then: REST symbols in verified set, WS-only instruments in ws_only list.
        """
        ws_symbols = {"BTC/USD"}

        async def mock_subscribe(raw: bool = False) -> Any:
            yield {"symbol": "BTC/USD", "base": "BTC", "quote": "USD"}
            yield {"symbol": "BTGOx/USD", "base": "BTGOx", "quote": "USD"}

        mock_client = MagicMock()
        mock_client.subscribe_instruments = mock_subscribe
        verified, ws_only = await updater._collect_verified_symbols(mock_client, ws_symbols, 5.0)
        assert verified == {"BTC/USD"}
        assert len(ws_only) == 1
        assert ws_only[0] == {"symbol": "BTGOx/USD", "base": "BTGOx", "quote": "USD"}

    @pytest.mark.asyncio
    async def test_collect_verified_symbols_ws_only_missing_base_quote(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify WS-only instruments without base/quote are skipped.

        Given: WS feed with unknown symbol missing base or quote fields,
        When: _collect_verified_symbols called,
        Then: Instrument not added to ws_only list.
        """
        ws_symbols = {"BTC/USD"}

        async def mock_subscribe(raw: bool = False) -> Any:
            yield {"symbol": "BTC/USD", "base": "BTC", "quote": "USD"}
            yield {"symbol": "UNKNOWN/X", "base": "", "quote": "USD"}
            yield {"symbol": "ANOTHER/Y", "base": "FOO", "quote": ""}
            yield {"symbol": "NOQUOTE/Z"}

        mock_client = MagicMock()
        mock_client.subscribe_instruments = mock_subscribe
        verified, ws_only = await updater._collect_verified_symbols(mock_client, ws_symbols, 5.0)
        assert verified == {"BTC/USD"}
        assert ws_only == []

    @pytest.mark.asyncio
    async def test_collect_verified_symbols_multiple_ws_only(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify multiple WS-only instruments are collected.

        Given: WS feed with several WS-only instruments,
        When: _collect_verified_symbols called,
        Then: All valid WS-only instruments in ws_only list.
        """
        ws_symbols = {"BTC/USD"}

        async def mock_subscribe(raw: bool = False) -> Any:
            yield {"symbol": "BTC/USD", "base": "BTC", "quote": "USD"}
            yield {"symbol": "FOO/USD", "base": "FOO", "quote": "USD"}
            yield {"symbol": "BAR/EUR", "base": "BAR", "quote": "EUR"}

        mock_client = MagicMock()
        mock_client.subscribe_instruments = mock_subscribe
        verified, ws_only = await updater._collect_verified_symbols(mock_client, ws_symbols, 5.0)
        assert verified == {"BTC/USD"}
        assert len(ws_only) == 2
        ws_only_symbols = {w["symbol"] for w in ws_only}
        assert ws_only_symbols == {"FOO/USD", "BAR/EUR"}


class TestKrakenBuildVerifiedMappingsWsOnly:
    """Test cases for WS-only symbol integration in build_verified_mappings."""

    @pytest.fixture
    def updater(self) -> KrakenSymbolUpdaterService:
        """Create updater instance for WS-only mapping tests."""
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            return KrakenSymbolUpdaterService()

    @pytest.mark.asyncio
    async def test_build_verified_mappings_includes_ws_only(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify WS-only symbols are included in final mappings.

        Given: REST returns BTC/USD, WS returns BTC/USD + BTGOx/USD,
        When: build_verified_mappings called,
        Then: Mappings contain both BTC-USD (REST) and BTGOX-USD (WS-only).
        """
        mock_rest_symbols = {
            "XXBTZUSD": {
                "base": "XXBT",
                "quote": "ZUSD",
                "ccxt_symbol": "BTC/USD",
                "asset_class": "currency",
            }
        }
        ws_only_instruments = [
            {"symbol": "BTGOx/USD", "base": "BTGOx", "quote": "USD"},
        ]
        with (
            patch.object(updater, "load_kraken_rest_symbols", new_callable=AsyncMock) as mock_load,
            patch.object(
                updater, "verify_websocket_symbols", new_callable=AsyncMock
            ) as mock_verify,
        ):
            mock_load.return_value = mock_rest_symbols
            mock_verify.return_value = ({"BTC/USD"}, ws_only_instruments)
            mappings, success = await updater.build_verified_mappings()

        assert success is True
        assert "BTC-USD" in mappings
        assert "BTGOX-USD" in mappings
        assert mappings["BTGOX-USD"]["ws_only"] == "true"
        assert mappings["BTGOX-USD"]["kraken_websocket_symbol"] == "BTGOx/USD"
        assert mappings["BTGOX-USD"]["kraken_rest_symbol"] == ""

    @pytest.mark.asyncio
    async def test_build_verified_mappings_ws_only_skips_duplicates(
        self, updater: KrakenSymbolUpdaterService
    ) -> None:
        """Verify WS-only symbols that duplicate REST symbols are skipped.

        Given: REST and WS-only both produce same native_symbol,
        When: build_verified_mappings called,
        Then: REST mapping kept, WS-only duplicate skipped.
        """
        mock_rest_symbols = {
            "XXBTZUSD": {
                "base": "XXBT",
                "quote": "ZUSD",
                "ccxt_symbol": "BTC/USD",
                "asset_class": "currency",
            }
        }
        ws_only_instruments = [
            {"symbol": "BTC/USD", "base": "BTC", "quote": "USD"},
        ]
        with (
            patch.object(updater, "load_kraken_rest_symbols", new_callable=AsyncMock) as mock_load,
            patch.object(
                updater, "verify_websocket_symbols", new_callable=AsyncMock
            ) as mock_verify,
        ):
            mock_load.return_value = mock_rest_symbols
            mock_verify.return_value = ({"BTC/USD"}, ws_only_instruments)
            mappings, success = await updater.build_verified_mappings()

        assert success is True
        assert "BTC-USD" in mappings
        assert mappings["BTC-USD"].get("ws_only") != "true"


class TestKrakenPersistHelpers:
    """Test cases for _persist_ws_only_symbol and _persist_rest_symbol helpers."""

    @pytest.fixture
    def updater(self) -> KrakenSymbolUpdaterService:
        """Create updater instance for persist helper tests."""
        with patch("snapper.config.settings.get_settings", return_value=_create_mock_settings()):
            return KrakenSymbolUpdaterService()

    @pytest.fixture
    def db_session_factory(self) -> sessionmaker:
        """Create in-memory SQLite session factory with schema."""
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        return sessionmaker(bind=engine)

    def test_persist_ws_only_symbol_creates_alias_and_capability(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify _persist_ws_only_symbol creates WS alias and market-data capability.

        Given: Empty database with catalog entry,
        When: _persist_ws_only_symbol called,
        Then: One WS alias + capability with can_trade=False created.
        """
        now = datetime.now(UTC)
        with db_session_factory() as session:
            session.add(
                SymbolCatalog(
                    native_symbol="BTGOX-USD",
                    base="BTGOx",
                    quote="USD",
                    asset_type="crypto",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()

        with db_session_factory() as session:
            symbol_data: dict[str, Any] = {
                "native_symbol": "BTGOX-USD",
                "kraken_websocket_symbol": "BTGOx/USD",
            }
            created, updated = updater._persist_ws_only_symbol(session, symbol_data, now)
            session.commit()
            assert created == 1
            assert updated == 0

        with db_session_factory() as session:
            alias = (
                session.query(SymbolAlias).filter_by(native_symbol="BTGOX-USD", channel="ws").one()
            )
            assert alias.exchange_symbol == "BTGOx/USD"
            cap = session.query(SymbolExchangeCapability).filter_by(native_symbol="BTGOX-USD").one()
            assert cap.can_trade is False
            assert cap.can_market_data is True
            assert cap.reason == "WS-only, not in REST markets"

    def test_persist_ws_only_symbol_empty_ws_symbol(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify _persist_ws_only_symbol skips alias when ws symbol is empty.

        Given: Symbol data with empty kraken_websocket_symbol,
        When: _persist_ws_only_symbol called,
        Then: No alias created, capability still created.
        """
        now = datetime.now(UTC)
        with db_session_factory() as session:
            session.add(
                SymbolCatalog(
                    native_symbol="FOO-USD",
                    base="FOO",
                    quote="USD",
                    asset_type="crypto",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()

        with db_session_factory() as session:
            symbol_data: dict[str, Any] = {
                "native_symbol": "FOO-USD",
                "kraken_websocket_symbol": "",
            }
            created, updated = updater._persist_ws_only_symbol(session, symbol_data, now)
            session.commit()
            assert created == 0
            assert updated == 0

        with db_session_factory() as session:
            aliases = session.query(SymbolAlias).filter_by(native_symbol="FOO-USD").all()
            assert len(aliases) == 0
            cap = session.query(SymbolExchangeCapability).filter_by(native_symbol="FOO-USD").one()
            assert cap.can_trade is False

    def test_persist_ws_only_symbol_updates_existing_alias(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify _persist_ws_only_symbol updates alias when symbol changes.

        Given: Existing WS alias with old exchange_symbol,
        When: _persist_ws_only_symbol called with new symbol,
        Then: Alias updated, updated count incremented.
        """
        now = datetime.now(UTC)
        with db_session_factory() as session:
            session.add(
                SymbolCatalog(
                    native_symbol="BTGOX-USD",
                    base="BTGOX",
                    quote="USD",
                    asset_type="crypto",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTGOX-USD",
                    exchange="kraken",
                    channel="ws",
                    exchange_symbol="OLD/USD",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()

        with db_session_factory() as session:
            symbol_data: dict[str, Any] = {
                "native_symbol": "BTGOX-USD",
                "kraken_websocket_symbol": "BTGOx/USD",
            }
            created, updated = updater._persist_ws_only_symbol(session, symbol_data, now)
            session.commit()
            assert created == 0
            assert updated == 1

        with db_session_factory() as session:
            alias = (
                session.query(SymbolAlias).filter_by(native_symbol="BTGOX-USD", channel="ws").one()
            )
            assert alias.exchange_symbol == "BTGOx/USD"

    def test_persist_ws_only_symbol_unchanged_alias(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify _persist_ws_only_symbol returns zero counts when alias unchanged.

        Given: Existing WS alias with identical exchange_symbol,
        When: _persist_ws_only_symbol called with same symbol,
        Then: No created or updated counts.
        """
        now = datetime.now(UTC)
        with db_session_factory() as session:
            session.add(
                SymbolCatalog(
                    native_symbol="BTGOX-USD",
                    base="BTGOX",
                    quote="USD",
                    asset_type="crypto",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                SymbolAlias(
                    native_symbol="BTGOX-USD",
                    exchange="kraken",
                    channel="ws",
                    exchange_symbol="BTGOx/USD",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()

        with db_session_factory() as session:
            symbol_data: dict[str, Any] = {
                "native_symbol": "BTGOX-USD",
                "kraken_websocket_symbol": "BTGOx/USD",
            }
            created, updated = updater._persist_ws_only_symbol(session, symbol_data, now)
            session.commit()
            assert created == 0
            assert updated == 0

    def test_persist_rest_symbol_creates_three_aliases_and_capability(
        self,
        updater: KrakenSymbolUpdaterService,
        db_session_factory: sessionmaker,
    ) -> None:
        """Verify _persist_rest_symbol creates ws/rest/ccxt aliases and full capability.

        Given: Empty database with catalog entry,
        When: _persist_rest_symbol called,
        Then: Three aliases + capability with can_trade=True created.
        """
        now = datetime.now(UTC)
        with db_session_factory() as session:
            session.add(
                SymbolCatalog(
                    native_symbol="BTC-USD",
                    base="BTC",
                    quote="USD",
                    asset_type="crypto",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()

        with db_session_factory() as session:
            symbol_data: dict[str, Any] = {
                "native_symbol": "BTC-USD",
                "kraken_websocket_symbol": "BTC/USD",
                "kraken_rest_symbol": "XXBTZUSD",
                "ccxt_symbol": "BTC/USD",
            }
            created, updated = updater._persist_rest_symbol(session, symbol_data, now)
            session.commit()
            assert created == 3
            assert updated == 0

        with db_session_factory() as session:
            aliases = session.query(SymbolAlias).filter_by(native_symbol="BTC-USD").all()
            channels = {a.channel for a in aliases}
            assert channels == {"ws", "rest", "ccxt"}
            cap = session.query(SymbolExchangeCapability).filter_by(native_symbol="BTC-USD").one()
            assert cap.can_trade is True
            assert cap.can_market_data is True
            assert cap.reason is None
