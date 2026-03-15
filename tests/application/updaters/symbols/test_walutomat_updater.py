"""Tests for Walutomat symbol updater service."""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from snapper.application.updaters.symbols.walutomat import WalutomatSymbolUpdaterService
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolCatalog
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient


class ExposedWalutomatSymbolUpdater(WalutomatSymbolUpdaterService):
    """Exposed updater for testing protected methods."""

    async def update_database_public(self, symbols: list[dict[str, Any]]) -> None:
        """Expose _update_database for testing."""
        await super()._update_database(symbols)

    def create_exchange_client_public(self) -> WalutomatExchangeClient:
        """Expose _create_exchange_client for testing."""
        return super()._create_exchange_client()

    def get_setting_key_public(self) -> str:
        """Expose _get_setting_key for testing."""
        return super()._get_setting_key()


@pytest.fixture()
def updater_with_repository(
    tmp_path: Path,
) -> Iterator[tuple[ExposedWalutomatSymbolUpdater, DatabaseRepository]]:
    """Provide Walutomat updater instance with test database."""
    db_path = tmp_path / "walutomat_symbols.sqlite"
    repository = DatabaseRepository(f"sqlite:///{db_path}")
    repository.create_all()
    updater = ExposedWalutomatSymbolUpdater(update_threshold_hours=1, force=True)
    updater.repository = repository
    yield updater, repository
    repository.engine.dispose()


@pytest.mark.asyncio()
async def test_update_database_creates_and_updates_mappings(
    updater_with_repository: tuple[ExposedWalutomatSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify update_database creates new and updates existing catalog/alias rows.

    Given: Existing EUR-PLN catalog and alias rows with old exchange_symbol,
    When: Update database called with updated EUR and new USD,
    Then: EUR ws alias updated with new symbol, USD catalog and aliases inserted.
    """
    updater, repository = updater_with_repository
    original_timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolCatalog(
                native_symbol="EUR-PLN",
                base="EUR",
                quote="PLN",
                asset_type="forex",
                created_at=original_timestamp,
                timestamp=original_timestamp,
            )
        )
        session.add(
            SymbolAlias(
                native_symbol="EUR-PLN",
                exchange="walutomat",
                channel="ws",
                exchange_symbol="EUR_PLN_OLD",
                created_at=original_timestamp,
                timestamp=original_timestamp,
            )
        )
        session.add(
            SymbolAlias(
                native_symbol="EUR-PLN",
                exchange="walutomat",
                channel="rest",
                exchange_symbol="EURNOT",
                created_at=original_timestamp,
                timestamp=original_timestamp,
            )
        )
        session.commit()
    symbols: list[dict[str, Any]] = [
        {
            "symbol": "EUR_PLN",
            "walutomat_rest_symbol": "EURPLN",
            "native_symbol": "EUR-PLN",
            "base": "EUR",
            "quote": "PLN",
        },
        {
            "symbol": "USD_PLN",
            "walutomat_rest_symbol": "USDPLN",
            "native_symbol": "USD-PLN",
            "base": "USD",
            "quote": "PLN",
        },
    ]
    await updater.update_database_public(symbols)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        eur_catalog = session.execute(
            select(SymbolCatalog).where(SymbolCatalog.native_symbol == "EUR-PLN")
        ).scalar_one()
        usd_catalog = session.execute(
            select(SymbolCatalog).where(SymbolCatalog.native_symbol == "USD-PLN")
        ).scalar_one()
        eur_ws_alias = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == "EUR-PLN",
                SymbolAlias.exchange == "walutomat",
                SymbolAlias.channel == "ws",
                SymbolAlias.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
        eur_rest_alias = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == "EUR-PLN",
                SymbolAlias.exchange == "walutomat",
                SymbolAlias.channel == "rest",
                SymbolAlias.known_to == KNOWN_TO_MAX,
            )
        ).scalar_one()
        usd_ws_alias = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == "USD-PLN",
                SymbolAlias.exchange == "walutomat",
                SymbolAlias.channel == "ws",
            )
        ).scalar_one()
        usd_rest_alias = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == "USD-PLN",
                SymbolAlias.exchange == "walutomat",
                SymbolAlias.channel == "rest",
            )
        ).scalar_one()
    assert eur_ws_alias.exchange_symbol == "EUR_PLN"
    assert eur_rest_alias.exchange_symbol == "EURPLN"
    assert eur_ws_alias.timestamp.replace(tzinfo=None) > original_timestamp.replace(tzinfo=None)
    assert eur_rest_alias.timestamp.replace(tzinfo=None) > original_timestamp.replace(tzinfo=None)
    assert eur_catalog.base == "EUR"
    assert eur_catalog.quote == "PLN"
    assert usd_ws_alias.exchange_symbol == "USD_PLN"
    assert usd_rest_alias.exchange_symbol == "USDPLN"
    assert usd_catalog.base == "USD"
    assert usd_catalog.quote == "PLN"
    assert usd_catalog.asset_type == "forex"
    assert usd_catalog.created_at == usd_catalog.timestamp


@pytest.mark.asyncio()
async def test_update_database_skips_when_mapping_unchanged(
    updater_with_repository: tuple[ExposedWalutomatSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify update_database skips update when alias rows unchanged.

    Given: Existing catalog and alias rows with identical values to payload,
    When: Update database called,
    Then: updated_at timestamps preserved unchanged on alias rows.
    """
    updater, repository = updater_with_repository
    original_timestamp = datetime(2023, 1, 1, tzinfo=UTC)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        session.add(
            SymbolCatalog(
                native_symbol="EUR-PLN",
                base="EUR",
                quote="PLN",
                asset_type="forex",
                created_at=original_timestamp,
                timestamp=original_timestamp,
            )
        )
        session.add(
            SymbolAlias(
                native_symbol="EUR-PLN",
                exchange="walutomat",
                channel="ws",
                exchange_symbol="EUR_PLN",
                created_at=original_timestamp,
                timestamp=original_timestamp,
            )
        )
        session.add(
            SymbolAlias(
                native_symbol="EUR-PLN",
                exchange="walutomat",
                channel="rest",
                exchange_symbol="EURPLN",
                created_at=original_timestamp,
                timestamp=original_timestamp,
            )
        )
        session.commit()
    symbols: list[dict[str, Any]] = [
        {
            "symbol": "EUR_PLN",
            "walutomat_rest_symbol": "EURPLN",
            "native_symbol": "EUR-PLN",
            "base": "EUR",
            "quote": "PLN",
        }
    ]
    await updater.update_database_public(symbols)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        ws_alias = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == "EUR-PLN",
                SymbolAlias.exchange == "walutomat",
                SymbolAlias.channel == "ws",
            )
        ).scalar_one()
        rest_alias = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == "EUR-PLN",
                SymbolAlias.exchange == "walutomat",
                SymbolAlias.channel == "rest",
            )
        ).scalar_one()
    assert ws_alias.exchange_symbol == "EUR_PLN"
    assert rest_alias.exchange_symbol == "EURPLN"
    assert ws_alias.timestamp.replace(tzinfo=None) == original_timestamp.replace(tzinfo=None)
    assert rest_alias.timestamp.replace(tzinfo=None) == original_timestamp.replace(tzinfo=None)


def test_get_default_kwargs_uses_weekly_threshold() -> None:
    """Verify get_default_kwargs returns weekly (168h) threshold.

    Given: AppSettings instance,
    When: get_default_kwargs called,
    Then: 168-hour threshold and force=False returned.
    """
    settings = AppSettings(BootstrapSettingsLoader())
    defaults = WalutomatSymbolUpdaterService.get_default_kwargs(settings)
    assert defaults == {"update_threshold_hours": 168, "force": False}


def test_create_exchange_client_uses_expected_configuration() -> None:
    """Verify _create_exchange_client uses correct config values.

    Given: Walutomat updater instance,
    When: _create_exchange_client called,
    Then: Client has expected polling interval and timeout.
    """
    updater = ExposedWalutomatSymbolUpdater(update_threshold_hours=1, force=True)
    client = updater.create_exchange_client_public()
    assert isinstance(client, WalutomatExchangeClient)
    assert client.polling_interval == pytest.approx(10.0)
    assert client.timeout == pytest.approx(5.0)


def test_get_setting_key_returns_expected_value() -> None:
    """Verify _get_setting_key returns correct identifier.

    Given: Walutomat updater instance,
    When: _get_setting_key called,
    Then: Expected setting key returned.
    """
    updater = ExposedWalutomatSymbolUpdater(update_threshold_hours=1, force=True)
    assert updater.get_setting_key_public() == "walutomat_symbols_last_update"


@pytest.mark.asyncio()
async def test_update_database_creates_capability_rows(
    updater_with_repository: tuple[ExposedWalutomatSymbolUpdater, DatabaseRepository],
) -> None:
    """Verify _update_database creates SymbolExchangeCapability rows.

    Given: Empty database,
    When: _update_database called with two symbols,
    Then: Capability row created per symbol with exchange=walutomat,
          can_market_data=True, can_trade=True, source=walutomat_updater.
    """
    updater, repository = updater_with_repository
    symbols: list[dict[str, Any]] = [
        {
            "symbol": "EUR_PLN",
            "walutomat_rest_symbol": "EURPLN",
            "native_symbol": "EUR-PLN",
            "base": "EUR",
            "quote": "PLN",
        },
        {
            "symbol": "USD_PLN",
            "walutomat_rest_symbol": "USDPLN",
            "native_symbol": "USD-PLN",
            "base": "USD",
            "quote": "PLN",
        },
    ]
    await updater.update_database_public(symbols)
    with repository.get_session() as session:
        assert isinstance(session, Session)
        caps = session.execute(select(SymbolExchangeCapability)).scalars().all()
        assert len(caps) == 2
        cap_map = {c.native_symbol: c for c in caps}
        for native_symbol in ("EUR-PLN", "USD-PLN"):
            cap = cap_map[native_symbol]
            assert cap.exchange == "walutomat"
            assert cap.can_market_data is True
            assert cap.can_trade is True
            assert cap.source == "walutomat_updater"
            assert cap.reason is None
