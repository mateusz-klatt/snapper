"""Unit tests for SignalReadService."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from snapper.application.services.signals.service import SignalReadService
from snapper.data.models import Instrument
from snapper.data.models import SignalEvent
from snapper.data.models import SymbolCatalog
from snapper.data.repository import SQLAlchemyRepository
from snapper.strategies.base import Signal


class TestSignalService:
    """Test cases for SignalReadService basic functionality."""

    @pytest.fixture
    async def test_repository(self, tmp_path: Path) -> AsyncGenerator[SQLAlchemyRepository]:
        """Create test SQLAlchemy repository with temporary database."""
        db_path = tmp_path / "test.db"
        url = f"sqlite+aiosqlite:///{db_path.as_posix()}"
        repo = SQLAlchemyRepository(url)
        await repo.create_all()
        async with repo.session() as s:
            for sym, base, quote in [("BTCUSD", "BTC", "USD"), ("ETHUSD", "ETH", "USD")]:
                s.add(
                    SymbolCatalog(
                        native_symbol=sym,
                        base=base,
                        quote=quote,
                        asset_type="crypto",
                        created_at=datetime.now(UTC),
                        updated_at=datetime.now(UTC),
                    )
                )
            await s.commit()
        yield repo

    @pytest.fixture
    def signal_service(self, test_repository: SQLAlchemyRepository) -> SignalReadService:
        """Create SignalReadService with test repository."""
        service = SignalReadService()
        service.repo = test_repository
        return service

    @pytest.fixture
    def sample_signal(self) -> Signal:
        """Create sample Signal object for test assertions."""
        return Signal(
            instrument="BTCUSD",
            side="buy",
            strength=0.8,
            reason="Test signal",
            price=50000.0,
        )

    async def test_store_signal(
        self,
        signal_service: SignalReadService,
        sample_signal: Signal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify store_signal persists signal with all metadata.

        Given: Repository with BTCUSD instrument,
        When: store_signal called with signal,
        Then: Signal stored with correct attributes.
        """
        await test_repository.upsert_instrument(
            symbol="BTCUSD",
            exchange="testexchange",
            base="BTC",
            quote="USD",
            tick_size=0.01,
            lot_size=0.001,
        )
        signal_id = await signal_service.store_signal(
            signal=sample_signal,
            exchange="testexchange",
            strategy_name="test_strategy",
            price=50000.0,
        )
        assert signal_id is not None
        assert isinstance(signal_id, int)
        async with test_repository.session() as session:
            stored_signal = await session.get(SignalEvent, signal_id)
            assert stored_signal is not None
            assert stored_signal.side == "buy"
            assert stored_signal.strength == pytest.approx(0.8)
            assert stored_signal.reason == "Test signal"
            assert stored_signal.strategy_name == "test_strategy"
            assert stored_signal.price == pytest.approx(50000.0)

    async def test_store_signal_without_price(
        self,
        signal_service: SignalReadService,
        sample_signal: Signal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify store_signal handles None price.

        Given: Repository with BTCUSD instrument,
        When: store_signal called with price=None,
        Then: Signal stored with price=None.
        """
        await test_repository.upsert_instrument(
            symbol="BTCUSD",
            exchange="testexchange",
            base="BTC",
            quote="USD",
            tick_size=0.01,
            lot_size=0.001,
        )
        signal_id = await signal_service.store_signal(
            signal=sample_signal, exchange="testexchange", strategy_name="test_strategy", price=None
        )
        assert signal_id is not None
        async with test_repository.session() as session:
            stored_signal = await session.get(SignalEvent, signal_id)
            assert stored_signal is not None
            assert stored_signal.price is None

    async def test_get_recent_signals(
        self,
        signal_service: SignalReadService,
        sample_signal: Signal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify get_recent_signals returns ordered signals with limit.

        Given: Three stored signals,
        When: get_recent_signals called with limit=2,
        Then: Two most recent signals returned.
        """
        await test_repository.upsert_instrument(
            symbol="BTCUSD",
            exchange="testexchange",
            base="BTC",
            quote="USD",
            tick_size=0.01,
            lot_size=0.001,
        )
        signal_ids = []
        for i in range(3):
            signal_id = await signal_service.store_signal(
                signal=sample_signal,
                exchange="testexchange",
                strategy_name=f"strategy_{i}",
                price=50000.0 + i * 100,
            )
            signal_ids.append(signal_id)
        recent_signals = await signal_service.get_recent_signals(limit=2)
        assert len(recent_signals) == 2
        assert recent_signals[0]["id"] == signal_ids[-1]
        assert recent_signals[1]["id"] == signal_ids[-2]

    async def test_get_recent_signals_by_strategy(
        self,
        signal_service: SignalReadService,
        sample_signal: Signal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify get_recent_signals filters by strategy name.

        Given: Signals from strategy_a and strategy_b,
        When: get_recent_signals called with strategy='strategy_b',
        Then: Only strategy_b signals returned.
        """
        await test_repository.upsert_instrument(
            symbol="BTCUSD",
            exchange="testexchange",
            base="BTC",
            quote="USD",
            tick_size=0.01,
            lot_size=0.001,
        )
        await signal_service.store_signal(sample_signal, "testexchange", "strategy_a", 50000.0)
        target_id = await signal_service.store_signal(
            sample_signal, "testexchange", "strategy_b", 51000.0
        )
        await signal_service.store_signal(sample_signal, "testexchange", "strategy_a", 52000.0)
        strategy_b_signals = await signal_service.get_recent_signals(
            strategy="strategy_b", limit=10
        )
        assert len(strategy_b_signals) == 1
        assert strategy_b_signals[0]["id"] == target_id
        assert strategy_b_signals[0]["strategy_name"] == "strategy_b"

    async def test_get_recent_signals_by_instrument(
        self, signal_service: SignalReadService, test_repository: SQLAlchemyRepository
    ) -> None:
        """Verify get_recent_signals filters by instrument.

        Given: Signals for BTCUSD and ETHUSD,
        When: get_recent_signals called with instrument='BTCUSD',
        Then: Only BTCUSD signals returned.
        """
        await test_repository.upsert_instrument(
            symbol="BTCUSD",
            exchange="testexchange",
            base="BTC",
            quote="USD",
            tick_size=0.01,
            lot_size=0.001,
        )
        await test_repository.upsert_instrument(
            symbol="ETHUSD",
            exchange="testexchange",
            base="ETH",
            quote="USD",
            tick_size=0.01,
            lot_size=0.001,
        )
        btc_signal = Signal(
            instrument="BTCUSD",
            side="buy",
            strength=0.8,
            reason="BTC signal",
            price=50000.0,
        )
        eth_signal = Signal(
            instrument="ETHUSD",
            side="sell",
            strength=0.6,
            reason="ETH signal",
            price=3000.0,
        )
        btc_id = await signal_service.store_signal(
            btc_signal, "testexchange", "strategy_a", 50000.0
        )
        await signal_service.store_signal(eth_signal, "testexchange", "strategy_a", 3000.0)
        btc_signals = await signal_service.get_recent_signals(instrument="BTCUSD", limit=10)
        assert len(btc_signals) == 1
        assert btc_signals[0]["id"] == btc_id
        assert btc_signals[0]["instrument"] == "BTCUSD"

    async def test_get_recent_signals_by_exchange(
        self, signal_service: SignalReadService, test_repository: SQLAlchemyRepository
    ) -> None:
        """Verify get_recent_signals filters by exchange.

        Given: Signals on exchange_a and exchange_b,
        When: get_recent_signals called with exchange='exchange_b',
        Then: Only exchange_b signals returned.
        """
        await test_repository.upsert_instrument(
            symbol="BTCUSD",
            exchange="exchange_a",
            base="BTC",
            quote="USD",
            tick_size=0.01,
            lot_size=0.001,
        )
        await test_repository.upsert_instrument(
            symbol="BTCUSD",
            exchange="exchange_b",
            base="BTC",
            quote="USD",
            tick_size=0.01,
            lot_size=0.001,
        )
        btc_signal = Signal(
            instrument="BTCUSD",
            side="buy",
            strength=0.8,
            reason="BTC signal",
            price=50000.0,
        )
        await signal_service.store_signal(btc_signal, "exchange_a", "strategy_a", 50000.0)
        target_id = await signal_service.store_signal(
            btc_signal, "exchange_b", "strategy_a", 51000.0
        )
        exchange_b_signals = await signal_service.get_recent_signals(
            exchange="exchange_b", limit=10
        )
        assert len(exchange_b_signals) == 1
        assert exchange_b_signals[0]["id"] == target_id
        assert exchange_b_signals[0]["exchange"] == "exchange_b"

    async def test_get_recent_signals_empty(self, signal_service: SignalReadService) -> None:
        """Verify get_recent_signals returns empty list when no signals.

        Given: Empty database,
        When: get_recent_signals called,
        Then: Empty list returned.
        """
        signals = await signal_service.get_recent_signals()
        assert signals == []

    async def test_signal_service_initialization(self) -> None:
        """Verify SignalReadService initializes with settings and repo.

        Given: Default SignalReadService,
        When: Instance created,
        Then: settings and repo attributes set.
        """
        service = SignalReadService()
        assert service.settings is not None
        assert service.repo is not None


class TestSignalServiceCoverage:
    """Test cases for SignalReadService coverage scenarios."""

    @pytest.fixture
    async def test_repository(self, tmp_path: Path) -> SQLAlchemyRepository:
        """Create test SQLAlchemy repository with in-memory database."""
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
        return repo

    @pytest.fixture
    def signal_service(self, test_repository: SQLAlchemyRepository) -> SignalReadService:
        """Create SignalReadService instance with test repository."""
        service = SignalReadService()
        service.repo = test_repository
        return service

    @pytest.fixture
    def sample_signal(self) -> Signal:
        """Create sample Signal object for testing."""
        return Signal(
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            reason="Test signal",
            price=50000.0,
        )

    async def test_store_signal_creates_new_instrument(
        self, signal_service: SignalReadService, sample_signal: Signal
    ) -> None:
        """Verify store_signal creates instrument if not exists.

        Given: No existing instrument for BTC-USD,
        When: store_signal called,
        Then: Instrument created with correct symbol, base, quote.
        """
        signal_id = await signal_service.store_signal(
            signal=sample_signal,
            exchange="testexchange",
            strategy_name="test_strategy",
            price=50000.0,
        )
        assert signal_id > 0
        async with signal_service.repo.session() as session:
            inst_query = await session.execute(
                select(Instrument).where(Instrument.symbol == "BTC-USD")
            )
            inst = inst_query.scalar_one_or_none()
            assert inst is not None
            assert inst.symbol == "BTC-USD"
            assert inst.base == "BTC"
            assert inst.quote == "USD"

    async def test_store_signal_error_handling(
        self, signal_service: SignalReadService, sample_signal: Signal
    ) -> None:
        """Verify store_signal returns -1 and logs error on exception.

        Given: Repository session that raises Exception,
        When: store_signal called,
        Then: Returns -1 and logger.error called with message.
        """
        with (
            patch.object(
                signal_service.repo, "session", side_effect=SQLAlchemyError("Database error")
            ),
            patch("snapper.application.services.signals.service.logger") as mock_logger,
        ):
            signal_id = await signal_service.store_signal(
                signal=sample_signal,
                exchange="testexchange",
                strategy_name="test_strategy",
                price=50000.0,
            )
            assert signal_id == -1
            mock_logger.error.assert_called_once()
            assert "Error storing signal" in str(mock_logger.error.call_args)

    async def test_store_signal_upsert_instrument_error(
        self, signal_service: SignalReadService, sample_signal: Signal
    ) -> None:
        """Verify store_signal returns -1 and logs error on upsert failure.

        Given: Repository upsert_instrument that raises Exception,
        When: store_signal called,
        Then: Returns -1 and logger.error called with message.
        """
        with (
            patch.object(
                signal_service.repo,
                "upsert_instrument",
                side_effect=SQLAlchemyError("Upsert error"),
            ),
            patch("snapper.application.services.signals.service.logger") as mock_logger,
        ):
            signal_id = await signal_service.store_signal(
                signal=sample_signal,
                exchange="testexchange",
                strategy_name="test_strategy",
                price=50000.0,
            )
            assert signal_id == -1
            mock_logger.error.assert_called_once()
            assert "Error storing signal" in str(mock_logger.error.call_args)

    async def test_get_recent_signals_error_handling(
        self, signal_service: SignalReadService
    ) -> None:
        """Verify get_recent_signals returns empty list and logs error on exception.

        Given: Repository session that raises Exception,
        When: get_recent_signals called,
        Then: Empty list returned and logger.error called with message.
        """
        with (
            patch.object(
                signal_service.repo, "session", side_effect=SQLAlchemyError("Database error")
            ),
            patch("snapper.application.services.signals.service.logger") as mock_logger,
        ):
            signals = await signal_service.get_recent_signals(
                instrument="BTC-USD", strategy="test_strategy", hours=24, limit=100
            )
            assert signals == []
            mock_logger.error.assert_called_once()
            assert "Error retrieving signals" in str(mock_logger.error.call_args)

    async def test_get_recent_signals_session_error(
        self, signal_service: SignalReadService
    ) -> None:
        """Verify get_recent_signals handles session execution error and logs it.

        Given: Session execute that raises Exception,
        When: get_recent_signals called,
        Then: Empty list returned and logger.error called with message.
        """
        mock_session = AsyncMock()
        mock_session.execute.side_effect = SQLAlchemyError("Session execution error")
        with (
            patch.object(signal_service.repo, "session") as mock_session_manager,
            patch("snapper.application.services.signals.service.logger") as mock_logger,
        ):
            mock_session_manager.return_value.__aenter__.return_value = mock_session
            signals = await signal_service.get_recent_signals()
            assert signals == []
            mock_logger.error.assert_called_once()
            assert "Error retrieving signals" in str(mock_logger.error.call_args)

    async def test_store_signal_session_add_error(
        self,
        signal_service: SignalReadService,
        sample_signal: Signal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify store_signal returns -1 and logs error when session.add fails.

        Given: Session with add that raises Exception,
        When: store_signal called,
        Then: Returns -1 and logger.error called with message.
        """
        await test_repository.upsert_instrument(
            symbol="BTC-USD",
            exchange="testexchange",
            base="BTC",
            quote="USD",
            tick_size=0.01,
            lot_size=0.001,
        )
        mock_session = MagicMock()
        mock_execute_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.first.return_value = MagicMock(id=1)
        mock_execute_result.scalars.return_value = mock_scalars
        mock_session.execute = AsyncMock(return_value=mock_execute_result)
        mock_session.add.side_effect = SQLAlchemyError("Add error")
        mock_session.commit = AsyncMock()
        mock_session.refresh = AsyncMock()
        with (
            patch.object(signal_service.repo, "session") as mock_session_manager,
            patch("snapper.application.services.signals.service.logger") as mock_logger,
        ):
            mock_session_manager.return_value.__aenter__.return_value = mock_session
            signal_id = await signal_service.store_signal(
                signal=sample_signal,
                exchange="testexchange",
                strategy_name="test_strategy",
                price=50000.0,
            )
            assert signal_id == -1
            mock_logger.error.assert_called_once()
            assert "Error storing signal" in str(mock_logger.error.call_args)

    async def test_store_signal_single_asset_symbol(
        self, signal_service: SignalReadService, test_repository: SQLAlchemyRepository
    ) -> None:
        """Verify store_signal parses single-asset symbol without dash.

        Given: No existing instrument for GOLD,
        When: store_signal called with instrument='GOLD' (no dash),
        Then: Instrument created with base='GOLD' and quote='USD'.
        """
        async with test_repository.session() as s:
            s.add(
                SymbolCatalog(
                    native_symbol="GOLD",
                    base="GOLD",
                    quote="USD",
                    asset_type="crypto",
                    created_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
            )
            await s.commit()
        single_asset_signal = Signal(
            instrument="GOLD",
            side="buy",
            strength=0.5,
            reason="Single asset test",
            price=2000.0,
        )
        signal_id = await signal_service.store_signal(
            signal=single_asset_signal,
            exchange="testexchange",
            strategy_name="test_strategy",
            price=2000.0,
        )
        assert signal_id > 0
        async with signal_service.repo.session() as session:
            inst_query = await session.execute(
                select(Instrument).where(Instrument.symbol == "GOLD")
            )
            inst = inst_query.scalar_one_or_none()
            assert inst is not None
            assert inst.symbol == "GOLD"
            assert inst.base == "GOLD"
            assert inst.quote == "USD"

    async def test_get_recent_signals_complex_query_error(
        self, signal_service: SignalReadService, test_repository: SQLAlchemyRepository
    ) -> None:
        """Verify get_recent_signals handles result processing error and logs it.

        Given: Result.all that raises Exception,
        When: get_recent_signals called with filters,
        Then: Empty list returned and logger.error called with message.
        """
        await test_repository.upsert_instrument(
            symbol="BTC-USD",
            exchange="testexchange",
            base="BTC",
            quote="USD",
            tick_size=0.01,
            lot_size=0.001,
        )
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.all.side_effect = SQLAlchemyError("Result processing error")
        mock_session.execute.return_value = mock_result
        with (
            patch.object(signal_service.repo, "session") as mock_session_manager,
            patch("snapper.application.services.signals.service.logger") as mock_logger,
        ):
            mock_session_manager.return_value.__aenter__.return_value = mock_session
            signals = await signal_service.get_recent_signals(
                instrument="BTC-USD", strategy="test_strategy"
            )
            assert signals == []
            mock_logger.error.assert_called_once()
            assert "Error retrieving signals" in str(mock_logger.error.call_args)
