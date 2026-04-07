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
from snapper.data.models import Signal
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.strategies.base import StrategySignal

FIXED_TEST_TIME = datetime(2024, 1, 1, tzinfo=UTC)
BTCUSD_SYMBOL_PUBLIC_ID = "00000000-0000-7000-8000-000000000001"
ETHUSD_SYMBOL_PUBLIC_ID = "00000000-0000-7000-8000-000000000002"
BTC_USD_SYMBOL_PUBLIC_ID = "00000000-0000-7000-8000-000000000003"


class TestSignalService:
    """Test cases for SignalReadService basic functionality."""

    @pytest.fixture
    async def test_repository(self, tmp_path: Path) -> AsyncGenerator[SQLAlchemyRepository]:
        """Create test SQLAlchemy repository with temporary database."""
        db_path = tmp_path / "test.db"
        url = f"sqlite+aiosqlite:///{db_path.as_posix()}"
        repo = SQLAlchemyRepository(url)
        await repo.create_all()
        tracker = SequenceTracker()
        async with repo.session() as s:
            for public_id, sym, base, quote in [
                (BTCUSD_SYMBOL_PUBLIC_ID, "BTCUSD", "BTC", "USD"),
                (ETHUSD_SYMBOL_PUBLIC_ID, "ETHUSD", "ETH", "USD"),
            ]:
                s.add(
                    Symbol(
                        public_id=public_id,
                        native_symbol=sym,
                        base=base,
                        quote=quote,
                        asset_type="crypto",
                        created_at=FIXED_TEST_TIME,
                        timestamp=FIXED_TEST_TIME,
                        session_id=tracker.session_id,
                        sequence_id=tracker.next_sequence("symbols"),
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
    def sample_signal(self) -> StrategySignal:
        """Create sample StrategySignal object for test assertions."""
        return StrategySignal(
            instrument="BTCUSD",
            side="buy",
            strength=0.8,
            reason="Test signal",
            price=50000.0,
        )

    async def test_store_signal(
        self,
        signal_service: SignalReadService,
        sample_signal: StrategySignal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify store_signal persists signal with all metadata.

        Given: Repository with BTCUSD instrument,
        When: store_signal called with signal,
        Then: StrategySignal stored with correct attributes.
        """
        await test_repository.ensure_instrument(
            symbol_public_id=BTCUSD_SYMBOL_PUBLIC_ID,
            exchange="testexchange",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
        )
        tracker = SequenceTracker()
        signal_id = await signal_service.store_signal(
            signal=sample_signal,
            exchange="testexchange",
            strategy_name="test_strategy",
            price=50000.0,
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
        )
        assert signal_id is not None
        assert isinstance(signal_id, str)
        assert len(signal_id) == 36
        async with test_repository.session() as session:
            result = await session.execute(select(Signal).where(Signal.public_id == signal_id))
            stored_signal = result.scalars().first()
            assert stored_signal is not None
            assert stored_signal.side == "buy"
            assert stored_signal.strength == pytest.approx(0.8)
            assert stored_signal.reason == "Test signal"
            assert stored_signal.strategy_name == "test_strategy"
            assert stored_signal.price == pytest.approx(50000.0)

    async def test_store_signal_without_price(
        self,
        signal_service: SignalReadService,
        sample_signal: StrategySignal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify store_signal handles None price.

        Given: Repository with BTCUSD instrument,
        When: store_signal called with price=None,
        Then: StrategySignal stored with price=None.
        """
        await test_repository.ensure_instrument(
            symbol_public_id=BTCUSD_SYMBOL_PUBLIC_ID,
            exchange="testexchange",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
        )
        tracker = SequenceTracker()
        signal_id = await signal_service.store_signal(
            signal=sample_signal,
            exchange="testexchange",
            strategy_name="test_strategy",
            price=None,
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
        )
        assert signal_id is not None
        async with test_repository.session() as session:
            result = await session.execute(select(Signal).where(Signal.public_id == signal_id))
            stored_signal = result.scalars().first()
            assert stored_signal is not None
            assert stored_signal.price is None

    async def test_store_signal_persists_multi_tenant_ids(
        self,
        signal_service: SignalReadService,
        sample_signal: StrategySignal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify store_signal persists wallet_public_id + operator_public_id.

        Given: Repository with BTCUSD instrument and a strategy that supplies
            wallet/operator IDs (Phase 0b.4 contract),
        When: store_signal is called with wallet_public_id + operator_public_id,
        Then: The persisted Signal row carries both IDs verbatim. Empty strings
            collapse to NULL so the existing zero-value path stays unchanged.
        """
        await test_repository.ensure_instrument(
            symbol_public_id=BTCUSD_SYMBOL_PUBLIC_ID,
            exchange="testexchange",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
        )
        tracker = SequenceTracker()
        wallet_pid = "01975a8b-3c7d-7000-8000-aaaaaaaaaaaa"
        operator_pid = "01975a8b-3c7d-7000-8000-bbbbbbbbbbbb"
        signal_id = await signal_service.store_signal(
            signal=sample_signal,
            exchange="testexchange",
            strategy_name="test_strategy",
            price=50000.0,
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
            wallet_public_id=wallet_pid,
            operator_public_id=operator_pid,
        )
        assert signal_id
        async with test_repository.session() as session:
            result = await session.execute(select(Signal).where(Signal.public_id == signal_id))
            stored = result.scalars().first()
            assert stored is not None
            assert stored.wallet_public_id == wallet_pid
            assert stored.operator_public_id == operator_pid

    async def test_store_signal_empty_multi_tenant_ids_become_null(
        self,
        signal_service: SignalReadService,
        sample_signal: StrategySignal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify empty-string IDs collapse to NULL on the persisted row.

        Given: Repository with BTCUSD instrument and a strategy with empty
            wallet/operator (Phase 0b transitional empty defaults),
        When: store_signal is called with empty-string IDs,
        Then: The persisted row stores NULL for both, preserving the existing
            no-tenant query semantics until Phase 0b.6 NOT NULL tightening.
        """
        await test_repository.ensure_instrument(
            symbol_public_id=BTCUSD_SYMBOL_PUBLIC_ID,
            exchange="testexchange",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
        )
        tracker = SequenceTracker()
        signal_id = await signal_service.store_signal(
            signal=sample_signal,
            exchange="testexchange",
            strategy_name="test_strategy",
            price=50000.0,
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
            wallet_public_id="",
            operator_public_id="",
        )
        assert signal_id
        async with test_repository.session() as session:
            result = await session.execute(select(Signal).where(Signal.public_id == signal_id))
            stored = result.scalars().first()
            assert stored is not None
            assert stored.wallet_public_id is None
            assert stored.operator_public_id is None

    async def test_store_signal_with_envelope_identity(
        self,
        signal_service: SignalReadService,
        sample_signal: StrategySignal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify store_signal uses public_id and timestamp from published envelope.

        Given: Repository with BTCUSD instrument and an explicit public_id and timestamp,
        When: store_signal called with public_id and timestamp,
        Then: DB row carries the exact same public_id and timestamp as the published envelope.
        """
        await test_repository.ensure_instrument(
            symbol_public_id=BTCUSD_SYMBOL_PUBLIC_ID,
            exchange="testexchange",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
        )
        envelope_public_id = "envelope-public-id-abc"
        envelope_timestamp = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        tracker = SequenceTracker()
        signal_id = await signal_service.store_signal(
            signal=sample_signal,
            exchange="testexchange",
            strategy_name="test_strategy",
            price=50000.0,
            session_id="",
            sequence_id=0,
            tracker=tracker,
            public_id=envelope_public_id,
            timestamp=envelope_timestamp,
        )
        assert signal_id == envelope_public_id
        async with test_repository.session() as session:
            result = await session.execute(
                select(Signal).where(Signal.public_id == envelope_public_id)
            )
            stored_signal = result.scalars().first()
            assert stored_signal is not None
            assert stored_signal.public_id == envelope_public_id
            assert stored_signal.timestamp == envelope_timestamp

    async def test_get_recent_signals(
        self,
        signal_service: SignalReadService,
        sample_signal: StrategySignal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify get_recent_signals returns ordered signals with limit.

        Given: Three stored signals,
        When: get_recent_signals called with limit=2,
        Then: Two most recent signals returned.
        """
        await test_repository.ensure_instrument(
            symbol_public_id=BTCUSD_SYMBOL_PUBLIC_ID,
            exchange="testexchange",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
        )
        tracker = SequenceTracker()
        signal_ids = []
        for i in range(3):
            signal_id = await signal_service.store_signal(
                signal=sample_signal,
                exchange="testexchange",
                strategy_name=f"strategy_{i}",
                price=50000.0 + i * 100,
                session_id="",
                sequence_id=0,
                timestamp=FIXED_TEST_TIME,
                tracker=tracker,
            )
            signal_ids.append(signal_id)
        recent_signals = await signal_service.get_recent_signals(as_of=FIXED_TEST_TIME, limit=2)
        assert len(recent_signals) == 2
        assert isinstance(recent_signals[0]["public_id"], str)
        assert recent_signals[0]["public_id"] > recent_signals[1]["public_id"]

    async def test_get_recent_signals_by_strategy(
        self,
        signal_service: SignalReadService,
        sample_signal: StrategySignal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify get_recent_signals filters by strategy name.

        Given: Signals from strategy_a and strategy_b,
        When: get_recent_signals called with strategy='strategy_b',
        Then: Only strategy_b signals returned.
        """
        await test_repository.ensure_instrument(
            symbol_public_id=BTCUSD_SYMBOL_PUBLIC_ID,
            exchange="testexchange",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
        )
        tracker = SequenceTracker()
        await signal_service.store_signal(
            sample_signal,
            "testexchange",
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
            strategy_name="strategy_a",
            price=50000.0,
        )
        await signal_service.store_signal(
            sample_signal,
            "testexchange",
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
            strategy_name="strategy_b",
            price=51000.0,
        )
        await signal_service.store_signal(
            sample_signal,
            "testexchange",
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
            strategy_name="strategy_a",
            price=52000.0,
        )
        strategy_b_signals = await signal_service.get_recent_signals(
            as_of=FIXED_TEST_TIME, strategy="strategy_b", limit=10
        )
        assert len(strategy_b_signals) == 1
        assert isinstance(strategy_b_signals[0]["public_id"], str)
        assert strategy_b_signals[0]["strategy_name"] == "strategy_b"

    async def test_get_recent_signals_by_instrument(
        self, signal_service: SignalReadService, test_repository: SQLAlchemyRepository
    ) -> None:
        """Verify get_recent_signals filters by instrument.

        Given: Signals for BTCUSD and ETHUSD,
        When: get_recent_signals called with instrument='BTCUSD',
        Then: Only BTCUSD signals returned.
        """
        await test_repository.ensure_instrument(
            symbol_public_id=BTCUSD_SYMBOL_PUBLIC_ID,
            exchange="testexchange",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
        )
        await test_repository.ensure_instrument(
            symbol_public_id=ETHUSD_SYMBOL_PUBLIC_ID,
            exchange="testexchange",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
        )
        btc_signal = StrategySignal(
            instrument="BTCUSD",
            side="buy",
            strength=0.8,
            reason="BTC signal",
            price=50000.0,
        )
        eth_signal = StrategySignal(
            instrument="ETHUSD",
            side="sell",
            strength=0.6,
            reason="ETH signal",
            price=3000.0,
        )
        tracker = SequenceTracker()
        await signal_service.store_signal(
            btc_signal,
            "testexchange",
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
            strategy_name="strategy_a",
            price=50000.0,
        )
        await signal_service.store_signal(
            eth_signal,
            "testexchange",
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
            strategy_name="strategy_a",
            price=3000.0,
        )
        btc_signals = await signal_service.get_recent_signals(
            as_of=FIXED_TEST_TIME, instrument="BTCUSD", limit=10
        )
        assert len(btc_signals) == 1
        assert isinstance(btc_signals[0]["public_id"], str)
        assert btc_signals[0]["instrument"] == "BTCUSD"

    async def test_get_recent_signals_by_exchange(
        self, signal_service: SignalReadService, test_repository: SQLAlchemyRepository
    ) -> None:
        """Verify get_recent_signals filters by exchange.

        Given: Signals on exchange_a and exchange_b,
        When: get_recent_signals called with exchange='exchange_b',
        Then: Only exchange_b signals returned.
        """
        await test_repository.ensure_instrument(
            symbol_public_id=BTCUSD_SYMBOL_PUBLIC_ID,
            exchange="exchange_a",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
        )
        await test_repository.ensure_instrument(
            symbol_public_id=BTCUSD_SYMBOL_PUBLIC_ID,
            exchange="exchange_b",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
        )
        btc_signal = StrategySignal(
            instrument="BTCUSD",
            side="buy",
            strength=0.8,
            reason="BTC signal",
            price=50000.0,
        )
        tracker = SequenceTracker()
        await signal_service.store_signal(
            btc_signal,
            "exchange_a",
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
            strategy_name="strategy_a",
            price=50000.0,
        )
        await signal_service.store_signal(
            btc_signal,
            "exchange_b",
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
            strategy_name="strategy_a",
            price=51000.0,
        )
        exchange_b_signals = await signal_service.get_recent_signals(
            as_of=FIXED_TEST_TIME, exchange="exchange_b", limit=10
        )
        assert len(exchange_b_signals) == 1
        assert isinstance(exchange_b_signals[0]["public_id"], str)
        assert exchange_b_signals[0]["exchange"] == "exchange_b"

    async def test_get_recent_signals_empty(self, signal_service: SignalReadService) -> None:
        """Verify get_recent_signals returns empty list when no signals.

        Given: Empty database,
        When: get_recent_signals called,
        Then: Empty list returned.
        """
        signals = await signal_service.get_recent_signals(as_of=datetime.now(UTC))
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
        tracker = SequenceTracker()
        async with repo.session() as s:
            s.add(
                Symbol(
                    public_id=BTC_USD_SYMBOL_PUBLIC_ID,
                    native_symbol="BTC-USD",
                    base="BTC",
                    quote="USD",
                    asset_type="crypto",
                    created_at=FIXED_TEST_TIME,
                    timestamp=FIXED_TEST_TIME,
                    session_id=tracker.session_id,
                    sequence_id=tracker.next_sequence("symbols"),
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
    def sample_signal(self) -> StrategySignal:
        """Create sample StrategySignal object for testing."""
        return StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            reason="Test signal",
            price=50000.0,
        )

    async def test_resolve_instrument_returns_none_when_no_symbol(
        self, signal_service: SignalReadService
    ) -> None:
        """Verify _resolve_instrument returns None when Symbol row is missing.

        Given: No Symbol row matching the signal instrument,
        When: store_signal is called,
        Then: Returns empty string because _resolve_instrument finds no Symbol.
        """
        unknown_signal = StrategySignal(
            instrument="NOSYMBOL",
            side="buy",
            strength=0.5,
            reason="Unknown symbol test",
            price=100.0,
        )
        tracker = SequenceTracker()
        signal_id = await signal_service.store_signal(
            signal=unknown_signal,
            exchange="testexchange",
            strategy_name="test_strategy",
            price=100.0,
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
        )
        assert signal_id == ""

    async def test_store_signal_creates_new_instrument(
        self, signal_service: SignalReadService, sample_signal: StrategySignal
    ) -> None:
        """Verify store_signal creates instrument if not exists.

        Given: No existing instrument for BTC-USD,
        When: store_signal called,
        Then: Instrument created with correct symbol_public_id and exchange.
        """
        signal_id = await signal_service.store_signal(
            signal=sample_signal,
            exchange="testexchange",
            strategy_name="test_strategy",
            price=50000.0,
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=SequenceTracker(),
        )
        assert len(signal_id) == 36
        async with signal_service.repo.session() as session:
            inst_query = await session.execute(
                select(Instrument).where(
                    Instrument.symbol_public_id == BTC_USD_SYMBOL_PUBLIC_ID,
                    Instrument.exchange == "testexchange",
                )
            )
            inst = inst_query.scalar_one_or_none()
            assert inst is not None
            assert inst.symbol_public_id == BTC_USD_SYMBOL_PUBLIC_ID
            assert inst.exchange == "testexchange"

    async def test_store_signal_error_handling(
        self, signal_service: SignalReadService, sample_signal: StrategySignal
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
            tracker = SequenceTracker()
            signal_id = await signal_service.store_signal(
                signal=sample_signal,
                exchange="testexchange",
                strategy_name="test_strategy",
                price=50000.0,
                session_id="",
                sequence_id=0,
                timestamp=FIXED_TEST_TIME,
                tracker=tracker,
            )
            assert signal_id == ""
            mock_logger.error.assert_called_once()
            assert "Error storing signal" in str(mock_logger.error.call_args)

    async def test_store_signal_returns_empty_when_symbol_not_resolved(
        self, signal_service: SignalReadService, sample_signal: StrategySignal
    ) -> None:
        """Verify store_signal returns empty string when Symbol cannot be resolved.

        Given: No existing instrument and no active Symbol row,
        When: store_signal is called,
        Then: The method returns an empty string and logs the reason.
        """
        with (
            patch(
                "snapper.application.services.signals.service.resolve_symbol_public_id",
                new=AsyncMock(return_value=None),
            ),
            patch("snapper.application.services.signals.service.logger") as mock_logger,
        ):
            tracker = SequenceTracker()
            signal_id = await signal_service.store_signal(
                signal=sample_signal,
                exchange="testexchange",
                strategy_name="test_strategy",
                price=50000.0,
                session_id="",
                sequence_id=0,
                timestamp=FIXED_TEST_TIME,
                tracker=tracker,
            )
            assert signal_id == ""
            mock_logger.error.assert_called_once()
            assert "No active Symbol row" in str(mock_logger.error.call_args)

    async def test_store_signal_ensure_instrument_error(
        self, signal_service: SignalReadService, sample_signal: StrategySignal
    ) -> None:
        """Verify store_signal returns -1 and logs error on upsert failure.

        Given: Repository ensure_instrument that raises Exception,
        When: store_signal called,
        Then: Returns -1 and logger.error called with message.
        """
        with (
            patch.object(
                signal_service.repo,
                "ensure_instrument",
                side_effect=SQLAlchemyError("Upsert error"),
            ),
            patch("snapper.application.services.signals.service.logger") as mock_logger,
        ):
            signal_id = await signal_service.store_signal(
                signal=sample_signal,
                exchange="testexchange",
                strategy_name="test_strategy",
                price=50000.0,
                session_id="",
                sequence_id=0,
                timestamp=FIXED_TEST_TIME,
                tracker=SequenceTracker(),
            )
            assert signal_id == ""
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
                as_of=datetime.now(UTC),
                instrument="BTC-USD",
                strategy="test_strategy",
                hours=24,
                limit=100,
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
            signals = await signal_service.get_recent_signals(as_of=datetime.now(UTC))
            assert signals == []
            mock_logger.error.assert_called_once()
            assert "Error retrieving signals" in str(mock_logger.error.call_args)

    async def test_store_signal_session_add_error(
        self,
        signal_service: SignalReadService,
        sample_signal: StrategySignal,
        test_repository: SQLAlchemyRepository,
    ) -> None:
        """Verify store_signal returns -1 and logs error when session.add fails.

        Given: Session with add that raises Exception,
        When: store_signal called,
        Then: Returns -1 and logger.error called with message.
        """
        await test_repository.ensure_instrument(
            symbol_public_id=BTC_USD_SYMBOL_PUBLIC_ID,
            exchange="testexchange",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
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
            tracker = SequenceTracker()
            signal_id = await signal_service.store_signal(
                signal=sample_signal,
                exchange="testexchange",
                strategy_name="test_strategy",
                price=50000.0,
                session_id="",
                sequence_id=0,
                timestamp=FIXED_TEST_TIME,
                tracker=tracker,
            )
            assert signal_id == ""
            mock_logger.error.assert_called_once()
            assert "Error storing signal" in str(mock_logger.error.call_args)

    async def test_store_signal_single_asset_symbol(
        self, signal_service: SignalReadService, test_repository: SQLAlchemyRepository
    ) -> None:
        """Verify store_signal parses single-asset symbol without dash.

        Given: No existing instrument for GOLD,
        When: store_signal called with instrument='GOLD' (no dash),
        Then: Instrument created with correct symbol_public_id and exchange.
        """
        gold_symbol_public_id = "00000000-0000-7000-8000-000000000099"
        tracker = SequenceTracker()
        async with test_repository.session() as s:
            s.add(
                Symbol(
                    public_id=gold_symbol_public_id,
                    native_symbol="GOLD",
                    base="GOLD",
                    quote="USD",
                    asset_type="crypto",
                    created_at=FIXED_TEST_TIME,
                    timestamp=FIXED_TEST_TIME,
                    session_id=tracker.session_id,
                    sequence_id=tracker.next_sequence("symbols"),
                )
            )
            await s.commit()
        single_asset_signal = StrategySignal(
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
            session_id="",
            sequence_id=0,
            timestamp=FIXED_TEST_TIME,
            tracker=SequenceTracker(),
        )
        assert len(signal_id) == 36
        async with signal_service.repo.session() as session:
            inst_query = await session.execute(
                select(Instrument).where(
                    Instrument.symbol_public_id == gold_symbol_public_id,
                    Instrument.exchange == "testexchange",
                )
            )
            inst = inst_query.scalar_one_or_none()
            assert inst is not None
            assert inst.symbol_public_id == gold_symbol_public_id
            assert inst.exchange == "testexchange"

    async def test_get_recent_signals_complex_query_error(
        self, signal_service: SignalReadService, test_repository: SQLAlchemyRepository
    ) -> None:
        """Verify get_recent_signals handles result processing error and logs it.

        Given: Result.all that raises Exception,
        When: get_recent_signals called with filters,
        Then: Empty list returned and logger.error called with message.
        """
        await test_repository.ensure_instrument(
            symbol_public_id=BTC_USD_SYMBOL_PUBLIC_ID,
            exchange="testexchange",
            session_id="test-session",
            sequence_id=1,
            timestamp=FIXED_TEST_TIME,
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
                as_of=datetime.now(UTC),
                instrument="BTC-USD",
                strategy="test_strategy",
            )
            assert signals == []
            mock_logger.error.assert_called_once()
            assert "Error retrieving signals" in str(mock_logger.error.call_args)

    @pytest.mark.asyncio
    async def test_store_signal_creates_instrument_with_tracker_provenance(
        self, signal_service: SignalReadService, sample_signal: StrategySignal
    ) -> None:
        """Verify store_signal uses tracker provenance for lazy instrument upsert.

        Given: No existing instrument and a tracker passed by the caller,
        When: store_signal is called with tracker,
        Then: Instrument row carries the tracker session_id and a non-zero sequence_id.
        """
        tracker = SequenceTracker()
        signal_id = await signal_service.store_signal(
            signal=sample_signal,
            exchange="testexchange",
            strategy_name="test_strategy",
            price=50000.0,
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence("signals"),
            timestamp=FIXED_TEST_TIME,
            tracker=tracker,
        )
        assert len(signal_id) == 36
        async with signal_service.repo.session() as session:
            inst_query = await session.execute(
                select(Instrument).where(
                    Instrument.symbol_public_id == BTC_USD_SYMBOL_PUBLIC_ID,
                    Instrument.exchange == "testexchange",
                )
            )
            inst = inst_query.scalar_one_or_none()
            assert inst is not None
            assert inst.session_id == tracker.session_id
            assert inst.sequence_id == 1
