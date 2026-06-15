"""Unit tests for base market data service."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.data.models import Instrument
from snapper.data.models import MarketSnapshot
from snapper.data.models import Symbol
from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.market_data.base import MarketSnapshotUpdaterService
from snapper.infrastructure.market_data.kraken import KrakenSnapshotUpdaterService


def _make_instrument_map(symbols: list[str]) -> dict[str, str]:
    """Build a deterministic symbol->instrument_public_id mapping for tests."""
    return {s: f"inst-{s.lower()}" for s in symbols}


def _capture_and_count(
    target: list[list[MarketSnapshot]],
) -> Any:
    """Return a callable that captures snapshots and returns their count."""

    def _handler(snaps: list[MarketSnapshot]) -> int:
        target.append(list(snaps))
        return len(snaps)

    return _handler


class TestMarketSnapshotServiceCoverage:
    """Tests for MarketSnapshotUpdaterService base functionality."""

    @pytest.fixture
    def mock_repository(self) -> MagicMock:
        """Create mock repository for testing."""
        return MagicMock()

    @pytest.fixture
    def service(self, mock_repository: MagicMock) -> KrakenSnapshotUpdaterService:
        """Create KrakenSnapshotUpdaterService with mocked dependencies."""
        mock_exchange = MagicMock()
        mock_exchange.disconnect_websocket = MagicMock(return_value=None)
        mock_exchange.disconnect_websocket.__name__ = "disconnect_websocket"

        async def async_disconnect() -> None:
            """Intentionally empty async stub for testing."""
            pass

        mock_exchange.disconnect_websocket.side_effect = async_disconnect
        return KrakenSnapshotUpdaterService(mock_exchange, mock_repository)

    @pytest.fixture
    def sample_ticker_data(self) -> TickerUpdate:
        """Create sample ticker data for testing."""
        return TickerUpdate(
            symbol="BTC-USD",
            bid=50000.0,
            bid_qty=1.5,
            ask=50100.0,
            ask_qty=2.0,
            last=50050.0,
            volume=1234.56,
            vwap=50025.0,
            low=49900.0,
            high=50200.0,
            change=150.0,
            change_pct=0.3,
        )

    async def test_update_market_snapshots_processes_ticker_data(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
        sample_ticker_data: TickerUpdate,
    ) -> None:
        """Test update_market_snapshots processes ticker data.

        Given: service with mocked repository and valid ticker,
        When: calling update_market_snapshots,
        Then: snapshot saved with correct instrument_public_id, prices, and spread.
        """
        persisted: list[list[MarketSnapshot]] = []

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield sample_ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        with (
            patch.object(
                service,
                "_resolve_batch_instrument_ids",
                return_value={"BTC-USD": "inst-btc-usd"},
            ),
            patch.object(
                service,
                "_persist_snapshots_scd2",
                side_effect=_capture_and_count(persisted),
            ),
        ):
            count = await service.update_market_snapshots()
        assert count == 1
        assert len(persisted) == 1
        assert len(persisted[0]) == 1
        snapshot = persisted[0][0]
        assert isinstance(snapshot, MarketSnapshot)
        assert snapshot.instrument_public_id == "inst-btc-usd"
        assert snapshot.bid == pytest.approx(50000.0)
        assert snapshot.ask == pytest.approx(50100.0)
        assert snapshot.spread == pytest.approx(100.0)
        assert snapshot.spread_pct is not None
        assert abs(snapshot.spread_pct - 0.1998) < 0.01

    async def test_update_market_snapshots_skips_unknown_symbols(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots skips unknown symbols.

        Given: service with ticker having empty symbol,
        When: calling update_market_snapshots,
        Then: count is 0 and no persist call.
        """
        ticker_data = TickerUpdate(
            symbol="",
            bid=100.0,
            bid_qty=1.0,
            ask=101.0,
            ask_qty=1.0,
            last=100.5,
            volume=10.0,
            vwap=100.25,
            low=99.0,
            high=102.0,
            change=1.0,
            change_pct=1.0,
        )

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        count = await service.update_market_snapshots()
        assert count == 0

    async def test_update_market_snapshots_skips_empty_native_symbol(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots skips empty native symbol.

        Given: service with ticker returning empty native symbol,
        When: calling update_market_snapshots,
        Then: count is 0 and no persist call.
        """
        ticker_data = TickerUpdate(
            symbol="",
            bid=100.0,
            bid_qty=1.0,
            ask=101.0,
            ask_qty=1.0,
            last=100.5,
            volume=10.0,
            vwap=100.25,
            low=99.0,
            high=102.0,
            change=1.0,
            change_pct=1.0,
        )

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        count = await service.update_market_snapshots()
        assert count == 0

    async def test_update_market_snapshots_deduplicates_symbols(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots deduplicates symbols.

        Given: service receiving 3 tickers with same symbol,
        When: calling update_market_snapshots,
        Then: only last ticker saved (1 snapshot, latest bid).
        """
        persisted: list[list[MarketSnapshot]] = []
        tickers = [
            TickerUpdate(
                symbol="BTC-USD",
                bid=50000.0 + i * 100,
                bid_qty=1.0,
                ask=50100.0 + i * 100,
                ask_qty=1.0,
                last=50050.0 + i * 100,
                volume=1000.0,
                vwap=50025.0,
                low=49900.0,
                high=50200.0,
                change=100.0,
                change_pct=0.2,
            )
            for i in range(3)
        ]

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            for ticker in tickers:
                yield ticker

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        with (
            patch.object(
                service,
                "_resolve_batch_instrument_ids",
                return_value={"BTC-USD": "inst-btc-usd"},
            ),
            patch.object(
                service,
                "_persist_snapshots_scd2",
                side_effect=_capture_and_count(persisted),
            ),
        ):
            count = await service.update_market_snapshots()
        assert count == 3
        assert len(persisted[0]) == 1
        assert persisted[0][0].bid == pytest.approx(50200.0)

    async def test_update_market_snapshots_stops_at_2000_snapshots(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots stops at 2000 snapshots.

        Given: service with infinite ticker generator,
        When: calling update_market_snapshots,
        Then: returns exactly 2000 (hard limit).
        """

        async def infinite_tickers(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            i = 0
            while True:
                yield TickerUpdate(
                    symbol=f"SYMBOL{i}-USD",
                    bid=100.0,
                    bid_qty=1.0,
                    ask=101.0,
                    ask_qty=1.0,
                    last=100.5,
                    volume=10.0,
                    vwap=100.25,
                    low=99.0,
                    high=102.0,
                    change=1.0,
                    change_pct=1.0,
                )
                i += 1

        service.exchange_client.subscribe_ticks = infinite_tickers
        with (
            patch.object(
                service,
                "_resolve_batch_instrument_ids",
                side_effect=lambda ns, ex, as_of: _make_instrument_map(list(ns)),
            ),
            patch.object(
                service,
                "_persist_snapshots_scd2",
                side_effect=lambda snaps: len(snaps),
            ),
        ):
            result_count = await service.update_market_snapshots()
        assert result_count == 2000

    async def test_update_market_snapshots_calculates_spread_correctly(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots calculates spread correctly.

        Given: ticker with bid=3000, ask=3010,
        When: calling update_market_snapshots,
        Then: spread=10 and spread_pct=(10/mid)*100.
        """
        persisted: list[list[MarketSnapshot]] = []
        ticker_data = TickerUpdate(
            symbol="ETH-USD",
            bid=3000.0,
            bid_qty=10.0,
            ask=3010.0,
            ask_qty=5.0,
            last=3005.0,
            volume=1000.0,
            vwap=3002.5,
            low=2990.0,
            high=3020.0,
            change=15.0,
            change_pct=0.5,
        )

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        with (
            patch.object(
                service,
                "_resolve_batch_instrument_ids",
                return_value={"ETH-USD": "inst-eth-usd"},
            ),
            patch.object(
                service,
                "_persist_snapshots_scd2",
                side_effect=_capture_and_count(persisted),
            ),
        ):
            await service.update_market_snapshots()
        snapshot = persisted[0][0]
        assert snapshot.spread == pytest.approx(10.0)
        mid = (3000.0 + 3010.0) / 2
        expected_spread_pct = (10.0 / mid) * 100
        assert snapshot.spread_pct is not None
        assert abs(snapshot.spread_pct - expected_spread_pct) < 0.001

    async def test_update_market_snapshots_handles_zero_mid_price(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
    ) -> None:
        """Test update_market_snapshots handles zero mid price.

        Given: ticker with bid=0, ask=0,
        When: calling update_market_snapshots,
        Then: spread_pct is 0.0 (no division by zero).
        """
        persisted: list[list[MarketSnapshot]] = []
        ticker_data = TickerUpdate(
            symbol="NULL-USD",
            bid=0.0,
            bid_qty=0.0,
            ask=0.0,
            ask_qty=0.0,
            last=0.0,
            volume=0.0,
            vwap=0.0,
            low=0.0,
            high=0.0,
            change=0.0,
            change_pct=0.0,
        )

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        with (
            patch.object(
                service,
                "_resolve_batch_instrument_ids",
                return_value={"NULL-USD": "inst-null-usd"},
            ),
            patch.object(
                service,
                "_persist_snapshots_scd2",
                side_effect=_capture_and_count(persisted),
            ),
        ):
            await service.update_market_snapshots()
        snapshot = persisted[0][0]
        assert snapshot.spread_pct == pytest.approx(0.0)

    async def test_update_market_snapshots_handles_exception(
        self,
        service: KrakenSnapshotUpdaterService,
    ) -> None:
        """Test update_market_snapshots handles exception.

        Given: subscribe_ticks raises RuntimeError,
        When: calling update_market_snapshots,
        Then: RuntimeError is propagated.
        """

        async def failing_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            raise RuntimeError("WebSocket connection lost")
            yield

        service.exchange_client.subscribe_ticks = failing_subscribe_ticks
        with pytest.raises(RuntimeError, match="WebSocket connection lost"):
            await service.update_market_snapshots()

    async def test_update_market_snapshots_creates_correct_timestamp(
        self,
        service: KrakenSnapshotUpdaterService,
        mock_repository: MagicMock,
        sample_ticker_data: TickerUpdate,
    ) -> None:
        """Test update_market_snapshots creates correct timestamp.

        Given: service with valid ticker,
        When: calling update_market_snapshots,
        Then: snapshot timestamp is UTC and within before/after bounds.
        """
        persisted: list[list[MarketSnapshot]] = []

        async def mock_subscribe_ticks(
            symbols: list[str], *, req_id: int | None = None
        ) -> AsyncIterator[TickerUpdate]:
            yield sample_ticker_data

        service.exchange_client.subscribe_ticks = mock_subscribe_ticks
        before = datetime.now(UTC)
        with (
            patch.object(
                service,
                "_resolve_batch_instrument_ids",
                return_value={"BTC-USD": "inst-btc-usd"},
            ),
            patch.object(
                service,
                "_persist_snapshots_scd2",
                side_effect=_capture_and_count(persisted),
            ),
        ):
            await service.update_market_snapshots()
        after = datetime.now(UTC)
        snapshot = persisted[0][0]
        assert before <= snapshot.timestamp <= after
        assert snapshot.timestamp.tzinfo == UTC


class StubMarketUpdater(MarketSnapshotUpdaterService):
    """Stub implementation of MarketSnapshotUpdaterService for testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        super().__init__(exchange_client=object(), repository=MagicMock())
        self.calls: list[dict[str, Any]] = []
        self.return_count = 0

    async def update_market_snapshots(self, **kwargs: object) -> int:
        """Record call and return configured count."""
        self.calls.append(dict(kwargs))
        return self.return_count


@pytest.mark.asyncio
async def test_start_invokes_update_and_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test start() invokes update_market_snapshots and logs.

    Given: StubMarketUpdater configured to return 3,
    When: calling start(),
    Then: update called once and log messages contain class name and count.
    """
    updater = StubMarketUpdater()
    updater.return_count = 3
    messages: list[str] = []

    def _fake_info(message: str) -> None:
        messages.append(message)

    monkeypatch.setattr(
        "snapper.infrastructure.market_data.base.logger.info",
        _fake_info,
    )
    await updater.start()
    assert updater.calls == [{}]
    assert any("Starting StubMarketUpdater" in msg for msg in messages)
    assert any("StubMarketUpdater completed - updated 3 snapshots" in msg for msg in messages)


class _FakeScalarResult:
    """Fake scalar result for mocking session.execute().scalars()."""

    def __init__(self, value: Any) -> None:
        self._value = value

    def first(self) -> Any:
        """Return configured value."""
        return self._value


class _FakeExecResult:
    """Fake execute result wrapping scalars."""

    def __init__(self, value: Any) -> None:
        self._value = value

    def scalars(self) -> _FakeScalarResult:
        """Return FakeScalarResult with configured value."""
        return _FakeScalarResult(self._value)


class _FakeSession:
    """Fake sync session for base method tests."""

    def __init__(self, responses: list[Any]) -> None:
        """Initialize with sequential responses for execute() calls."""
        self._responses = list(responses)
        self._call_idx = 0
        self.added: list[Any] = []
        self.committed = False

    def execute(self, stmt: Any) -> _FakeExecResult:
        """Return next configured response."""
        idx = self._call_idx
        self._call_idx += 1
        if idx < len(self._responses):
            return _FakeExecResult(self._responses[idx])
        return _FakeExecResult(None)

    def add(self, obj: Any) -> None:
        """Track added objects."""
        self.added.append(obj)

    def commit(self) -> None:
        """Mark as committed."""
        self.committed = True

    def __enter__(self) -> _FakeSession:
        """Enter the context manager."""
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        """Exit the context manager."""
        return None


def _make_updater_with_session(session: _FakeSession) -> StubMarketUpdater:
    """Create a StubMarketUpdater whose repository.session_factory returns session."""
    updater = StubMarketUpdater()
    updater.repository = MagicMock()
    updater.repository.session_factory.return_value = session
    return updater


def test_resolve_instrument_public_id_found() -> None:
    """Resolve instrument_public_id via 2-hop lookup.

    Given: Session returning symbol_public_id then instrument_public_id,
    When: _resolve_instrument_public_id is called,
    Then: Returns the instrument_public_id.
    """
    session = _FakeSession(["sym-pub-1", "inst-pub-1"])
    updater = _make_updater_with_session(session)
    result = updater._resolve_instrument_public_id("BTC-USD", "kraken", datetime.now(UTC))
    assert result == "inst-pub-1"


def test_resolve_instrument_public_id_symbol_not_found() -> None:
    """Return None when symbol lookup fails.

    Given: Session returning None for symbol lookup,
    When: _resolve_instrument_public_id is called,
    Then: Returns None without querying instrument table.
    """
    session = _FakeSession([None])
    updater = _make_updater_with_session(session)
    result = updater._resolve_instrument_public_id("UNKNOWN", "kraken", datetime.now(UTC))
    assert result is None


def test_resolve_instrument_public_id_instrument_not_found() -> None:
    """Return None when instrument lookup fails.

    Given: Session returning symbol_public_id but None for instrument,
    When: _resolve_instrument_public_id is called,
    Then: Returns None.
    """
    session = _FakeSession(["sym-pub-1", None])
    updater = _make_updater_with_session(session)
    result = updater._resolve_instrument_public_id("BTC-USD", "kraken", datetime.now(UTC))
    assert result is None


def _seed_resolver_db(
    tmp_path: Path, native_symbol: str = "BTC-USD", exchange: str = "kraken"
) -> tuple[StubMarketUpdater, str]:
    """Seed a sync repo with one Symbol+Instrument and return (updater, inst_pid)."""
    repo = DatabaseRepository(f"sqlite:///{tmp_path / 'md.db'}")
    repo.create_all()
    now = datetime(2024, 1, 1, tzinfo=UTC)
    with repo.session_factory() as session:
        sym = Symbol(
            native_symbol=native_symbol,
            base="BTC",
            quote="USD",
            asset_type="crypto",
            created_at=now,
            timestamp=now,
            session_id="s1",
            sequence_id=1,
        )
        session.add(sym)
        session.commit()
        session.refresh(sym)
        inst = Instrument(
            symbol_public_id=sym.public_id,
            exchange=exchange,
            requires_ai_review=False,
            timestamp=now,
            session_id="s1",
            sequence_id=2,
        )
        session.add(inst)
        session.commit()
        session.refresh(inst)
        inst_pid = inst.public_id
    updater = StubMarketUpdater()
    updater.repository = repo
    return updater, inst_pid


def test_resolve_batch_instrument_ids_resolves_via_join(tmp_path: Path) -> None:
    """The batch resolver maps a native symbol to its instrument in one JOIN.

    Given: a seeded Symbol+Instrument for BTC-USD on kraken,
    When: _resolve_batch_instrument_ids is called for {BTC-USD},
    Then: it returns {BTC-USD: instrument_public_id}.
    """
    updater, inst_pid = _seed_resolver_db(tmp_path)
    result = updater._resolve_batch_instrument_ids({"BTC-USD"}, "kraken", as_of=datetime.now(UTC))
    assert result == {"BTC-USD": inst_pid}


def test_resolve_batch_instrument_ids_skips_unresolved(tmp_path: Path) -> None:
    """Symbols with no active instrument are omitted from the result.

    Given: BTC-USD is resolvable but NOPE is not,
    When: _resolve_batch_instrument_ids is called for both,
    Then: only BTC-USD appears in the result.
    """
    updater, inst_pid = _seed_resolver_db(tmp_path)
    result = updater._resolve_batch_instrument_ids(
        {"BTC-USD", "NOPE"}, "kraken", as_of=datetime.now(UTC)
    )
    assert result == {"BTC-USD": inst_pid}


def test_resolve_batch_instrument_ids_exchange_filter(tmp_path: Path) -> None:
    """An instrument on a different exchange does not resolve.

    Given: BTC-USD's instrument exists only on kraken,
    When: _resolve_batch_instrument_ids is queried for exchange walutomat,
    Then: it returns an empty mapping.
    """
    updater, _ = _seed_resolver_db(tmp_path)
    result = updater._resolve_batch_instrument_ids(
        {"BTC-USD"}, "walutomat", as_of=datetime.now(UTC)
    )
    assert result == {}


def test_resolve_batch_instrument_ids_empty_input(tmp_path: Path) -> None:
    """An empty symbol set short-circuits without querying.

    Given: a seeded repo but an empty native-symbol set,
    When: _resolve_batch_instrument_ids is called,
    Then: it returns an empty mapping.
    """
    updater, _ = _seed_resolver_db(tmp_path)
    result = updater._resolve_batch_instrument_ids(set(), "kraken", as_of=datetime.now(UTC))
    assert result == {}


def test_persist_snapshots_scd2_empty_list() -> None:
    """Return 0 for empty snapshot list.

    Given: Empty snapshot list,
    When: _persist_snapshots_scd2 is called,
    Then: Returns 0 without opening session.
    """
    updater = StubMarketUpdater()
    result = updater._persist_snapshots_scd2([])
    assert result == 0


def test_persist_snapshots_scd2_calls_close_and_insert() -> None:
    """Persist snapshots via close_and_insert_sync.

    Given: List of two snapshots,
    When: _persist_snapshots_scd2 is called,
    Then: close_and_insert_sync called once per snapshot and session committed.
    """
    session = _FakeSession([])
    updater = _make_updater_with_session(session)
    now = datetime.now(UTC)
    snap1 = MarketSnapshot(
        instrument_public_id="inst-1",
        bid=100.0,
        bid_volume=1.0,
        ask=101.0,
        ask_volume=1.5,
        last_price=100.5,
        volume_24h=5000.0,
        vwap_24h=100.3,
        low_24h=99.0,
        high_24h=102.0,
        change_24h=1.0,
        spread=1.0,
        spread_pct=1.0,
        timestamp=now,
        session_id="s1",
        sequence_id=1,
    )
    snap2 = MarketSnapshot(
        instrument_public_id="inst-2",
        bid=200.0,
        bid_volume=2.0,
        ask=201.0,
        ask_volume=2.5,
        last_price=200.5,
        volume_24h=3000.0,
        vwap_24h=200.3,
        low_24h=199.0,
        high_24h=202.0,
        change_24h=0.5,
        spread=1.0,
        spread_pct=0.5,
        timestamp=now,
        session_id="s1",
        sequence_id=2,
    )
    with patch("snapper.infrastructure.market_data.base.close_and_insert_sync") as mock_ci:
        result = updater._persist_snapshots_scd2([snap1, snap2])
    assert result == 2
    assert mock_ci.call_count == 2
    assert session.committed is True
    first_call = mock_ci.call_args_list[0]
    assert first_call.kwargs["session"] is session
    assert first_call.kwargs["model"] is MarketSnapshot
    assert first_call.kwargs["bus_time"] == now


def test_persist_snapshots_scd2_passes_snapshot_timestamp_directly() -> None:
    """Pass snap.timestamp directly as bus_time without fallback.

    Given: Snapshot with explicit timestamp,
    When: _persist_snapshots_scd2 is called,
    Then: close_and_insert_sync receives the exact snapshot timestamp as bus_time.
    """
    session = _FakeSession([])
    updater = _make_updater_with_session(session)
    now = datetime.now(UTC)
    snap = MarketSnapshot(
        instrument_public_id="inst-1",
        bid=100.0,
        bid_volume=1.0,
        ask=101.0,
        ask_volume=1.5,
        last_price=100.5,
        volume_24h=5000.0,
        vwap_24h=100.3,
        low_24h=99.0,
        high_24h=102.0,
        change_24h=1.0,
        spread=1.0,
        spread_pct=1.0,
        timestamp=now,
        session_id="s1",
        sequence_id=1,
    )
    with patch("snapper.infrastructure.market_data.base.close_and_insert_sync") as mock_ci:
        updater._persist_snapshots_scd2([snap])
    bus_time = mock_ci.call_args.kwargs["bus_time"]
    assert bus_time is now
