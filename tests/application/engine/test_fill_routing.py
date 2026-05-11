"""Tests for wallet-safe fill routing in TraderCoordinator."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.trader import TraderCoordinator
from snapper.messaging.schemas.data import ExecutionData

WALLET_A = "01968a3b-7c4d-7e0f-8a1b-2c3d4e5f6a7b"
WALLET_B = "02978b4c-8d5e-8f10-9b2c-3d4e5f6a7b8c"
NOW = datetime(2026, 4, 13, tzinfo=UTC)


def _make_fill(
    client_order_id: str = "cid-1",
    instrument: str = "BTC-USD",
    exchange: str = "kraken",
    wallet_public_id: str = "",
) -> ExecutionData:
    """Create a minimal ExecutionData for fill routing tests."""
    return ExecutionData(
        public_id="fill-1",
        timestamp=NOW,
        session_id="s1",
        sequence_id=1,
        trade_id="t1",
        exchange_order_id="ex-1",
        client_order_id=client_order_id,
        instrument=instrument,
        exchange=exchange,
        side="buy",
        size=1.0,
        price=50000.0,
        last_size=1.0,
        last_price=50000.0,
        fee=0.5,
        fee_asset="USD",
        status="filled",
        executed_at=NOW,
        wallet_public_id=wallet_public_id,
    )


def _make_engine(
    instrument: str = "BTC-USD",
    exchange: str = "kraken",
    wallet_public_id: str = "",
    pending_client_order_id: str | None = None,
) -> TradingEngineService:
    """Create a minimal TradingEngineService stub for routing tests."""
    engine = TradingEngineService(
        instrument=instrument,
        execution_socket=AsyncMock(),
        exchange=exchange,
        wallet_public_id=wallet_public_id,
    )
    engine.pending_client_order_id = pending_client_order_id
    return engine


@patch("snapper.application.engine.trader.get_repository")
@patch("snapper.application.engine.trader.get_settings")
def _make_coordinator(
    mock_settings: MagicMock,
    mock_repo: MagicMock,
) -> TraderCoordinator:
    """Create a TraderCoordinator with mocked dependencies."""
    settings = MagicMock()
    settings.instruments = {}
    settings.db_url = "sqlite:///:memory:"
    settings.zmq_execution_sub = "sub"
    settings.zmq_signal_sub = "sub"
    settings.zmq_broker_xpub = "xpub"
    settings.zmq_broker_xsub = "xsub"
    settings.master_password = None
    settings.trading_mode = "paper"
    mock_settings.return_value = settings
    mock_repo.return_value = MagicMock()
    return TraderCoordinator(signal_topics=[])


class TestFindEngineForFill:
    """Tests for wallet-safe _find_engine_for_fill routing."""

    def test_exact_match_by_client_order_id(self) -> None:
        """Fill matched by pending_client_order_id ignores wallet mismatch.

        Given: engine with pending_client_order_id matching the fill,
        When: _find_engine_for_fill is called,
        Then: engine is returned (exact match takes priority).
        """
        coord = _make_coordinator()
        engine = _make_engine(wallet_public_id=WALLET_A, pending_client_order_id="cid-1")
        coord.engines = {"BTC-USD": engine}
        fill = _make_fill(client_order_id="cid-1", wallet_public_id=WALLET_B)
        assert coord._find_engine_for_fill(fill) is engine

    def test_wallet_scoped_fallback_matches_correct_wallet(self) -> None:
        """Fill with wallet routes to engine with matching wallet.

        Given: two engines for same instrument but different wallets,
        When: fill with wallet_B arrives (no client_order_id match),
        Then: engine with wallet_B is returned.
        """
        coord = _make_coordinator()
        engine_a = _make_engine(wallet_public_id=WALLET_A)
        engine_b = _make_engine(wallet_public_id=WALLET_B)
        coord.engines = {"BTC-USD.A": engine_a, "BTC-USD.B": engine_b}
        fill = _make_fill(client_order_id="unknown", wallet_public_id=WALLET_B)
        assert coord._find_engine_for_fill(fill) is engine_b

    def test_wallet_scoped_fallback_no_match_returns_none(self) -> None:
        """Fill with wallet that has no engine returns None.

        Given: engine for wallet_A only,
        When: fill with wallet_B arrives,
        Then: None is returned (no cross-wallet routing).
        """
        coord = _make_coordinator()
        engine_a = _make_engine(wallet_public_id=WALLET_A)
        coord.engines = {"BTC-USD": engine_a}
        fill = _make_fill(client_order_id="unknown", wallet_public_id=WALLET_B)
        assert coord._find_engine_for_fill(fill) is None

    def test_legacy_fallback_no_wallet_on_fill(self) -> None:
        """Fill without wallet uses legacy instrument+exchange fallback.

        Given: engine without wallet,
        When: fill without wallet arrives (no client_order_id match),
        Then: engine is returned via legacy path.
        """
        coord = _make_coordinator()
        engine = _make_engine(wallet_public_id="")
        coord.engines = {"BTC-USD": engine}
        fill = _make_fill(client_order_id="unknown", wallet_public_id="")
        assert coord._find_engine_for_fill(fill) is engine

    def test_legacy_fallback_returns_first_instrument_match(self) -> None:
        """Legacy path with no wallet returns first instrument match.

        Given: two engines for different wallets on same instrument,
        When: legacy fill (no wallet) arrives,
        Then: first matching engine is returned (legacy behavior).
        """
        coord = _make_coordinator()
        engine_a = _make_engine(wallet_public_id=WALLET_A)
        engine_b = _make_engine(wallet_public_id=WALLET_B)
        coord.engines = {"BTC-USD.A": engine_a, "BTC-USD.B": engine_b}
        fill = _make_fill(client_order_id="unknown", wallet_public_id="")
        result = coord._find_engine_for_fill(fill)
        assert result in (engine_a, engine_b)

    def test_no_engine_returns_none(self) -> None:
        """Fill for untracked instrument returns None.

        Given: engine for ETH-USD only,
        When: fill for BTC-USD arrives,
        Then: None is returned.
        """
        coord = _make_coordinator()
        engine = _make_engine(instrument="ETH-USD")
        coord.engines = {"ETH-USD": engine}
        fill = _make_fill(instrument="BTC-USD", client_order_id="unknown")
        assert coord._find_engine_for_fill(fill) is None


class TestFindEngineForFillIndexed:
    """Exercise the production O(1) lookup indices on the auto-indexing engines registry.

    These complement :py:class:`TestFindEngineForFill` (which uses
    direct ``coord.engines = {...}`` dict replacement to verify the
    fallback path still routes correctly) by inserting engines through
    the auto-indexing :py:class:`~snapper.application.engine.trader._EngineRegistry`
    that production code uses. A regression that drops the indices
    would silently fall through to the legacy O(n_engines) scan, so
    these tests pin the index contract directly.
    """

    def test_coid_index_resolves_pending_coid_after_assignment(self) -> None:
        """Setting pending_client_order_id auto-indexes the engine in the registry.

        Given: An engine inserted through the registry,
        When: ``engine.pending_client_order_id = coid`` is assigned,
        Then: ``coord._engines_by_pending_coid[coid] is engine`` — the
        property setter fires the registered listener on every change.
        """
        coord = _make_coordinator()
        engine = _make_engine()
        coord.engines["BTC-USD@kraken-paper"] = engine
        engine.pending_client_order_id = "cid-new"
        assert coord._engines_by_pending_coid.get("cid-new") is engine
        engine.pending_client_order_id = None
        assert "cid-new" not in coord._engines_by_pending_coid

    def test_scope_index_keyed_by_wallet_tuple(self) -> None:
        """Engine with wallet_public_id is keyed in the wallet-scoped index.

        Given: An engine registered with a non-empty wallet_public_id,
        When: A fill for the same (exchange, instrument, wallet) arrives,
        Then: ``_find_engine_by_fill_scope`` returns it via O(1) lookup
        without scanning ``self.engines``.
        """
        coord = _make_coordinator()
        engine = _make_engine(wallet_public_id=WALLET_A)
        coord.engines["BTC-USD@kraken-WA"] = engine
        assert coord._engines_by_scope.get(("kraken", "BTC-USD", WALLET_A)) is engine
        fill = _make_fill(client_order_id="unknown", wallet_public_id=WALLET_A)
        assert coord._find_engine_by_fill_scope(fill) is engine

    def test_legacy_scope_index_keyed_by_exchange_instrument(self) -> None:
        """Engine without wallet_public_id surfaces via legacy index.

        Given: An engine registered without wallet_public_id,
        When: A wallet-less fill matches (exchange, instrument),
        Then: legacy index returns the engine in O(1).
        """
        coord = _make_coordinator()
        engine = _make_engine(wallet_public_id="")
        coord.engines["BTC-USD@kraken-legacy"] = engine
        assert coord._engines_by_scope_legacy.get(("kraken", "BTC-USD")) is engine
        fill = _make_fill(client_order_id="unknown", wallet_public_id="")
        assert coord._find_engine_by_fill_scope(fill) is engine
