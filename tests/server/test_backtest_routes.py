"""Tests for backtest REST API endpoints."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency
from snapper.server.backtest_routes import _bt_repo
from snapper.server.backtest_routes import _normalise_pair
from snapper.server.backtest_routes import _resolve_as_of
from snapper.strategies.factory import StrategyFactory
from snapper.strategies.rsi import RSIReversion

NOW = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)
_MOCK_STRATEGIES: dict[str, Any] = {"sma_cross": MagicMock()}


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


def _make_run_row(
    public_id: str = "run-1",
    status: str = "pending",
    wallet: str = "wallet-1",
    target_execution_exchange: str | None = None,
) -> dict[str, Any]:
    """Build a minimal BacktestRunRow dict."""
    return {
        "public_id": public_id,
        "timestamp": NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "wallet_public_id": wallet,
        "operator_public_id": None,
        "strategy_name": "sma_cross",
        "strategy_params": {"fast": 10},
        "instrument_public_id": "BTC-USD",
        "exchange": "kraken",
        "mode": "paper",
        "timeframe": "1h",
        "start_date": NOW,
        "end_date": NOW + timedelta(days=30),
        "initial_cash": 10000.0,
        "status": status,
        "execution_mode": "direct_db",
        "fill_model": "market",
        "slippage_bps": 0.0,
        "commission_bps": 0.0,
        "target_execution_exchange": target_execution_exchange,
        "created_by_user_id": "test",
        "started_at": None,
        "completed_at": None,
        "error": None,
        "process_name": None,
    }


def _make_trade_row() -> dict[str, Any]:
    """Build a minimal BacktestTradeRow dict."""
    return {
        "public_id": "trade-1",
        "timestamp": NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "run_public_id": "run-1",
        "executed_at": NOW,
        "instrument": "BTC-USD",
        "side": "buy",
        "quantity": 0.5,
        "price": 50000.0,
        "fee": 25.0,
        "pnl": None,
        "position_after": 0.5,
        "signal_public_id": "sig-pid-linked",
    }


def _make_signal_row() -> dict[str, Any]:
    """Build a minimal BacktestSignalRow dict."""
    return {
        "public_id": "sig-1",
        "timestamp": NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "run_public_id": "run-1",
        "signal_time": NOW,
        "signal_type": "buy",
        "instrument": "BTC-USD",
        "price": 50000.0,
        "indicators": {"sma": 49000},
    }


def _make_event_row() -> dict[str, Any]:
    """Build a minimal BacktestEventRow dict."""
    return {
        "public_id": "evt-1",
        "timestamp": NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "run_public_id": "run-1",
        "event_type": "run_started",
        "detail": {"strategy": "sma_cross"},
    }


def _wrap_command(
    payload: dict[str, Any], cmd_type: str = "backtest_create_command"
) -> dict[str, Any]:
    """Wrap a payload dict in a provenance command envelope."""
    return {
        "type": cmd_type,
        "public_id": "req-1",
        "session_id": "s1",
        "sequence_id": 1,
        "timestamp": NOW.isoformat(),
        "payload": payload,
    }


def _create_body() -> dict[str, Any]:
    """Build a valid create command for POST /api/backtests."""
    return _wrap_command(
        {
            "strategy_class": "sma_cross",
            "instrument_public_id": "BTC-USD",
            "exchange": "kraken",
            "timeframe": "1h",
            "start_date": NOW.isoformat(),
            "end_date": (NOW + timedelta(days=30)).isoformat(),
            "initial_cash": 10000.0,
            "strategy_params": {},
        }
    )


def _cancel_body() -> dict[str, Any]:
    """Build a valid cancel command for POST /api/backtests/{id}/cancel."""
    return _wrap_command({"reason": "test"}, "backtest_cancel_command")


def _create_client(
    role: UserRole = UserRole.ADMIN,
    wallet: str | None = "wallet-1",
    launch_error: Exception | None = None,
    resolved_map: dict[str, str] | None = None,
    resolved_symbol: str | None = "BTC-USD",
) -> TestClient:
    """Create test client with mocked BacktestRepository and auth bypassed.

    The ``BacktestRepository`` mock is NOT injected here. Every caller
    installs it by patching ``backtest_routes._bt_repo``, which is the
    only wiring the routes actually read; the factory previously also
    registered a ``dependency_overrides`` entry for a placeholder
    callable no route depended on, which did nothing but suggest a
    second, non-existent injection path.

    Both wallet planes default to the same single-wallet answer, so a
    test that says nothing gets a wallet that is both readable and
    tradable. Tests that need the planes to disagree — a read grant
    without trade authority, or a revoked read grant — reach the mock
    through :func:`_repo_of` and override one plane.

    Args:
        role: Role the bypassed authentication dependency returns.
        wallet: Active wallet claim, and the wallet both plane lookups
            return. ``None`` clears the claim and empties both planes.
        launch_error: Optional exception raised by the process factory.
        resolved_map: Optional symbol-to-instrument resolution table.
        resolved_symbol: Optional reverse instrument-to-symbol answer.

    Returns:
        A ``TestClient`` wired to the mocked application.
    """
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    mock_settings = MagicMock()
    mock_settings.db_url = "sqlite://"
    app.state.settings = mock_settings
    app.state.rest_tracker = SequenceTracker()

    mock_factory = MagicMock()
    if launch_error:
        mock_factory.start_process = AsyncMock(side_effect=launch_error)
    else:
        mock_factory.start_process = AsyncMock()
    app.state.process_factory = mock_factory

    symbol_to_uuid = {"BTC-USD": "instrument-uuid-1"} if resolved_map is None else resolved_map

    def _resolve_by_symbol(*, native_symbol: str, exchange: str, as_of: Any) -> str | None:
        return symbol_to_uuid.get(native_symbol)

    mock_repo = MagicMock()
    mock_repo.session_factory = MagicMock()
    mock_repo.get_instrument_public_id_by_symbol = AsyncMock(side_effect=_resolve_by_symbol)
    mock_repo.get_symbol_for_instrument = AsyncMock(return_value=resolved_symbol)
    wallet_rows = [{"public_id": wallet}] if wallet is not None else []
    mock_repo.list_accessible_wallets_for_operators = AsyncMock(return_value=wallet_rows)
    mock_repo.list_readable_wallets_for_user = AsyncMock(return_value=wallet_rows)

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(
            username="test_user",
            role=role,
            active_wallet_public_id=wallet,
        )

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: mock_repo
    return TestClient(app)


def _repo_of(client: TestClient) -> MagicMock:
    """Return the repository mock one test client's app is wired to.

    Reaching the mock through the override keeps the trade-plane wallet
    set configurable per test without growing ``_create_client``'s
    argument list past its complexity baseline.
    """
    app = cast(FastAPI, client.app)
    return cast(MagicMock, app.dependency_overrides[get_repository_dependency]())


class TestListBacktests:
    """Tests for GET /api/backtests."""

    def test_list_empty(self) -> None:
        """Empty list returns count=0."""
        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests")
            assert response.status_code == 200
            data = response.json()
            assert data["count"] == 0
            assert data["payload"] == []
            client.close()

    def test_list_with_runs(self) -> None:
        """Returns runs from repository."""
        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[_make_run_row()])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests")
            assert response.status_code == 200
            data = response.json()
            assert data["count"] == 1
            assert data["payload"][0]["strategy_name"] == "sma_cross"
            client.close()


class TestListStrategyClasses:
    """Tests for GET /api/backtests/strategy-classes."""

    def test_returns_sorted_registry_keys(self) -> None:
        """Endpoint returns StrategyFactory registry keys, sorted, with count.

        Registers two sentinel keys in deliberately unsorted insertion order
        (reusing a real strategy class as a type-safe value) to prove the
        endpoint sorts them, then restores the registry. Reaching this static
        route at all also proves it is matched ahead of the dynamic
        ``/{run_id}`` route.
        """
        saved = dict(StrategyFactory.STRATEGY_CLASSES)
        try:
            StrategyFactory.STRATEGY_CLASSES.clear()
            StrategyFactory.STRATEGY_CLASSES["ZetaStrategy"] = RSIReversion
            StrategyFactory.STRATEGY_CLASSES["AlphaStrategy"] = RSIReversion
            client = _create_client()
            try:
                response = client.get("/api/backtests/strategy-classes")
            finally:
                client.close()
        finally:
            StrategyFactory.STRATEGY_CLASSES.clear()
            StrategyFactory.STRATEGY_CLASSES.update(saved)
        assert response.status_code == 200
        body = response.json()
        assert body["type"] == "backtest_strategy_class_list"
        assert body["payload"] == ["AlphaStrategy", "ZetaStrategy"]
        assert body["count"] == 2


class TestGetBacktest:
    """Tests for GET /api/backtests/{run_id}."""

    def test_get_found(self) -> None:
        """Existing run returns 200."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row())
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1")
            assert response.status_code == 200
            assert response.json()["payload"]["public_id"] == "run-1"
            client.close()

    def test_get_returns_phase2_fields_non_default(self) -> None:
        """Non-default execution-config fields round-trip through the response projection."""
        row = _make_run_row()
        row["execution_mode"] = "zmq_replay"
        row["fill_model"] = "market"
        row["slippage_bps"] = 12.5
        row["commission_bps"] = 7.5
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=row)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1")
            payload = response.json()["payload"]
            assert payload["execution_mode"] == "zmq_replay"
            assert payload["slippage_bps"] == pytest.approx(12.5)
            assert payload["commission_bps"] == pytest.approx(7.5)
            client.close()

    def test_get_not_found(self) -> None:
        """Missing run returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/nonexistent")
            assert response.status_code == 404
            client.close()

    def test_get_wallet_mismatch(self) -> None:
        """Run from different wallet returns 404 for scoped user."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other-wallet"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.get("/api/backtests/run-1")
            assert response.status_code == 404
            client.close()

    def test_get_completed_inlines_result(self) -> None:
        """Completed run returns inline result with all metrics."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(status="completed"))
        bt.get_result = AsyncMock(
            return_value={
                "total_trades": 10,
                "winning_trades": 7,
                "losing_trades": 3,
                "total_pnl": 1234.5,
                "max_drawdown": 0.12,
                "sharpe_ratio": 1.6,
                "win_rate": 0.7,
                "profit_factor": 2.1,
                "final_equity": 11234.5,
                "max_equity": 11500.0,
                "extra_metrics": {"trades_per_day": 0.4},
            }
        )
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1")
            payload = response.json()["payload"]
            assert payload["result"] is not None
            assert payload["result"]["total_trades"] == 10
            assert payload["result"]["sharpe_ratio"] == pytest.approx(1.6)
            assert payload["result"]["extra_metrics"] == {"trades_per_day": 0.4}
            client.close()

    def test_get_completed_no_result_row_yields_none(self) -> None:
        """Completed run without persisted result still returns 200 with result=None."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(status="completed"))
        bt.get_result = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1")
            assert response.json()["payload"]["result"] is None
            client.close()

    def test_get_pending_omits_result(self) -> None:
        """Pending run returns result=None without querying get_result."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(status="pending"))
        bt.get_result = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1")
            assert response.json()["payload"]["result"] is None
            bt.get_result.assert_not_called()
            client.close()

    def test_get_run_detail_exposes_all_advanced_metrics(self) -> None:
        """All 8 typed metrics surface on the detail response.

        Fails hard if any future metric addition forgets to extend the
        ``_project_inline_result`` helper.
        """
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(status="completed"))
        bt.get_result = AsyncMock(
            return_value={
                "total_trades": 10,
                "winning_trades": 7,
                "losing_trades": 3,
                "total_pnl": 1234.5,
                "max_drawdown": 0.12,
                "sharpe_ratio": 1.6,
                "win_rate": 0.7,
                "profit_factor": 2.1,
                "final_equity": 11234.5,
                "max_equity": 11500.0,
                "sortino_ratio": 1.9,
                "cagr": 0.18,
                "calmar_ratio": 1.5,
                "expectancy": 123.45,
                "avg_trade_pnl": 123.45,
                "max_drawdown_duration_seconds": 7200.0,
                "exposure_ratio": 0.8,
                "turnover_ratio": 4.2,
                "extra_metrics": {},
            }
        )
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1")
            payload = response.json()["payload"]["result"]
            assert payload["sortino_ratio"] == pytest.approx(1.9)
            assert payload["cagr"] == pytest.approx(0.18)
            assert payload["calmar_ratio"] == pytest.approx(1.5)
            assert payload["expectancy"] == pytest.approx(123.45)
            assert payload["avg_trade_pnl"] == pytest.approx(123.45)
            assert payload["max_drawdown_duration_seconds"] == pytest.approx(7200.0)
            assert payload["exposure_ratio"] == pytest.approx(0.8)
            assert payload["turnover_ratio"] == pytest.approx(4.2)
            client.close()

    def test_get_run_detail_json_fallback_for_pre_0005_rows(self) -> None:
        """Pre-0005 rows have the 5 promoted values only in extra_metrics.

        Read-side fallback collapses JSON into the typed slot without
        JSON truthiness collapsing a legitimate ``0.0`` typed column.
        """
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(status="completed"))
        bt.get_result = AsyncMock(
            return_value={
                "total_trades": 10,
                "winning_trades": 5,
                "losing_trades": 5,
                "total_pnl": 0.0,
                "max_drawdown": 0.1,
                "sharpe_ratio": 1.0,
                "win_rate": 0.5,
                "profit_factor": 1.0,
                "final_equity": 10000.0,
                "max_equity": 10500.0,
                "sortino_ratio": None,
                "cagr": None,
                "calmar_ratio": None,
                "expectancy": None,
                "avg_trade_pnl": None,
                "max_drawdown_duration_seconds": None,
                "exposure_ratio": None,
                "turnover_ratio": None,
                "extra_metrics": {
                    "sortino_ratio": 1.4,
                    "cagr": 0.08,
                    "calmar_ratio": 0.8,
                    "expectancy": 0.0,
                    "avg_trade_pnl": 0.0,
                },
            }
        )
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1")
            payload = response.json()["payload"]["result"]
            assert payload["sortino_ratio"] == pytest.approx(1.4)
            assert payload["cagr"] == pytest.approx(0.08)
            assert payload["calmar_ratio"] == pytest.approx(0.8)
            assert payload["expectancy"] == pytest.approx(0.0)
            assert payload["avg_trade_pnl"] == pytest.approx(0.0)
            assert payload["max_drawdown_duration_seconds"] is None
            assert payload["exposure_ratio"] is None
            assert payload["turnover_ratio"] is None
            client.close()

    def test_get_run_detail_strips_promoted_names_from_extra_metrics(self) -> None:
        """Pre-0005 promoted-name JSON keys surface only via typed fields.

        Defends against the double-emission gap where a mixed-vintage
        row exposes the 5 promoted metrics once in the typed slot AND
        again inside ``extra_metrics``. ``PROMOTED_METRIC_NAMES`` is
        the source of truth for what gets stripped.
        """
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(status="completed"))
        bt.get_result = AsyncMock(
            return_value={
                "total_trades": 3,
                "winning_trades": 2,
                "losing_trades": 1,
                "total_pnl": 50.0,
                "max_drawdown": 0.05,
                "sharpe_ratio": 1.1,
                "win_rate": 0.66,
                "profit_factor": 1.5,
                "final_equity": 10050.0,
                "max_equity": 10100.0,
                "sortino_ratio": None,
                "cagr": None,
                "calmar_ratio": None,
                "expectancy": None,
                "avg_trade_pnl": None,
                "max_drawdown_duration_seconds": None,
                "exposure_ratio": None,
                "turnover_ratio": None,
                "extra_metrics": {
                    "sortino_ratio": 1.4,
                    "cagr": 0.08,
                    "calmar_ratio": 0.8,
                    "expectancy": 12.5,
                    "avg_trade_pnl": 12.5,
                    "custom_non_promoted": 42.0,
                },
            }
        )
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1")
            payload = response.json()["payload"]["result"]
            assert payload["sortino_ratio"] == pytest.approx(1.4)
            assert payload["cagr"] == pytest.approx(0.08)
            assert payload["expectancy"] == pytest.approx(12.5)
            promoted_keys = {
                "sortino_ratio",
                "cagr",
                "calmar_ratio",
                "expectancy",
                "avg_trade_pnl",
            }
            assert set(payload["extra_metrics"]).isdisjoint(promoted_keys)
            assert payload["extra_metrics"]["custom_non_promoted"] == pytest.approx(42.0)
            client.close()

    def test_get_run_detail_typed_zero_beats_json_fallback(self) -> None:
        """Post-0005 row with ``expectancy=0.0`` must NOT be overridden by JSON.

        Guards against Python truthiness trap (``x or y`` collapses 0.0).
        """
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(status="completed"))
        bt.get_result = AsyncMock(
            return_value={
                "total_trades": 5,
                "winning_trades": 2,
                "losing_trades": 3,
                "total_pnl": 0.0,
                "max_drawdown": 0.0,
                "sharpe_ratio": None,
                "win_rate": 0.4,
                "profit_factor": 1.0,
                "final_equity": 10000.0,
                "max_equity": 10000.0,
                "sortino_ratio": 0.0,
                "cagr": 0.0,
                "calmar_ratio": 0.0,
                "expectancy": 0.0,
                "avg_trade_pnl": 0.0,
                "max_drawdown_duration_seconds": None,
                "exposure_ratio": 0.0,
                "turnover_ratio": 0.0,
                "extra_metrics": {
                    "sortino_ratio": 999.9,
                    "cagr": 999.9,
                    "calmar_ratio": 999.9,
                    "expectancy": 999.9,
                    "avg_trade_pnl": 999.9,
                },
            }
        )
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1")
            payload = response.json()["payload"]["result"]
            assert payload["sortino_ratio"] == pytest.approx(0.0)
            assert payload["cagr"] == pytest.approx(0.0)
            assert payload["calmar_ratio"] == pytest.approx(0.0)
            assert payload["expectancy"] == pytest.approx(0.0)
            assert payload["avg_trade_pnl"] == pytest.approx(0.0)
            assert payload["exposure_ratio"] == pytest.approx(0.0)
            assert payload["turnover_ratio"] == pytest.approx(0.0)
            client.close()


class TestGetBacktestEquity:
    """Tests for GET /api/backtests/{run_id}/equity."""

    @staticmethod
    def _make_equity_row(point_time: datetime, equity: float) -> dict[str, Any]:
        return {
            "public_id": f"eq-{equity}",
            "timestamp": point_time,
            "session_id": "s1",
            "sequence_id": 1,
            "run_public_id": "run-1",
            "point_time": point_time,
            "equity": equity,
            "cash": equity,
            "position_value": 0.0,
            "drawdown": 0.0,
        }

    def test_equity_returns_paginated_points(self) -> None:
        """Endpoint returns points ordered by point_time and respects pagination args."""
        rows = [self._make_equity_row(NOW + timedelta(minutes=i), 10000.0 + i) for i in range(3)]
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row())
        bt.get_equity_points = AsyncMock(return_value=rows)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            cursor = NOW.isoformat().replace("+", "%2B")
            response = client.get(f"/api/backtests/run-1/equity?limit=50&after={cursor}")
            assert response.status_code == 200
            data = response.json()
            assert data["count"] == 3
            assert data["payload"][0]["equity"] == pytest.approx(10000.0)
            kwargs = bt.get_equity_points.await_args.kwargs
            assert kwargs["limit"] == 50
            assert kwargs["after"] is not None
            client.close()

    def test_equity_404_when_run_missing(self) -> None:
        """Missing run returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/missing/equity")
            assert response.status_code == 404
            client.close()

    def test_equity_404_when_wallet_mismatch(self) -> None:
        """Cross-wallet read returns 404 (not 403, matching list endpoint)."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.get("/api/backtests/run-1/equity")
            assert response.status_code == 404
            client.close()

    def test_equity_limit_above_cap_rejected(self) -> None:
        """Limit > 20000 fails query validation (422)."""
        bt = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1/equity?limit=20001")
            assert response.status_code == 422
            client.close()


class TestCancelBacktest:
    """Tests for POST /api/backtests/{run_id}/cancel."""

    def test_cancel_running(self) -> None:
        """Running run transitions to cancel_requested."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(
            side_effect=[
                _make_run_row(status="running"),
                _make_run_row(status="cancel_requested"),
            ]
        )
        bt.update_run_status = AsyncMock(return_value=1)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.post("/api/backtests/run-1/cancel", json=_cancel_body())
            assert response.status_code == 200
            assert response.json()["payload"]["status"] == "cancel_requested"
            client.close()

    def test_cancel_completed_409(self) -> None:
        """Completed run returns 409."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(status="completed"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.post("/api/backtests/run-1/cancel", json=_cancel_body())
            assert response.status_code == 409
            client.close()

    def test_cancel_not_found(self) -> None:
        """Missing run returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.post("/api/backtests/run-1/cancel", json=_cancel_body())
            assert response.status_code == 404
            client.close()


class TestGetTrades:
    """Tests for GET /api/backtests/{run_id}/trades."""

    def test_trades_found(self) -> None:
        """Returns trades for existing run."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row())
        bt.get_trades = AsyncMock(return_value=[_make_trade_row()])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1/trades")
            assert response.status_code == 200
            data = response.json()
            assert data["count"] == 1
            assert data["payload"][0]["side"] == "buy"
            assert data["payload"][0]["signal_public_id"] == "sig-pid-linked"
            client.close()

    def test_trades_run_not_found(self) -> None:
        """Missing run returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1/trades")
            assert response.status_code == 404
            client.close()


class TestGetSignals:
    """Tests for GET /api/backtests/{run_id}/signals."""

    def test_signals_found(self) -> None:
        """Returns signals for existing run."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row())
        bt.get_signals = AsyncMock(return_value=[_make_signal_row()])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1/signals")
            assert response.status_code == 200
            data = response.json()
            assert data["count"] == 1
            assert data["payload"][0]["signal_type"] == "buy"
            client.close()


class TestGetEvents:
    """Tests for GET /api/backtests/{run_id}/events."""

    def test_events_found(self) -> None:
        """Returns events for existing run."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row())
        bt.get_events = AsyncMock(return_value=[_make_event_row()])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1/events")
            assert response.status_code == 200
            data = response.json()
            assert data["count"] == 1
            assert data["payload"][0]["event_type"] == "run_started"
            client.close()


@patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", _MOCK_STRATEGIES)
class TestCreateBacktest:
    """Tests for POST /api/backtests."""

    def test_create_success(self) -> None:
        """Valid create body launches runner and returns run."""
        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-new"))
        bt.get_run = AsyncMock(return_value=_make_run_row(public_id="run-new"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.post("/api/backtests", json=_create_body())
            assert response.status_code == 200
            assert response.json()["payload"]["public_id"] == "run-new"
            client.close()

    def test_create_resolves_symbol_to_instrument_public_id(self) -> None:
        """The submitted native symbol is resolved to a UUID before the INSERT.

        Guards the Postgres regression where a raw symbol reached the
        ``UUID``-typed ``instrument_public_id`` column and raised a DataError.
        """
        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-new"))
        bt.get_run = AsyncMock(return_value=_make_run_row(public_id="run-new"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.post("/api/backtests", json=_create_body())
            assert response.status_code == 200
            inserted = bt.create_run.await_args.kwargs["row"]
            assert inserted["instrument_public_id"] == "instrument-uuid-1"
            assert inserted["instrument_public_id"] != "BTC-USD"
            client.close()

    def test_create_unknown_instrument_returns_422(self) -> None:
        """A symbol that resolves to no instrument returns 422, not a 500/DataError."""
        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-new"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1", resolved_map={})
            response = client.post("/api/backtests", json=_create_body())
            assert response.status_code == 422
            assert response.json()["detail"]["error_code"] == "unknown_instrument"
            bt.create_run.assert_not_called()
            client.close()

    def test_create_accepts_existing_instrument_uuid(self) -> None:
        """A UUID reference (e.g. rerun replay) is accepted and stored normalized.

        The submitted UUID is upper-case; the resolver canonicalises it before the
        exchange round-trip and the INSERT, so the stored value is the normalized
        lower-case form (not the raw input).
        """
        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-new"))
        bt.get_run = AsyncMock(return_value=_make_run_row(public_id="run-new"))
        body = _create_body()
        body["payload"]["instrument_public_id"] = "019EDA8D-9D34-73DE-B365-920301E26549"
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(
                wallet="wallet-1",
                resolved_map={"BTC-USD": "019eda8d-9d34-73de-b365-920301e26549"},
                resolved_symbol="BTC-USD",
            )
            response = client.post("/api/backtests", json=body)
            assert response.status_code == 200
            inserted = bt.create_run.await_args.kwargs["row"]
            assert inserted["instrument_public_id"] == "019eda8d-9d34-73de-b365-920301e26549"
            client.close()

    def test_create_rejects_nonexistent_instrument_uuid(self) -> None:
        """A UUID that resolves to no active instrument returns 422."""
        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-new"))
        body = _create_body()
        body["payload"]["instrument_public_id"] = "019eda8d-9d34-73de-b365-920301e26549"
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(
                wallet="wallet-1",
                resolved_map={},
                resolved_symbol=None,
            )
            response = client.post("/api/backtests", json=body)
            assert response.status_code == 422
            bt.create_run.assert_not_called()
            client.close()

    def test_create_rejects_instrument_uuid_on_wrong_exchange(self) -> None:
        """A UUID whose instrument is not on the submitted exchange returns 422.

        The UUID resolves to a symbol, but that symbol on the requested exchange
        maps to a different instrument public_id, so the reference is rejected.
        """
        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-new"))
        body = _create_body()
        body["payload"]["instrument_public_id"] = "019eda8d-9d34-73de-b365-920301e26549"
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(
                wallet="wallet-1",
                resolved_map={"BTC-USD": "different-instrument-uuid"},
                resolved_symbol="BTC-USD",
            )
            response = client.post("/api/backtests", json=body)
            assert response.status_code == 422
            bt.create_run.assert_not_called()
            client.close()

    def test_create_launch_failure_500(self) -> None:
        """Process launch failure returns 500 and marks run failed."""
        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-fail"))
        bt.update_run_status = AsyncMock(return_value=1)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1", launch_error=RuntimeError("no thread"))
            response = client.post("/api/backtests", json=_create_body())
            assert response.status_code == 500
            bt.update_run_status.assert_called_once()
            assert bt.update_run_status.call_args.kwargs["new_status"] == "failed"
            client.close()

    def test_create_with_cross_asset_target_exchange(self) -> None:
        """Cross-asset run sets target_execution_exchange on the persisted row."""
        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-cross"))
        bt.get_run = AsyncMock(
            return_value=_make_run_row(public_id="run-cross", target_execution_exchange="kraken")
        )
        body = _create_body()
        body["payload"]["target_execution_exchange"] = "kraken"
        body["payload"]["exchange"] = "kraken_futures"
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.post("/api/backtests", json=body)

            assert response.status_code == 200
            assert response.json()["payload"]["target_execution_exchange"] == "kraken"
            inserted = bt.create_run.call_args.kwargs["row"]

            assert inserted["target_execution_exchange"] == "kraken"
            assert inserted["exchange"] == "kraken_futures"
            client.close()

    def test_create_rejects_invalid_target_exchange(self) -> None:
        """Non order-capable target exchange rejected by request schema validator."""
        bt = AsyncMock()
        body = _create_body()
        body["payload"]["target_execution_exchange"] = "kraken_equities"
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.post("/api/backtests", json=body)

            assert response.status_code == 422
            bt.create_run.assert_not_called()
            client.close()

    def test_create_omits_target_exchange_persists_null(self) -> None:
        """Single-exchange create body persists target_execution_exchange as None."""
        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-single"))
        bt.get_run = AsyncMock(return_value=_make_run_row(public_id="run-single"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.post("/api/backtests", json=_create_body())

            assert response.status_code == 200
            assert response.json()["payload"]["target_execution_exchange"] is None
            inserted = bt.create_run.call_args.kwargs["row"]

            assert inserted["target_execution_exchange"] is None
            client.close()


class TestWalletScopingOnSubResources:
    """Tests for wallet scoping on trades/signals/events endpoints."""

    def test_trades_wallet_mismatch(self) -> None:
        """Trades for run owned by different wallet returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.get("/api/backtests/run-1/trades")
            assert response.status_code == 404
            client.close()

    def test_signals_wallet_mismatch(self) -> None:
        """Signals for run owned by different wallet returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.get("/api/backtests/run-1/signals")
            assert response.status_code == 404
            client.close()

    def test_events_wallet_mismatch(self) -> None:
        """Events for run owned by different wallet returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.get("/api/backtests/run-1/events")
            assert response.status_code == 404
            client.close()

    def test_signals_run_not_found(self) -> None:
        """Missing run on signals endpoint returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1/signals")
            assert response.status_code == 404
            client.close()

    def test_events_run_not_found(self) -> None:
        """Missing run on events endpoint returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/run-1/events")
            assert response.status_code == 404
            client.close()

    def test_cancel_wallet_mismatch(self) -> None:
        """Cancel for run owned by different wallet returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.post("/api/backtests/run-1/cancel", json=_cancel_body())
            assert response.status_code == 404
            client.close()


class TestResolveAsOf:
    """Tests for _resolve_as_of and _bt_repo helpers."""

    def test_resolve_as_of_none_returns_now(self) -> None:
        """None as_of falls back to now()."""
        result = _resolve_as_of(None)
        assert result.tzinfo is not None

    def test_resolve_as_of_passes_through(self) -> None:
        """Explicit as_of passes through unchanged."""
        result = _resolve_as_of(NOW)
        assert result == NOW

    def test_bt_repo_builds_from_session_factory(self) -> None:
        """_bt_repo builds BacktestRepository from session_factory."""
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        result = _bt_repo(mock_repo)
        assert result is not None


@patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", _MOCK_STRATEGIES)
class TestCreateWalletGuard:
    """Tests for wallet guard on POST /api/backtests."""

    def test_no_wallet_returns_400(self) -> None:
        """Create without active wallet returns 400."""
        bt = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet=None)
            response = client.post("/api/backtests", json=_create_body())
            assert response.status_code == 400
            assert "wallet" in response.json()["detail"].lower()
            client.close()


class TestCreateEdgeCases:
    """Edge cases for POST /api/backtests (create + cancel post-get failures)."""

    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", _MOCK_STRATEGIES)
    def test_create_run_vanishes_after_insert(self) -> None:
        """Run created but not found after insert returns 500."""
        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-ghost"))
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.post("/api/backtests", json=_create_body())
            assert response.status_code == 500
            assert "not found" in response.json()["detail"]
            client.close()

    def test_cancel_updated_run_vanishes(self) -> None:
        """Cancel succeeds but re-read returns None → 500."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(side_effect=[_make_run_row(status="running"), None])
        bt.update_run_status = AsyncMock(return_value=1)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.post("/api/backtests/run-1/cancel", json=_cancel_body())
            assert response.status_code == 500
            assert "not found" in response.json()["detail"]
            client.close()


@patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", _MOCK_STRATEGIES)
class TestRerunBacktest:
    """Tests for POST /api/backtests/{run_id}/rerun."""

    def test_rerun_not_found(self) -> None:
        """Missing original run returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.post("/api/backtests/run-1/rerun")
            assert response.status_code == 404
            client.close()

    def test_rerun_wallet_mismatch(self) -> None:
        """Rerun with wallet mismatch returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.post("/api/backtests/run-1/rerun")
            assert response.status_code == 404
            client.close()

    def test_rerun_success(self) -> None:
        """Rerun creates new run with same config."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(
            side_effect=[
                _make_run_row(status="completed"),
                _make_run_row(public_id="run-rerun"),
            ]
        )
        bt.create_run = AsyncMock(return_value=(2, "run-rerun"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.post("/api/backtests/run-1/rerun")
            assert response.status_code == 200
            assert response.json()["payload"]["public_id"] == "run-rerun"
            client.close()

    def test_rerun_preserves_cross_asset_target_exchange(self) -> None:
        """Rerun of a cross-asset run carries target_execution_exchange forward."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(
            side_effect=[
                _make_run_row(status="completed", target_execution_exchange="kraken"),
                _make_run_row(public_id="run-rerun", target_execution_exchange="kraken"),
            ]
        )
        bt.create_run = AsyncMock(return_value=(2, "run-rerun"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet="wallet-1")
            response = client.post("/api/backtests/run-1/rerun")

            assert response.status_code == 200
            inserted = bt.create_run.call_args.kwargs["row"]

            assert inserted["target_execution_exchange"] == "kraken"
            client.close()


def _make_equity_row(equity: float = 10_000.0) -> dict[str, Any]:
    """Build a minimal BacktestEquityPointRow dict."""
    return {
        "public_id": f"ep-{equity}",
        "timestamp": NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "run_public_id": "run-1",
        "point_time": NOW,
        "equity": equity,
        "cash": equity,
        "position_value": 0.0,
        "drawdown": 0.0,
    }


def _make_comparison_row(
    public_id: str = "cmp-1",
    run_a: str = "run-aa",
    run_b: str = "run-bb",
    pairing_mode: str = "auto",
    config_hash: str | None = "h" * 64,
) -> dict[str, Any]:
    """Build a minimal BacktestComparisonRow dict."""
    return {
        "public_id": public_id,
        "timestamp": NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "created_by_user_id": "test_user",
        "run_a_public_id": run_a,
        "run_b_public_id": run_b,
        "config_hash": config_hash,
        "pairing_mode": pairing_mode,
        "anchor_run_public_id": None,
    }


def _wrap_compare(payload: dict[str, Any]) -> dict[str, Any]:
    """Build a compare-request envelope."""
    return _wrap_command(payload, "backtest_compare_request")


class TestNoActiveWalletGuards:
    """Cover backtest_routes 400 fail-closed branches for missing wallets."""

    def test_list_no_active_wallet_returns_400(self) -> None:
        """GET /api/backtests with no active wallet → 400.

        Given: A principal with active_wallet_public_id=None,
        When: GET /api/backtests is called,
        Then: 400 with no active wallet detail.
        """
        bt = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet=None)
            response = client.get("/api/backtests")
            assert response.status_code == 400
            client.close()

    def test_get_detail_no_active_wallet_returns_400(self) -> None:
        """GET /api/backtests/{id} with no active wallet → 400.

        Given: A principal with active_wallet_public_id=None,
        When: GET /api/backtests/run-1 is called and the run exists,
        Then: 400 fail-closed response.
        """
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row())
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet=None)
            response = client.get("/api/backtests/run-1")
            assert response.status_code == 400
            client.close()


class TestCompareCreate:
    """POST /api/backtests/compare — auto + manual flows."""

    @pytest.mark.parametrize(
        ("role", "expected_status"),
        [
            pytest.param(UserRole.AI_RESEARCHER, 403, id="ai-researcher-denied"),
            pytest.param(UserRole.AI_REVIEWER, 409, id="ai-reviewer-allowed"),
            pytest.param(UserRole.AI_DELEGATE, 409, id="ai-delegate-allowed"),
            pytest.param(UserRole.VIEWER, 403, id="viewer-denied"),
            pytest.param(UserRole.OPERATOR, 409, id="operator-allowed"),
            pytest.param(UserRole.ADMIN, 409, id="admin-allowed"),
        ],
    )
    def test_role_permission_matrix(self, role: UserRole, expected_status: int) -> None:
        """Comparison persistence follows CREATE_BACKTEST_COMPARISONS.

        Given: Each named permission set with a selected wallet that the
            fixture also makes TRADABLE (``_create_client`` returns it from
            ``list_accessible_wallets_for_operators``), isolating the
            permission gate from the trade-plane wallet gate,
        When: The principal requests a valid auto comparison with no runs,
        Then: Granted principals reach the handler's 409 and denied principals get 403.
        """
        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(role=role)
            body = _wrap_compare({"mode": "auto", "config_hash": "a" * 64})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == expected_status
            if expected_status == 403:
                bt.list_runs.assert_not_awaited()
            else:
                bt.list_runs.assert_awaited_once()
            client.close()

    def test_no_active_wallet_returns_400(self) -> None:
        """Caller without active wallet → 400.

        Given: A principal with active_wallet_public_id=None,
        When: POST /api/backtests/compare runs,
        Then: 400 with no active wallet detail.
        """
        bt = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet=None)
            body = _wrap_compare({"mode": "auto", "config_hash": "a" * 64})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 400
            client.close()

    def test_auto_missing_config_hash_returns_422(self) -> None:
        """Auto mode without config_hash → 422.

        Given: mode=auto and config_hash omitted,
        When: POST /api/backtests/compare runs,
        Then: 422 with config_hash-required detail.
        """
        bt = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare({"mode": "auto"})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 422
            client.close()

    def test_auto_too_few_terminal_runs_returns_409(self) -> None:
        """Auto mode with fewer than 2 terminal runs → 409.

        Given: list_runs returns 1 terminal candidate,
        When: POST /api/backtests/compare with mode=auto,
        Then: 409 not enough runs.
        """
        bt = AsyncMock()
        bt.list_runs = AsyncMock(
            return_value=[_make_run_row(public_id="run-only", status="completed")]
        )
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare({"mode": "auto", "config_hash": "a" * 64})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 409
            client.close()

    def test_auto_creates_pair_from_two_terminal(self) -> None:
        """Two terminal runs → comparison created.

        Given: list_runs returns 2 terminal candidates,
        When: POST /api/backtests/compare with mode=auto,
        Then: 200 with the created comparison payload.
        """
        bt = AsyncMock()
        bt.list_runs = AsyncMock(
            return_value=[
                _make_run_row(public_id="run-a", status="completed"),
                _make_run_row(public_id="run-b", status="completed"),
            ]
        )
        bt.get_comparison_by_pair = AsyncMock(return_value=None)
        bt.create_comparison = AsyncMock(return_value=(1, "cmp-new"))
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row(public_id="cmp-new"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare({"mode": "auto", "config_hash": "a" * 64})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 200
            assert response.json()["payload"]["public_id"] == "cmp-new"
            client.close()

    def test_auto_anchor_not_found_404(self) -> None:
        """Anchor public_id absent from candidates → 404.

        Given: anchor_run_public_id not in list_runs result,
        When: POST /api/backtests/compare,
        Then: 404 anchor not found.
        """
        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "auto",
                    "config_hash": "a" * 64,
                    "anchor_run_public_id": "missing",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 404
            client.close()

    def test_auto_anchor_not_terminal_409(self) -> None:
        """Anchor present but not terminal → 409.

        Given: anchor row exists but status='running',
        When: POST /api/backtests/compare,
        Then: 409 anchor not terminal.
        """
        bt = AsyncMock()
        bt.list_runs = AsyncMock(
            return_value=[_make_run_row(public_id="anchor-1", status="running")]
        )
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "auto",
                    "config_hash": "a" * 64,
                    "anchor_run_public_id": "anchor-1",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 409
            client.close()

    def test_auto_anchor_null_hash_409(self) -> None:
        """Anchor exists, terminal, but config_hash is None → 409.

        Given: anchor present without config_hash,
        When: POST /api/backtests/compare with auto mode,
        Then: 409 anchor has no config_hash.
        """
        run_no_hash = _make_run_row(public_id="anchor-1", status="completed")
        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[run_no_hash])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "auto",
                    "config_hash": "a" * 64,
                    "anchor_run_public_id": "anchor-1",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 409
            client.close()

    def test_auto_anchor_hash_mismatch_422(self) -> None:
        """Anchor terminal with different config_hash → 422.

        Given: anchor has config_hash != request,
        When: POST /api/backtests/compare,
        Then: 422 hash mismatch.
        """
        run = _make_run_row(public_id="anchor-1", status="completed")
        run["config_hash"] = "b" * 64
        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[run])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "auto",
                    "config_hash": "a" * 64,
                    "anchor_run_public_id": "anchor-1",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 422
            client.close()

    def test_auto_anchor_no_counterpart_409(self) -> None:
        """Anchor valid but only candidate → 409.

        Given: anchor is the only terminal candidate,
        When: POST /api/backtests/compare,
        Then: 409 no counterpart.
        """
        anchor = _make_run_row(public_id="anchor-1", status="completed")
        anchor["config_hash"] = "a" * 64
        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[anchor])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "auto",
                    "config_hash": "a" * 64,
                    "anchor_run_public_id": "anchor-1",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 409
            client.close()

    def test_auto_anchor_with_counterpart_creates(self) -> None:
        """Anchor + other terminal → comparison created.

        Given: anchor + one other terminal candidate,
        When: POST /api/backtests/compare,
        Then: 200 with created comparison.
        """
        anchor = _make_run_row(public_id="anchor-1", status="completed")
        anchor["config_hash"] = "a" * 64
        other = _make_run_row(public_id="other-1", status="completed")
        other["config_hash"] = "a" * 64
        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[anchor, other])
        bt.get_comparison_by_pair = AsyncMock(return_value=None)
        bt.create_comparison = AsyncMock(return_value=(1, "cmp-new"))
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row())
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "auto",
                    "config_hash": "a" * 64,
                    "anchor_run_public_id": "anchor-1",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 200
            client.close()

    def test_auto_no_anchor_prefers_cross_execution_mode(self) -> None:
        """No-anchor auto-pair picks cross-mode partner.

        Given:
            Three recent terminal runs for the same config_hash —
            most recent is a direct_db run, second-most-recent is
            another direct_db run, third is a zmq_replay run.

        When:
            POST /api/backtests/compare with mode=auto and no anchor.

        Then:
            The pair resolves to (direct_db_most_recent,
            zmq_replay) rather than the two same-mode direct_db runs
            — Direct-DB and ZMQ-replay are comparable on the same
            config by design and we prefer the cross-mode partner
            when available.
        """
        direct_new = _make_run_row(public_id="direct-new", status="completed")
        direct_new["config_hash"] = "a" * 64
        direct_new["execution_mode"] = "direct_db"
        direct_old = _make_run_row(public_id="direct-old", status="completed")
        direct_old["config_hash"] = "a" * 64
        direct_old["execution_mode"] = "direct_db"
        zmq_run = _make_run_row(public_id="zmq-run", status="completed")
        zmq_run["config_hash"] = "a" * 64
        zmq_run["execution_mode"] = "zmq_replay"
        captured: dict[str, Any] = {}

        async def fake_create_comparison(row: dict[str, Any], **_kwargs: Any) -> tuple[int, str]:
            captured["run_a_public_id"] = row["run_a_public_id"]
            captured["run_b_public_id"] = row["run_b_public_id"]
            return 1, "cmp-new"

        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[direct_new, direct_old, zmq_run])
        bt.get_comparison_by_pair = AsyncMock(return_value=None)
        bt.create_comparison = fake_create_comparison
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row())
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare({"mode": "auto", "config_hash": "a" * 64})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 200
            normalised = _normalise_pair("direct-new", "zmq-run")
            assert captured["run_a_public_id"] == normalised[0]
            assert captured["run_b_public_id"] == normalised[1]
            client.close()

    def test_auto_no_anchor_falls_back_to_same_mode_when_no_cross(self) -> None:
        """No-anchor auto-pair falls back when no cross-mode.

        Given:
            Two recent terminal runs, both direct_db.

        When:
            POST /api/backtests/compare with mode=auto and no anchor.

        Then:
            The pair resolves to (terminal[0], terminal[1]) as
            before — the cross-mode preference is a preference, not a
            requirement; a single-mode wallet is still allowed to
            auto-compare its two most-recent runs.
        """
        r0 = _make_run_row(public_id="run-0", status="completed")
        r0["config_hash"] = "a" * 64
        r0["execution_mode"] = "direct_db"
        r1 = _make_run_row(public_id="run-1", status="completed")
        r1["config_hash"] = "a" * 64
        r1["execution_mode"] = "direct_db"
        captured: dict[str, Any] = {}

        async def fake_create_comparison(row: dict[str, Any], **_kwargs: Any) -> tuple[int, str]:
            captured["pair"] = (row["run_a_public_id"], row["run_b_public_id"])
            return 1, "cmp-new"

        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[r0, r1])
        bt.get_comparison_by_pair = AsyncMock(return_value=None)
        bt.create_comparison = fake_create_comparison
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row())
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare({"mode": "auto", "config_hash": "a" * 64})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 200
            assert captured["pair"] == _normalise_pair("run-0", "run-1")
            client.close()

    def test_auto_anchor_prefers_opposite_execution_mode(self) -> None:
        """Anchored auto-pair picks opposite-mode counterpart.

        Given:
            Direct-DB anchor plus two candidates — a more-recent
            direct_db non-anchor and an older zmq_replay run.

        When:
            POST /api/backtests/compare with mode=auto and the
            direct_db anchor.

        Then:
            Counterpart resolves to the zmq_replay run, not the
            direct_db run, even though the direct_db candidate is
            more recent in list order. Anchored pair prefers the
            **opposite** execution_mode when available.
        """
        anchor = _make_run_row(public_id="anchor-direct", status="completed")
        anchor["config_hash"] = "a" * 64
        anchor["execution_mode"] = "direct_db"
        newer_direct = _make_run_row(public_id="direct-newer", status="completed")
        newer_direct["config_hash"] = "a" * 64
        newer_direct["execution_mode"] = "direct_db"
        older_zmq = _make_run_row(public_id="zmq-older", status="completed")
        older_zmq["config_hash"] = "a" * 64
        older_zmq["execution_mode"] = "zmq_replay"
        captured: dict[str, Any] = {}

        async def fake_create_comparison(row: dict[str, Any], **_kwargs: Any) -> tuple[int, str]:
            captured["pair"] = (row["run_a_public_id"], row["run_b_public_id"])
            return 1, "cmp-new"

        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[anchor, newer_direct, older_zmq])
        bt.get_comparison_by_pair = AsyncMock(return_value=None)
        bt.create_comparison = fake_create_comparison
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row())
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "auto",
                    "config_hash": "a" * 64,
                    "anchor_run_public_id": "anchor-direct",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 200
            assert captured["pair"] == _normalise_pair("anchor-direct", "zmq-older")
            client.close()

    def test_auto_anchor_falls_back_to_same_mode_when_no_opposite(self) -> None:
        """Anchored auto-pair falls back when no opposite-mode.

        Given:
            Direct-DB anchor plus a single direct_db counterpart
            (wallet has only Direct-DB runs for this config).

        When:
            POST /api/backtests/compare with mode=auto and the anchor.

        Then:
            Counterpart resolves to the only available same-mode
            candidate — the preference is cross-mode but the gate is
            availability. Asserts that a single-mode wallet still
            creates the comparison successfully.
        """
        anchor = _make_run_row(public_id="anchor-direct", status="completed")
        anchor["config_hash"] = "a" * 64
        anchor["execution_mode"] = "direct_db"
        counterpart = _make_run_row(public_id="direct-counter", status="completed")
        counterpart["config_hash"] = "a" * 64
        counterpart["execution_mode"] = "direct_db"
        captured: dict[str, Any] = {}

        async def fake_create_comparison(row: dict[str, Any], **_kwargs: Any) -> tuple[int, str]:
            captured["pair"] = (row["run_a_public_id"], row["run_b_public_id"])
            return 1, "cmp-new"

        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[anchor, counterpart])
        bt.get_comparison_by_pair = AsyncMock(return_value=None)
        bt.create_comparison = fake_create_comparison
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row())
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "auto",
                    "config_hash": "a" * 64,
                    "anchor_run_public_id": "anchor-direct",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 200
            assert captured["pair"] == _normalise_pair("anchor-direct", "direct-counter")
            client.close()

    def test_manual_missing_run_id_returns_422(self) -> None:
        """Manual mode without both run ids → 422.

        Given: mode=manual but run_b_public_id omitted,
        When: POST /api/backtests/compare,
        Then: 422 required.
        """
        bt = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare({"mode": "manual", "run_a_public_id": "x"})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 422
            client.close()

    def test_manual_self_compare_returns_422(self) -> None:
        """Manual mode with run_a == run_b → 422.

        Given: identical run_a / run_b,
        When: POST /api/backtests/compare,
        Then: 422 self-compare.
        """
        bt = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "manual",
                    "run_a_public_id": "x",
                    "run_b_public_id": "x",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 422
            client.close()

    def test_manual_run_not_in_wallet_returns_404(self) -> None:
        """Manual mode with foreign-wallet leg → 404.

        Given: get_run returns None for one leg,
        When: POST /api/backtests/compare,
        Then: 404 run not found.
        """
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "manual",
                    "run_a_public_id": "a",
                    "run_b_public_id": "b",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 404
            client.close()

    def test_manual_non_terminal_returns_409(self) -> None:
        """Manual mode with running leg → 409.

        Given: both legs found but one is status='running',
        When: POST /api/backtests/compare,
        Then: 409 must be terminal.
        """
        run_a = _make_run_row(public_id="a", status="completed")
        run_b = _make_run_row(public_id="b", status="running")
        bt = AsyncMock()
        bt.get_run = AsyncMock(side_effect=[run_a, run_b])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "manual",
                    "run_a_public_id": "a",
                    "run_b_public_id": "b",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 409
            client.close()

    def test_manual_creates_pair_with_shared_hash(self) -> None:
        """Manual mode with two terminal runs sharing hash → 200.

        Given: both legs terminal with same config_hash,
        When: POST /api/backtests/compare,
        Then: 200 + comparison created with config_hash populated.
        """
        run_a = _make_run_row(public_id="a", status="completed")
        run_a["config_hash"] = "a" * 64
        run_b = _make_run_row(public_id="b", status="completed")
        run_b["config_hash"] = "a" * 64
        bt = AsyncMock()
        bt.get_run = AsyncMock(side_effect=[run_a, run_b])
        bt.get_comparison_by_pair = AsyncMock(return_value=None)
        bt.create_comparison = AsyncMock(return_value=(1, "cmp-new"))
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row())
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare(
                {
                    "mode": "manual",
                    "run_a_public_id": "a",
                    "run_b_public_id": "b",
                }
            )
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 200
            client.close()

    def test_idempotent_returns_existing(self) -> None:
        """SELECT-first finds existing pair → 200 returns it (no INSERT).

        Given: get_comparison_by_pair returns an existing row,
        When: POST /api/backtests/compare,
        Then: 200 with existing public_id; create_comparison not called.
        """
        bt = AsyncMock()
        bt.list_runs = AsyncMock(
            return_value=[
                _make_run_row(public_id="a", status="completed"),
                _make_run_row(public_id="b", status="completed"),
            ]
        )
        bt.get_comparison_by_pair = AsyncMock(
            return_value=_make_comparison_row(public_id="cmp-existing")
        )
        bt.create_comparison = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare({"mode": "auto", "config_hash": "a" * 64})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 200
            assert response.json()["payload"]["public_id"] == "cmp-existing"
            bt.create_comparison.assert_not_called()
            client.close()

    def test_integrity_error_race_recovers_with_existing(self) -> None:
        """IntegrityError race → re-SELECT returns 200 with the now-visible row.

        Given: SELECT first returns None, INSERT raises IntegrityError,
            re-SELECT finds the row,
        When: POST /api/backtests/compare,
        Then: 200 with the recovered public_id.
        """
        existing = _make_comparison_row(public_id="cmp-race")
        bt = AsyncMock()
        bt.list_runs = AsyncMock(
            return_value=[
                _make_run_row(public_id="a", status="completed"),
                _make_run_row(public_id="b", status="completed"),
            ]
        )
        bt.get_comparison_by_pair = AsyncMock(side_effect=[None, existing])
        bt.create_comparison = AsyncMock(side_effect=IntegrityError("INSERT", {}, MagicMock()))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare({"mode": "auto", "config_hash": "a" * 64})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 200
            assert response.json()["payload"]["public_id"] == "cmp-race"
            client.close()

    def test_integrity_error_unrecoverable_500(self) -> None:
        """IntegrityError with empty re-SELECT → 500.

        Given: INSERT raises and re-SELECT also returns None,
        When: POST /api/backtests/compare,
        Then: 500 race recovery failed.
        """
        bt = AsyncMock()
        bt.list_runs = AsyncMock(
            return_value=[
                _make_run_row(public_id="a", status="completed"),
                _make_run_row(public_id="b", status="completed"),
            ]
        )
        bt.get_comparison_by_pair = AsyncMock(side_effect=[None, None])
        bt.create_comparison = AsyncMock(side_effect=IntegrityError("INSERT", {}, MagicMock()))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            body = _wrap_compare({"mode": "auto", "config_hash": "a" * 64})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 500
            client.close()


class TestCompareList:
    """GET /api/backtests/compare — wallet-scoped list."""

    def test_list_no_active_wallet_400(self) -> None:
        """Caller without active wallet → 400."""
        bt = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet=None)
            response = client.get("/api/backtests/compare")
            assert response.status_code == 400
            client.close()

    def test_list_returns_rows(self) -> None:
        """Wallet-scoped repository call returns the projected list."""
        bt = AsyncMock()
        bt.list_comparisons = AsyncMock(
            return_value=[_make_comparison_row(public_id=f"c{i}") for i in range(3)]
        )
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/compare")
            assert response.status_code == 200
            data = response.json()
            assert data["count"] == 3
            client.close()


class TestCompareDetail:
    """GET /api/backtests/compare/{id} — recomputed diff."""

    def test_no_active_wallet_400(self) -> None:
        """Caller without active wallet → 400."""
        bt = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(wallet=None)
            response = client.get("/api/backtests/compare/cmp-1")
            assert response.status_code == 400
            client.close()

    def test_comparison_not_found_404(self) -> None:
        """Repository returns None → 404."""
        bt = AsyncMock()
        bt.get_comparison = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/compare/cmp-1")
            assert response.status_code == 404
            client.close()

    def test_comparison_foreign_wallet_404(self) -> None:
        """Comparison wallet ≠ caller wallet → 404 (no info leak)."""
        cmp_row = _make_comparison_row()
        cmp_row["wallet_public_id"] = "other-wallet"
        bt = AsyncMock()
        bt.get_comparison = AsyncMock(return_value=cmp_row)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/compare/cmp-1")
            assert response.status_code == 404
            client.close()

    def test_underlying_run_missing_404(self) -> None:
        """One run gone → 404 underlying run not found."""
        bt = AsyncMock()
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row())
        bt.get_run = AsyncMock(side_effect=[_make_run_row(), None])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/compare/cmp-1")
            assert response.status_code == 404
            client.close()

    def test_diff_recomputed_from_artifacts(self) -> None:
        """Happy path: returns comparison + diff payload computed on GET."""
        bt = AsyncMock()
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row())
        bt.get_run = AsyncMock(
            side_effect=[
                _make_run_row(public_id="run-a"),
                _make_run_row(public_id="run-b"),
            ]
        )
        bt.get_result = AsyncMock(return_value=None)
        bt.get_equity_points = AsyncMock(return_value=[_make_equity_row()])
        bt.get_trades = AsyncMock(return_value=[])
        bt.get_signals = AsyncMock(return_value=[])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client()
            response = client.get("/api/backtests/compare/cmp-1")
            assert response.status_code == 200
            data = response.json()["payload"]
            assert data["comparison"]["public_id"] == "cmp-1"
            assert "metrics_diff" in data
            assert "equity_overlay" in data
            client.close()


_NO_WALLET_DETAIL = "no active wallet selected"


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/backtests/run-1"),
        ("GET", "/api/backtests/run-1/trades"),
        ("GET", "/api/backtests/run-1/signals"),
        ("GET", "/api/backtests/run-1/events"),
        ("GET", "/api/backtests/run-1/equity"),
        ("POST_CANCEL", "/api/backtests/run-1/cancel"),
        ("POST_RERUN", "/api/backtests/run-1/rerun"),
    ],
)
def test_detail_routes_fail_closed_without_active_wallet(method: str, path: str) -> None:
    """Detail routes reject caller with no active wallet.

    Given:
        The principal has ``active_wallet_public_id=None`` (selected
        "All wallets" in the UI / cleared the wallet claim on refresh)
        and the backend repository returns a valid run row.

    When:
        The caller hits any of the 7 wallet-scoped ``/{run_id}/*``
        routes (detail, trades, signals, events, equity, cancel,
        rerun).

    Then:
        The fail-closed guard fires 400 with the ``no active wallet
        selected`` detail before any repo mutation happens. This pins the
        contract — the original truthy guard
        (``if principal.active_wallet_public_id and ...``) was a no-op
        under a cleared wallet claim and leaked cross-tenant reads. The
        five GET rows are guarded by ``_enforce_wallet_scope``; the two
        POST rows are guarded earlier, by
        ``require_tradable_active_wallet``, which raises the SAME detail
        so the surface stays uniform.
    """
    bt = AsyncMock()
    bt.get_run = AsyncMock(return_value=_make_run_row())
    bt.update_run_status = AsyncMock()
    bt.create_run = AsyncMock()
    with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
        client = _create_client(wallet=None)
        if method == "GET":
            response = client.get(path)
        elif method == "POST_CANCEL":
            response = client.post(path, json=_cancel_body())
        else:
            response = client.post(path)
        assert response.status_code == 400
        assert response.json()["detail"] == _NO_WALLET_DETAIL
        client.close()


@patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", _MOCK_STRATEGIES)
class TestReadGrantConfersNoWriteAuthority:
    """The READ-plane wallet claim must not authorize a wallet-scoped write.

    ``POST /auth/refresh`` validates ``active_wallet_public_id`` against
    the READ plane (``list_readable_wallets_for_user`` — operator scope
    grants UNION the user's personal ``wallet_user_read_grants`` rows) and
    mints it into the JWT. That is deliberate: a read-granted user must be
    able to pin the hint and receive that wallet's read surfaces.

    ``AI_REVIEWER`` turns the claim into a reachable privilege escalation
    unless the write surfaces re-resolve it: the role holds
    ``CREATE_BACKTEST_COMPARISONS``, and the seed loader refuses read
    grants only to permission sets holding ``CREATE_ORDERS`` or
    ``IMPERSONATE_OPERATOR``, so a read-granted reviewer is a legal
    principal. These tests pin both halves — the write is refused, and the
    reads it was granted still work.
    """

    def test_read_granted_reviewer_cannot_persist_a_comparison(self) -> None:
        """A read-only wallet claim is refused at the comparison write.

        Given:
            An ``AI_REVIEWER`` principal holding
            ``CREATE_BACKTEST_COMPARISONS``, with zero operator
            memberships and ``active_wallet_public_id`` pinned to a wallet
            it sees only through a personal read grant, so the TRADE plane
            (``list_accessible_wallets_for_operators``) returns nothing.
            The repository is stocked with two terminal runs and an
            insert that succeeds, so every remaining precondition for
            persistence is satisfied — the wallet gate is the only thing
            standing between this request and a written row.

        When:
            The principal posts a valid auto comparison request.

        Then:
            The request is refused with 403 and no ``BacktestComparison``
            row is written — neither the idempotent pair lookup nor the
            insert is reached, so the escalation is closed BEFORE
            persistence rather than reported after it. Removing
            ``require_tradable_active_wallet`` from ``create_comparison``
            turns this into a 200 with ``create_comparison`` awaited.
        """
        bt = AsyncMock()
        bt.list_runs = AsyncMock(
            return_value=[
                _make_run_row(public_id="run-a", status="completed"),
                _make_run_row(public_id="run-b", status="completed"),
            ]
        )
        bt.get_comparison_by_pair = AsyncMock(return_value=None)
        bt.create_comparison = AsyncMock(return_value=(1, "cmp-new"))
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row(public_id="cmp-new"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(role=UserRole.AI_REVIEWER)
            _repo_of(client).list_accessible_wallets_for_operators = AsyncMock(return_value=[])
            body = _wrap_compare({"mode": "auto", "config_hash": "a" * 64})
            response = client.post("/api/backtests/compare", json=body)
            assert response.status_code == 403
            bt.list_runs.assert_not_awaited()
            bt.get_comparison_by_pair.assert_not_awaited()
            bt.create_comparison.assert_not_awaited()
            client.close()

    @pytest.mark.parametrize(
        "path",
        [
            "/api/backtests",
            "/api/backtests/compare",
            "/api/backtests/compare/cmp-1",
            "/api/backtests/run-1",
            "/api/backtests/run-1/trades",
            "/api/backtests/run-1/signals",
            "/api/backtests/run-1/events",
            "/api/backtests/run-1/equity",
        ],
    )
    def test_read_granted_reviewer_still_reads_the_wallet(self, path: str) -> None:
        """The write gate does not over-correct into the read surfaces.

        Given:
            The same read-granted ``AI_REVIEWER`` principal whose wallet is
            absent from the trade plane, and repository fixtures owned by
            the pinned wallet.

        When:
            Each wallet-scoped backtest READ route is requested.

        Then:
            Every one answers 200. Read authority conferring read
            visibility is the feature the read/trade split exists to
            deliver, so the fix must refuse writes without touching this.
        """
        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[_make_run_row()])
        bt.list_comparisons = AsyncMock(return_value=[_make_comparison_row()])
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row())
        bt.get_run = AsyncMock(return_value=_make_run_row())
        bt.get_result = AsyncMock(return_value=None)
        bt.get_trades = AsyncMock(return_value=[])
        bt.get_signals = AsyncMock(return_value=[])
        bt.get_events = AsyncMock(return_value=[])
        bt.get_equity_points = AsyncMock(return_value=[])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(role=UserRole.AI_REVIEWER)
            _repo_of(client).list_accessible_wallets_for_operators = AsyncMock(return_value=[])
            response = client.get(path)
            assert response.status_code == 200
            client.close()

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            ("POST_CREATE", "/api/backtests"),
            ("POST_COMPARE", "/api/backtests/compare"),
            ("POST_CANCEL", "/api/backtests/run-1/cancel"),
            ("POST_RERUN", "/api/backtests/run-1/rerun"),
        ],
    )
    def test_untradable_claim_is_refused_on_every_write_route(self, method: str, path: str) -> None:
        """All four backtest write consumers re-resolve the wallet claim.

        Given:
            An ``OPERATOR`` principal — the only named set holding both
            ``MANAGE_BACKTESTS`` and ``CREATE_BACKTEST_COMPARISONS`` — whose
            active wallet claim is outside its operator scope grants, and a
            repository that would happily return a run for the path.

        When:
            Each of the four wallet-writing backtest routes is called.

        Then:
            Every one answers 403 and nothing is persisted or even read:
            the gate is a dependency, so it runs ahead of the handler
            body. Parametrising all four is the point — the reported
            defect was on ``/compare`` alone, and a fix applied only there
            would leave create, cancel and rerun open.
        """
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(status="running"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(role=UserRole.OPERATOR)
            _repo_of(client).list_accessible_wallets_for_operators = AsyncMock(
                return_value=[{"public_id": "wallet-elsewhere"}]
            )
            if method == "POST_CREATE":
                response = client.post(path, json=_create_body())
            elif method == "POST_COMPARE":
                response = client.post(
                    path, json=_wrap_compare({"mode": "auto", "config_hash": "a" * 64})
                )
            elif method == "POST_CANCEL":
                response = client.post(path, json=_cancel_body())
            else:
                response = client.post(path)
            assert response.status_code == 403
            bt.get_run.assert_not_awaited()
            bt.create_run.assert_not_awaited()
            bt.update_run_status.assert_not_awaited()
            bt.create_comparison.assert_not_awaited()
            client.close()

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            ("POST_CANCEL", "/api/backtests/run-1/cancel"),
            ("POST_RERUN", "/api/backtests/run-1/rerun"),
        ],
    )
    def test_tradable_wallet_still_gates_foreign_runs(self, method: str, path: str) -> None:
        """A tradable wallet does not unlock another wallet's run.

        Given:
            An ``OPERATOR`` principal whose active wallet IS tradable, and
            a run owned by a different wallet.

        When:
            The cancel or rerun route is called for that run.

        Then:
            404 is returned by ``_enforce_run_wallet`` and no status
            update happens, proving the ownership half of the old
            ``_enforce_wallet_scope`` survived the split intact.
        """
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other-wallet", status="running"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(role=UserRole.OPERATOR)
            if method == "POST_CANCEL":
                response = client.post(path, json=_cancel_body())
            else:
                response = client.post(path)
            assert response.status_code == 404
            bt.update_run_status.assert_not_awaited()
            bt.create_run.assert_not_awaited()
            client.close()


class TestRevokedReadGrantStopsBacktestReads:
    """The wallet claim must not outlive the read grant that justified it.

    ``active_wallet_public_id`` is validated once, at mint, and then
    carried forward by every refresh — including the documented zero-body
    refreshes, which run forever. The wallet-scoped backtest readers used
    to authorize from that claim alone, so an admin revoking a personal
    ``wallet_user_read_grants`` row never stopped the holder: they kept
    listing that wallet's runs, comparisons, trades, signals, events and
    equity indefinitely. Every OTHER read surface in the tree resolves
    ``resolve_readable_wallets`` per request and was revocation-immediate
    already; these tests pin that the backtest family now is too.

    The mirror — a live grant still answering 200 on all eight routes —
    is ``TestReadGrantConfersNoWriteAuthority``'s
    ``test_read_granted_reviewer_still_reads_the_wallet``, which runs the
    same principal through the same gate with the read plane populated.
    """

    @pytest.mark.parametrize(
        ("path", "unreached"),
        [
            ("/api/backtests", "list_runs"),
            ("/api/backtests/compare", "list_comparisons"),
            ("/api/backtests/compare/cmp-1", "get_comparison"),
            ("/api/backtests/run-1", "get_result"),
            ("/api/backtests/run-1/trades", "get_trades"),
            ("/api/backtests/run-1/signals", "get_signals"),
            ("/api/backtests/run-1/events", "get_events"),
            ("/api/backtests/run-1/equity", "get_equity_points"),
        ],
    )
    def test_revoked_read_grant_is_refused_on_every_read_route(
        self, path: str, unreached: str
    ) -> None:
        """A claim whose grant is gone stops working on the next request.

        Given:
            An ``AI_REVIEWER`` with zero operator memberships whose claim
            still names the wallet, but whose read grant has been
            revoked, so BOTH planes now return nothing. Every repository
            fixture the route would need is stocked, so the gate is the
            only thing standing between the request and the rows.

        When:
            Each of the eight wallet-scoped backtest READ routes is
            requested.

        Then:
            Every one answers 403 and the route's payload fetch is never
            awaited. Parametrising all eight is the point: the claim was
            read raw in three handlers and in the shared
            ``_enforce_wallet_scope``, so a fix applied to one shape
            would leave the other open.
        """
        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[_make_run_row()])
        bt.list_comparisons = AsyncMock(return_value=[_make_comparison_row()])
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row())
        bt.get_run = AsyncMock(return_value=_make_run_row(status="completed"))
        bt.get_result = AsyncMock(return_value=None)
        bt.get_trades = AsyncMock(return_value=[])
        bt.get_signals = AsyncMock(return_value=[])
        bt.get_events = AsyncMock(return_value=[])
        bt.get_equity_points = AsyncMock(return_value=[])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(role=UserRole.AI_REVIEWER)
            repo = _repo_of(client)
            repo.list_readable_wallets_for_user = AsyncMock(return_value=[])
            repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
            response = client.get(path)
            assert response.status_code == 403
            getattr(bt, unreached).assert_not_awaited()
            client.close()
