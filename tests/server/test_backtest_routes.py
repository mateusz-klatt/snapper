"""Tests for backtest REST API endpoints."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
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
    bt_repo_mock: AsyncMock,
    role: UserRole = UserRole.ADMIN,
    wallet: str | None = "wallet-1",
    launch_error: Exception | None = None,
) -> TestClient:
    """Create test client with mocked BacktestRepository and auth bypassed."""
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

    mock_repo = MagicMock()
    mock_repo.session_factory = MagicMock()

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
    app.dependency_overrides[_bt_repo_dep] = lambda: bt_repo_mock
    return TestClient(app)


def _bt_repo_dep() -> None:
    """Placeholder for BacktestRepository dependency override."""


class TestListBacktests:
    """Tests for GET /api/backtests."""

    def test_list_empty(self) -> None:
        """Empty list returns count=0."""
        bt = AsyncMock()
        bt.list_runs = AsyncMock(return_value=[])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt)
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
            client = _create_client(bt)
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
        bt = AsyncMock()
        saved = dict(StrategyFactory.STRATEGY_CLASSES)
        try:
            StrategyFactory.STRATEGY_CLASSES.clear()
            StrategyFactory.STRATEGY_CLASSES["ZetaStrategy"] = RSIReversion
            StrategyFactory.STRATEGY_CLASSES["AlphaStrategy"] = RSIReversion
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
            response = client.get("/api/backtests/nonexistent")
            assert response.status_code == 404
            client.close()

    def test_get_wallet_mismatch(self) -> None:
        """Run from different wallet returns 404 for scoped user."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other-wallet"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt, wallet="wallet-1")
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
            client = _create_client(bt)
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
            client = _create_client(bt)
            response = client.get("/api/backtests/run-1")
            assert response.json()["payload"]["result"] is None
            client.close()

    def test_get_pending_omits_result(self) -> None:
        """Pending run returns result=None without querying get_result."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(status="pending"))
        bt.get_result = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
            response = client.get("/api/backtests/missing/equity")
            assert response.status_code == 404
            client.close()

    def test_equity_404_when_wallet_mismatch(self) -> None:
        """Cross-wallet read returns 404 (not 403, matching list endpoint)."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt, wallet="wallet-1")
            response = client.get("/api/backtests/run-1/equity")
            assert response.status_code == 404
            client.close()

    def test_equity_limit_above_cap_rejected(self) -> None:
        """Limit > 20000 fails query validation (422)."""
        bt = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt)
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
            client = _create_client(bt)
            response = client.post("/api/backtests/run-1/cancel", json=_cancel_body())
            assert response.status_code == 200
            assert response.json()["payload"]["status"] == "cancel_requested"
            client.close()

    def test_cancel_completed_409(self) -> None:
        """Completed run returns 409."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(status="completed"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt)
            response = client.post("/api/backtests/run-1/cancel", json=_cancel_body())
            assert response.status_code == 409
            client.close()

    def test_cancel_not_found(self) -> None:
        """Missing run returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt, wallet="wallet-1")
            response = client.post("/api/backtests", json=_create_body())
            assert response.status_code == 200
            assert response.json()["payload"]["public_id"] == "run-new"
            client.close()

    def test_create_launch_failure_500(self) -> None:
        """Process launch failure returns 500 and marks run failed."""
        bt = AsyncMock()
        bt.create_run = AsyncMock(return_value=(1, "run-fail"))
        bt.update_run_status = AsyncMock(return_value=1)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt, wallet="wallet-1", launch_error=RuntimeError("no thread"))
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
            client = _create_client(bt, wallet="wallet-1")
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
            client = _create_client(bt, wallet="wallet-1")
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
            client = _create_client(bt, wallet="wallet-1")
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
            client = _create_client(bt, wallet="wallet-1")
            response = client.get("/api/backtests/run-1/trades")
            assert response.status_code == 404
            client.close()

    def test_signals_wallet_mismatch(self) -> None:
        """Signals for run owned by different wallet returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt, wallet="wallet-1")
            response = client.get("/api/backtests/run-1/signals")
            assert response.status_code == 404
            client.close()

    def test_events_wallet_mismatch(self) -> None:
        """Events for run owned by different wallet returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt, wallet="wallet-1")
            response = client.get("/api/backtests/run-1/events")
            assert response.status_code == 404
            client.close()

    def test_signals_run_not_found(self) -> None:
        """Missing run on signals endpoint returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt)
            response = client.get("/api/backtests/run-1/signals")
            assert response.status_code == 404
            client.close()

    def test_events_run_not_found(self) -> None:
        """Missing run on events endpoint returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt)
            response = client.get("/api/backtests/run-1/events")
            assert response.status_code == 404
            client.close()

    def test_cancel_wallet_mismatch(self) -> None:
        """Cancel for run owned by different wallet returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt, wallet="wallet-1")
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
            client = _create_client(bt, wallet=None)
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
            client = _create_client(bt, wallet="wallet-1")
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
            client = _create_client(bt)
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
            client = _create_client(bt)
            response = client.post("/api/backtests/run-1/rerun")
            assert response.status_code == 404
            client.close()

    def test_rerun_wallet_mismatch(self) -> None:
        """Rerun with wallet mismatch returns 404."""
        bt = AsyncMock()
        bt.get_run = AsyncMock(return_value=_make_run_row(wallet="other"))
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt, wallet="wallet-1")
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
            client = _create_client(bt, wallet="wallet-1")
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
            client = _create_client(bt, wallet="wallet-1")
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
            client = _create_client(bt, wallet=None)
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
            client = _create_client(bt, wallet=None)
            response = client.get("/api/backtests/run-1")
            assert response.status_code == 400
            client.close()


class TestCompareCreate:
    """POST /api/backtests/compare — auto + manual flows."""

    def test_no_active_wallet_returns_400(self) -> None:
        """Caller without active wallet → 400.

        Given: A principal with active_wallet_public_id=None,
        When: POST /api/backtests/compare runs,
        Then: 400 with no active wallet detail.
        """
        bt = AsyncMock()
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt, wallet=None)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt)
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
            client = _create_client(bt, wallet=None)
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
            client = _create_client(bt)
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
            client = _create_client(bt, wallet=None)
            response = client.get("/api/backtests/compare/cmp-1")
            assert response.status_code == 400
            client.close()

    def test_comparison_not_found_404(self) -> None:
        """Repository returns None → 404."""
        bt = AsyncMock()
        bt.get_comparison = AsyncMock(return_value=None)
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt)
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
            client = _create_client(bt)
            response = client.get("/api/backtests/compare/cmp-1")
            assert response.status_code == 404
            client.close()

    def test_underlying_run_missing_404(self) -> None:
        """One run gone → 404 underlying run not found."""
        bt = AsyncMock()
        bt.get_comparison = AsyncMock(return_value=_make_comparison_row())
        bt.get_run = AsyncMock(side_effect=[_make_run_row(), None])
        with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
            client = _create_client(bt)
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
            client = _create_client(bt)
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
        The ``_enforce_wallet_scope`` guard fires 400 with the
        ``no active wallet selected`` detail before any repo mutation
        happens. This pins the fail-closed contract — the previous
        truthy guard (``if principal.active_wallet_public_id and ...``)
        was a no-op under a cleared wallet claim and leaked
        cross-tenant reads; the new guard must raise 400 on every
        detail surface.
    """
    bt = AsyncMock()
    bt.get_run = AsyncMock(return_value=_make_run_row())
    bt.update_run_status = AsyncMock()
    bt.create_run = AsyncMock()
    with patch("snapper.server.backtest_routes._bt_repo", return_value=bt):
        client = _create_client(bt, wallet=None)
        if method == "GET":
            response = client.get(path)
        elif method == "POST_CANCEL":
            response = client.post(path, json=_cancel_body())
        else:
            response = client.post(path)
        assert response.status_code == 400
        assert response.json()["detail"] == _NO_WALLET_DETAIL
        client.close()
