"""Tests for the P&L timeline series route (Phase 5A).

Exercises the ``GET /api/portfolio/pnl/series`` endpoint end to end with a mocked
repository: the happy path and response shape, every 400 parameter guard, the
total-work budget, current-only horizon, timezone coercion, wallet/mode
consistency, extreme datetime handling, wallet-scope 403, and the 500 wrapper on
an unexpected reconstruction failure.
"""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import WalletRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency

_FROM = "2026-07-20T10:00:00Z"
_TO = "2026-07-20T10:02:00Z"
_WALLET = "0000face-0000-7000-8000-0000000000a1"


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


def _candle(open_at: datetime, close: float) -> CandleRow:
    """Build one finalized 1m candle row for the mock repo."""
    return {
        "open_at": open_at,
        "timeframe": "1m",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1.0,
        "vwap": close,
        "trades": 1,
        "source": "test",
        "complete": True,
        "public_id": "candle-1",
        "timestamp": open_at,
        "session_id": "s1",
        "sequence_id": 1,
    }


def _wallet_row(*, is_paper: bool = False) -> WalletRow:
    """Build the active wallet row used by route validation."""
    return {
        "public_id": _WALLET,
        "label": "main",
        "description": None,
        "is_paper": is_paper,
        "timestamp": datetime(2026, 7, 20, 9, 0, tzinfo=UTC),
        "session_id": "s1",
        "sequence_id": 1,
    }


def _seeded_repo() -> AsyncMock:
    """Build a repo mock returning one buy on a USD instrument with marks."""
    t0 = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
    repo = AsyncMock()
    repo.get_pnl_timeline_executions = AsyncMock(
        return_value=[
            {
                "instrument_public_id": "i1",
                "exchange": "kraken",
                "scope_sequence": 1,
                "order_public_id": "o1",
                "side": "buy",
                "size": 1.0,
                "price": 100.0,
                "fee": 0.5,
                "fee_asset": "USD",
                "executed_at": None,
                "timestamp": t0,
                "exec_id": "e1",
                "trade_id": "t1",
            }
        ]
    )
    repo.get_accruals_for_pnl = AsyncMock(return_value=[])
    repo.get_instrument_symbol_refs = AsyncMock(
        return_value=[
            {
                "instrument_public_id": "i1",
                "native_symbol": "BTC-USD",
                "exchange": "kraken",
                "quote_currency": "USD",
            }
        ]
    )
    repo.get_pnl_timeline_candles = AsyncMock(
        return_value=[
            {
                "instrument_public_id": "i1",
                "open_at": t0 - timedelta(minutes=1),
                "close": 105.0,
            },
            {
                "instrument_public_id": "i1",
                "open_at": t0,
                "close": 110.0,
            },
            {
                "instrument_public_id": "i1",
                "open_at": t0 + timedelta(minutes=1),
                "close": 120.0,
            },
        ]
    )
    repo.get_candles = AsyncMock(
        return_value=[
            _candle(t0 - timedelta(minutes=1), 105.0),
            _candle(t0, 110.0),
            _candle(t0 + timedelta(minutes=1), 120.0),
        ]
    )
    repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
    repo.list_active_wallets = AsyncMock(return_value=[_wallet_row()])
    repo.get_fill_shard_keys_for_scope = AsyncMock(return_value=[])
    repo.shard_has_fill_gap = AsyncMock(return_value=False)
    return repo


def _create_client(mock_repo: AsyncMock, role: UserRole = UserRole.ADMIN) -> TestClient:
    """Create a test client with auth/csrf bypassed and the mock repo bound."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    app.state.rest_tracker = SequenceTracker()

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(username="tester", role=role)

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: mock_repo
    return TestClient(app)


def _url(**params: str) -> str:
    """Build the series URL with the given query params."""
    base = {"wallet_public_id": _WALLET, "from": _FROM, "to": _TO}
    base.update(params)
    query = "&".join(f"{k}={v}" for k, v in base.items())
    return f"/api/portfolio/pnl/series?{query}"


class TestHappyPath:
    """Cover the successful reconstruction and response shape."""

    def test_returns_series_with_expected_decomposition(self) -> None:
        """A valid request returns the decomposed series with provenance."""
        repo = _seeded_repo()
        client = _create_client(repo)
        response = client.get(_url())
        assert response.status_code == 200
        body = response.json()
        assert body["type"] == "pnl_series"
        payload = body["payload"]
        assert payload["wallet_public_id"] == _WALLET
        assert payload["mode"] == "live"
        assert payload["granularity"] == "1m"
        assert payload["valuation_ccy"] == "USD"
        assert payload["mark_source"] == "finalized_1m_candle_close"
        assert payload["calc_version"] == "5A.2"
        points = payload["points"]
        assert len(points) == 3
        assert points[0]["valuation_status"] == "complete"
        assert points[0]["fee_pnl"] == -0.5
        assert points[0]["unrealized_pnl"] == 5.0
        assert points[0]["net_pnl"] == 4.5
        assert points[0]["per_instrument"][0]["instrument_public_id"] == "i1"
        repo.get_pnl_timeline_candles.assert_awaited_once()
        repo.get_candles.assert_not_awaited()

    def test_naive_datetimes_are_accepted_and_coerced(self) -> None:
        """A window without a timezone suffix is treated as UTC and matches marks."""
        client = _create_client(_seeded_repo())
        response = client.get(_url(**{"from": "2026-07-20T10:00:00", "to": "2026-07-20T10:02:00"}))
        assert response.status_code == 200
        assert response.json()["payload"]["points"][0]["net_pnl"] == 4.5

    def test_equal_window_endpoints_emit_one_point(self) -> None:
        """An inclusive zero-span window is valid because ``to`` may equal ``from``."""
        client = _create_client(_seeded_repo())
        response = client.get(_url(**{"to": _FROM}))
        assert response.status_code == 200
        points = response.json()["payload"]["points"]
        assert len(points) == 1
        assert points[0]["point_time"] == _FROM

    def test_read_horizon_is_current_and_cannot_be_overridden(self) -> None:
        """An ``as_of`` query value is not part of v1 and cannot time-travel the read."""
        repo = _seeded_repo()
        client = _create_client(repo)
        before = datetime.now(UTC)
        response = client.get(_url(as_of="2000-01-01T00:00:00Z"))
        after = datetime.now(UTC)
        assert response.status_code == 200
        response_as_of = datetime.fromisoformat(response.json()["payload"]["as_of"])
        assert before <= response_as_of <= after
        execution_as_of = repo.get_pnl_timeline_executions.await_args.args[2]
        assert execution_as_of == response_as_of

    def test_paper_mode_accepts_a_paper_wallet(self) -> None:
        """A paper request succeeds when the active wallet is also paper."""
        repo = _seeded_repo()
        repo.list_active_wallets = AsyncMock(return_value=[_wallet_row(is_paper=True)])
        client = _create_client(repo)
        response = client.get(_url(mode="paper"))
        assert response.status_code == 200
        assert response.json()["payload"]["mode"] == "paper"

    def test_downsampled_granularity_selects_endpoint(self) -> None:
        """A 1h bucket collapses the 1m grid to its endpoint point."""
        client = _create_client(_seeded_repo())
        response = client.get(_url(granularity="1h"))
        assert response.status_code == 200
        points = response.json()["payload"]["points"]
        assert len(points) == 1
        assert points[0]["net_pnl"] == 19.5


class TestParameterGuards:
    """Cover the 400 request guards."""

    def test_missing_wallet_is_rejected(self) -> None:
        """A blank wallet id is a 400."""
        client = _create_client(_seeded_repo())
        response = client.get(f"/api/portfolio/pnl/series?from={_FROM}&to={_TO}")
        assert response.status_code == 400
        assert "wallet_public_id" in response.json()["detail"]

    def test_empty_wallet_value_is_rejected(self) -> None:
        """An explicitly empty wallet id is a 400."""
        client = _create_client(_seeded_repo())
        response = client.get(_url(wallet_public_id=""))
        assert response.status_code == 400

    def test_missing_from_is_rejected(self) -> None:
        """A request without ``from`` is a 400."""
        client = _create_client(_seeded_repo())
        response = client.get(f"/api/portfolio/pnl/series?wallet_public_id={_WALLET}&to={_TO}")
        assert response.status_code == 400
        assert "from and to" in response.json()["detail"]

    def test_invalid_granularity_is_rejected(self) -> None:
        """An unsupported granularity is a 400."""
        client = _create_client(_seeded_repo())
        response = client.get(_url(granularity="30m"))
        assert response.status_code == 400
        assert "granularity" in response.json()["detail"]

    def test_invalid_mode_is_rejected(self) -> None:
        """Only the exact lower-case live and paper mode values are accepted."""
        repo = _seeded_repo()
        client = _create_client(repo)
        response = client.get(_url(mode="LIVE"))
        assert response.status_code == 400
        assert "expected 'live' or 'paper'" in response.json()["detail"]
        repo.list_active_wallets.assert_not_awaited()

    def test_unknown_wallet_is_rejected(self) -> None:
        """An admin cannot receive a plausible zero series for a missing wallet."""
        repo = _seeded_repo()
        repo.list_active_wallets = AsyncMock(return_value=[])
        client = _create_client(repo)
        response = client.get(_url())
        assert response.status_code == 400
        assert response.json()["detail"] == f"Unknown wallet_public_id: {_WALLET}"
        repo.get_pnl_timeline_executions.assert_not_awaited()

    def test_live_mode_rejects_paper_wallet(self) -> None:
        """A paper wallet cannot be queried under live mode."""
        repo = _seeded_repo()
        repo.list_active_wallets = AsyncMock(return_value=[_wallet_row(is_paper=True)])
        client = _create_client(repo)
        response = client.get(_url())
        assert response.status_code == 400
        assert "not compatible with mode 'live'" in response.json()["detail"]

    def test_paper_mode_rejects_live_wallet(self) -> None:
        """A live wallet cannot be queried under paper mode."""
        client = _create_client(_seeded_repo())
        response = client.get(_url(mode="paper"))
        assert response.status_code == 400
        assert "not compatible with mode 'paper'" in response.json()["detail"]

    def test_from_after_to_is_rejected(self) -> None:
        """A non-increasing window is a 400."""
        client = _create_client(_seeded_repo())
        response = client.get(_url(**{"from": _TO, "to": _FROM}))
        assert response.status_code == 400
        assert "greater than or equal" in response.json()["detail"]

    def test_malformed_datetime_is_rejected_as_400(self) -> None:
        """A malformed ISO datetime is a route-level 400 rather than a 422."""
        client = _create_client(_seeded_repo())
        response = client.get(_url(**{"from": "not-a-datetime"}))
        assert response.status_code == 400
        assert "supported ISO-8601 datetime" in response.json()["detail"]

    def test_offset_normalization_underflow_is_rejected_as_400(self) -> None:
        """A representable local datetime that underflows in UTC returns 400."""
        client = _create_client(_seeded_repo())
        response = client.get(
            _url(
                **{
                    "from": "0001-01-01T00:00:00%2B01:00",
                    "to": "0001-01-01T00:01:00%2B01:00",
                }
            )
        )
        assert response.status_code == 400
        assert "supported ISO-8601 datetime" in response.json()["detail"]

    def test_leading_candle_underflow_is_rejected_as_400(self) -> None:
        """The minimum UTC minute is rejected before candle-start subtraction."""
        client = _create_client(_seeded_repo())
        response = client.get(
            _url(
                **{
                    "from": "0001-01-01T00:00:00Z",
                    "to": "0001-01-01T00:00:00Z",
                }
            )
        )
        assert response.status_code == 400
        assert "safely representable" in response.json()["detail"]

    def test_grid_increment_overflow_is_rejected_as_400(self) -> None:
        """The final UTC minute is rejected before the pure grid can overflow."""
        client = _create_client(_seeded_repo())
        response = client.get(
            _url(
                **{
                    "from": "9999-12-31T23:59:00Z",
                    "to": "9999-12-31T23:59:00Z",
                }
            )
        )
        assert response.status_code == 400
        assert "safely representable" in response.json()["detail"]

    def test_oversized_window_is_rejected(self) -> None:
        """A request above the minute-instrument work budget is an actionable 400."""
        client = _create_client(_seeded_repo())
        response = client.get(
            _url(**{"from": "2020-01-01T00:00:00Z", "to": "2026-01-01T00:00:00Z"})
        )
        assert response.status_code == 400
        assert "minute-instrument work units" in response.json()["detail"]
        assert "Shorten the window or narrow the wallet scope" in response.json()["detail"]


class TestScopeAndFailures:
    """Cover the wallet-scope 403 and the internal-error wrapper."""

    def test_foreign_wallet_is_forbidden_for_non_admin(self) -> None:
        """A non-admin requesting an inaccessible wallet gets a 403."""
        repo = _seeded_repo()
        client = _create_client(repo, role=UserRole.OPERATOR)
        response = client.get(_url())
        assert response.status_code == 403
        repo.list_active_wallets.assert_not_awaited()

    def test_reconstruction_failure_is_wrapped_as_500(self) -> None:
        """An unexpected repository error is surfaced as a 500."""
        repo = _seeded_repo()
        repo.get_pnl_timeline_executions = AsyncMock(side_effect=RuntimeError("boom"))
        client = _create_client(repo)
        response = client.get(_url())
        assert response.status_code == 500
        assert response.json()["detail"] == "Failed to build P&L series"
