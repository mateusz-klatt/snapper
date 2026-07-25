"""Tests for the P&L timeline series and decision-marker routes.

Exercises the ``GET /api/portfolio/pnl/series`` endpoint end to end with a mocked
repository: the happy path and response shape, every 400 parameter guard, the
total-work budget, bitemporal horizon, timezone coercion, wallet/mode
consistency, extreme datetime handling, wallet-scope 403, and the 500 wrapper on
an unexpected reconstruction failure. The response checks include causal,
instrument-identified incompleteness records, including a real multi-cause point.
"""

import json
from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.application.portfolio.pnl_anchor_identity import portfolio_pnl_anchor_public_id
from snapper.application.portfolio.pnl_timeline_service import PNL_SAMPLE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MARK_SOURCE
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MARKER_LIMIT
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import PortfolioPnlAnchorRow
from snapper.data.repository_types import PortfolioPnlSampleRow
from snapper.data.repository_types import WalletRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server import portfolio_timeline_routes
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency

_FROM = "2026-07-20T10:00:00Z"
_TO = "2026-07-20T10:02:00Z"
_AS_OF = "2026-07-20T10:02:30Z"
_WALLET = "0000face-0000-7000-8000-0000000000a1"
_ANCHOR_TIME = datetime(2026, 7, 1, tzinfo=UTC)
_ANCHOR_SESSION = "0000face-0000-7000-8000-0000000000a2"


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


def _empty_anchor(
    mode: Literal["live", "paper"] = "live",
    valuation_ccy: str = "USD",
) -> PortfolioPnlAnchorRow:
    """Build the canonical empty anchor used by route-only mocks."""
    public_id = portfolio_pnl_anchor_public_id(_WALLET, mode, valuation_ccy)
    return {
        "public_id": public_id,
        "session_id": _ANCHOR_SESSION,
        "sequence_id": 1,
        "timestamp": _ANCHOR_TIME,
        "wallet_public_id": _WALLET,
        "mode": mode,
        "valuation_ccy": valuation_ccy,
        "point_time": _ANCHOR_TIME,
        "point_kind": "anchor",
        "epoch_public_id": public_id,
        "calc_version": PNL_TIMELINE_CALC_VERSION,
        "valuation_status": "complete",
        "realized_pnl": 0.0,
        "fee_pnl": 0.0,
        "accrual_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "external_flow_adjustment": 0.0,
        "cash_usd": None,
        "position_value_usd": None,
        "drawdown": None,
        "mark_source": PNL_TIMELINE_MARK_SOURCE,
        "mark_time": _ANCHOR_TIME,
        "watermarks_json": "{}",
        "opening_basket_json": json.dumps(
            {"annulments": [], "native_basket": {}, "pools": [], "schema_version": 3},
            separators=(",", ":"),
            sort_keys=True,
        ),
        "contributions_json": json.dumps(
            {"pools": [], "schema_version": 3},
            separators=(",", ":"),
            sort_keys=True,
        ),
    }


def _anchor_for_read(
    _wallet_public_id: str,
    mode: Literal["live", "paper"],
    valuation_ccy: str,
    _as_of: datetime | None,
) -> PortfolioPnlAnchorRow:
    """Return a scope-matching empty anchor for one mocked repository read."""
    return _empty_anchor(mode, valuation_ccy)


def _seeded_repo() -> AsyncMock:
    """Build a repo mock returning one buy on a USD instrument with marks."""
    t0 = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
    repo = AsyncMock()
    execution_rows = [
        {
            "public_id": "execution-1",
            "instrument_public_id": "i1",
            "exchange": "kraken",
            "scope_sequence": 1,
            "order_public_id": "o1",
            "client_order_id": "c1",
            "shard_key": "kraken:live:i1:untagged",
            "side": "buy",
            "status": "filled",
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
    execution_prefix = {
        "watermarks": {"kraken": 1},
        "executions": execution_rows,
        "annulments": [],
    }
    repo.get_pnl_timeline_execution_prefix = AsyncMock(return_value=execution_prefix)
    repo.get_pnl_timeline_execution_prefix_bundle = AsyncMock(
        return_value={
            "request": execution_prefix,
            "activation": execution_prefix,
        }
    )
    repo.get_portfolio_pnl_anchor = AsyncMock(side_effect=_anchor_for_read)
    repo.record_portfolio_pnl_anchor = AsyncMock()
    repo.get_pnl_timeline_execution_lineage = AsyncMock(
        return_value=[
            {
                "order_public_id": "o1",
                "source_surface": "strategy",
                "plan_public_id": None,
                "signal_public_id": "signal-1",
                "origin": "live",
                "strategy_name": "momentum",
            }
        ]
    )
    repo.get_pnl_timeline_signals = AsyncMock(
        return_value=[
            {
                "public_id": "signal-1",
                "instrument_public_id": "i1",
                "fired_at": t0 + timedelta(seconds=10),
                "side": "buy",
                "strategy_name": "momentum",
                "strength": 0.8,
                "reason": "breakout",
                "price": 101.0,
                "has_execution": False,
            }
        ]
    )
    repo.get_pnl_timeline_ai_decisions = AsyncMock(
        return_value=[
            {
                "event_public_id": "ai-event-1",
                "review_public_id": "ai-review-1",
                "instrument_public_id": "i1",
                "strategy_public_id": "strategy-1",
                "occurred_at": t0 + timedelta(seconds=20),
                "new_status": "resolved_rejected",
                "payload": {"decision": "reject", "rationale": "risk too high"},
                "has_execution": False,
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
                "instrument_exchange": "kraken",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "valid_from": t0 - timedelta(days=1),
                "valid_to": datetime.max.replace(tzinfo=UTC),
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
    repo.pnl_timeline_scope_has_fill_gap = AsyncMock(return_value=False)
    repo.get_portfolio_pnl_samples = AsyncMock(return_value=[])
    return repo


_CORRECTION_ID = "00000000-0000-7000-8000-0000000000d9"
_PHANTOM_EXECUTION_ID = "00000000-0000-7000-8000-0000000000e1"


def _corrected_repo() -> AsyncMock:
    """Build a repo mock whose certified prefix folded one operator correction."""
    repo = _seeded_repo()
    prefix = repo.get_pnl_timeline_execution_prefix.return_value
    prefix["annulments"] = [
        {
            "public_id": _CORRECTION_ID,
            "target_execution_public_id": _PHANTOM_EXECUTION_ID,
            "target_execution_digest": "ab" * 32,
            "exchange": "kraken",
            "scope_sequence": 7,
            "reason": "unwitnessed_phantom",
            "correction_time": datetime(2026, 7, 20, 9, 55, tzinfo=UTC),
        }
    ]
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


def _timeline_url(**params: str) -> str:
    """Build the marker-bearing timeline URL with the given query params."""
    return _url(**params).replace("/pnl/series?", "/pnl/timeline?")


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
        assert payload["rate_sources"] == []
        assert payload["calc_version"] == "5A.13"
        points = payload["points"]
        assert len(points) == 3
        assert points[0]["valuation_status"] == "complete"
        assert points[0]["incompleteness_reasons"] == []
        assert points[0]["fee_pnl"] == -0.5
        assert points[0]["unrealized_pnl"] == 5.0
        assert points[0]["net_pnl"] == 4.5
        assert points[0]["per_instrument"][0]["instrument_public_id"] == "i1"
        assert points[0]["per_instrument"][0]["native_symbol"] == "BTC-USD"
        assert points[0]["per_instrument"][0]["exchange"] == "kraken"
        assert points[0]["attribution"] == [
            {
                "origin": "system",
                "strategy_name": "momentum",
                "realized_pnl": 0.0,
                "fee_pnl": -0.5,
                "accrual_pnl": 0.0,
                "unrealized_pnl": 5.0,
            }
        ]
        repo.get_pnl_timeline_candles.assert_awaited_once()
        repo.get_candles.assert_not_awaited()

    def test_exposes_the_selected_fx_rate_source(self) -> None:
        """A converted series identifies every venue plane used by a contribution."""
        repo = _seeded_repo()
        t0 = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
        repo.get_instrument_symbol_refs = AsyncMock(
            return_value=[
                {
                    "instrument_public_id": "i1",
                    "native_symbol": "BTC-EUR",
                    "exchange": "kraken",
                    "instrument_exchange": "kraken",
                    "base_currency": "BTC",
                    "quote_currency": "EUR",
                    "valid_from": t0 - timedelta(days=1),
                    "valid_to": datetime.max.replace(tzinfo=UTC),
                }
            ]
        )
        repo.get_pnl_fx_rate_exchanges = AsyncMock(return_value=[("EUR", "USD", "kraken")])
        repo.get_pnl_fx_rate_candles = AsyncMock(
            return_value=[
                {
                    "base": "EUR",
                    "quote": "USD",
                    "exchange": "kraken",
                    "open_at": t0 - timedelta(minutes=1),
                    "close": 1.2,
                },
                {
                    "base": "EUR",
                    "quote": "USD",
                    "exchange": "kraken",
                    "open_at": t0,
                    "close": 1.2,
                },
                {
                    "base": "EUR",
                    "quote": "USD",
                    "exchange": "kraken",
                    "open_at": t0 + timedelta(minutes=1),
                    "close": 1.2,
                },
            ]
        )
        response = _create_client(repo).get(_url(as_of=_AS_OF))
        assert response.status_code == 200
        assert response.json()["payload"]["rate_sources"] == [
            {
                "source_currency": "EUR",
                "valuation_currency": "USD",
                "base_currency": "EUR",
                "quote_currency": "USD",
                "exchange": "kraken",
            }
        ]

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

    def test_explicit_as_of_is_shared_and_exposed(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An explicit knowledge horizon is used by every series input read."""
        repo = _seeded_repo()
        build = AsyncMock(wraps=portfolio_timeline_routes.build_wallet_pnl_series)
        monkeypatch.setattr(portfolio_timeline_routes, "build_wallet_pnl_series", build)
        client = _create_client(repo)
        response = client.get(_url(as_of=_AS_OF))
        assert response.status_code == 200
        response_as_of = datetime.fromisoformat(response.json()["payload"]["as_of"])
        assert response_as_of == datetime.fromisoformat(_AS_OF)
        assert repo.list_active_wallets.await_args.args[0] == response_as_of
        assert repo.get_pnl_timeline_execution_prefix_bundle.await_args.args[2] == response_as_of
        assert repo.get_pnl_timeline_execution_lineage.await_args.args[1] == response_as_of
        assert repo.get_accruals_for_pnl.await_args.args[2] == response_as_of
        assert repo.get_instrument_symbol_refs.await_args.args[1] == response_as_of
        assert repo.get_pnl_timeline_candles.await_args.args[3] == response_as_of
        build_call = build.await_args
        assert build_call is not None
        assert build_call.kwargs["policy"].allow_anchor_creation is False
        assert build_call.kwargs["policy"].current_truth is False

    def test_default_read_horizon_remains_current(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Omitting ``as_of`` captures one current UTC horizon as before."""
        repo = _seeded_repo()
        build = AsyncMock(wraps=portfolio_timeline_routes.build_wallet_pnl_series)
        monkeypatch.setattr(portfolio_timeline_routes, "build_wallet_pnl_series", build)
        client = _create_client(repo)
        before = datetime.now(UTC)
        response = client.get(_url())
        after = datetime.now(UTC)
        assert response.status_code == 200
        response_as_of = datetime.fromisoformat(response.json()["payload"]["as_of"])
        assert before <= response_as_of <= after
        assert repo.list_active_wallets.await_args.args[0] == response_as_of
        assert repo.get_pnl_timeline_execution_prefix_bundle.await_args.args[2] is None
        assert repo.get_pnl_timeline_execution_prefix_bundle.await_args.args[3] == response_as_of
        build_call = build.await_args
        assert build_call is not None
        assert build_call.kwargs["policy"].allow_anchor_creation is True
        assert build_call.kwargs["policy"].current_truth is True

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

    def test_fill_gap_withholds_attribution_components(self) -> None:
        """A proven untrusted prefix transports bucket values as null."""
        repo = _seeded_repo()
        repo.pnl_timeline_scope_has_fill_gap = AsyncMock(return_value=True)
        response = _create_client(repo).get(_url())
        assert response.status_code == 200
        point = response.json()["payload"]["points"][0]
        assert point["valuation_status"] == "incomplete"
        assert point["realized_pnl"] is None
        assert point["fee_pnl"] is None
        assert point["accrual_pnl"] is None
        assert point["unrealized_pnl"] is None
        assert point["net_pnl"] is None
        assert point["incompleteness_reasons"] == [
            {
                "reason": "fill_evidence_gap",
                "withholding_tier": "untrusted",
                "withholding_scope": "global",
                "trigger_instrument_public_id": None,
            }
        ]
        assert point["attribution"] == [
            {
                "origin": "system",
                "strategy_name": "momentum",
                "realized_pnl": None,
                "fee_pnl": None,
                "accrual_pnl": None,
                "unrealized_pnl": None,
            }
        ]

    def test_transports_multi_cause_instrument_reasons(self) -> None:
        """A MARK cause and an UNTRUSTED cause retain both instrument identities."""
        repo = _seeded_repo()
        t0 = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
        repo.get_pnl_timeline_execution_prefix_bundle.return_value["request"]["executions"].append(
            {
                "public_id": "execution-2",
                "instrument_public_id": "i2",
                "exchange": "kraken",
                "scope_sequence": 2,
                "order_public_id": "o2",
                "client_order_id": "c2",
                "shard_key": "kraken:live:i2:untagged",
                "side": "buy",
                "status": "filled",
                "size": 1.0,
                "price": 100.0,
                "fee": 0.0,
                "fee_asset": "USD",
                "executed_at": None,
                "timestamp": t0,
                "exec_id": "e2",
                "trade_id": "t2",
            }
        )
        repo.get_pnl_timeline_execution_prefix_bundle.return_value["request"]["watermarks"][
            "kraken"
        ] = 2
        repo.get_pnl_timeline_candles = AsyncMock(return_value=[])
        response = _create_client(repo).get(_url(**{"to": _FROM}))
        assert response.status_code == 200
        point = response.json()["payload"]["points"][0]
        assert point["valuation_status"] == "incomplete"
        assert point["incompleteness_reasons"] == [
            {
                "reason": "mark_unavailable",
                "withholding_tier": "mark_incomplete",
                "withholding_scope": "instrument",
                "trigger_instrument_public_id": "i1",
            },
            {
                "reason": "execution_price_provenance_unproven",
                "withholding_tier": "untrusted",
                "withholding_scope": "instrument",
                "trigger_instrument_public_id": "i2",
            },
        ]
        assert point["per_instrument"] == [
            {
                "instrument_public_id": "i1",
                "native_symbol": "BTC-USD",
                "exchange": "kraken",
                "realized_pnl": 0.0,
                "fee_pnl": -0.5,
                "accrual_pnl": 0.0,
                "unrealized_pnl": None,
            },
            {
                "instrument_public_id": "i2",
                "native_symbol": None,
                "exchange": None,
                "realized_pnl": None,
                "fee_pnl": None,
                "accrual_pnl": None,
                "unrealized_pnl": None,
            },
        ]


class TestMarkerTimeline:
    """Cover the marker-bearing endpoint response and failure disclosure."""

    def test_default_read_horizon_remains_current(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The marker endpoint keeps live behavior when ``as_of`` is omitted."""
        repo = _seeded_repo()
        build = AsyncMock(wraps=portfolio_timeline_routes.build_wallet_pnl_timeline)
        monkeypatch.setattr(portfolio_timeline_routes, "build_wallet_pnl_timeline", build)
        client = _create_client(repo)
        before = datetime.now(UTC)
        response = client.get(_timeline_url())
        after = datetime.now(UTC)
        assert response.status_code == 200
        response_as_of = datetime.fromisoformat(response.json()["payload"]["as_of"])
        assert before <= response_as_of <= after
        assert repo.get_pnl_timeline_execution_prefix_bundle.await_args.args[2] is None
        assert repo.get_pnl_timeline_execution_prefix_bundle.await_args.args[3] == response_as_of
        assert repo.get_pnl_timeline_signals.await_args.args[4] == response_as_of
        assert repo.get_pnl_timeline_ai_decisions.await_args.args[4] == response_as_of
        build_call = build.await_args
        assert build_call is not None
        assert build_call.kwargs["policy"].allow_anchor_creation is True
        assert build_call.kwargs["policy"].current_truth is True

    def test_returns_all_marker_kinds_with_no_fill_and_rejection(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Independent signal and AI reads preserve decisions without fills."""
        repo = _seeded_repo()
        build = AsyncMock(wraps=portfolio_timeline_routes.build_wallet_pnl_timeline)
        monkeypatch.setattr(portfolio_timeline_routes, "build_wallet_pnl_timeline", build)
        client = _create_client(repo)
        response = client.get(_timeline_url(as_of=_AS_OF))
        assert response.status_code == 200
        body = response.json()
        assert body["type"] == "pnl_timeline"
        payload = body["payload"]
        assert payload["wallet_public_id"] == _WALLET
        assert payload["granularity"] == "1m"
        assert payload["marker_limit"] == PNL_TIMELINE_MARKER_LIMIT
        assert payload["markers_truncated"] is False
        assert payload["rate_sources"] == []
        assert len(payload["points"]) == 3
        markers = {marker["kind"]: marker for marker in payload["markers"]}
        assert set(markers) == {"fill", "signal", "ai_decision"}
        assert markers["fill"]["execution_public_id"] == "execution-1"
        assert markers["fill"]["order_public_id"] == "o1"
        assert markers["fill"]["status"] == "filled"
        assert markers["fill"]["outcome"] == "executed"
        assert markers["signal"]["signal_public_id"] == "signal-1"
        assert markers["signal"]["outcome"] == "no_fill"
        assert markers["signal"]["status"] == "no_fill"
        assert markers["ai_decision"]["event_public_id"] == "ai-event-1"
        assert markers["ai_decision"]["decision"] == "reject"
        assert markers["ai_decision"]["rationale"] == "risk too high"
        assert markers["ai_decision"]["outcome"] == "rejected"
        response_as_of = datetime.fromisoformat(payload["as_of"])
        assert response_as_of == datetime.fromisoformat(_AS_OF)
        repo.get_pnl_timeline_execution_prefix_bundle.assert_awaited_once()
        assert repo.get_pnl_timeline_execution_prefix_bundle.await_args.args[2] == response_as_of
        assert repo.get_pnl_timeline_signals.await_args.args[4] == response_as_of
        assert repo.get_pnl_timeline_ai_decisions.await_args.args[4] == response_as_of
        assert repo.get_pnl_timeline_signals.await_args.args[5] == 2_001
        assert repo.get_pnl_timeline_ai_decisions.await_args.args[5] == 2_001
        build_call = build.await_args
        assert build_call is not None
        assert build_call.kwargs["policy"].allow_anchor_creation is False
        assert build_call.kwargs["policy"].current_truth is False

    def test_withholds_fill_marker_price_without_denomination_proof(self) -> None:
        """The API keeps a fill marker but emits null for its unproved price."""
        repo = _seeded_repo()
        repo.get_instrument_symbol_refs = AsyncMock(return_value=[])
        response = _create_client(repo).get(_timeline_url(**{"to": _FROM}))
        assert response.status_code == 200
        fill = next(
            marker for marker in response.json()["payload"]["markers"] if marker["kind"] == "fill"
        )
        assert fill["price"] is None

    def test_marker_truncation_is_disclosed_and_latest_are_retained(self) -> None:
        """A busy response states truncation and deterministically drops oldest."""
        repo = _seeded_repo()
        t0 = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
        repo.get_pnl_timeline_signals = AsyncMock(
            return_value=[
                {
                    "public_id": f"signal-{index:04d}",
                    "instrument_public_id": "i1",
                    "fired_at": t0,
                    "side": "buy",
                    "strategy_name": None,
                    "strength": 0.5,
                    "reason": "test",
                    "price": None,
                    "has_execution": False,
                }
                for index in reversed(range(PNL_TIMELINE_MARKER_LIMIT + 1))
            ]
        )
        repo.get_pnl_timeline_ai_decisions = AsyncMock(return_value=[])
        response = _create_client(repo).get(_timeline_url(**{"to": _FROM}))
        assert response.status_code == 200
        payload = response.json()["payload"]
        assert payload["marker_limit"] == PNL_TIMELINE_MARKER_LIMIT
        assert payload["markers_truncated"] is True
        assert len(payload["markers"]) == PNL_TIMELINE_MARKER_LIMIT
        assert payload["markers"][0]["signal_public_id"] == "signal-0001"
        assert payload["markers"][-1]["signal_public_id"] == "signal-2000"

    def test_work_budget_error_matches_series_validation(self) -> None:
        """Timeline requests surface excessive reconstruction work as 400."""
        repo = _seeded_repo()
        response = _create_client(repo).get(
            _timeline_url(
                **{
                    "from": "2020-01-01T00:00:00Z",
                    "to": "2026-01-01T00:00:00Z",
                }
            )
        )
        assert response.status_code == 400
        assert "minute-instrument work units" in response.json()["detail"]
        repo.get_pnl_timeline_signals.assert_not_awaited()
        repo.get_pnl_timeline_ai_decisions.assert_not_awaited()

    def test_unexpected_marker_read_failure_is_wrapped_as_500(self) -> None:
        """An unexpected independent marker-read error gets a stable 500."""
        repo = _seeded_repo()
        repo.get_pnl_timeline_signals = AsyncMock(side_effect=RuntimeError("boom"))
        response = _create_client(repo).get(_timeline_url())
        assert response.status_code == 500
        assert response.json()["detail"] == "Failed to build P&L timeline"

    def test_invalid_mode_uses_the_same_request_guard(self) -> None:
        """The marker endpoint shares the exact live/paper mode validation."""
        repo = _seeded_repo()
        response = _create_client(repo).get(_timeline_url(mode="LIVE"))
        assert response.status_code == 400
        assert "expected 'live' or 'paper'" in response.json()["detail"]
        repo.list_active_wallets.assert_not_awaited()


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
        repo.get_pnl_timeline_execution_prefix_bundle.assert_not_awaited()

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

    def test_malformed_as_of_is_rejected_as_400(self) -> None:
        """A malformed knowledge horizon is a route-level 400 rather than a 422."""
        client = _create_client(_seeded_repo())
        response = client.get(_url(as_of="not-a-datetime"))
        assert response.status_code == 400
        assert response.json()["detail"] == "as_of is not a supported ISO-8601 datetime"

    def test_as_of_before_safe_range_is_rejected_as_400(self) -> None:
        """The minimum UTC minute is rejected as an unsafe knowledge horizon."""
        client = _create_client(_seeded_repo())
        response = client.get(_url(as_of="0001-01-01T00:00:00Z"))
        assert response.status_code == 400
        assert "window or as_of" in response.json()["detail"]

    def test_as_of_after_safe_range_is_rejected_as_400(self) -> None:
        """The final UTC minute is rejected as an unsafe knowledge horizon."""
        client = _create_client(_seeded_repo())
        response = client.get(_url(as_of="9999-12-31T23:59:59Z"))
        assert response.status_code == 400
        assert "window or as_of" in response.json()["detail"]

    def test_window_cannot_extend_past_as_of(self) -> None:
        """Both endpoints reject a window past the knowledge horizon actionably."""
        repo = _seeded_repo()
        client = _create_client(repo)
        responses = [
            client.get(_url(as_of="2026-07-20T10:01:59Z")),
            client.get(_timeline_url(as_of="2026-07-20T10:01:59Z")),
        ]
        assert [response.status_code for response in responses] == [400, 400]
        assert {response.json()["detail"] for response in responses} == {
            "to must be less than or equal to as_of; shorten the window or move as_of forward"
        }
        repo.list_active_wallets.assert_not_awaited()
        repo.get_pnl_timeline_execution_prefix_bundle.assert_not_awaited()

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
        repo.get_pnl_timeline_execution_prefix_bundle = AsyncMock(side_effect=RuntimeError("boom"))
        client = _create_client(repo)
        response = client.get(_url())
        assert response.status_code == 500
        assert response.json()["detail"] == "Failed to build P&L series"


class TestValuationCurrency:
    """Cover the valuation-currency parameter and its validation."""

    def test_defaults_to_usd(self) -> None:
        """Omitting the parameter values the series in USD."""
        repo = _seeded_repo()
        response = _create_client(repo).get(_url())
        assert response.status_code == 200
        assert response.json()["payload"]["valuation_ccy"] == "USD"

    def test_requested_currency_reaches_the_reconstruction(self) -> None:
        """A requested currency is normalized and threaded to the series build.

        A scope trading natively in another currency (a PLN walutomat wallet)
        only resolves fully when asked for in that currency, so the parameter has
        to reach the reconstruction rather than merely being echoed.
        """
        repo = _seeded_repo()
        response = _create_client(repo).get(_url(valuation_ccy="pln"))
        assert response.status_code == 200
        assert response.json()["payload"]["valuation_ccy"] == "PLN"

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("US", id="too_short"),
            pytest.param("USDT", id="too_long"),
            pytest.param("US1", id="not_alphabetic"),
            pytest.param("", id="empty"),
        ],
    )
    def test_malformed_currency_is_rejected(self, value: str) -> None:
        """A code that is not three letters is a 400, not a silent fallback."""
        response = _create_client(_seeded_repo()).get(_url(valuation_ccy=value))
        assert response.status_code == 400
        assert "valuation_ccy" in response.json()["detail"]

    def test_wellformed_but_unpriceable_currency_still_answers(self) -> None:
        """An unpriceable-but-well-formed code returns honest points, not a 400.

        The resolvable set is whatever our own candle plane can price and it grows
        as venues are added, so shape is validated rather than an allowlist; the
        series then withholds what it cannot value instead of refusing outright.
        """
        response = _create_client(_seeded_repo()).get(_url(valuation_ccy="JPY"))
        assert response.status_code == 200
        assert response.json()["payload"]["valuation_ccy"] == "JPY"


def _sample_audit() -> str:
    """Serialize one complete sample's canonical coverage-bearing audit envelope."""
    return json.dumps(
        {
            "coverage": {
                "leveraged_inventory_excluded": False,
                "non_finite_position_excluded": False,
                "venue_scope": "spot_only",
                "external_flows_adjusted": False,
            },
            "observations": [],
            "valuation": [],
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _sample_row(
    point_time: datetime,
    cash_usd: float,
    position_value_usd: float,
    drawdown: float,
) -> PortfolioPnlSampleRow:
    """Build one complete persisted sample row for the current live USD epoch."""
    epoch = portfolio_pnl_anchor_public_id(_WALLET, "live", "USD")
    return {
        "public_id": f"sample-{point_time.isoformat()}",
        "session_id": _ANCHOR_SESSION,
        "sequence_id": 1,
        "timestamp": point_time,
        "wallet_public_id": _WALLET,
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": point_time,
        "point_kind": "sample",
        "epoch_public_id": epoch,
        "calc_version": PNL_SAMPLE_CALC_VERSION,
        "valuation_status": "complete",
        "realized_pnl": 0.0,
        "fee_pnl": 0.0,
        "accrual_pnl": 0.0,
        "external_flow_adjustment": 0.0,
        "unrealized_pnl": 5.0,
        "cash_usd": cash_usd,
        "position_value_usd": position_value_usd,
        "drawdown": drawdown,
        "mark_source": PNL_TIMELINE_MARK_SOURCE,
        "mark_time": point_time,
        "audit_json": _sample_audit(),
        "watermarks_json": "{}",
    }


class TestEquityOverlayResponse:
    """Cover the observed-equity overlay projected onto both endpoints (D13)."""

    def test_series_response_exposes_equity_overlay_and_coverage(self) -> None:
        """A current USD series carries per-point equity and a sampled disclosure."""
        t0 = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
        repo = _seeded_repo()
        repo.get_portfolio_pnl_samples = AsyncMock(
            return_value=[
                _sample_row(t0, 100.0, 50.0, 0.02),
                _sample_row(t0 + timedelta(minutes=1), 400.0, 600.0, 0.1),
                _sample_row(t0 + timedelta(minutes=2), 700.0, 300.0, 0.2),
            ]
        )
        response = _create_client(repo).get(_url())
        assert response.status_code == 200
        payload = response.json()["payload"]
        assert payload["equity_coverage"] == {
            "sampled": True,
            "venue_scope": "spot_only",
            "external_flows_adjusted": False,
            "complete_minutes": 3,
            "first_minute": "2026-07-20T10:00:00Z",
            "last_minute": "2026-07-20T10:02:00Z",
            "sample_calc_version": "5B.1",
        }
        points = payload["points"]
        assert (points[0]["equity"], points[0]["cash"], points[0]["position_value"]) == (
            150.0,
            100.0,
            50.0,
        )
        assert points[0]["drawdown"] == 0.02
        assert points[1]["equity"] == 1000.0
        assert points[2]["equity"] == 1000.0
        repo.get_portfolio_pnl_samples.assert_awaited_once()

    def test_series_without_samples_is_unsampled_with_null_points(self) -> None:
        """A scope with no complete sample nulls each point and reports unsampled."""
        response = _create_client(_seeded_repo()).get(_url())
        assert response.status_code == 200
        payload = response.json()["payload"]
        assert payload["equity_coverage"] == {
            "sampled": False,
            "venue_scope": None,
            "external_flows_adjusted": None,
            "complete_minutes": 0,
            "first_minute": None,
            "last_minute": None,
            "sample_calc_version": None,
        }
        assert all(point["equity"] is None for point in payload["points"])
        assert all(point["drawdown"] is None for point in payload["points"])

    def test_historical_request_reads_no_samples(self) -> None:
        """A historical ``as_of`` request never reads or overlays samples."""
        repo = _seeded_repo()
        response = _create_client(repo).get(_url(as_of=_AS_OF))
        assert response.status_code == 200
        assert response.json()["payload"]["equity_coverage"]["sampled"] is False
        repo.get_portfolio_pnl_samples.assert_not_awaited()

    def test_timeline_response_exposes_equity_coverage(self) -> None:
        """The marker-bearing timeline envelope also carries the overlay disclosure."""
        t0 = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
        repo = _seeded_repo()
        repo.get_portfolio_pnl_samples = AsyncMock(
            return_value=[_sample_row(t0 + timedelta(minutes=2), 400.0, 600.0, 0.1)]
        )
        response = _create_client(repo).get(_timeline_url())
        assert response.status_code == 200
        payload = response.json()["payload"]
        assert payload["type"] == "pnl_timeline"
        assert payload["equity_coverage"]["sampled"] is True
        assert payload["equity_coverage"]["complete_minutes"] == 1
        assert payload["points"][2]["equity"] == 1000.0
        assert payload["points"][0]["equity"] is None


class TestExecutionHistoryDisclosure:
    """Cover the permanent operator-correction channel on both endpoints (A3)."""

    def test_series_response_names_every_folded_correction(self) -> None:
        """A corrected scope transports the correction, never just the numbers."""
        response = _create_client(_corrected_repo()).get(_url())
        assert response.status_code == 200
        assert response.json()["payload"]["execution_history"] == {
            "status": "operator_corrected",
            "corrections": [
                {
                    "correction_public_id": _CORRECTION_ID,
                    "target_execution_public_id": _PHANTOM_EXECUTION_ID,
                    "exchange": "kraken",
                    "scope_sequence": 7,
                    "reason": "unwitnessed_phantom",
                    "correction_time": "2026-07-20T09:55:00Z",
                }
            ],
        }

    def test_timeline_response_names_every_folded_correction(self) -> None:
        """The marker-bearing envelope carries the identical disclosure."""
        response = _create_client(_corrected_repo()).get(_timeline_url())
        assert response.status_code == 200
        payload = response.json()["payload"]
        assert payload["type"] == "pnl_timeline"
        assert payload["execution_history"]["status"] == "operator_corrected"
        assert payload["execution_history"]["corrections"][0]["scope_sequence"] == 7

    def test_uncorrected_scope_reports_an_as_recorded_history(self) -> None:
        """Both endpoints state the history is the raw ledger when it is."""
        series = _create_client(_seeded_repo()).get(_url())
        timeline = _create_client(_seeded_repo()).get(_timeline_url())
        assert series.status_code == 200
        assert timeline.status_code == 200
        as_recorded = {"status": "as_recorded", "corrections": []}
        assert series.json()["payload"]["execution_history"] == as_recorded
        assert timeline.json()["payload"]["execution_history"] == as_recorded

    def test_correction_digest_is_not_transported(self) -> None:
        """The disclosure names what was corrected without leaking the row digest."""
        response = _create_client(_corrected_repo()).get(_url())
        assert response.status_code == 200
        correction = response.json()["payload"]["execution_history"]["corrections"][0]
        assert "target_execution_digest" not in correction
