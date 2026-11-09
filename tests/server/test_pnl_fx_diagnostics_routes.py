"""Prove FX diagnostics preserve actual response bytes and repository requests."""

import inspect
import json
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import datetime
from typing import Literal
from typing import cast
from uuid import UUID

import pytest
from fastapi import FastAPI
from freezegun import freeze_time
from starlette.requests import Request

from snapper.api.schemas.pnl_timeline import PnlSeriesResponse
from snapper.api.schemas.pnl_timeline import PnlTimelineResponse
from snapper.application.portfolio import pnl_read_timing
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.json_types import JsonObject
from snapper.data.repository import Repository
from snapper.data.repository_types import WalletRow
from snapper.messaging.infrastructure import publisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server import portfolio_timeline_routes
from tests.application.portfolio.test_pnl_timeline_service import _I1
from tests.application.portfolio.test_pnl_timeline_service import _W1
from tests.application.portfolio.test_pnl_timeline_service import FakeRepo
from tests.application.portfolio.test_pnl_timeline_service import _candle
from tests.application.portfolio.test_pnl_timeline_service import _exec_row
from tests.application.portfolio.test_pnl_timeline_service import _m
from tests.application.portfolio.test_pnl_timeline_service import _ref

type _Handler = Callable[..., Awaitable[PnlSeriesResponse | PnlTimelineResponse]]


class _FxRepo(FakeRepo):
    """Retain real service reads and add only the route's wallet catalogue."""

    async def list_active_wallets(self, _as_of: datetime) -> list[WalletRow]:
        """Return the synthetic readable scope without exposing its label in logs."""
        return [
            {
                "public_id": _W1,
                "label": "wallet-label-private-sentinel",
                "description": None,
                "is_paper": False,
                "timestamp": _m(-100),
                "session_id": _I1,
                "sequence_id": 1,
            }
        ]


def _repo() -> _FxRepo:
    """Build a request whose price and mark conversions lack an elected FX plane."""
    return _FxRepo(
        executions=[_exec_row(_I1, 1, 0, "buy", 1.0, 987654.125, 0.0, "USD")],
        refs=[_ref(_I1, "BTC-EUR", "EUR")],
        candles=[_candle(_m(-1), 876543.125), _candle(_m(0), 876543.125)],
    )


async def _response(handler: _Handler, repo: _FxRepo) -> PnlSeriesResponse | PnlTimelineResponse:
    """Call an actual route with fixed evidence and fresh deterministic provenance."""
    app = FastAPI()
    app.state.rest_tracker = SequenceTracker()
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/request-path-private-sentinel",
            "headers": [],
            "app": app,
        }
    )
    principal = AuthPrincipal(username="username-private-sentinel", role=UserRole.AUDITOR)
    return await handler(
        request,
        principal,
        None,
        cast(Repository, repo),
        wallet_public_id=_W1,
        from_time=_m(0).isoformat(),
        to_time=_m(1).isoformat(),
        as_of=_m(3).isoformat(),
    )


@pytest.mark.parametrize("kind", ["series", "timeline"])
@pytest.mark.parametrize("logging_fails", [False, True])
async def test_diagnostics_preserve_actual_response_and_queries(
    monkeypatch: pytest.MonkeyPatch,
    kind: Literal["series", "timeline"],
    logging_fails: bool,
) -> None:
    """A real FX refusal has identical HTTP data and reads with or without observation.

    Given: The actual handler, service and builder reconstruct an unproven foreign price.
    When: The handler runs through its observer, including a failing logging sink.
    Then: Full serialized responses and repository calls match the unwrapped handler.
    """
    messages: list[str] = []

    def capture(message: str, *args: object) -> None:
        """Capture message text before optionally simulating the configured sink's failure."""
        messages.append(message.format(*args))
        if logging_fails:
            raise RuntimeError("logging-sink-private-sentinel")

    def identity() -> UUID:
        """Keep generated provenance identical for the observed and baseline calls."""
        return UUID("00000000-0000-7000-8000-000000000f01")

    monkeypatch.setattr(pnl_read_timing.logger, "info", capture)
    monkeypatch.setattr(portfolio_timeline_routes, "uuid7", identity)
    monkeypatch.setattr(publisher, "uuid7", identity)
    handler = (
        portfolio_timeline_routes.get_pnl_series
        if kind == "series"
        else portfolio_timeline_routes.get_pnl_timeline
    )
    observed_repo, baseline_repo = _repo(), _repo()
    with freeze_time(_m(10)):
        baseline = await _response(cast(_Handler, inspect.unwrap(handler)), baseline_repo)
        observed = await _response(handler, observed_repo)
    assert observed.model_dump_json() == baseline.model_dump_json()
    assert observed_repo.__dict__ == baseline_repo.__dict__
    assert "fx_diagnostics" not in observed.model_dump_json()
    summaries = [message for message in messages if message.startswith("PnlReadTiming ")]
    assert len(summaries) == 1
    prefix, suffix = summaries[0].split(" fx_diagnostics=", 1)
    assert f"kind={kind} outcome=success" in prefix
    summary = cast(JsonObject, json.loads(suffix))
    failure = cast(JsonObject, summary["first_failure"])
    assert failure["instrument"] == _I1
    assert failure["reason"] == "no_pin"
    elections = summary["elections"]
    assert isinstance(elections, list)
    election = cast(JsonObject, elections[0])
    assert election["event_count"] == 1
    assert election["complete_plane_exists"] is False
    assert "\n" not in summaries[0]
    for private_value in (
        _W1,
        "987654.125",
        "876543.125",
        "username-private-sentinel",
        "wallet-label-private-sentinel",
        "request-path-private-sentinel",
        "logging-sink-private-sentinel",
    ):
        assert private_value not in summaries[0]
