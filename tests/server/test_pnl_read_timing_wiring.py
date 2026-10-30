"""Exercise timing through actual handlers, services and pure core construction."""

import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from typing import cast

import pytest
from fastapi import FastAPI
from starlette.requests import Request

from snapper.application.portfolio import pnl_read_timing
from snapper.application.portfolio.pnl_anchor_identity import portfolio_pnl_anchor_public_id
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MARK_SOURCE
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineAiDecisionMarkerRow
from snapper.data.repository_types import PnlTimelineExecutionLineageRow
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineExecutionPrefixBundle
from snapper.data.repository_types import PnlTimelineSignalMarkerRow
from snapper.data.repository_types import PortfolioPnlAnchorRow
from snapper.data.repository_types import WalletRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server import portfolio_timeline_routes

_T0 = datetime(2026, 10, 28, 17, 33, tzinfo=UTC)
_H = _T0 + timedelta(minutes=5)
_WALLET = "00000000-0000-7000-8000-000000000801"
_SESSION = "00000000-0000-7000-8000-000000000802"


def _anchor(point_time: datetime) -> PortfolioPnlAnchorRow:
    """Use the accepted empty opening format without mocking reconstruction math."""
    identity = portfolio_pnl_anchor_public_id(_WALLET, "live", "USD")
    return {
        "public_id": identity,
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": point_time,
        "wallet_public_id": _WALLET,
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": point_time,
        "point_kind": "anchor",
        "epoch_public_id": identity,
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
        "mark_time": point_time,
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


class _ReadRepo:
    """Supply typed empty durable evidence while leaving service/core logic real."""

    def __init__(self, point_time: datetime | None) -> None:
        """Retain one synthetic scope and count the shared evidence reads."""
        self.anchor = None if point_time is None else _anchor(point_time)
        self.anchor_calls = 0
        self.prefix_calls = 0
        self.bundle_calls = 0

    async def list_active_wallets(self, _as_of: datetime) -> list[WalletRow]:
        """Provide the real route's wallet-mode validation evidence."""
        return [
            {
                "public_id": _WALLET,
                "label": "private-label-sentinel",
                "description": None,
                "is_paper": False,
                "timestamp": _T0 - timedelta(days=1),
                "session_id": _SESSION,
                "sequence_id": 1,
            }
        ]

    async def get_portfolio_pnl_anchor(self, *_args: object) -> PortfolioPnlAnchorRow | None:
        """Return one visible or absent anchor without a write implementation."""
        self.anchor_calls += 1
        return self.anchor

    async def get_pnl_timeline_execution_prefix(self, *_args: object) -> PnlTimelineExecutionPrefix:
        """Supply one already sealed, empty execution cut."""
        self.prefix_calls += 1
        return {"watermarks": {}, "executions": [], "annulments": []}

    async def get_pnl_timeline_execution_prefix_bundle(
        self,
        _wallet: str,
        _mode: str,
        request_as_of: datetime | None,
        activation_as_of: datetime | None,
    ) -> PnlTimelineExecutionPrefixBundle:
        """Retain both explicit historical cuts for the missing-anchor marker path."""
        assert request_as_of is not None
        assert activation_as_of is not None
        self.bundle_calls += 1
        prefix: PnlTimelineExecutionPrefix = {"watermarks": {}, "executions": [], "annulments": []}
        return {
            "request": prefix,
            "activation": prefix,
            "request_as_of": request_as_of,
            "activation_as_of": activation_as_of,
        }

    async def pnl_timeline_scope_has_fill_gap(self, *_args: object) -> bool:
        """Represent the empty scope's matching recorded and consumed multisets."""
        return False

    async def get_pnl_timeline_execution_lineage(
        self, *_args: object
    ) -> list[PnlTimelineExecutionLineageRow]:
        """Supply no initiating orders for the empty scope."""
        return []

    async def get_accruals_for_pnl(self, *_args: object) -> list[PnlTimelineAccrualRow]:
        """Supply no funding flows rather than replacing any financial calculation."""
        return []

    async def get_pnl_timeline_signals(self, *_args: object) -> list[PnlTimelineSignalMarkerRow]:
        """Keep the real timeline's independent signal read active."""
        return []

    async def get_pnl_timeline_ai_decisions(
        self, *_args: object
    ) -> list[PnlTimelineAiDecisionMarkerRow]:
        """Keep the real timeline's independent decision read active."""
        return []


@pytest.mark.parametrize("kind", ["series", "timeline"])
@pytest.mark.parametrize("state", ["normal", "preactivation", "missing"])
async def test_actual_handler_and_service_stage_wiring(
    monkeypatch: pytest.MonkeyPatch,
    kind: Literal["series", "timeline"],
    state: Literal["normal", "preactivation", "missing"],
) -> None:
    """Observe real service/core paths, skipped work and shared prefix reuse.

    Given: A real handler and typed normal, preactivation or missing-anchor reads.
    When: The unchanged service and pure core reconstruct the synthetic scope.
    Then: One safe summary reports entered stages and retains exact read counts.
    """
    messages: list[str] = []

    def capture(message: str, *args: object) -> None:
        """Capture only rendered timing text, with no dependency on logger extras."""
        messages.append(message.format(*args))

    monkeypatch.setattr(pnl_read_timing.logger, "info", capture)
    point_times = {
        "normal": _T0 - timedelta(minutes=1),
        "preactivation": _T0 + timedelta(minutes=3),
        "missing": None,
    }
    repo = _ReadRepo(point_times[state])
    app = FastAPI()
    app.state.rest_tracker = SequenceTracker()
    request = Request(
        {"type": "http", "method": "GET", "path": "/not-logged", "headers": [], "app": app}
    )
    principal = AuthPrincipal(username="private-username-sentinel", role=UserRole.AUDITOR)
    handler = (
        portfolio_timeline_routes.get_pnl_series
        if kind == "series"
        else portfolio_timeline_routes.get_pnl_timeline
    )
    result = await handler(
        request,
        principal,
        None,
        cast(Repository, repo),
        wallet_public_id=_WALLET,
        from_time=_T0.isoformat(),
        to_time=(_T0 + timedelta(minutes=2)).isoformat(),
        as_of=_H.isoformat(),
    )
    assert len(result.payload.points) == (0 if state == "missing" else 3)
    summaries = [message for message in messages if message.startswith("PnlReadTiming ")]
    assert len(summaries) == 1
    fields = dict(item.split("=", 1) for item in summaries[0].split()[1:])
    assert fields["kind"] == kind
    assert fields["outcome"] == "success"
    assert float(fields["handler_ms"]) >= 0
    assert float(fields["series_ms"]) >= 0
    if kind == "timeline":
        assert float(fields["prefix_ms"]) >= 0
        assert float(fields["timeline_extra_ms"]) >= 0
    else:
        assert fields["prefix_ms"] == "unobserved"
        assert fields["timeline_extra_ms"] == "unobserved"
    if state == "missing":
        assert fields["core_ms"] == "unobserved"
    else:
        assert float(fields["core_ms"]) >= 0
    if state == "normal":
        assert float(fields["fx_ms"]) >= 0
    else:
        assert fields["fx_ms"] == "unobserved"
    assert repo.anchor_calls == 1
    for name in (
        "fx_discovery_mark_ms",
        "fx_discovery_event_ms",
        "fx_load_mark_ms",
        "fx_load_event_ms",
        "fx_election_ms",
        "fx_fold_final_ms",
        "fx_mark_candidate_planes",
        "fx_event_candidate_planes",
        "fx_mark_rows",
        "fx_event_rows",
        "fx_selected_rows",
        "fx_event_read_mode",
        "fx_event_intervals_observed",
    ):
        assert fields[name] == "unobserved"
    assert repo.prefix_calls == int(
        state != "missing" and (kind == "timeline" or state == "normal")
    )
    assert repo.bundle_calls == int(kind == "timeline" and state == "missing")
    for private_value in (_WALLET, _SESSION, "private-username-sentinel", "private-label-sentinel"):
        assert private_value not in summaries[0]
