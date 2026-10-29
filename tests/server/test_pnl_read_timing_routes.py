"""Require one privacy-safe timing summary from each actual P&L handler."""

from typing import Literal
from typing import cast

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.server import portfolio_timeline_routes


@pytest.mark.parametrize("kind", ["series", "timeline"])
async def test_invalid_scope_handler_emits_one_failure_summary(
    monkeypatch: pytest.MonkeyPatch,
    kind: Literal["series", "timeline"],
) -> None:
    """Observe real validation refusal without changing status or logging inputs.

    Given: An actual P&L handler receiving a missing wallet scope.
    When: Its existing validation rejects the request before reconstruction.
    Then: The same 400 response has one failure summary with no private inputs.
    """
    messages: list[str] = []

    def capture(message: str, *args: object) -> None:
        """Capture the rendered message that survives the configured log sinks."""
        messages.append(message.format(*args))

    monkeypatch.setattr(portfolio_timeline_routes.logger, "info", capture)
    request = Request({"type": "http", "method": "GET", "path": "/not-logged", "headers": []})
    principal = AuthPrincipal(username="private-username-sentinel", role=UserRole.VIEWER)
    repo = cast(Repository, object())
    with pytest.raises(HTTPException) as rejected:
        if kind == "series":
            await portfolio_timeline_routes.get_pnl_series(request, principal, None, repo)
        else:
            await portfolio_timeline_routes.get_pnl_timeline(request, principal, None, repo)
    assert rejected.value.status_code == 400
    summaries = [message for message in messages if message.startswith("PnlReadTiming ")]
    assert len(summaries) == 1
    assert f"kind={kind} outcome=failure" in summaries[0]
    assert "series_ms=unobserved" in summaries[0]
    assert "private-username-sentinel" not in summaries[0]
    assert "/not-logged" not in summaries[0]
