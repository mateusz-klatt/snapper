"""OAuth log hygiene with sentinel credentials through the real recording paths.

The control table is written by the real ``ClientProvenanceMiddleware`` around
the real OAuth-enabled ``create_app()``. The access log is exercised with the
same record shape and target helper uvicorn itself uses.
"""

import base64
import logging
from collections.abc import AsyncGenerator
from pathlib import Path
from urllib.parse import parse_qs
from urllib.parse import urlsplit
from uuid import uuid7

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from uvicorn.protocols.utils import get_path_with_query_string

from snapper.data.models import Control
from snapper.data.repository import get_repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.access_log import ACCESS_LOGGER
from snapper.server.access_log import OAuthQueryRedactionFilter
from snapper.server.access_log import redact_request_target
from snapper.server.provenance_middleware import ClientProvenanceMiddleware
from tests.mcp.test_oauth_http import HttpCase
from tests.mcp.test_oauth_http import _authorize_params
from tests.mcp.test_oauth_http import _build_case
from tests.mcp.test_oauth_http import _envelope
from tests.mcp.test_oauth_service import _CLIENT_ID
from tests.mcp.test_oauth_service import _CLIENT_SECRET
from tests.mcp.test_oauth_service import _OPERATOR_A
from tests.mcp.test_oauth_service import _VERIFIER
from tests.mcp.test_oauth_service import _build_harness
from tests.mcp.test_oauth_service import _principal
from tests.mcp.test_oauth_service import _seed_user_with_membership

_SENTINEL_STATE = "state-SENTINEL-7d1f"


@pytest.fixture
async def recorded(tmp_path: Path) -> AsyncGenerator[tuple[HttpCase, TestClient, str]]:
    """Wrap the enabled app in a real provenance recorder with its own control table."""
    harness = await _build_harness(tmp_path)
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(harness.repository, user_public_id=owner_id)
    case = _build_case(harness, _principal(owner_id, membership_public_id=membership), enabled=True)
    control_url = f"sqlite+aiosqlite:///{tmp_path / 'control.db'}"
    control = get_repository(control_url)
    await control.create_all()
    recorder = ClientProvenanceMiddleware(case.app, db_url=control_url, tracker=SequenceTracker())
    try:
        yield case, TestClient(recorder, follow_redirects=False), control_url
    finally:
        await control.engine.dispose()
        await harness.repository.engine.dispose()


async def _control_text(control_url: str) -> str:
    """Concatenate every persisted control payload and detail."""
    async with get_repository(control_url).session() as session:
        rows = (await session.execute(select(Control))).scalars().all()
    return "\n".join(f"{row.message_type} {row.payload} {row.detail}" for row in rows)


async def test_control_records_never_persist_oauth_credentials(
    recorded: tuple[HttpCase, TestClient, str],
) -> None:
    """Consent capabilities, codes, client secrets and tokens stay out of the control table.

    Given the real provenance recorder around the OAuth-enabled app.
    When a full authorize, consent approval, code exchange and revocation run.
    Then no credential value appears in any control row, while the operations are recorded.
    """
    _case, client, control_url = recorded
    response = client.get("/api/mcp/authorize", params=_authorize_params(state=_SENTINEL_STATE))
    request_id = parse_qs(urlsplit(response.headers["location"]).fragment)["request"][0]
    approved = client.post(
        "/api/mcp-oauth/consent/approve",
        json=_envelope(
            "mcp_oauth_consent_approve_request",
            {
                "oauth_request": request_id,
                "operator_public_id": _OPERATOR_A,
                "replace_existing_grant": False,
            },
        ),
    )
    callback = parse_qs(urlsplit(approved.json()["payload"]["redirect_to"]).query)
    basic = base64.b64encode(f"{_CLIENT_ID}:{_CLIENT_SECRET}".encode()).decode()
    headers = {
        "content-type": "application/x-www-form-urlencoded",
        "authorization": f"Basic {basic}",
    }
    tokens = client.post(
        "/api/mcp/token",
        headers=headers,
        data={
            "grant_type": "authorization_code",
            "code": callback["code"][0],
            "code_verifier": _VERIFIER,
            "redirect_uri": _authorize_params()["redirect_uri"],
            "resource": _authorize_params()["resource"],
        },
    )
    assert tokens.status_code == 200
    issued = tokens.json()
    client.post("/api/mcp/revoke", headers=headers, data={"token": "revoke-SENTINEL-9a2c"})
    persisted = await _control_text(control_url)
    assert "POST /api/mcp-oauth/consent/approve" in persisted
    assert "POST /api/mcp/token" in persisted
    assert "[REDACTED]" in persisted
    for secret in [
        request_id,
        callback["code"][0],
        _CLIENT_SECRET,
        basic,
        "revoke-SENTINEL-9a2c",
        _SENTINEL_STATE,
        issued["access_token"],
        issued["refresh_token"],
    ]:
        assert secret not in persisted


def test_access_log_redacts_the_authorize_query(caplog: pytest.LogCaptureFixture) -> None:
    """Uvicorn's own request-line format keeps method, path and status but no query.

    Given the redaction filter on uvicorn's access logger and the target uvicorn derives.
    When an authorize request line with a sentinel state is logged.
    Then the line keeps method, path and status, and neither state nor challenge appears.
    """
    access = logging.getLogger(ACCESS_LOGGER)
    redaction = OAuthQueryRedactionFilter()
    access.addFilter(redaction)
    scope = {
        "path": "/api/mcp/authorize",
        "query_string": f"client_id=x&state={_SENTINEL_STATE}&code_challenge=c".encode(),
    }
    try:
        with caplog.at_level(logging.INFO, logger=ACCESS_LOGGER):
            access.info(
                '%s - "%s %s HTTP/%s" %d',
                "127.0.0.1:5000",
                "GET",
                get_path_with_query_string(scope),
                "1.1",
                302,
            )
    finally:
        access.removeFilter(redaction)
    line = caplog.records[-1].getMessage()
    assert line == '127.0.0.1:5000 - "GET /api/mcp/authorize?[REDACTED] HTTP/1.1" 302'
    assert _SENTINEL_STATE not in line


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("/api/mcp/authorize", "/api/mcp/authorize"),
        ("/api/mcp/authorize?state=s", "/api/mcp/authorize?[REDACTED]"),
        ("/api/mcp/authorize/?state=s", "/api/mcp/authorize/?[REDACTED]"),
        ("/api/orders?limit=5", "/api/orders?limit=5"),
        ("/api/mcp/authorizex?state=s", "/api/mcp/authorizex?state=s"),
    ],
)
def test_only_the_credential_bearing_query_is_redacted(target: str, expected: str) -> None:
    """Other request lines keep their query for operations.

    Given a request target.
    When it is prepared for the access log.
    Then only the authorize query is replaced.
    """
    assert redact_request_target(target) == expected


def test_access_filter_passes_unexpected_record_shapes_unchanged() -> None:
    """Records that are not uvicorn request lines are never altered or dropped.

    Given a plain message record and a record with a non-string target.
    When the filter runs.
    Then each record is kept with its arguments intact.
    """
    redaction = OAuthQueryRedactionFilter()
    plain = logging.LogRecord(ACCESS_LOGGER, logging.INFO, __file__, 1, "ready", None, None)
    shaped = logging.LogRecord(
        ACCESS_LOGGER, logging.INFO, __file__, 1, "%s %s %s", ("a", "b", 3), None
    )
    assert redaction.filter(plain) and plain.args is None
    assert redaction.filter(shaped) and shaped.args == ("a", "b", 3)
