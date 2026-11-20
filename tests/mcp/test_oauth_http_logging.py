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


async def _control_rows(control_url: str) -> list[Control]:
    """Return every persisted control row."""
    async with get_repository(control_url).session() as session:
        return list((await session.execute(select(Control))).scalars().all())


async def test_control_records_never_persist_oauth_credentials(
    recorded: tuple[HttpCase, TestClient, str],
) -> None:
    """Consent capabilities, codes, client secrets and tokens stay out of the control table.

    Given the real provenance recorder around the OAuth-enabled app.
    When a full authorize, consent approval, code exchange and revocation run.
    Then no credential value appears in any control row, credential payloads are
        suppressed as ``None``, and the operations remain recorded.
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
    rows = await _control_rows(control_url)
    persisted = "\n".join(f"{row.message_type} {row.payload} {row.detail}" for row in rows)
    assert "POST /api/mcp-oauth/consent/approve" in persisted
    assert "POST /api/mcp/token" in persisted
    assert "POST /api/mcp/revoke" in persisted
    assert all(row.payload is None for row in rows)
    for secret in [
        request_id,
        callback["code"][0],
        _CLIENT_SECRET,
        _VERIFIER,
        basic,
        "revoke-SENTINEL-9a2c",
        _SENTINEL_STATE,
        issued["access_token"],
        issued["refresh_token"],
    ]:
        assert secret not in persisted


class _CapturingHandler(logging.Handler):
    """Collect formatted records from exactly one logger."""

    def __init__(self) -> None:
        """Start with no captured lines."""
        super().__init__(logging.INFO)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        """Keep the fully formatted message."""
        self.lines.append(record.getMessage())


@pytest.mark.parametrize("prior_state", ["default", "disabled", "non_propagating"])
def test_access_log_redacts_the_authorize_query(prior_state: str) -> None:
    """Uvicorn's own request-line format keeps method, path and status but no query.

    The test owns the access logger's state for its duration, because the app's
    logging setup, or a dictConfig with disable_existing_loggers, can leave
    ``uvicorn.access`` disabled or non-propagating in the same test process.
    Every attribute it changes is restored afterwards.

    Given the redaction filter and a handler attached directly to uvicorn's access
    logger, after the logger was left default, disabled or non-propagating.
    When an authorize request line with a sentinel state is logged.
    Then the line keeps method, path and status, and neither state nor challenge appears.
    """
    access = logging.getLogger(ACCESS_LOGGER)
    saved = (access.disabled, access.propagate, access.level)
    access.disabled = prior_state == "disabled"
    access.propagate = prior_state != "non_propagating"
    redaction = OAuthQueryRedactionFilter()
    handler = _CapturingHandler()
    scope = {
        "path": "/api/mcp/authorize",
        "query_string": f"client_id=x&state={_SENTINEL_STATE}&code_challenge=c".encode(),
    }
    try:
        access.disabled = False
        access.setLevel(logging.INFO)
        access.addFilter(redaction)
        access.addHandler(handler)
        access.info(
            '%s - "%s %s HTTP/%s" %d',
            "127.0.0.1:5000",
            "GET",
            get_path_with_query_string(scope),
            "1.1",
            302,
        )
    finally:
        access.removeHandler(handler)
        access.removeFilter(redaction)
        access.disabled, access.propagate = saved[0], saved[1]
        access.setLevel(saved[2])
    assert handler.lines == ['127.0.0.1:5000 - "GET /api/mcp/authorize?[REDACTED] HTTP/1.1" 302']
    assert _SENTINEL_STATE not in handler.lines[0]
    assert (access.disabled, access.propagate, access.level) == saved


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("/api/mcp/authorize", "/api/mcp/authorize"),
        ("/api/mcp/token", "/api/mcp/token"),
        ("/api/mcp-oauth/consent", "/api/mcp-oauth/consent"),
        ("/api/mcp/authorize?state=s", "/api/mcp/authorize?[REDACTED]"),
        ("/api/mcp/authorize/?state=s", "/api/mcp/authorize/?[REDACTED]"),
        ("/api/mcp/token?code=SENTINEL", "/api/mcp/token?[REDACTED]"),
        ("/api/mcp-oauth/consent?oauth_request=SENTINEL", "/api/mcp-oauth/consent?[REDACTED]"),
        ("/api/mcp?foo=bar", "/api/mcp?[REDACTED]"),
        ("/api/mcp/?foo=bar", "/api/mcp/?[REDACTED]"),
        (
            "/.well-known/oauth-authorization-server/api/mcp?foo=bar",
            "/.well-known/oauth-authorization-server/api/mcp?[REDACTED]",
        ),
        (
            "/.well-known/oauth-protected-resource/api/mcp?foo=bar",
            "/.well-known/oauth-protected-resource/api/mcp?[REDACTED]",
        ),
        (
            "/.well-known/openid-configuration?foo=bar",
            "/.well-known/openid-configuration?[REDACTED]",
        ),
        ("/api/orders?limit=5", "/api/orders?limit=5"),
        ("/api/mcp/authorizex?state=s", "/api/mcp/authorizex?[REDACTED]"),
        ("/api/mcpx?state=s", "/api/mcpx?state=s"),
        ("/api/mcp-oauthx?x=1", "/api/mcp-oauthx?x=1"),
    ],
)
def test_only_oauth_surface_queries_are_redacted(target: str, expected: str) -> None:
    """Every OAuth surface path has its query replaced while sibling/non-OAuth queries stay intact.

    Given a request target.
    When it is prepared for the access log.
    Then OAuth surface targets replace their query with ``[REDACTED]``, while
        queryless paths, non-OAuth paths, and boundary-bypass siblings stay unchanged.

    Args:
        target: Raw request target passed to the redactor.
        expected: Expected target string written to the log.
    """
    assert redact_request_target(target) == expected


@pytest.mark.parametrize(
    ("method", "target", "status_code", "expected_target", "sentinel"),
    [
        (
            "GET",
            "/api/mcp-oauth/consent?oauth_request=SENTINEL",
            200,
            "/api/mcp-oauth/consent?[REDACTED]",
            "SENTINEL",
        ),
        (
            "POST",
            "/api/mcp/token?code=SENTINEL",
            400,
            "/api/mcp/token?[REDACTED]",
            "SENTINEL",
        ),
        (
            "GET",
            "/api/mcp-oauthx?x=1",
            404,
            "/api/mcp-oauthx?x=1",
            None,
        ),
        (
            "GET",
            "/api/orders?limit=5",
            200,
            "/api/orders?limit=5",
            None,
        ),
    ],
    ids=["consent-query", "token-query", "oauthx-boundary", "non-oauth"],
)
def test_access_filter_with_real_uvicorn_log_records(
    method: str,
    target: str,
    status_code: int,
    expected_target: str,
    sentinel: str | None,
) -> None:
    """Filter rewrites real ``logging.LogRecord`` targets without altering method, protocol, or status.

    Given a real uvicorn-shaped ``logging.LogRecord`` and ``OAuthQueryRedactionFilter``.
    When the filter processes the record.
    Then OAuth surface queries become ``[REDACTED]``, non-OAuth and boundary paths
        stay untouched, and method, protocol, and status remain intact.

    Args:
        method: HTTP method in the access log tuple.
        target: Request target path with query string.
        status_code: HTTP status code in the access log tuple.
        expected_target: Expected formatted target after filtering.
        sentinel: Optional secret value that must not appear in the formatted message.
    """
    redaction = OAuthQueryRedactionFilter()
    record = logging.LogRecord(
        ACCESS_LOGGER,
        logging.INFO,
        __file__,
        1,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1:5000", method, target, "1.1", status_code),
        None,
    )
    assert redaction.filter(record) is True
    assert record.args == ("127.0.0.1:5000", method, expected_target, "1.1", status_code)
    formatted = record.getMessage()
    assert formatted == f'127.0.0.1:5000 - "{method} {expected_target} HTTP/1.1" {status_code}'
    if sentinel is not None:
        assert sentinel not in formatted


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
