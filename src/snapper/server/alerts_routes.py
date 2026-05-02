"""REST routes for iOS Push Foundation alert reads (BE-1c).

Two endpoints, both authenticated and server-side scoped on
``user_public_id == principal.user_public_id``:

- ``GET /api/alerts/history?limit=&before=`` — paged keyset history
  ordered ``(timestamp DESC, public_id DESC)``.
- ``GET /api/alerts/{public_id}`` — singleton by id.

The history cursor is an **opaque base64url(JSON)** token carrying the
snapshotted anchor ``(timestamp, public_id)`` pair — guarantees
cursor-stability under SCD2 revision of the anchor alert_event.
The route layer is the encode/decode boundary;
the repo takes the internal ``AlertListCursor`` TypedDict and applies
the keyset predicate directly from the pair without hitting the DB.
Malformed or tampered cursors are treated as "no cursor" — we return
page 1 instead of raising, to match the iOS client's expectation that
a cursor round-trip is lossless but never fatal.
"""

import base64
import datetime as dt
import json
from datetime import datetime
from typing import Annotated
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import status
from loguru import logger

from snapper.api.schemas.alerts import AlertEventInfo
from snapper.api.schemas.alerts import AlertEventResponse
from snapper.api.schemas.alerts import AlertHistoryResponse
from snapper.auth.dependencies import require_authentication
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.json_types import JsonObject
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventRow
from snapper.data.repository_types import AlertListCursor
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

router = APIRouter(prefix="/alerts", tags=["alerts"])

_REST_STREAM = "rest.alerts"
_DEFAULT_LIMIT = 50
_MAX_LIMIT = 200


def _encode_cursor(anchor: AlertEventRow) -> str:
    """Encode the last-row anchor of a page into an opaque cursor string.

    Args:
        anchor: Last ``AlertEventRow`` of the returned page.

    Returns:
        URL-safe base64(JSON(``{t: iso8601, p: public_id}``)) — opaque
        from the client's POV.
    """
    raw = json.dumps(
        {"t": anchor["timestamp"].isoformat(), "p": anchor["public_id"]},
        separators=(",", ":"),
    )
    return base64.urlsafe_b64encode(raw.encode("ascii")).decode("ascii").rstrip("=")


def _decode_cursor(token: str | None) -> AlertListCursor | None:
    """Decode an opaque cursor string back to the internal pair.

    Returns ``None`` for empty / malformed / tampered tokens so the
    route falls back to the first page rather than raising — the iOS
    client uses this endpoint in a retry loop and a 400 on a stale
    cursor would surface as a UI error for an otherwise-recoverable
    transient.
    """
    if token is None or token == "":
        return None
    padding = "=" * (-len(token) % 4)
    try:
        raw = base64.urlsafe_b64decode((token + padding).encode("ascii")).decode("ascii")
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            logger.debug(
                "alert cursor rejected — JSON root is not an object (got {kind})",
                kind=type(obj).__name__,
            )
            return None
        ts_raw = obj.get("t")
        pid_raw = obj.get("p")
        if not isinstance(ts_raw, str) or not isinstance(pid_raw, str):
            logger.debug("alert cursor rejected — missing or wrong-type t/p fields")
            return None
        ts = datetime.fromisoformat(ts_raw)
    except ValueError as exc:
        logger.debug(
            "alert cursor rejected — decode failed ({kind}: {err})",
            kind=type(exc).__name__,
            err=str(exc)[:80],
        )
        return None
    return AlertListCursor(timestamp=ts, public_id=pid_raw)


def _alert_info_from_row(row: AlertEventRow) -> AlertEventInfo:
    """Project an ``AlertEventRow`` TypedDict into the wire schema."""
    payload_val = row.get("payload")
    narrowed_payload: JsonObject | None = payload_val if payload_val is not None else None
    return AlertEventInfo(
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        user_public_id=row["user_public_id"],
        operator_public_id=row.get("operator_public_id"),
        wallet_public_id=row.get("wallet_public_id"),
        alert_type=row["alert_type"],
        priority=row["priority"],
        is_safety_critical=row["is_safety_critical"],
        title=row["title"],
        body=row["body"],
        payload=narrowed_payload,
        dedup_key=row.get("dedup_key"),
        thread_key=row.get("thread_key"),
        source_topic=row.get("source_topic"),
    )


@router.get("/history")
async def list_alert_history(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    limit: Annotated[int, Query(ge=1, le=_MAX_LIMIT)] = _DEFAULT_LIMIT,
    before: Annotated[str | None, Query(max_length=160)] = None,
) -> AlertHistoryResponse:
    """Return a keyset-paginated page of the caller's active alert_events.

    Order: ``(timestamp DESC, public_id DESC)``. The ``before`` cursor
    is opaque — decode it back into the internal ``AlertListCursor``,
    delegate to ``Repository.list_recent_alerts_for_user`` (which
    applies the keyset predicate from the pair directly, not from a
    re-derivation of the anchor's current timestamp), and mint the
    next-page cursor from the last row of the returned page.

    Args:
        request: FastAPI request (provides REST tracker).
        principal: Authenticated caller.
        repo: Repository dependency.
        limit: Page size, 1..200 (default 50).
        before: Opaque cursor token from a previous page's
            ``next_cursor``; ``None`` or malformed values map to
            page 1.

    Returns:
        ``AlertHistoryResponse`` with the active page + a
        ``next_cursor`` that is ``None`` when the page is the last.
    """
    cursor = _decode_cursor(before)
    rows = await repo.list_recent_alerts_for_user(
        user_public_id=principal.user_public_id,
        limit=limit,
        before=cursor,
    )
    items = [_alert_info_from_row(r) for r in rows]
    next_cursor = _encode_cursor(rows[-1]) if len(rows) == limit else None
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return AlertHistoryResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=items,
        count=len(items),
        next_cursor=next_cursor,
    )


@router.get("/{alert_public_id}")
async def get_alert_event(
    request: Request,
    alert_public_id: str,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> AlertEventResponse:
    """Return a single active ``alert_events`` row if owned by the caller.

    Ownership is enforced server-side via ``user_public_id`` equality.
    404 is returned for unknown ids AND for ids owned by other users,
    so the endpoint never leaks the existence of foreign alert events.

    Args:
        request: FastAPI request (provides REST tracker).
        alert_public_id: Target alert id from the path param.
        principal: Authenticated caller.
        repo: Repository dependency.

    Returns:
        ``AlertEventResponse`` with the active alert_event projection.

    Raises:
        HTTPException: 404 when the alert is unknown or owned by
            another user.
    """
    row = await repo.get_alert_event_by_public_id(alert_public_id)
    if row is None or row["user_public_id"] != principal.user_public_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Alert {alert_public_id} not found.",
        )
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return AlertEventResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=_alert_info_from_row(row),
    )
