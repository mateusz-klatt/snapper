"""REST routes for caller-scoped user-level alert default preferences.

Two endpoints, both gated by the authenticated principal and scoped
server-side to the caller's ``user_public_id``:

- ``GET /api/alert_defaults`` — list active per-(user, alert_type)
  fallback rows for the caller. Empty list = no overrides set;
  ``application/notify/routing`` falls through to the built-in
  defaults documented there.
- ``PATCH /api/alert_defaults`` — upsert one ``(user, alert_type)``
  fallback row via ``Repository.upsert_user_alert_default`` which
  owns the SCD2 close+insert + IntegrityError-retry pattern.

User-level fallbacks are consulted by the routing layer when no
device-scoped override matches the inbound alert — they are the
second-narrowest tier above the in-code built-in defaults.

Ownership scoping mirrors ``device_routes``: every read + write is
joined to ``principal.user_public_id`` so callers cannot manipulate
another user's defaults. There is no public_id path parameter on
these routes — the active row is uniquely identified by the
``(user, alert_type)`` tuple.
"""

import datetime as dt
from datetime import datetime
from typing import Annotated
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request

from snapper.api.schemas.devices import UpdateUserAlertDefaultCommand
from snapper.api.schemas.devices import UserAlertDefaultInfo
from snapper.api.schemas.devices import UserAlertDefaultListResponse
from snapper.api.schemas.devices import UserAlertDefaultResponse
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.data.repository_types import UserAlertDefaultRow
from snapper.data.repository_types import UserAlertDefaultUpsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

router = APIRouter(prefix="/alert_defaults", tags=["alert-defaults"])

_REST_STREAM = "rest.alert_defaults"


def _next_provenance(request: Request) -> tuple[str, int, datetime, str]:
    """Mint a fresh ``(session_id, sequence_id, timestamp, public_id)`` tuple.

    Mirrors the helper in ``device_routes`` — kept local to avoid
    cross-module reach into private helpers and to keep the rest
    tracker handle confined to this router's stream namespace.
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return sid, seq, ts, pid


def _user_alert_default_info_from_row(row: UserAlertDefaultRow) -> UserAlertDefaultInfo:
    """Project a ``UserAlertDefaultRow`` TypedDict into the wire schema.

    Drops SCD2-internal fields (``known_to``) so callers see only
    the active projection.
    """
    return UserAlertDefaultInfo(
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        user_public_id=row["user_public_id"],
        alert_type=row["alert_type"],
        enabled=row["enabled"],
        min_priority=row["min_priority"],
    )


@router.get("")
async def list_alert_defaults(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> UserAlertDefaultListResponse:
    """List the caller's active user-level fallback prefs.

    Args:
        request: FastAPI request (provides REST tracker).
        principal: Authenticated caller.
        repo: Repository dependency.

    Returns:
        ``UserAlertDefaultListResponse`` with zero-or-more active
        ``UserAlertDefaultInfo`` rows scoped to the caller. Empty
        list is the legitimate "no overrides" state — clients should
        fall through to the in-app default UI.
    """
    rows = await repo.list_user_alert_defaults(principal.user_public_id)
    items = [_user_alert_default_info_from_row(r) for r in rows]
    sid, seq, ts, pid = _next_provenance(request)
    return UserAlertDefaultListResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=items,
        count=len(items),
    )


@router.patch("", openapi_extra=openapi_schema(UpdateUserAlertDefaultCommand))
async def update_alert_default(
    request: Request,
    command: Annotated[
        UpdateUserAlertDefaultCommand, Depends(json_body(UpdateUserAlertDefaultCommand))
    ],
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> UserAlertDefaultResponse:
    """Upsert one ``(user, alert_type)`` fallback for the caller.

    The repo's ``upsert_user_alert_default`` owns the atomic SCD2
    close + insert and idempotent retry on same-key contention; it
    returns the stable ``public_id`` (preserved across SCD2
    versions) so the response is synthesized from the validated
    body without an extra read that could race against other writers
    on the same key (mirrors the pattern Copilot BE-1c locked in for
    ``upsert_device_alert_pref``).

    Args:
        request: FastAPI request (provides REST tracker).
        command: Typed request envelope with provenance + body.
        principal: Authenticated caller.
        repo: Repository dependency.

    Returns:
        ``UserAlertDefaultResponse`` with the now-active fallback
        row. ``user_public_id`` is sourced from the principal — the
        body cannot specify a foreign user.
    """
    body = command.payload
    sid, seq, ts, _ = _next_provenance(request)
    pref_public_id = await repo.upsert_user_alert_default(
        UserAlertDefaultUpsertRow(
            session_id=sid,
            sequence_id=seq,
            timestamp=ts,
            user_public_id=principal.user_public_id,
            alert_type=body.alert_type,
            enabled=body.enabled,
            min_priority=body.min_priority,
        )
    )
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(request)
    return UserAlertDefaultResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=UserAlertDefaultInfo(
            session_id=sid,
            sequence_id=seq,
            public_id=pref_public_id,
            timestamp=ts,
            user_public_id=principal.user_public_id,
            alert_type=body.alert_type,
            enabled=body.enabled,
            min_priority=body.min_priority,
        ),
    )
