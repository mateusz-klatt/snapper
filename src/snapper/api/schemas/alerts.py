"""Pydantic schemas for iOS Push Foundation alert read endpoints.

Covers the two alert endpoints:
``GET /api/alerts/history?limit=&before=`` and ``GET /api/alerts/{id}``.
Both are read-only projections of the currently-active
``alert_events`` SCD2 row for the authenticated caller, filtered
server-side on ``user_public_id == principal.user_public_id``.

The ``AlertCursor`` wire form is an opaque base64-urlsafe JSON blob
``{"t": iso8601_timestamp, "p": public_id}``. It maps 1:1 onto the
internal ``AlertListCursor`` TypedDict consumed by
``Repository.list_recent_alerts_for_user`` — the route layer decodes
the opaque string into the internal pair and re-encodes the last row
of each page into a ``next_cursor`` for the client. Because the
cursor carries a snapshotted ``(timestamp, public_id)`` pair,
pagination is stable even if the anchor alert_event is later
SCD2-revised.
"""

from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictDataSchema
from snapper.core.json_types import JsonObject


class AlertEventInfo(StrictDataSchema[Literal["alert_event_info"]]):
    """Read projection of a single active ``alert_events`` SCD2 row.

    Attributes:
        type: Payload item type discriminator.
        user_public_id: Owner UUID7 — always equals the caller.
        operator_public_id: Optional scope (operator the alert is
            about, if any).
        wallet_public_id: Optional scope (wallet the alert is about,
            if any).
        alert_type: One of the enumerated alert types.
        priority: ``low``, ``medium`` or ``high``.
        is_safety_critical: True for alerts that bypass user prefs
            (e.g. ``critical_system_error``).
        title: Short title string (APNs ``aps.alert.title``).
        body: Localised body string (APNs ``aps.alert.body``).
        payload: Optional structured context rendered by the iOS
            client when expanding the notification.
        dedup_key: Optional idempotency key — server collapses
            duplicate emits that share the same key within a window.
        thread_key: Optional APNs ``aps.thread-id`` for iOS
            notification grouping.
        source_topic: The ZMQ topic the alert originated from, when
            available. Informational only.
    """

    type: Literal["alert_event_info"] = "alert_event_info"
    user_public_id: str
    operator_public_id: str | None = None
    wallet_public_id: str | None = None
    alert_type: str
    priority: str
    is_safety_critical: bool
    title: str
    body: str
    payload: JsonObject | None = None
    dedup_key: str | None = None
    thread_key: str | None = None
    source_topic: str | None = None


class AlertEventResponse(PayloadResponse[Literal["alert_event_response"], AlertEventInfo]):
    """Singleton wrapper returned by ``GET /api/alerts/{public_id}``."""

    type: Literal["alert_event_response"] = "alert_event_response"


class AlertHistoryResponse(PayloadListResponse[Literal["alert_history_response"], AlertEventInfo]):
    """List wrapper for ``GET /api/alerts/history``.

    The ``payload`` field carries the page items directly (flat list
    mirrors other list responses); ``next_cursor`` is lifted to the
    response envelope so clients can page without unwrapping a nested
    object. ``None`` cursor = last page.
    """

    type: Literal["alert_history_response"] = "alert_history_response"
    next_cursor: str | None = Field(
        default=None,
        description="Opaque cursor for the next page (None = last page).",
    )
