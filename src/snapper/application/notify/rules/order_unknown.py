"""``order_unknown`` rule — fires on every ``orders.events.*.unknown`` event."""

from datetime import datetime

from loguru import logger

from snapper.application.notify.rules.base import AlertRule
from snapper.application.notify.rules.dedup import check_dedup_window
from snapper.core.json_types import JsonValue
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message


class OrderUnknownRule(AlertRule):
    """Safety-critical ambiguous-submit alert — order state is UNRESOLVED.

    An ``unknown`` event means the venue call failed in a way where the
    order MAY exist on the exchange: the executor parked the command
    and venue verification is pending. The operator must NOT
    assume a flat position — the alert exists precisely because neither
    accepted nor rejected can be claimed yet, and automated layers are
    deliberately holding the in-flight guard instead of re-emitting.

    Strategy-originated orders carry no ``user_public_id``; dropping the
    alert for them would silence exactly the orders that have NO human
    watching, so those fan out to every admin holding
    ``read:system_status`` (same recipient set as
    ``CriticalSystemErrorRule``).
    """

    alert_type = "order_unknown"
    subscribe_topic_prefixes = ("orders.events.",)
    priority = "high"
    is_safety_critical = True
    thread_key_prefix = "snapper.order"
    suppression_window_seconds = 300

    _PERMISSION_FOR_FAN_OUT = "read:system_status"

    async def evaluate(
        self,
        topic: str,
        payload: bytes,
        repo: Repository,
        now: datetime,
    ) -> list[AlertEventInsertRow]:
        """Return one row per ``unknown`` event with a dedup window.

        The 300s suppression window exists because the recon loop may
        re-touch an unresolved entry every cycle; one page per order
        per window is enough — the alert clears semantically when the
        terminal accepted/rejected resolution event arrives.

        Args:
            topic: ZMQ topic string the event arrived on.
            payload: Raw JSON payload bytes.
            repo: Repository handle for dedup-window lookups.
            now: Entry-boundary timestamp threaded from the sidecar.

        Returns:
            One ``AlertEventInsertRow`` per recipient (the order's user,
            or the admin fan-out set for strategy orders); empty list
            for malformed payloads, non-``unknown`` topics, dedup hits,
            or an empty recipient set.
        """
        if not topic.endswith(".unknown"):
            return []
        try:
            data = parse_message(payload.decode("utf-8"))
        except (UnicodeDecodeError, MessageParseError):
            return []
        if not isinstance(data, OrderData):
            return []
        if data.user_public_id:
            recipients = [data.user_public_id]
        else:
            recipients = await repo.list_users_with_permission(self._PERMISSION_FOR_FAN_OUT)
            if not recipients:
                logger.warning(
                    "order_unknown: strategy order with no user scope and no admins "
                    "hold read:system_status — alert dropped (client_order_id={coid})",
                    coid=data.client_order_id,
                )
                return []
        dedup_key = f"order_unknown.{data.client_order_id}"
        reason_text = data.reason or data.error or "ambiguous venue response"
        body_args: list[JsonValue] = [
            data.side.upper(),
            str(data.size),
            data.instrument,
            reason_text,
        ]
        body = (
            f"{body_args[0]} {body_args[1]} {body_args[2]} state UNKNOWN: {body_args[3]}"
            f" — venue verification pending, do not assume flat"
        )
        rows: list[AlertEventInsertRow] = []
        for recipient in recipients:
            if await check_dedup_window(
                repo=repo,
                user_public_id=recipient,
                dedup_key=dedup_key,
                window_seconds=self.suppression_window_seconds,
                now=now,
            ):
                continue
            rows.append(
                AlertEventInsertRow(
                    user_public_id=recipient,
                    operator_public_id=data.operator_public_id,
                    wallet_public_id=data.wallet_public_id or None,
                    alert_type=self.alert_type,
                    priority=self.priority,
                    is_safety_critical=self.is_safety_critical,
                    title="Order state unknown",
                    body=body,
                    payload={
                        "deep_link_path": f"/orders/{data.client_order_id}",
                        "client_order_id": data.client_order_id,
                        "reason": data.reason,
                        "error": data.error,
                        "body_suppressed": False,
                        "title_loc_key": "alerts.title.order_unknown",
                        "body_loc_key": "alerts.body.order_unknown",
                        "body_loc_args": body_args,
                    },
                    dedup_key=dedup_key,
                    thread_key=f"{self.thread_key_prefix}.{data.client_order_id}",
                    source_topic=topic,
                )
            )
        return rows
