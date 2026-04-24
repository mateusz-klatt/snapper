"""``order_rejected`` rule — fires on every ``orders.events.*.rejected`` event (§D6.1 Rule 2)."""

from datetime import datetime

from loguru import logger

from snapper.application.notify.rules.base import AlertRule
from snapper.application.notify.rules.dedup import check_dedup_window
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message


class OrderRejectedRule(AlertRule):
    """Safety-critical rejection alert — user wanted a trade, it didn't happen."""

    alert_type = "order_rejected"
    subscribe_topic_prefixes = ("orders.events.",)
    priority = "high"
    is_safety_critical = True
    thread_key_prefix = "snapper.order"
    suppression_window_seconds = 0

    async def evaluate(
        self,
        topic: str,
        payload: bytes,
        repo: Repository,
        now: datetime,
    ) -> list[AlertEventInsertRow]:
        """Return one row per ``rejected`` event (no dedup, no enrichment).

        Args:
            topic: ZMQ topic string the event arrived on.
            payload: Raw JSON payload bytes.
            repo: Repository handle for dedup-window lookups.
            now: Entry-boundary timestamp threaded from the sidecar.

        Returns:
            Exactly one ``AlertEventInsertRow`` when the rejection
            carries a user scope; empty list for malformed payloads,
            missing user, non-``rejected`` topics, or dedup hits.
        """
        if not topic.endswith(".rejected"):
            return []
        try:
            data = parse_message(payload.decode("utf-8"))
        except (UnicodeDecodeError, MessageParseError):
            return []
        if not isinstance(data, OrderData):
            return []
        user_public_id = data.user_public_id
        if not user_public_id:
            logger.info(
                "order_rejected: dropping alert — OrderData has no user_public_id"
                " (client_order_id={coid})",
                coid=data.client_order_id,
            )
            return []
        dedup_key = f"order_rejected.{data.client_order_id}"
        if await check_dedup_window(
            repo=repo,
            user_public_id=user_public_id,
            dedup_key=dedup_key,
            window_seconds=self.suppression_window_seconds,
            now=now,
        ):
            return []
        reason_text = data.reason or data.error or "unknown reason"
        body = f"{data.side.upper()} {data.size} {data.instrument} rejected: {reason_text}"
        row = AlertEventInsertRow(
            user_public_id=user_public_id,
            operator_public_id=data.operator_public_id,
            wallet_public_id=data.wallet_public_id or None,
            alert_type=self.alert_type,
            priority=self.priority,
            is_safety_critical=self.is_safety_critical,
            title="Order rejected",
            body=body,
            payload={
                "deep_link_path": f"/orders/{data.client_order_id}",
                "client_order_id": data.client_order_id,
                "reason": data.reason,
                "error": data.error,
                "body_suppressed": False,
            },
            dedup_key=dedup_key,
            thread_key=f"{self.thread_key_prefix}.{data.client_order_id}",
            source_topic=topic,
        )
        return [row]
