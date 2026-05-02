"""``margin_warning`` rule — fires on margin-related order rejections."""

from datetime import datetime

from loguru import logger

from snapper.application.notify.rules.base import AlertRule
from snapper.application.notify.rules.dedup import check_dedup_window
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message

_MARGIN_KEYWORDS: tuple[str, ...] = (
    "margin",
    "leverage",
    "insufficient funds",
    "insufficient balance",
    "stop_out",
    "stop out",
    "liquidat",
    "collateral",
)
"""Substring tokens checked (case-insensitively) against
``OrderData.reason`` and ``OrderData.error`` to identify
margin-related rejections.

The match is intentionally broad — a venue's plain-English
``insufficient funds`` message and Kraken-Futures'
``Margin requirement not met`` should both flag. False positives
(an arbitrary order with the literal string ``margin`` in an
unrelated error) are tolerated: the resulting alert at most says
"your trade did not go through, possibly margin-related" which is
still actionable. False negatives are the dangerous direction —
silently dropping a margin signal is worse than over-alerting.
"""


def is_margin_related_rejection(reason: str | None, error: str | None) -> bool:
    """Return True iff a rejected order's reason / error mentions margin.

    Used by ``MarginWarningRule.evaluate`` to decide whether to fire
    the safety-critical margin alert AND by ``OrderRejectedRule`` to
    decide whether to suppress its own (lower-criticality) rejection
    alert — preventing the same rejection from emitting two iOS
    pushes (one ``order_rejected`` + one ``margin_warning``).

    Args:
        reason: ``OrderData.reason`` (free-form rejection string from
            the venue or local guards).
        error: ``OrderData.error`` (lower-level error string).

    Returns:
        ``True`` when either field contains any of the
        ``_MARGIN_KEYWORDS`` substrings (case-insensitive). ``False``
        when both are ``None`` / empty / non-margin.
    """
    haystacks: list[str] = []
    if reason:
        haystacks.append(reason.lower())
    if error:
        haystacks.append(error.lower())
    if not haystacks:
        return False
    for haystack in haystacks:
        for keyword in _MARGIN_KEYWORDS:
            if keyword in haystack:
                return True
    return False


class MarginWarningRule(AlertRule):
    """Safety-critical margin alert — venue rejected for margin reasons.

    No upstream balance / margin-level topic exists on the bus today.
    The next best signal we can deliver is the venue's own
    ``insufficient margin`` rejection: the moment the exchange tells
    Snapper "your account does not have enough margin to size this
    order", that is a *de facto* margin event the user should know
    about even when they have quiet hours configured.

    Subscribes on the same ``orders.events.`` prefix as
    ``OrderRejectedRule``. Both rules see every ``.rejected`` event;
    this rule fires only when ``reason`` / ``error`` contains a
    margin token, and ``OrderRejectedRule`` is now configured to
    *skip* the same subset so the user gets one alert, not two.

    Dedup mirrors ``order_rejected``: keyed on
    ``client_order_id`` so a re-publish does not double-fire.
    """

    alert_type = "margin_warning"
    subscribe_topic_prefixes = ("orders.events.",)
    priority = "high"
    is_safety_critical = True
    thread_key_prefix = "snapper.margin"
    suppression_window_seconds = 0

    async def evaluate(
        self,
        topic: str,
        payload: bytes,
        repo: Repository,
        now: datetime,
    ) -> list[AlertEventInsertRow]:
        """Return one row when a rejection's reason / error mentions margin.

        Args:
            topic: ZMQ topic string the event arrived on.
            payload: Raw JSON payload bytes.
            repo: Repository handle for dedup-window lookups.
            now: Entry-boundary timestamp threaded from the sidecar.

        Returns:
            Exactly one ``AlertEventInsertRow`` when the rejection is
            margin-related and carries a user scope. Empty list for
            non-rejection topics, non-margin rejections, malformed
            payloads, missing user, or dedup hits.
        """
        if not topic.endswith(".rejected"):
            return []
        try:
            data = parse_message(payload.decode("utf-8"))
        except (UnicodeDecodeError, MessageParseError):
            return []
        if not isinstance(data, OrderData):
            return []
        if not is_margin_related_rejection(data.reason, data.error):
            return []
        user_public_id = data.user_public_id
        if not user_public_id:
            logger.info(
                "margin_warning: dropping alert — OrderData has no user_public_id"
                " (client_order_id={coid})",
                coid=data.client_order_id,
            )
            return []
        dedup_key = f"margin_warning.{data.client_order_id}"
        if await check_dedup_window(
            repo=repo,
            user_public_id=user_public_id,
            dedup_key=dedup_key,
            window_seconds=self.suppression_window_seconds,
            now=now,
        ):
            return []
        reason_text = data.reason or data.error or "unknown reason"
        body = (
            f"{data.side.upper()} {data.size} {data.instrument}"
            f" blocked by margin: {reason_text}"
        )
        row = AlertEventInsertRow(
            user_public_id=user_public_id,
            operator_public_id=data.operator_public_id,
            wallet_public_id=data.wallet_public_id or None,
            alert_type=self.alert_type,
            priority=self.priority,
            is_safety_critical=self.is_safety_critical,
            title="Margin warning",
            body=body,
            payload={
                "deep_link_path": f"/orders/{data.client_order_id}",
                "client_order_id": data.client_order_id,
                "reason": data.reason,
                "error": data.error,
                "body_suppressed": False,
            },
            dedup_key=dedup_key,
            thread_key=(f"{self.thread_key_prefix}.{data.wallet_public_id or 'no-wallet'}"),
            source_topic=topic,
        )
        return [row]
