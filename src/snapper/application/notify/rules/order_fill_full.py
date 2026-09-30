"""``order_fill_full`` rule — fires when an execution completes in full."""

from datetime import datetime
from decimal import Decimal

from loguru import logger

from snapper.application.notify.rules.base import AlertRule
from snapper.application.notify.rules.dedup import check_dedup_window
from snapper.core.json_types import JsonValue
from snapper.core.types import FillStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message


def _execution_quote(data: ExecutionData, refs: list[InstrumentSymbolRefRow]) -> str | None:
    """Require an unambiguous historical currency pair covering the execution.

    Args:
        data: Execution identifying the venue, symbol and event time.
        refs: Instrument reference history known when the alert is evaluated.

    Returns:
        The verified quote unit, or ``None`` when history has currency-leg
        revisions or no reference covers this execution. Knowledge-time
        revisions cannot prove whether a currency change was a correction.
    """
    pairs = {(ref["base_currency"], ref["quote_currency"]) for ref in refs}
    if len(pairs) != 1:
        return None
    for ref in refs:
        if (
            ref["native_symbol"] == data.instrument
            and ref["instrument_exchange"] == data.exchange
            and ref["valid_from"] <= data.executed_at < ref["valid_to"]
        ):
            return ref["quote_currency"]
    return None


async def _quoted_price(data: ExecutionData, repo: Repository, now: datetime) -> str:
    """Preserve execution precision and append only an authoritative quote unit.

    Args:
        data: Execution carrying the venue's native symbol and price.
        repo: Repository used to resolve the instrument's quote metadata.
        now: Knowledge horizon for checking all known currency-leg revisions.

    Returns:
        The unrounded decimal price with its known quote currency, or the
        plain price when the instrument or quote metadata is unavailable.
    """
    price = format(Decimal(str(data.price)), "f")
    if data.executed_at.utcoffset() is None:
        return price
    instrument_id = await repo.get_instrument_public_id_by_symbol(
        data.instrument, data.exchange, data.executed_at
    )
    if instrument_id is None:
        return price
    refs = await repo.get_instrument_symbol_refs([instrument_id], now)
    quote = _execution_quote(data, refs)
    return f"{price} {quote}" if quote else price


class OrderFillFullRule(AlertRule):
    """Emit one alert per order once it reaches ``FillStatusEnum.FILLED``.

    Partial fills are ignored — the alert is deliberately once-per-order
    so iOS users get a single "filled" notification no matter how many
    partial fills a venue delivered. ``dedup_key`` is
    ``order_fill_full.{client_order_id}`` which makes the suppression
    invariant explicit.
    """

    alert_type = "order_fill_full"
    subscribe_topic_prefixes = ("orders.events.",)
    priority = "medium"
    is_safety_critical = False
    thread_key_prefix = "snapper.order"
    suppression_window_seconds = 0

    async def evaluate(
        self,
        topic: str,
        payload: bytes,
        repo: Repository,
        now: datetime,
    ) -> list[AlertEventInsertRow]:
        """Return one row on final fill; empty on partial / non-execution topic.

        Args:
            topic: ZMQ topic string the event arrived on.
            payload: Raw JSON payload bytes.
            repo: Repository handle for deduplication and instrument quote metadata.
            now: Entry-boundary timestamp threaded from the sidecar.

        Returns:
            Exactly one ``AlertEventInsertRow`` on a final-fill
            ``.executed`` event; empty list for partials, non-executed
            topics, malformed payloads, missing user scope, or a hit
            in the dedup window.
        """
        if not topic.endswith(".executed"):
            return []
        try:
            data = parse_message(payload.decode("utf-8"))
        except (UnicodeDecodeError, MessageParseError):
            return []
        if not isinstance(data, ExecutionData):
            return []
        if data.status != FillStatusEnum.FILLED:
            return []
        user_public_id = data.user_public_id
        if not user_public_id:
            logger.info(
                "order_fill_full: dropping alert — ExecutionData has no user_public_id"
                " (client_order_id={coid})",
                coid=data.client_order_id,
            )
            return []
        dedup_key = f"order_fill_full.{data.client_order_id}"
        if await check_dedup_window(
            repo=repo,
            user_public_id=user_public_id,
            dedup_key=dedup_key,
            window_seconds=self.suppression_window_seconds,
            now=now,
        ):
            return []
        title = "Order filled"
        body_args: list[JsonValue] = [
            data.side.upper(),
            str(abs(data.size)),
            data.instrument,
            await _quoted_price(data, repo, now),
            data.exchange,
        ]
        body = f"{body_args[0]} {body_args[1]} {body_args[2]} @ {body_args[3]} filled on {body_args[4]}"
        row = AlertEventInsertRow(
            user_public_id=user_public_id,
            operator_public_id=data.operator_public_id,
            wallet_public_id=data.wallet_public_id or None,
            alert_type=self.alert_type,
            priority=self.priority,
            is_safety_critical=self.is_safety_critical,
            title=title,
            body=body,
            payload={
                "deep_link_path": f"/orders/{data.client_order_id}",
                "client_order_id": data.client_order_id,
                "exchange_order_id": data.exchange_order_id,
                "body_suppressed": False,
                "title_loc_key": "alerts.title.order_fill_full",
                "body_loc_key": "alerts.body.order_fill_full_quoted",
                "body_loc_args": body_args,
            },
            dedup_key=dedup_key,
            thread_key=f"{self.thread_key_prefix}.{data.client_order_id}",
            source_topic=topic,
        )
        return [row]
