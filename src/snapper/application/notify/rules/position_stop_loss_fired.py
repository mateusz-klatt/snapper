"""``position_stop_loss_fired`` rule — fires on bracket/trailing stop loss."""

from datetime import datetime

from loguru import logger

from snapper.application.notify.rules.base import AlertRule
from snapper.application.notify.rules.dedup import check_dedup_window
from snapper.core.json_types import JsonValue
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow
from snapper.messaging.schemas.data import ExecutionPlanDecisionEventData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message

_KNOWN_LOSS_REASONS: frozenset[str] = frozenset({"sl_hit", "trailing_stop_hit"})


class PositionStopLossFiredRule(AlertRule):
    """High-priority alert on bracket or trailing-stop loss firings.

    Subscribes on the ``plans.decisions.*`` topic family
    and filters the free-form ``reason`` field to the
    two known-loss values — ``"sl_hit"`` from
    ``src/snapper/application/plans/bracket.py`` + ``"trailing_stop_hit"``
    from ``src/snapper/application/plans/trailing_stop.py``. ``"tp_hit"``
    is explicitly excluded because take-profit is a non-loss outcome.
    Other free-form reasons (e.g. ``"evaluator emitted command"`` or
    ``"Cycle <x> closed before command dispatch"``) fall through to
    ``[]`` — they're logged decisions that don't warrant a push.

    Enriches via one ``get_execution_plan`` + one
    ``get_symbol_for_instrument`` read per fire so the notification
    body can name the exact instrument. Stop-loss firings are rare
    per wallet (≤10/day) so the extra reads are safe on the hot path.
    """

    alert_type = "position_stop_loss_fired"
    subscribe_topic_prefixes = ("plans.decisions.",)
    priority = "high"
    is_safety_critical = True
    thread_key_prefix = "snapper.position"
    suppression_window_seconds = 0

    async def evaluate(
        self,
        topic: str,
        payload: bytes,
        repo: Repository,
        now: datetime,
    ) -> list[AlertEventInsertRow]:
        """Return one row when ``reason`` is a known-loss value.

        Args:
            topic: ZMQ topic string — ``plans.decisions.{plan_public_id}``.
            payload: ``ExecutionPlanDecisionEventData`` JSON bytes.
            repo: Repository handle for ``get_execution_plan`` /
                ``get_symbol_for_instrument`` / dedup-window reads.
            now: Entry-boundary timestamp threaded from the sidecar.

        Returns:
            Exactly one ``AlertEventInsertRow`` when ``reason`` is
            ``sl_hit`` / ``trailing_stop_hit`` AND enrichment resolves
            a user scope. Empty list for non-loss reasons,
            strategy-created plans (no user), missing plan rows,
            malformed payloads, or dedup hits.
        """
        try:
            data = parse_message(payload.decode("utf-8"))
        except (UnicodeDecodeError, MessageParseError):
            return []
        if not isinstance(data, ExecutionPlanDecisionEventData):
            return []
        if data.reason not in _KNOWN_LOSS_REASONS:
            return []
        plan = await repo.get_execution_plan(data.plan_public_id, as_of=now)
        if plan is None:
            logger.info(
                "position_stop_loss_fired: plan {pid} not found — dropping alert",
                pid=data.plan_public_id,
            )
            return []
        user_public_id = plan.get("created_by_user_id")
        if not user_public_id:
            logger.info(
                "position_stop_loss_fired: plan {pid} has no user_id — dropping alert",
                pid=data.plan_public_id,
            )
            return []
        native_symbol = await repo.get_symbol_for_instrument(
            plan["instrument_public_id"], as_of=now
        )
        if native_symbol is None:
            native_symbol = plan["instrument_public_id"]
        dedup_key = f"stop_loss.{data.decision_public_id}"
        if await check_dedup_window(
            repo=repo,
            user_public_id=user_public_id,
            dedup_key=dedup_key,
            window_seconds=self.suppression_window_seconds,
            now=now,
        ):
            return []
        exchange = plan["exchange"]
        title = "Stop-loss fired"
        body_args: list[JsonValue] = [native_symbol, exchange]
        body = f"Stop-loss fired on {body_args[0]} ({body_args[1]})"
        row = AlertEventInsertRow(
            user_public_id=user_public_id,
            operator_public_id=plan.get("operator_public_id"),
            wallet_public_id=plan.get("wallet_public_id"),
            alert_type=self.alert_type,
            priority=self.priority,
            is_safety_critical=self.is_safety_critical,
            title=title,
            body=body,
            payload={
                "deep_link_path": f"/positions/{plan['instrument_public_id']}",
                "plan_public_id": data.plan_public_id,
                "decision_public_id": data.decision_public_id,
                "reason": data.reason,
                "body_suppressed": False,
                "title_loc_key": "alerts.title.position_stop_loss_fired",
                "body_loc_key": "alerts.body.position_stop_loss_fired",
                "body_loc_args": body_args,
            },
            dedup_key=dedup_key,
            thread_key=(
                f"{self.thread_key_prefix}.{plan.get('wallet_public_id') or 'no-wallet'}"
                f".{plan['instrument_public_id']}"
            ),
            source_topic=topic,
        )
        return [row]
