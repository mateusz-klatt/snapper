"""``drift`` rule for durable portfolio-drift episode transitions."""

from datetime import datetime

from loguru import logger

from snapper.application.notify.rules.base import AlertRule
from snapper.application.notify.rules.dedup import check_dedup_since
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow
from snapper.messaging.schemas.data import PortfolioDriftEpisodeEventData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message

_TOPIC = "bus.portfolio_drift_episode"


class PortfolioDriftRule(AlertRule):
    """Page wallet-owning operators when a drift episode opens or resolves.

    Ownership is derived only from active wallet scope grants. Each distinct
    operator's active user memberships are expanded into notification
    recipients, and a user belonging to multiple owning operators is retained
    once under the lexicographically first operator. No administrator fallback
    exists, keeping the alert within the owning scope.

    Episode-lifetime deduplication starts at the episode's own ``opened_at``
    timestamp. Open and resolved transitions use different keys so resolution
    is a separate notice rather than another page or a duplicate suppressed by
    the opening alert.
    """

    alert_type = "drift"
    subscribe_topic_prefixes = (_TOPIC,)
    priority = "high"
    is_safety_critical = True
    thread_key_prefix = "snapper.drift"
    suppression_window_seconds = 0

    async def evaluate(
        self,
        topic: str,
        payload: bytes,
        repo: Repository,
        now: datetime,
    ) -> list[AlertEventInsertRow]:
        """Return owner-scoped alerts for one drift lifecycle transition.

        Args:
            topic: Exact drift-episode lifecycle topic.
            payload: Raw ``PortfolioDriftEpisodeEventData`` JSON bytes.
            repo: Repository for scope ownership, memberships, and dedup reads.
            now: Entry-boundary timestamp used for active ownership reads.

        Returns:
            One alert row per distinct owning user that has not already
            received this lifecycle transition. Empty for unrelated topics,
            malformed or wrong payloads, ownerless wallets, and dedup hits.
        """
        if topic != _TOPIC:
            return []
        try:
            data = parse_message(payload.decode("utf-8"))
        except (UnicodeDecodeError, MessageParseError):
            return []
        if not isinstance(data, PortfolioDriftEpisodeEventData):
            return []
        recipient_operators = await self._recipient_operators(
            repo=repo,
            wallet_public_id=data.wallet_public_id,
            now=now,
        )
        if not recipient_operators:
            logger.info(
                "portfolio_drift: no owning operator users for wallet {wallet} — alert dropped",
                wallet=data.wallet_public_id,
            )
            return []
        is_open = data.lifecycle == "opened"
        dedup_key = (
            f"drift.{data.episode_public_id}"
            if is_open
            else f"drift.resolved.{data.episode_public_id}"
        )
        thread_key = f"{self.thread_key_prefix}.{data.episode_public_id}"
        title = "Portfolio drift detected" if is_open else "Portfolio drift resolved"
        body = self._body(data)
        rows: list[AlertEventInsertRow] = []
        for user_public_id in sorted(recipient_operators):
            if await check_dedup_since(
                repo=repo,
                user_public_id=user_public_id,
                dedup_key=dedup_key,
                since=data.opened_at,
            ):
                continue
            rows.append(
                AlertEventInsertRow(
                    user_public_id=user_public_id,
                    operator_public_id=recipient_operators[user_public_id],
                    wallet_public_id=data.wallet_public_id,
                    alert_type=self.alert_type,
                    priority=self.priority,
                    is_safety_critical=self.is_safety_critical,
                    title=title,
                    body=body,
                    payload={
                        "deep_link_path": "/portfolio/accounts",
                        "episode_public_id": data.episode_public_id,
                        "lifecycle": data.lifecycle,
                        "exchange": data.exchange,
                        "mode": data.mode,
                        "mismatch_count": data.mismatch_count,
                        "opened_at": data.opened_at.isoformat(),
                        "closed_at": (
                            data.closed_at.isoformat() if data.closed_at is not None else None
                        ),
                        "resolution_reason": data.resolution_reason,
                        "body_suppressed": False,
                    },
                    dedup_key=dedup_key,
                    thread_key=thread_key,
                    source_topic=topic,
                )
            )
        return rows

    @staticmethod
    async def _recipient_operators(
        *,
        repo: Repository,
        wallet_public_id: str,
        now: datetime,
    ) -> dict[str, str]:
        """Map each distinct owning user to one deterministic operator."""
        grants = await repo.list_active_scope_grants_for_wallet(wallet_public_id, now)
        operator_public_ids = sorted({grant["operator_public_id"] for grant in grants})
        recipients: dict[str, str] = {}
        for operator_public_id in operator_public_ids:
            user_public_ids = await repo.list_users_with_operator_membership(
                operator_public_id,
                now,
            )
            for user_public_id in sorted(set(user_public_ids)):
                recipients.setdefault(user_public_id, operator_public_id)
        return recipients

    @staticmethod
    def _body(data: PortfolioDriftEpisodeEventData) -> str:
        """Render concise English fallback copy for one lifecycle event."""
        if data.lifecycle == "opened":
            return (
                f"Portfolio drift detected on {data.exchange} for wallet "
                f"{data.wallet_public_id} after {data.mismatch_count} consecutive full mismatches"
            )
        reason = data.resolution_reason or "reconciliation matched"
        return (
            f"Portfolio drift resolved on {data.exchange} for wallet "
            f"{data.wallet_public_id}: {reason}"
        )
