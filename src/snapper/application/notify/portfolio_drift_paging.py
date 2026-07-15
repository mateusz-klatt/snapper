"""Shared owner-scoped paging for open portfolio-drift episodes."""

from datetime import datetime
from typing import Final

from loguru import logger

from snapper.application.notify.rules.dedup import check_dedup_since
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow

PORTFOLIO_DRIFT_TOPIC = "bus.portfolio_drift_episode"
PORTFOLIO_DRIFT_ALERT_TYPE = "drift"
PORTFOLIO_DRIFT_PRIORITY = "high"
PORTFOLIO_DRIFT_THREAD_KEY_PREFIX = "snapper.drift"
PORTFOLIO_DRIFT_OPEN_MISMATCH_COUNT: Final[int] = 3


async def resolve_portfolio_drift_recipient_operators(
    *,
    repo: Repository,
    wallet_public_id: str,
    now: datetime,
) -> dict[str, str]:
    """Map each distinct owning user to one deterministic operator.

    Args:
        repo: Repository for current scope grants and memberships.
        wallet_public_id: Wallet whose active owners receive the alert.
        now: Entry-boundary timestamp for active ownership reads.

    Returns:
        Mapping from user public id to the lexicographically first owning
        operator public id. Empty when no active owning user exists.
    """
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


async def build_open_portfolio_drift_alert_rows(
    *,
    repo: Repository,
    wallet_public_id: str,
    exchange: str,
    mode: str,
    episode_public_id: str,
    opened_at: datetime,
    mismatch_count: int,
    now: datetime,
) -> list[AlertEventInsertRow]:
    """Build missing owner pages for one durable open drift episode.

    Both the real-time rule and durable recovery scanner use this boundary,
    keeping recipient resolution, lifetime deduplication, copy, priority,
    payload, thread identity, and source identity identical.

    Args:
        repo: Repository for owner resolution and alert-event dedup reads.
        wallet_public_id: Wallet whose portfolio is drifting.
        exchange: Lowercase exchange identifier.
        mode: Durable episode mode, currently always ``"live"``.
        episode_public_id: Stable drift-episode public identifier.
        opened_at: Durable episode opening time and dedup lower bound.
        mismatch_count: Consecutive full-mismatch count carried by the opening
            transition. Production opening transitions occur at three.
        now: Entry-boundary timestamp for active ownership reads.

    Returns:
        One exact Stage 1 alert row per owning user without a persisted
        ``drift.<episode_public_id>`` alert since ``opened_at``.
    """
    recipient_operators = await resolve_portfolio_drift_recipient_operators(
        repo=repo,
        wallet_public_id=wallet_public_id,
        now=now,
    )
    if not recipient_operators:
        logger.info(
            "portfolio_drift: no owning operator users for wallet {wallet} — alert dropped",
            wallet=wallet_public_id,
        )
        return []
    dedup_key = f"drift.{episode_public_id}"
    rows: list[AlertEventInsertRow] = []
    for user_public_id in sorted(recipient_operators):
        if await check_dedup_since(
            repo=repo,
            user_public_id=user_public_id,
            dedup_key=dedup_key,
            since=opened_at,
        ):
            continue
        rows.append(
            AlertEventInsertRow(
                user_public_id=user_public_id,
                operator_public_id=recipient_operators[user_public_id],
                wallet_public_id=wallet_public_id,
                alert_type=PORTFOLIO_DRIFT_ALERT_TYPE,
                priority=PORTFOLIO_DRIFT_PRIORITY,
                is_safety_critical=True,
                title="Portfolio drift detected",
                body=(
                    f"Portfolio drift detected on {exchange} for wallet "
                    f"{wallet_public_id} after {mismatch_count} consecutive full mismatches"
                ),
                payload={
                    "deep_link_path": "/portfolio/accounts",
                    "episode_public_id": episode_public_id,
                    "lifecycle": "opened",
                    "exchange": exchange,
                    "mode": mode,
                    "mismatch_count": mismatch_count,
                    "opened_at": opened_at.isoformat(),
                    "closed_at": None,
                    "resolution_reason": None,
                    "body_suppressed": False,
                },
                dedup_key=dedup_key,
                thread_key=f"{PORTFOLIO_DRIFT_THREAD_KEY_PREFIX}.{episode_public_id}",
                source_topic=PORTFOLIO_DRIFT_TOPIC,
            )
        )
    return rows
