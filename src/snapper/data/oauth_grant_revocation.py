"""Retire OAuth authority inside the transaction that changes its binding."""

from datetime import datetime

from sqlalchemy import ColumnElement
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken


async def revoke_oauth_grants_in_tx(
    session: AsyncSession, revoked_at: datetime, *grant_filters: ColumnElement[bool]
) -> None:
    """Revoke matching grants and all refresh rows using caller-held authority locks.

    The caller already serializes changes to the relevant user and membership.
    No client lock is taken because token operations lock the client first.

    Args:
        session: Caller-owned authority mutation transaction.
        revoked_at: UTC boundary of the authority change.
        grant_filters: Exact owner, delegate or operator predicates.
    """
    await session.execute(
        update(OAuthGrant)
        .where(*grant_filters, OAuthGrant.revoked_at.is_(None))
        .values(revoked_at=revoked_at)
    )
    grant_ids = select(OAuthGrant.public_id).where(*grant_filters)
    await session.execute(
        update(OAuthRefreshToken)
        .where(
            OAuthRefreshToken.grant_public_id.in_(grant_ids), OAuthRefreshToken.revoked_at.is_(None)
        )
        .values(revoked_at=revoked_at)
    )
