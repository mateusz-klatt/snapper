"""Backfill ai_delegates operational rows for pre-fix delegates.

Delegate minting creates the AI_DELEGATE user, caps, membership, and
token inventory rows, but until the accompanying service fix it never
created the ``ai_delegates`` operational row. WS auth resolves
``principal.delegate_public_id`` by looking that row up per
``user_public_id`` and AI-review admission requires its
``last_seen_at`` inside the heartbeat window — so a delegate without
the row can never be woken. This data migration inserts the missing
row (fresh UUID7, ``last_seen_at`` NULL, zero in-flight counter) for
every delegate user minted before the fix.
"""

from collections.abc import Sequence
from uuid import uuid7

from alembic import op
from sqlalchemy import text

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Insert missing ai_delegates rows for role='ai_delegate' users.

    Returns:
        None.
    """
    connection = op.get_bind()
    missing = connection.execute(
        text(
            "SELECT DISTINCT u.public_id FROM users u "
            "LEFT JOIN ai_delegates d ON d.user_public_id = u.public_id "
            "WHERE u.role = 'ai_delegate' AND d.user_public_id IS NULL"
        )
    ).fetchall()
    for (user_public_id,) in missing:
        connection.execute(
            text(
                "INSERT INTO ai_delegates "
                "(public_id, user_public_id, last_seen_at, active_reviews_count, "
                "created_at, updated_at) "
                "VALUES (:public_id, :user_public_id, NULL, 0, "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            ),
            {
                "public_id": str(uuid7()),
                "user_public_id": user_public_id,
            },
        )


def downgrade() -> None:
    """No-op: backfilled rows are indistinguishable from minted ones.

    Removing operational rows would break liveness for delegates that
    connected after the backfill, so the data change is intentionally
    left in place on downgrade.

    Returns:
        None.
    """
