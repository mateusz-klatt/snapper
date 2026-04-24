"""Rule-side dedup helper — queries the ``ix_alert_events_dedup`` window.

Each ``AlertRule`` minting an ``AlertEventInsertRow`` stamps a
``dedup_key`` scoped to the logical event (e.g.
``f"order_fill_full.{client_order_id}"``). Before returning the row,
the rule asks ``check_dedup_window`` whether an earlier alert sharing
that key was already emitted inside the rule's configured suppression
window; a hit drops the duplicate silently so that replay loops and
outbox drains cannot page the user twice for the same underlying
event.

Defence in depth: the ``alert_events`` table's ``ix_alert_events_dedup``
index is intentionally non-unique (see ``src/snapper/data/models.py``
line ~2443) because pre-insert deduplication belongs at the rule
boundary, not inside the write path. Any duplicate that slips through
(e.g. after sidecar restart mid-burst) is caught on the downstream
replay via this helper.
"""

from datetime import datetime
from datetime import timedelta

from snapper.data.repository import Repository


async def check_dedup_window(
    *,
    repo: Repository,
    user_public_id: str,
    dedup_key: str,
    window_seconds: int,
    now: datetime,
) -> bool:
    """Return True when a same-key event already fired inside ``window_seconds``.

    Args:
        repo: Repository for the ``alert_events`` index read.
        user_public_id: Recipient user UUID7 — scopes the index cut.
        dedup_key: Rule-minted suppression key.
        window_seconds: Rule's configured ``suppression_window_seconds``.
            A window of 0 short-circuits to ``False`` (no dedup) so
            rules that expect every fire to deliver (e.g.
            ``order_fill_full`` with ``window=0``) pay nothing for the
            lookup.
        now: Entry-boundary timestamp threaded from the caller per
            ``feedback_timestamp_discipline.md``.

    Returns:
        ``True`` when at least one active row exists in the window —
        the caller drops its pending alert. ``False`` otherwise.
    """
    if window_seconds <= 0:
        return False
    since = now - timedelta(seconds=window_seconds)
    matches = await repo.list_alert_events_with_dedup_key(
        user_public_id=user_public_id,
        dedup_key=dedup_key,
        since=since,
    )
    return len(matches) > 0
