"""Effective venue account-state status derivation (PnL Phase 3).

The ``sync_status`` stored on a ``venue_account_states`` row is the raw
outcome of the last observation attempt. Read surfaces must never serve a
stale or clock-inconsistent row as live truth, so they derive the EFFECTIVE
status here at read time from the stored observation timestamps and the
authoritative window. This is a pure function with no I/O so it is trivially
testable and shared by every read surface (REST, MCP).
"""

from datetime import datetime

EFFECTIVE_STALE = "stale"
"""An observed row whose authoritative window has elapsed — visibly present
but no longer live truth."""
EFFECTIVE_CLOCK_ERROR = "clock_error"
"""A row observed with a future-dated clock — never trusted."""


def derive_effective_account_status(
    *,
    sync_status: str,
    balance_observed_at: datetime | None,
    position_observed_at: datetime | None,
    authoritative_until: datetime | None,
    now: datetime,
) -> str:
    """Derive the effective read status for a venue account-state row.

    Fail-closed: a future-dated observation clock (either component) is never
    trusted, and an ``observed`` row is demoted to ``stale`` UNLESS it carries a
    still-valid authority window — a missing ``authoritative_until`` (None means
    "never authoritative") or an elapsed one both demote to ``stale`` rather
    than serving the row as live truth forever. Every other stored status
    passes through unchanged — ``simulated``/``unsupported``/``error`` are
    already non-authoritative labels that must survive verbatim.

    Args:
        sync_status: The stored roll-up status.
        balance_observed_at: When the balance was last observed (None if never).
        position_observed_at: When positions were last observed (None if never).
        authoritative_until: The instant past which an observed row is stale
            (None when the row was never authoritative — treated as expired).
        now: The current instant.

    Returns:
        ``clock_error`` when an observation clock is in the future, ``stale``
        when an observed row lacks a still-valid authority window, else the
        stored ``sync_status``.
    """
    if (balance_observed_at is not None and balance_observed_at > now) or (
        position_observed_at is not None and position_observed_at > now
    ):
        return EFFECTIVE_CLOCK_ERROR
    if sync_status == "observed" and (authoritative_until is None or now > authoritative_until):
        return EFFECTIVE_STALE
    return sync_status
