"""Effective venue account-state status derivation (PnL Phase 3).

The ``sync_status`` stored on a ``venue_account_states`` row is the raw
outcome of the last observation attempt. Read surfaces must never serve a
stale or clock-inconsistent row as live truth, so they derive the EFFECTIVE
status here at read time from the stored observation timestamps and the
authoritative window. This is a pure function with no I/O so it is trivially
testable and shared by every read surface (REST, MCP).
"""

from datetime import datetime
from datetime import timedelta

EFFECTIVE_STALE = "stale"
"""An observed row whose authoritative window has elapsed — visibly present
but no longer live truth."""
EFFECTIVE_CLOCK_ERROR = "clock_error"
"""A row observed with a future-dated clock — never trusted."""

ACCOUNT_FRESHNESS_CEILING_S = 300.0
"""Authority window stamped on a freshly observed account balance, in seconds.

The account observer writes ``authoritative_until = balance_observed_at +
ACCOUNT_FRESHNESS_CEILING_S`` (the messaging executor imports this exact value so
the two never drift). Past this window the read layer demotes an ``observed`` row
to ``stale`` rather than serving it as live truth. Hosted here — alongside
:data:`AUTHORITY_MAX_WINDOW` — so any surface that must reconstruct a balance's
effective authority window from ``balance_observed_at`` alone (for example the
Phase-5B basket-observation gate) shares the same constant instead of copying the
literal."""

AUTHORITY_MAX_WINDOW = timedelta(seconds=900)
"""Hard cap on how far past the balance observation an ``observed`` row may
still read as live truth, independent of the stored ``authoritative_until``.

The observer writes ``authoritative_until = balance_observed_at + ~5min``, so a
genuine row always expires (via ``now > authoritative_until``) long before this
cap. A forged/tampered/migration-bypassed row that pairs an old observation
with an arbitrarily far-future ``authoritative_until`` (e.g. year 2099) would
otherwise read as ``observed`` forever; bounding the effective authority end by
``balance_observed_at + AUTHORITY_MAX_WINDOW`` closes that off. Set to 3× the
observer's freshness ceiling so it never demotes a legitimate row."""


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
    still-valid authority window bounded to the observation. A missing
    ``authoritative_until`` (None means "never authoritative"), a missing
    ``balance_observed_at`` (an observed row with no observation time cannot be
    fresh), an elapsed window, OR a window that extends implausibly far past
    EITHER observation (capped at ``balance_observed_at + AUTHORITY_MAX_WINDOW``
    AND, when positions are observed, ``position_observed_at + AUTHORITY_MAX_WINDOW``
    so a fresh balance can never launder a weeks-old position observation into
    live truth) all demote to ``stale`` rather than serving the row as live
    truth. Every other
    stored status passes through unchanged — ``simulated``/``unsupported``/
    ``error`` are already non-authoritative labels that must survive verbatim.

    Args:
        sync_status: The stored roll-up status.
        balance_observed_at: When the balance was last observed (None if never).
        position_observed_at: When positions were last observed (None if never).
        authoritative_until: The instant past which an observed row is stale
            (None when the row was never authoritative — treated as expired).
        now: The current instant.

    Returns:
        ``clock_error`` when an observation clock is in the future, ``stale``
        when an observed row lacks a still-valid, observation-bounded authority
        window, else the stored ``sync_status``.
    """
    if (balance_observed_at is not None and balance_observed_at > now) or (
        position_observed_at is not None and position_observed_at > now
    ):
        return EFFECTIVE_CLOCK_ERROR
    if sync_status == "observed":
        if authoritative_until is None or balance_observed_at is None:
            return EFFECTIVE_STALE
        effective_until = min(authoritative_until, balance_observed_at + AUTHORITY_MAX_WINDOW)
        if position_observed_at is not None:
            effective_until = min(effective_until, position_observed_at + AUTHORITY_MAX_WINDOW)
        if now > effective_until:
            return EFFECTIVE_STALE
    return sync_status
