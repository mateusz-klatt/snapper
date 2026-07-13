"""Tests for the effective venue account-state status derivation (Phase 3).

Pins the fail-closed read-time contract of
:func:`snapper.application.portfolio.account_status.derive_effective_account_status`:
a future-dated observation clock (either the balance or the position
component) is demoted to ``clock_error`` and never trusted; an ``observed``
row is demoted to ``stale`` unless it still carries a valid authority window
— a MISSING ``authoritative_until`` (None) is treated as "never authoritative"
and demotes to ``stale`` exactly like an elapsed window (the fail-closed
rework: an observed row can no longer be served as live truth forever just
because it lacks an expiry); and every non-authoritative label
(``simulated``/``unsupported``/``error``) survives VERBATIM even once the
authoritative window is None or elapsed — a non-observed status is never
laundered into ``stale``. Covers every branch of the two guard clauses.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

from snapper.application.portfolio.account_status import EFFECTIVE_CLOCK_ERROR
from snapper.application.portfolio.account_status import EFFECTIVE_STALE
from snapper.application.portfolio.account_status import derive_effective_account_status

_NOW = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
_PAST = _NOW - timedelta(hours=1)
_FUTURE = _NOW + timedelta(hours=1)


def test_clock_error_when_balance_observed_in_future() -> None:
    """A future-dated balance clock is never trusted.

    Given: an ``observed`` row whose balance_observed_at is AFTER now
        while the position clock and window are otherwise sane,
    When: the effective status is derived,
    Then: it is ``clock_error`` — the future balance clock short-circuits
        the first clause before any window check runs.
    """
    result = derive_effective_account_status(
        sync_status="observed",
        balance_observed_at=_FUTURE,
        position_observed_at=_PAST,
        authoritative_until=_FUTURE,
        now=_NOW,
    )
    assert result == EFFECTIVE_CLOCK_ERROR


def test_clock_error_when_position_observed_in_future() -> None:
    """A future-dated position clock is never trusted, balance aside.

    Given: an ``observed`` row whose balance clock is absent (None) so the
        first clock clause is False, but position_observed_at is AFTER now,
    When: the effective status is derived,
    Then: it is ``clock_error`` — the SECOND clause of the clock guard fires
        even when the balance component supplies no timestamp.
    """
    result = derive_effective_account_status(
        sync_status="observed",
        balance_observed_at=None,
        position_observed_at=_FUTURE,
        authoritative_until=_FUTURE,
        now=_NOW,
    )
    assert result == EFFECTIVE_CLOCK_ERROR


def test_observed_fresh_within_authoritative_window_passes_through() -> None:
    """A fresh observed row strictly inside its window stays live truth.

    Given: an ``observed`` row with sane past clocks whose
        authoritative_until is AFTER now (window still open),
    When: the effective status is derived,
    Then: it is ``observed`` — not stale, not clock_error.
    """
    result = derive_effective_account_status(
        sync_status="observed",
        balance_observed_at=_PAST,
        position_observed_at=_PAST,
        authoritative_until=_FUTURE,
        now=_NOW,
    )
    assert result == "observed"


def test_observed_at_authoritative_boundary_passes_through() -> None:
    """The authority boundary is inclusive: now == until stays observed.

    Given: an ``observed`` row whose authoritative_until equals now exactly
        (the boundary instant) with sane past clocks,
    When: the effective status is derived,
    Then: it is ``observed`` — the demotion is gated on ``now > until``, so
        the boundary itself is still live truth.
    """
    result = derive_effective_account_status(
        sync_status="observed",
        balance_observed_at=_PAST,
        position_observed_at=_PAST,
        authoritative_until=_NOW,
        now=_NOW,
    )
    assert result == "observed"


def test_observed_with_null_authoritative_until_is_stale() -> None:
    """An observed row with no authority window fails closed to stale (NEW).

    Given: an ``observed`` row with sane past clocks but a NULL
        authoritative_until (it never carried an authority window),
    When: the effective status is derived,
    Then: it is ``stale`` — the fail-closed rework treats a missing window as
        "never authoritative" so the row is never served as live truth
        forever. This is the behavioural inversion versus the prior contract,
        which passed a NULL-window observed row through unchanged.
    """
    result = derive_effective_account_status(
        sync_status="observed",
        balance_observed_at=_PAST,
        position_observed_at=_PAST,
        authoritative_until=None,
        now=_NOW,
    )
    assert result == EFFECTIVE_STALE


def test_observed_past_authoritative_window_is_stale() -> None:
    """An observed row whose authority has elapsed demotes to stale.

    Given: an ``observed`` row with sane past clocks whose
        authoritative_until is BEFORE now (window closed),
    When: the effective status is derived,
    Then: it is ``stale`` — visibly present but no longer live truth.
    """
    result = derive_effective_account_status(
        sync_status="observed",
        balance_observed_at=_PAST,
        position_observed_at=_PAST,
        authoritative_until=_PAST,
        now=_NOW,
    )
    assert result == EFFECTIVE_STALE


def test_simulated_survives_verbatim_when_window_is_none() -> None:
    """A simulated status is never laundered into stale, window aside.

    Given: a ``simulated`` row with no clocks and a NULL authoritative_until
        (the same None that demotes an observed row),
    When: the effective status is derived,
    Then: it is ``simulated`` verbatim — the stale demotion is gated on
        ``observed`` and must never touch a non-authoritative label.
    """
    result = derive_effective_account_status(
        sync_status="simulated",
        balance_observed_at=None,
        position_observed_at=None,
        authoritative_until=None,
        now=_NOW,
    )
    assert result == "simulated"
    assert result != EFFECTIVE_STALE


def test_unsupported_survives_verbatim_when_window_elapsed() -> None:
    """An unsupported status is never laundered into stale.

    Given: an ``unsupported`` row whose authoritative_until has elapsed,
    When: the effective status is derived,
    Then: it is ``unsupported`` verbatim, not ``stale``.
    """
    result = derive_effective_account_status(
        sync_status="unsupported",
        balance_observed_at=_PAST,
        position_observed_at=_PAST,
        authoritative_until=_PAST,
        now=_NOW,
    )
    assert result == "unsupported"
    assert result != EFFECTIVE_STALE


def test_error_survives_verbatim_when_window_elapsed() -> None:
    """An error status is never laundered into stale.

    Given: an ``error`` row whose authoritative_until has elapsed,
    When: the effective status is derived,
    Then: it is ``error`` verbatim, not ``stale`` — a failed attempt must
        surface as an error, never masquerade as an expired-but-real row.
    """
    result = derive_effective_account_status(
        sync_status="error",
        balance_observed_at=_PAST,
        position_observed_at=_PAST,
        authoritative_until=_PAST,
        now=_NOW,
    )
    assert result == "error"
    assert result != EFFECTIVE_STALE
