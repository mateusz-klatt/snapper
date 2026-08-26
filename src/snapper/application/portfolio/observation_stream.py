"""Reconstruct latest-at-minute observation maps from one bounded stream.

One repository round-trip per window replaces one temporal query per grid
minute. The stream contract (:meth:`Repository.get_venue_account_observation_attempt_stream`)
guarantees at most one opening seed per exchange at or before the window start
and every subsequent attempt through the window end, ordered by bus timestamp;
a pure cursor fold then answers exactly what the per-minute query answered.
The equivalence was proven before this module existed: a production probe
compared stream-built maps against per-minute maps over sixty minutes and
found zero mismatches.

Two traps the fold guards, both found in review of the original:
the cursor advances on ``timestamp <= minute`` so a sub-minute attempt
belongs to the minute it precedes, and callers derive currency sets from the
reconstructed maps rather than the raw rows, because a raw row may already be
superseded at every requested minute.

Extracted from the minimum-activation gate so the snapshotter and the gate
share one fold; the gate keeps its request-shaped wrapper.
"""

from collections.abc import Mapping
from collections.abc import Sequence
from datetime import datetime

from snapper.data.repository_types import VenueAccountObservationAttemptRow


def fold_observation_attempts(
    rows: Sequence[VenueAccountObservationAttemptRow],
    wallet_public_id: str,
    mode: str,
    exchanges: frozenset[str],
    minutes: Sequence[datetime],
) -> tuple[Mapping[str, VenueAccountObservationAttemptRow], ...]:
    """Reconstruct every latest-at-minute observation map from one stream.

    Args:
        rows: Opening seeds and bounded subsequent attempts from the
            repository, for exactly this scope.
        wallet_public_id: The scope's wallet; a foreign row fails loudly.
        mode: The scope's execution mode; a foreign row fails loudly.
        exchanges: The exact venue set the stream was requested for.
        minutes: The ascending grid minutes to reconstruct.

    Returns:
        One latest-at-minute exchange map for every requested grid minute.

    Raises:
        ValueError: When the stream contains a row outside the requested
            scope or window, which would silently corrupt every later map.
    """
    end = minutes[-1]
    for row in rows:
        if row["wallet_public_id"] != wallet_public_id:
            raise ValueError("observation stream contains a foreign wallet")
        if row["mode"] != mode:
            raise ValueError("observation stream contains a foreign mode")
        if row["exchange"] not in exchanges:
            raise ValueError("observation stream contains an unexpected exchange")
        if row["timestamp"] > end:
            raise ValueError("observation stream extends beyond the requested window")
    ordered = sorted(rows, key=lambda row: (row["timestamp"], row["id"]))
    cursor: dict[str, VenueAccountObservationAttemptRow] = {}
    snapshots: list[Mapping[str, VenueAccountObservationAttemptRow]] = []
    position = 0
    for minute in minutes:
        while position < len(ordered) and ordered[position]["timestamp"] <= minute:
            row = ordered[position]
            cursor[row["exchange"]] = row
            position += 1
        snapshots.append(dict(cursor))
    return tuple(snapshots)
