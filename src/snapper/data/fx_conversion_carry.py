"""Bound on how far an FX mark may be carried into a gap minute.

Walutomat publishes a quote only when the polled payload changes, so roughly a
quarter of every hour carries no candle at all and a conversion landing in such
a gap was previously unprovable. This bound caps how stale a rate may be when
it values an execution; beyond it the election stays fail-closed rather than
guessing. Zero reproduces the original exact-minute rule.

The constant lives in its own module because both the ORM model and the
migrations that shaped the database CHECK must agree on it, and a migration
cannot import the model layer.

Raised from 15 to 20 on 2026-08-04 (migration 0047): the 2026-07-19 19:22Z
USD-PLN conversion sits 16 minutes after walutomat's last published mark
(candle open 19:05Z, closed 19:06Z), so the original bound missed a real
production gap by one minute. The EUR-PLN conversion in the same window needs
only 6. Anything the new bound still cannot reach stays fail-closed.
"""

MAX_CARRIED_MINUTES = 20
