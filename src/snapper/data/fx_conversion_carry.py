"""Bound on how far an FX mark may be carried into a gap minute.

Walutomat publishes a quote only when the polled payload changes, so roughly a
quarter of every hour carries no candle at all and a conversion landing in such
a gap was previously unprovable. This bound caps how stale a rate may be when
it values an execution; beyond it the election stays fail-closed rather than
guessing. Zero reproduces the original exact-minute rule.

The constant lives in its own module because both the ORM model and the
migration that widened the database CHECK must agree on it, and the migration
cannot import the model layer.
"""

MAX_CARRIED_MINUTES = 15
