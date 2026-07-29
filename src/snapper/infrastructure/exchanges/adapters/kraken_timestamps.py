"""Shared timestamp normalization for Kraken exchange adapters."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

_MICROSECONDS_PER_MILLISECOND = 1_000
_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def normalize_kraken_timestamp(value: int | str) -> datetime:
    """Return one exact UTC millisecond for every Kraken timestamp representation.

    Kraken Futures emits integer Unix milliseconds on WebSocket paths and ISO
    strings on REST paths, while the other Kraken adapters emit ISO strings.
    Converting integer milliseconds through floating-point seconds can shift a
    trade by one or more microseconds, which would make a partition-key-inclusive
    deduplication constraint treat the same exchange trade as distinct rows. This
    function keeps integer inputs in integer arithmetic, converts aware ISO values
    to the canonical UTC timezone, and floors sub-millisecond ISO precision to the
    beginning of its containing millisecond.

    Args:
        value: Integer Unix milliseconds or a timezone-aware ISO-8601 timestamp.

    Returns:
        A timezone-aware UTC datetime with exact millisecond precision.

    Raises:
        ValueError: If an ISO timestamp has no timezone offset.
    """
    if isinstance(value, int):
        return _UNIX_EPOCH + timedelta(milliseconds=value)

    iso_value = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(iso_value)
    if parsed.tzinfo is None:
        raise ValueError("Kraken ISO timestamp must include a timezone offset")
    utc_value = parsed.astimezone(UTC)
    normalized_microsecond = (
        utc_value.microsecond // _MICROSECONDS_PER_MILLISECOND
    ) * _MICROSECONDS_PER_MILLISECOND
    return utc_value.replace(microsecond=normalized_microsecond)
