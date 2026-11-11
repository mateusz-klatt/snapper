"""Deterministic public identities for candle natural keys.

A candle's ``public_id`` is shared by every SCD2 version of its natural key
(instrument, timeframe, opening). When the id is a pure function of that key,
every writer in every process computes the same id without reading storage, so
publication and storage cannot disagree for keys created that way.

The id is a custom UUIDv8 (RFC 9562, section 5.8), the version RFC 9562
assigns to name-based identifiers built on a modern hash:

- ``custom_a`` (48 bits) is the opening in Unix milliseconds, so ids sort by
  opening;
- the version nibble is 8 and the variant is ``10``;
- ``custom_b`` (12 bits) and ``custom_c`` (62 bits) are the first 74 bits of a
  SHA-256 digest of the key.

The digest input is unambiguous: a fixed ASCII label, the 16 bytes of the
instrument UUID, one byte holding the timeframe's UTF-8 length followed by
those bytes, and the opening as signed 64-bit big-endian microseconds since the
Unix epoch. A deterministic id can never equal a random UUID7, because the
version nibbles differ.

Functions:
    deterministic_candle_public_id: The id of one natural key.
    is_deterministic_candle_public_id: Whether a stored id is that key's id.
    parse_candle_identity_cutover: The cutover instant of a setting value.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from hashlib import sha256
from typing import Final
from uuid import UUID

CANDLE_IDENTITY_ALGORITHM: Final[str] = "snapper.candle.public_id.v1"
"""Label of the identity algorithm, hashed as the digest's first bytes."""

_LABEL: Final[bytes] = CANDLE_IDENTITY_ALGORITHM.encode("ascii")
_EPOCH: Final[datetime] = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND: Final[timedelta] = timedelta(microseconds=1)
_MAX_TIMEFRAME_CHARACTERS: Final[int] = 8
_DIGEST_PREFIX_BYTES: Final[int] = 10
_DIGEST_BITS: Final[int] = 74
_CUSTOM_C_BITS: Final[int] = 62
_VERSION: Final[int] = 0x8
_VARIANT: Final[int] = 0b10


def _opening_microseconds(open_at: datetime) -> int:
    """Return the exact microseconds of an aware opening since the Unix epoch.

    Args:
        open_at: Aware candle opening, in any time zone.

    Returns:
        Microseconds since 1970-01-01T00:00:00Z, computed without floats.

    The 48-bit millisecond field reaches the year 10889, beyond the last
    ``datetime``, so only an opening before the epoch can fall outside it.

    Raises:
        ValueError: When the opening is naive or before the Unix epoch.
    """
    if open_at.tzinfo is None or open_at.utcoffset() is None:
        raise ValueError("a deterministic candle id requires an aware opening")
    microseconds = (open_at - _EPOCH) // _MICROSECOND
    if microseconds < 0:
        raise ValueError("a deterministic candle id requires an opening at or after the epoch")
    return microseconds


def _timeframe_bytes(timeframe: str) -> bytes:
    """Return the length-prefixed UTF-8 encoding of a timeframe label.

    Args:
        timeframe: Candle timeframe label, at most the column's 8 characters.

    Returns:
        One byte holding the UTF-8 length, followed by the UTF-8 bytes.

    Raises:
        ValueError: When the label is empty or longer than 8 characters.
    """
    if not timeframe or len(timeframe) > _MAX_TIMEFRAME_CHARACTERS:
        raise ValueError("a deterministic candle id requires a timeframe of 1 to 8 characters")
    encoded = timeframe.encode("utf-8")
    return bytes([len(encoded)]) + encoded


def deterministic_candle_public_id(
    instrument_public_id: str, timeframe: str, open_at: datetime
) -> str:
    """Return the deterministic public id of one candle natural key.

    Args:
        instrument_public_id: Instrument public id, in any form ``uuid.UUID``
            accepts.
        timeframe: Candle timeframe label.
        open_at: Aware candle opening; an equal instant in another zone gives
            the same id.

    Returns:
        The canonical lowercase string of the key's custom UUIDv8.

    Raises:
        ValueError: When the instrument id is not a UUID, the timeframe is
            empty or too long, or the opening is naive or before the epoch.
    """
    instrument = UUID(instrument_public_id)
    microseconds = _opening_microseconds(open_at)
    digest = sha256(
        _LABEL
        + instrument.bytes
        + _timeframe_bytes(timeframe)
        + microseconds.to_bytes(8, "big", signed=True)
    ).digest()
    bits = int.from_bytes(digest[:_DIGEST_PREFIX_BYTES], "big") >> (
        _DIGEST_PREFIX_BYTES * 8 - _DIGEST_BITS
    )
    custom_b = bits >> _CUSTOM_C_BITS
    custom_c = bits & ((1 << _CUSTOM_C_BITS) - 1)
    value = (
        (microseconds // 1000) << 80
        | _VERSION << 76
        | custom_b << 64
        | _VARIANT << _CUSTOM_C_BITS
        | custom_c
    )
    return str(UUID(int=value))


def is_deterministic_candle_public_id(
    public_id: str, instrument_public_id: str, timeframe: str, open_at: datetime
) -> bool:
    """Return whether a stored id is the deterministic id of its natural key.

    Args:
        public_id: Stored candle public id, in any case.
        instrument_public_id: Instrument public id of the key.
        timeframe: Candle timeframe label of the key.
        open_at: Aware candle opening of the key.

    Returns:
        ``True`` exactly when ``public_id`` names the same UUID as the key's
        deterministic id; ``False`` for any other UUID or a non-UUID string.

    Raises:
        ValueError: When the key itself is invalid, as for
            :func:`deterministic_candle_public_id`.
    """
    expected = UUID(deterministic_candle_public_id(instrument_public_id, timeframe, open_at))
    try:
        return UUID(public_id) == expected
    except ValueError:
        return False


def parse_candle_identity_cutover(raw: object) -> datetime | None:
    """Parse the setting that names the cutover to deterministic live ids.

    From the cutover on, the live spot path computes a new key's id instead of
    reading it, which is safe only for keys whose opening is at or after an
    instant when every writer already mints deterministic ids. The instant is
    therefore exact: an ISO-8601 UTC time on a whole minute.

    Args:
        raw: The stored setting value; ``None`` or blank text means no cutover.

    Returns:
        The cutover as an aware UTC datetime, or ``None`` when none is set.

    Raises:
        ValueError: When the value is not text, not ISO-8601, not UTC, or not
            on a whole minute.
    """
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError("the cutover must be ISO-8601 text")
    text = raw.strip()
    if not text:
        return None
    instant = datetime.fromisoformat(text)
    if instant.utcoffset() != timedelta(0):
        raise ValueError("the cutover must be a UTC instant")
    if instant.second or instant.microsecond:
        raise ValueError("the cutover must be on a whole minute")
    return instant.astimezone(UTC)
