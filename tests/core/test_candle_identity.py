"""Tests for ``snapper.core.candle_identity``."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from hashlib import sha256
from uuid import UUID
from uuid import uuid7

import pytest

from snapper.core.candle_identity import CANDLE_IDENTITY_ALGORITHM
from snapper.core.candle_identity import deterministic_candle_public_id
from snapper.core.candle_identity import is_deterministic_candle_public_id
from snapper.core.candle_identity import parse_candle_identity_cutover
from snapper.core.ids import is_uuid7

_INSTRUMENT = "00000000-0000-4000-8000-000000000101"
_OPEN = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)

_GOLDEN = [
    (_INSTRUMENT, "1m", _OPEN, "019f1d8c-e200-87a4-b6b0-e5a938769eb8"),
    (
        "0199a2b3-c4d5-7e6f-8a9b-0c1d2e3f4a5b",
        "1d",
        datetime(2026, 11, 12, 0, 0, tzinfo=UTC),
        "01a1cd0d-dc00-8487-8685-2a43d9af3f7c",
    ),
    (
        "ffffffff-ffff-4fff-bfff-ffffffffffff",
        "4h",
        datetime(1970, 1, 1, 0, 0, tzinfo=UTC),
        "00000000-0000-8c2a-9f59-26761561e589",
    ),
]


def _reference_id(instrument_public_id: str, timeframe: str, open_at: datetime) -> str:
    """Rebuild the documented layout independently of the implementation.

    Args:
        instrument_public_id: Instrument UUID string.
        timeframe: Candle timeframe label.
        open_at: Aware UTC opening.

    Returns:
        The custom UUIDv8 assembled field by field from the specification.
    """
    microseconds = (open_at - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)
    label = timeframe.encode("utf-8")
    digest = sha256(
        CANDLE_IDENTITY_ALGORITHM.encode("ascii")
        + UUID(instrument_public_id).bytes
        + bytes([len(label)])
        + label
        + microseconds.to_bytes(8, "big", signed=True)
    ).digest()
    first_74_bits = int.from_bytes(digest[:10], "big") >> 6
    custom_b = first_74_bits >> 62
    custom_c = first_74_bits & ((1 << 62) - 1)
    milliseconds = microseconds // 1000
    value = (milliseconds << 80) + (8 << 76) + (custom_b << 64) + (2 << 62) + custom_c
    return str(UUID(int=value))


class TestDeterministicCandlePublicId:
    """The id of one candle natural key."""

    @pytest.mark.parametrize(("instrument", "timeframe", "opening", "expected"), _GOLDEN)
    def test_golden_vectors_are_stable(
        self, instrument: str, timeframe: str, opening: datetime, expected: str
    ) -> None:
        """A key's id never changes across releases.

        Given: Three keys whose ids were recorded when the algorithm shipped.
        When: Their ids are computed again.
        Then: Each equals its recorded id and the field-by-field reference.
        """
        assert deterministic_candle_public_id(instrument, timeframe, opening) == expected
        assert _reference_id(instrument, timeframe, opening) == expected

    def test_layout_is_a_custom_uuid8_prefixed_by_the_opening(self) -> None:
        """The id carries version 8, the RFC variant and the opening in milliseconds.

        Given: A key opening at a known instant.
        When: Its id is parsed as a UUID.
        Then: The version is 8, the variant is RFC 4122, and the top 48 bits are the opening.
        """
        parsed = UUID(deterministic_candle_public_id(_INSTRUMENT, "1m", _OPEN))

        assert parsed.version == 8
        assert parsed.variant == "specified in RFC 4122"
        assert parsed.int >> 80 == int(_OPEN.timestamp()) * 1000

    def test_is_never_a_uuid7(self) -> None:
        """A deterministic id can never be mistaken for a random UUID7.

        Given: The id of a key.
        When: The UUID7 format check reads it.
        Then: The check rejects it, because the version nibble is 8.
        """
        assert not is_uuid7(deterministic_candle_public_id(_INSTRUMENT, "1m", _OPEN))

    def test_ids_sort_by_opening(self) -> None:
        """Ids of later openings sort after ids of earlier ones.

        Given: The same instrument and timeframe at three consecutive minutes.
        When: Their ids are compared as strings.
        Then: They are in opening order.
        """
        ids = [
            deterministic_candle_public_id(_INSTRUMENT, "1m", _OPEN + timedelta(minutes=offset))
            for offset in range(3)
        ]

        assert ids == sorted(ids)

    def test_each_key_part_changes_the_id(self) -> None:
        """Instrument, timeframe and opening each take part in the id.

        Given: A base key and three keys that each differ in one part.
        When: Their ids are computed.
        Then: All four ids are distinct.
        """
        ids = {
            deterministic_candle_public_id(_INSTRUMENT, "1m", _OPEN),
            deterministic_candle_public_id("00000000-0000-4000-8000-000000000102", "1m", _OPEN),
            deterministic_candle_public_id(_INSTRUMENT, "5m", _OPEN),
            deterministic_candle_public_id(_INSTRUMENT, "1m", _OPEN + timedelta(minutes=1)),
        }

        assert len(ids) == 4

    def test_equal_instants_in_other_zones_share_the_id(self) -> None:
        """The opening is an instant, whatever zone it is written in.

        Given: The same opening written in UTC and at a +02:00 offset.
        When: Both ids are computed.
        Then: They are equal.
        """
        shifted = _OPEN.astimezone(timezone(timedelta(hours=2)))

        assert deterministic_candle_public_id(
            _INSTRUMENT, "1m", shifted
        ) == deterministic_candle_public_id(_INSTRUMENT, "1m", _OPEN)

    def test_instrument_forms_that_name_the_same_uuid_share_the_id(self) -> None:
        """Any accepted spelling of one instrument UUID gives one id.

        Given: The same instrument UUID in canonical, upper-case and braced form.
        When: Their ids are computed.
        Then: They are equal.
        """
        forms = [_INSTRUMENT, _INSTRUMENT.upper(), "{" + _INSTRUMENT + "}"]

        assert len({deterministic_candle_public_id(form, "1m", _OPEN) for form in forms}) == 1

    @pytest.mark.parametrize(
        ("instrument", "timeframe", "opening"),
        [
            ("not-a-uuid", "1m", _OPEN),
            (_INSTRUMENT, "", _OPEN),
            (_INSTRUMENT, "123456789", _OPEN),
            (_INSTRUMENT, "1m", _OPEN.replace(tzinfo=None)),
            (_INSTRUMENT, "1m", datetime(1969, 12, 31, 23, 59, tzinfo=UTC)),
        ],
    )
    def test_invalid_keys_are_refused(
        self, instrument: str, timeframe: str, opening: datetime
    ) -> None:
        """A key that cannot name a candle gets no id.

        Given: A non-UUID instrument, an empty or nine-character timeframe, a naive
            opening, or an opening before the Unix epoch.
        When: Its id is requested.
        Then: A ValueError is raised.
        """
        with pytest.raises(ValueError):
            deterministic_candle_public_id(instrument, timeframe, opening)

    def test_the_last_datetime_fits_the_timestamp_field(self) -> None:
        """Every representable opening fits the 48-bit millisecond field.

        Given: The last minute a ``datetime`` can hold.
        When: Its id is computed.
        Then: The top 48 bits carry that minute's milliseconds.
        """
        last = datetime(9999, 12, 31, 23, 59, tzinfo=UTC)

        parsed = UUID(deterministic_candle_public_id(_INSTRUMENT, "1m", last))

        assert parsed.int >> 80 == int(last.timestamp()) * 1000

    def test_multibyte_timeframe_is_length_prefixed_in_bytes(self) -> None:
        """The length byte counts UTF-8 bytes, not characters.

        Given: A two-character timeframe whose second character takes two bytes.
        When: Its id is computed.
        Then: It equals the reference built with the byte length.
        """
        assert deterministic_candle_public_id(_INSTRUMENT, "1é", _OPEN) == _reference_id(
            _INSTRUMENT, "1é", _OPEN
        )


class TestIsDeterministicCandlePublicId:
    """Recognition of a key's own deterministic id."""

    def test_accepts_the_keys_id_in_any_case(self) -> None:
        """A stored id that names the key's deterministic id is recognised.

        Given: The deterministic id of a key, in lower and upper case.
        When: Each is checked against that key.
        Then: Both are recognised.
        """
        public_id = deterministic_candle_public_id(_INSTRUMENT, "1m", _OPEN)

        assert is_deterministic_candle_public_id(public_id, _INSTRUMENT, "1m", _OPEN)
        assert is_deterministic_candle_public_id(public_id.upper(), _INSTRUMENT, "1m", _OPEN)

    def test_rejects_another_keys_id_a_random_id_and_a_non_uuid(self) -> None:
        """Anything other than the key's own id is rejected.

        Given: Another key's deterministic id, a random UUID7 and a non-UUID string.
        When: Each is checked against the key.
        Then: None is recognised.
        """
        other = deterministic_candle_public_id(_INSTRUMENT, "5m", _OPEN)

        assert not is_deterministic_candle_public_id(other, _INSTRUMENT, "1m", _OPEN)
        assert not is_deterministic_candle_public_id(str(uuid7()), _INSTRUMENT, "1m", _OPEN)
        assert not is_deterministic_candle_public_id("not-a-uuid", _INSTRUMENT, "1m", _OPEN)

    def test_refuses_an_invalid_key(self) -> None:
        """An invalid key is an error, not a mismatch.

        Given: A naive opening.
        When: An id is checked against that key.
        Then: A ValueError is raised.
        """
        with pytest.raises(ValueError):
            is_deterministic_candle_public_id(
                str(uuid7()), _INSTRUMENT, "1m", _OPEN.replace(tzinfo=None)
            )


class TestParseCandleIdentityCutover:
    """Parsing of the cutover setting."""

    @pytest.mark.parametrize("raw", [None, "", "   "])
    def test_unset_values_mean_no_cutover(self, raw: str | None) -> None:
        """An absent or blank setting names no cutover.

        Given: None, an empty string or blank text.
        When: It is parsed.
        Then: The result is None.
        """
        assert parse_candle_identity_cutover(raw) is None

    @pytest.mark.parametrize("raw", ["2026-11-12T23:00:00Z", " 2026-11-12T23:00:00+00:00 "])
    def test_a_utc_minute_is_returned_as_an_aware_utc_datetime(self, raw: str) -> None:
        """A UTC instant on a whole minute is accepted.

        Given: The same UTC minute with a Z suffix and with a +00:00 offset.
        When: It is parsed.
        Then: The result is that minute as an aware UTC datetime.
        """
        parsed = parse_candle_identity_cutover(raw)

        assert parsed == datetime(2026, 11, 12, 23, 0, tzinfo=UTC)
        assert parsed is not None
        assert parsed.tzinfo is UTC

    @pytest.mark.parametrize(
        "raw",
        [
            12,
            "tomorrow",
            "2026-11-12T23:00:00",
            "2026-11-13T00:00:00+01:00",
            "2026-11-12T23:00:30Z",
            "2026-11-12T23:00:00.5Z",
        ],
    )
    def test_malformed_values_are_refused(self, raw: object) -> None:
        """Anything other than a UTC whole minute is an error.

        Given: A number, free text, a naive time, a non-UTC offset, or a time
            with seconds or a fraction.
        When: It is parsed.
        Then: A ValueError is raised.
        """
        with pytest.raises(ValueError):
            parse_candle_identity_cutover(raw)
