"""Tests for ``snapper.core.ids.is_uuid7``."""

from uuid import uuid7

from snapper.core.ids import is_uuid7


class TestIsUuid7:
    """Canonical-form UUID7 detection."""

    def test_generated_uuid7_accepted(self) -> None:
        """Freshly-generated UUID7 strings pass the check."""
        for _ in range(10):
            value = str(uuid7())
            assert is_uuid7(value), f"uuid7() output rejected: {value}"

    def test_uppercase_hex_accepted(self) -> None:
        """Upper-case hex digits are accepted (case-insensitive regex)."""
        value = str(uuid7()).upper()
        assert is_uuid7(value)

    def test_wrong_length_rejected(self) -> None:
        """Too-short candidates are rejected."""
        assert not is_uuid7("short-id")
        assert not is_uuid7("")

    def test_wrong_version_nibble_rejected(self) -> None:
        """Version nibble other than 7 rejects (e.g. UUID4)."""
        value = str(uuid7())
        parts = value.split("-")
        parts[2] = "4" + parts[2][1:]
        assert not is_uuid7("-".join(parts))

    def test_wrong_variant_nibble_rejected(self) -> None:
        """Variant nibble must be in {8, 9, a, b}."""
        value = str(uuid7())
        parts = value.split("-")
        parts[3] = "0" + parts[3][1:]
        assert not is_uuid7("-".join(parts))

    def test_non_hex_rejected(self) -> None:
        """Characters outside 0-9a-f are rejected."""
        assert not is_uuid7("zzzzzzzz-zzzz-7zzz-8zzz-zzzzzzzzzzzz")
