r"""Coverage for :func:`snapper.mcp.output_sanitizer.sanitize_output`.

Exercises the three normalizations the sanitizer applies:

    - HTML-escape of string leaves (prompt-injection hardening).
    - Control-character stripping (keeps ``\t``, ``\n``, ``\r``;
      removes the rest below 0x20 plus 0x7F DEL).
    - 4096-char clip on individual string leaves.

Plus recursive container handling (``dict`` / ``list`` / ``tuple``)
and the pass-through behavior for non-string leaves.
"""

from decimal import Decimal
from typing import Any

import pytest

from snapper.mcp.output_sanitizer import sanitize_output


class TestStringNormalization:
    """Per-string leaf sanitizations."""

    def test_html_escapes_angle_brackets_and_quotes(self) -> None:
        """Given: ``<script>`` + quotes, Then: HTML-entity escaped.

        Prompt-injection vector of ``<script>alert(1)</script>`` is
        neutered to printable-but-harmless entities before the MCP
        client forwards it to the LLM-side consumer.
        """
        raw = '<script>alert("x")</script>'
        out = sanitize_output(raw)
        assert "&lt;" in out
        assert "&gt;" in out
        assert "&quot;" in out
        assert "<" not in out
        assert ">" not in out

    def test_strips_control_chars_but_keeps_whitespace(self) -> None:
        r"""Given: NUL / bell + real whitespace, Then: only the former are removed.

        Real MCP clients commonly render ``\t`` / ``\n`` / ``\r``
        for log-like payloads; stripping them would break UX.
        """
        raw = "hello\x00\x07world\tgoodbye\nthere\rok\x7f"
        out = sanitize_output(raw)
        assert "\x00" not in out
        assert "\x07" not in out
        assert "\x7f" not in out
        assert "\t" in out
        assert "\n" in out
        assert "\r" in out

    def test_clips_to_4096_chars(self) -> None:
        """Given: 5000-char input, Then: output is exactly 4096 chars."""
        raw = "x" * 5000
        out = sanitize_output(raw)
        assert len(out) == 4096

    def test_short_string_is_not_padded_or_truncated(self) -> None:
        """Given: a 12-char plain string, Then: unchanged length."""
        raw = "plain-string"
        out = sanitize_output(raw)
        assert out == raw

    def test_bytes_are_decoded_and_sanitized(self) -> None:
        """Given: raw bytes with latin-1 content, Then: sanitized str returned."""
        raw = b"hello\x00<world>"
        out = sanitize_output(raw)
        assert isinstance(out, str)
        assert "\x00" not in out
        assert "&lt;" in out


class TestContainerWalk:
    """Recursive descent into dict / list / tuple."""

    def test_dict_values_are_sanitized_keys_are_not(self) -> None:
        """Given: a dict with an injection-shaped value, Then: value escaped.

        Dict keys stay intact because they're structural identifiers
        — a tool author controls them, not the upstream data source.
        """
        raw = {"instrument": "<b>BTC</b>", "exchange": "kraken"}
        out = sanitize_output(raw)
        assert list(out.keys()) == ["instrument", "exchange"]
        assert out["instrument"] == "&lt;b&gt;BTC&lt;/b&gt;"
        assert out["exchange"] == "kraken"

    def test_list_walks_recursively(self) -> None:
        """Given: a list of mixed strings + nested dicts, Then: each element sanitized."""
        raw = ["<x>", {"k": "<y>"}, 42]
        out = sanitize_output(raw)
        assert out[0] == "&lt;x&gt;"
        assert out[1] == {"k": "&lt;y&gt;"}
        assert out[2] == 42

    def test_tuple_shape_preserved(self) -> None:
        """Given: a tuple of strings, Then: sanitized tuple returned (not list)."""
        raw: tuple[Any, ...] = ("<a>", "<b>")
        out = sanitize_output(raw)
        assert isinstance(out, tuple)
        assert out == ("&lt;a&gt;", "&lt;b&gt;")


class TestPassThrough:
    """Leaves that don't need sanitization are left alone."""

    @pytest.mark.parametrize(
        ("value",),
        [
            (42,),
            (3.14,),
            (True,),
            (False,),
            (None,),
            (Decimal("0.5"),),
        ],
    )
    def test_numeric_and_sentinel_leaves_unchanged(self, value: Any) -> None:
        """Given: a non-string leaf, Then: returned identity-equal."""
        assert sanitize_output(value) is value
