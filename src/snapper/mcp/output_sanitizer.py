"""Output normalization for MCP tool responses (plan §3.2, §7 item 12).

Every value an MCP tool returns passes through :func:`sanitize_output`
before serialization to the MCP client. The goal is two-fold:

    1. **Prompt-injection hardening.** Market-data / user-supplied
       strings that reach the LLM-side MCP client can contain
       instruction-shaped text ("ignore previous instructions...")
       that an attacker planted upstream (e.g. in a venue instrument
       description). Control-character stripping + HTML-escape
       defuses the common vectors.
    2. **Transport hygiene.** 4096-char clipping prevents an
       accidental full-file dump from inflating a tool response to
       multi-megabyte JSON.

The sanitizer walks dict / list / tuple containers recursively so
nested payloads are covered. Non-string leaves (numbers, bool, None)
pass through unchanged. Bytes are decoded best-effort (latin-1
fallback) then sanitized as text.
"""

import html
from typing import Any

_CLIP_LEN = 4096
_CONTROL_CHARS = frozenset(chr(c) for c in range(0x20) if chr(c) not in {"\t", "\n", "\r"}) | {
    chr(0x7F)
}


def sanitize_output(value: Any) -> Any:
    r"""Return ``value`` with strings escaped + clipped + control-stripped.

    Behavior by type:

        - ``str``: strip control chars (except ``\t``, ``\n``, ``\r``),
          HTML-escape ``&``, ``<``, ``>``, ``"``, ``'``, then clip to
          4096 chars. Shorter strings pass through with only the
          strip + escape applied.
        - ``dict``: recursive on each value; keys are NOT sanitized
          (they're structural identifiers, not content; an MCP tool
          author controls them).
        - ``list`` / ``tuple``: recursive on each element; list
          shape preserved (``tuple`` stays ``tuple``).
        - ``bytes``: decoded via latin-1 (lossless on arbitrary
          bytes), then sanitized as a str.
        - anything else (int, float, bool, None, Decimal, datetime,
          custom objects): passed through unchanged. Tools returning
          these types rely on the downstream JSON serializer.

    Args:
        value: Arbitrary tool return value — typically a dict of
            strings + numbers, but recursive containers are
            supported.

    Returns:
        A sanitized shape-preserving copy. Inputs are not mutated.
    """
    if isinstance(value, str):
        return _sanitize_str(value)
    if isinstance(value, bytes):
        return _sanitize_str(value.decode("latin-1"))
    if isinstance(value, dict):
        return {k: sanitize_output(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_output(v) for v in value]
    if isinstance(value, tuple):
        return tuple(sanitize_output(v) for v in value)
    return value


def _sanitize_str(s: str) -> str:
    """Strip control chars, HTML-escape, clip to 4096 chars.

    Args:
        s: Raw string from a tool response — may contain
            venue-sourced content with embedded control chars or
            injection-shaped HTML.

    Returns:
        Escaped + clipped copy. Newlines / tabs / carriage returns
        are preserved because MCP clients commonly render them;
        everything else below 0x20 and the DEL (0x7F) is removed.
    """
    filtered = "".join(ch for ch in s if ch not in _CONTROL_CHARS)
    escaped = html.escape(filtered, quote=True)
    if len(escaped) > _CLIP_LEN:
        return escaped[:_CLIP_LEN]
    return escaped
