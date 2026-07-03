"""UUID7 format helpers used across transport validation layers.

UUID7 is the canonical public-id format across Snapper (chosen over
UUID5 for time-ordering): time-ordered, collision-resistant,
128-bit. Segments embedded in WebSocket topics and HTTP paths must be
proven to carry that format before the dispatcher trusts them —
length-only checks would accept a random 36-byte string while a
malformed version or variant nibble would still slip past layout
checks.

``is_uuid7`` pins the RFC 4122 layout with version nibble = 7 and
variant nibble in ``{8, 9, a, b}``. Used by the backtest topic
validator (``messaging/topics/validation.py::_validate_backtest_topic``)
for the wallet + run segments, and by any other transport layer that
embeds a UUID7 inside a dotted topic or URL path.
"""

import re
from typing import Final

_UUID7_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def is_uuid7(value: str) -> bool:
    """Return ``True`` iff ``value`` is a canonical-form UUID7 string.

    Enforces the RFC 4122 layout with version nibble = 7 and variant
    nibble in ``{8, 9, a, b}``. Length-only checks are insufficient —
    the topic validator needs format-level proof to reject malformed
    segments. Uses ``fullmatch`` so a trailing newline (e.g. from a
    config-sourced value) can never sneak past the ``$`` anchor into a
    topic segment or DB row.

    Args:
        value: Candidate UUID7 string.

    Returns:
        ``True`` when the layout, version, and variant all match;
        ``False`` otherwise.
    """
    return _UUID7_PATTERN.fullmatch(value) is not None
