"""Stable archive symbol resolution for filesystem paths.

Provides deterministic, cross-platform symbol-to-directory mapping.
Archive symbols are derived from the anchor row (first version) of
each Symbol entity, ensuring stability across native_symbol renames.
"""

import re

_WINDOWS_RESERVED = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


def safe_path(name: str) -> str:
    """Normalize a symbol name for safe use as a filesystem directory.

    Rules applied (in order):
        1. Strip leading/trailing whitespace
        2. Convert to UPPERCASE (case-insensitive FS safety)
        3. Replace all non-alphanumeric characters except ``-`` with ``_``
        4. Collapse consecutive ``_`` into one
        5. Strip leading/trailing ``_``
        6. Append ``_`` to Windows reserved names (CON, PRN, NUL, etc.)

    Args:
        name: Raw symbol name (e.g. ``BTC-USD``, ``X:BTCUSD``).

    Returns:
        Filesystem-safe string (e.g. ``BTC-USD``, ``X_BTCUSD``).
    """
    s = name.strip().upper()
    s = re.sub(r"[^A-Z0-9-]", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    if s in _WINDOWS_RESERVED:
        s = f"{s}_"
    return s


def resolve_archive_symbols(
    rows: list[tuple[str, str]],
) -> dict[str, str]:
    """Build stable archive_symbol mapping from DB anchor rows.

    Symbols are processed in seniority order (oldest first). The oldest
    symbol reserves the clean name; younger duplicates after ``safe_path``
    normalization receive ``-2``, ``-3``, etc. suffixes.

    Args:
        rows: ``[(public_id, native_symbol)]`` pre-sorted by
              ``timestamp ASC, id ASC`` (caller drops extra SQL columns).

    Returns:
        ``{public_id: archive_symbol}`` with seniority-based collision
        suffixes.
    """
    result: dict[str, str] = {}
    used: dict[str, int] = {}
    for public_id, native_symbol in rows:
        base = safe_path(native_symbol)
        count = used.get(base, 0) + 1
        used[base] = count
        archive = base if count == 1 else f"{base}-{count}"
        result[public_id] = archive
    return result
