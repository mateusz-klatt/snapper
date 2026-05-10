"""Executor template + per-wallet instance name classification.

The per-wallet executor system uses two distinct process names:

- **Template**: ``executor_<exchange>`` — a config-only entry that
  carries the operator-tunable ``parameters``/``note``/``mode``/
  ``parameters_schema`` for an exchange. Templates are never directly
  runnable; the spawner expands them into per-wallet instances.
- **Instance**: ``executor_<exchange>_w<wallet_short>`` — the
  runnable per-wallet process. ``wallet_short`` is the last 12
  lowercase hex characters of the wallet UUID7 (the random portion;
  see :mod:`snapper.core.wallet_short`).

These helpers classify a process name and parse instance suffixes so
the API surface can present templates and instances distinctly.
"""

import re

_EXECUTOR_INSTANCE_PATTERN = re.compile(
    r"^executor_(?P<exchange>[a-z_]+?)_w(?P<short>[a-f0-9]{12})$"
)
_EXECUTOR_PREFIX = "executor_"


def is_executor_instance(name: str) -> bool:
    """Return True iff ``name`` matches the per-wallet instance pattern.

    Args:
        name: Process name to classify.

    Returns:
        True when ``name`` is ``executor_<exchange>_w<12-hex>``,
        False otherwise.
    """
    return _EXECUTOR_INSTANCE_PATTERN.match(name) is not None


def is_executor_template(name: str) -> bool:
    """Return True iff ``name`` is a bare executor template name.

    A template is any ``executor_*`` name that is NOT an instance —
    e.g. ``executor_kraken``, ``executor_kraken_futures``.

    Args:
        name: Process name to classify.

    Returns:
        True when ``name`` starts with ``executor_`` and is not a
        per-wallet instance, False otherwise.
    """
    if not name.startswith(_EXECUTOR_PREFIX):
        return False
    return not is_executor_instance(name)


def parse_executor_instance(name: str) -> tuple[str, str] | None:
    """Parse a per-wallet instance name into ``(exchange, wallet_short)``.

    Args:
        name: Process name to parse.

    Returns:
        ``(exchange, wallet_short)`` tuple when ``name`` matches the
        instance pattern, ``None`` otherwise.
    """
    match = _EXECUTOR_INSTANCE_PATTERN.match(name)
    if match is None:
        return None
    return match.group("exchange"), match.group("short")


def parent_template_for_instance(name: str) -> str | None:
    """Return ``executor_<exchange>`` for a per-wallet instance name.

    Args:
        name: Per-wallet instance name to resolve.

    Returns:
        ``executor_<exchange>`` template name when ``name`` matches the
        instance pattern, ``None`` otherwise. Callers using this for
        response synthesis treat None as "no parent template".
    """
    parsed = parse_executor_instance(name)
    if parsed is None:
        return None
    exchange, _ = parsed
    return f"{_EXECUTOR_PREFIX}{exchange}"
