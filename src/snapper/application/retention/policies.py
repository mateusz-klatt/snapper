"""Declarative retention policies for the periodic archive + purge loop.

Each :class:`RetentionPolicy` says: *"for table T, keep at most
``retain_days`` of rows in the DB. On every scheduler tick, archive +
purge older rows in batches of at most ``backlog_lookback_days`` days."*

Policies are validated at module import: every ``policy.table`` MUST be
a key in :data:`snapper.data.archiver.EVENT_TABLES`. v1 supports event
tables only; ``StateArchiver`` tables fail this validation, as does
``executions`` (the archiver refuses to purge it).

Three operator-controlled env vars (read directly via
``os.environ.get``):

* ``RETENTION_INTERVAL_SECONDS`` (default 3600) — scheduler loop period.
* ``RETENTION_DISABLED`` (default ``false``) — disables the loop + eager
  run + the metrics route surface.
* ``RETENTION_DRY_RUN`` (default ``false``) — forces ``purge=False`` on
  every archiver call regardless of the policy.
"""

from dataclasses import dataclass

from snapper.data.archiver import EVENT_TABLES

DEFAULT_INTERVAL_SECONDS = 3600.0
_INTERVAL_ENV_VAR = "RETENTION_INTERVAL_SECONDS"
_DISABLED_ENV_VAR = "RETENTION_DISABLED"
_DRY_RUN_ENV_VAR = "RETENTION_DRY_RUN"
_OUTPUT_DIR_ENV_VAR = "RETENTION_OUTPUT_DIR"
_DEFAULT_OUTPUT_DIR = "data"
_TRUTHY_ENV_VALUES = frozenset({"1", "true", "yes"})
_PURGE_REFUSED_TABLE = "executions"
"""Event table the archiver refuses to purge, hence ineligible for retention."""
ENV_VARS: frozenset[str] = frozenset(
    {_INTERVAL_ENV_VAR, _DISABLED_ENV_VAR, _DRY_RUN_ENV_VAR, _OUTPUT_DIR_ENV_VAR}
)
"""Public allowlist of env vars retention reads via ``os.environ``.

Consumed by :mod:`snapper.config.env_contract` to validate ``.env`` keys
against the union of every subsystem's contract.
"""


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Per-table retention rule for the periodic archive + purge loop.

    Attributes:
        table: Event-table name; MUST be a key in
            :data:`snapper.data.archiver.EVENT_TABLES`.
        retain_days: Maximum DB residency in whole UTC days. Rows older
            than ``today_utc - retain_days`` are eligible for archive +
            purge.
        backlog_lookback_days: Per-tick cap on days walked backward from
            the eligible-day boundary. Steady state after backlog drains
            processes one new day per tick.
    """

    table: str
    retain_days: int
    backlog_lookback_days: int


RETENTION_POLICIES: tuple[RetentionPolicy, ...] = (
    RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30),
)


def validate_policies(policies: tuple[RetentionPolicy, ...]) -> None:
    """Raise :class:`ValueError` if any policy targets an unsupported table.

    v1 supports the keys of
    :data:`snapper.data.archiver.EVENT_TABLES` only; ``StateArchiver``
    tables are not supported and are rejected here.

    ``executions`` is rejected additionally: the retention loop calls
    the archiver with ``purge=not dry_run``, and the archiver refuses
    execution purge
    (:class:`snapper.data.archiver.ExecutionPurgeUnsupportedError`)
    because its protection reads and delete cannot be serialized against
    the spot reconciliation anchor writer. Rejecting at import time
    fails a misconfiguration loudly at startup rather than once per
    scheduler tick, and keeps the refusal from being silently reopened
    by adding a policy.

    Args:
        policies: Tuple of :class:`RetentionPolicy` to validate.

    Raises:
        ValueError: When any policy's ``table`` is not in
            :data:`EVENT_TABLES`, or is ``executions``.
    """
    for policy in policies:
        if policy.table not in EVENT_TABLES:
            raise ValueError(
                f"RetentionPolicy table {policy.table!r} not in EVENT_TABLES; "
                "v1 supports event tables only."
            )
        if policy.table == _PURGE_REFUSED_TABLE:
            raise ValueError(
                f"RetentionPolicy table {policy.table!r} is not eligible for "
                "retention: the archiver refuses execution purge because it "
                "cannot serialize its protection reads and delete against the "
                "spot reconciliation anchor writer."
            )


validate_policies(RETENTION_POLICIES)


def resolve_interval(env_value: str | None) -> float:
    """Coerce ``RETENTION_INTERVAL_SECONDS`` to a positive float seconds.

    Empty / unset / unparseable / non-positive values fall back to the
    default :data:`DEFAULT_INTERVAL_SECONDS` (3600).

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        Loop interval in seconds.
    """
    if env_value is None or env_value.strip() == "":
        return DEFAULT_INTERVAL_SECONDS
    try:
        value = float(env_value)
    except ValueError:
        return DEFAULT_INTERVAL_SECONDS
    if value <= 0:
        return DEFAULT_INTERVAL_SECONDS
    return value


def resolve_disabled(env_value: str | None) -> bool:
    """Parse ``RETENTION_DISABLED`` as a boolean.

    Truthy values: ``"1"``, ``"true"``, ``"yes"`` (case-insensitive).
    Everything else (including ``None``) is ``False``.

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        ``True`` iff the scheduler loop should be disabled.
    """
    if env_value is None:
        return False
    return env_value.strip().lower() in _TRUTHY_ENV_VALUES


def resolve_dry_run(env_value: str | None) -> bool:
    """Parse ``RETENTION_DRY_RUN`` as a boolean.

    Truthy values: ``"1"``, ``"true"``, ``"yes"`` (case-insensitive).
    Everything else (including ``None``) is ``False``.

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        ``True`` iff the scheduler should force ``purge=False`` on every
        archiver call.
    """
    if env_value is None:
        return False
    return env_value.strip().lower() in _TRUTHY_ENV_VALUES


def resolve_output_dir(env_value: str | None) -> str:
    """Coerce ``RETENTION_OUTPUT_DIR`` to a base path string.

    Empty / unset values fall back to the default ``"data"`` (matches
    the CLI default at ``snapper archive``).

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        Base directory path string.
    """
    if env_value is None or env_value.strip() == "":
        return _DEFAULT_OUTPUT_DIR
    return env_value.strip()
