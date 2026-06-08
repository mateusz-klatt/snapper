"""Shared ``.env`` contract validator across all subsystems.

:class:`snapper.config.bootstrap.BootstrapSettingsLoader` only owns a
subset of ``.env`` keys (bootstrap-tier: DB_URL, MASTER_PASSWORD,
SERVER_*, ZMQ_*, telemetry, coordinator). Other subsystems read their
own keys directly via ``os.getenv`` (e.g. ``RETENTION_*``,
``SYSTEM_METRICS_*``, ``DB_METRICS_*``).

Without a unifying check, a typo like ``MASTER_PASWORD`` would silently
fall through to the defaulted ``MASTER_PASSWORD`` — exactly the safety
``extra='forbid'`` was giving us on bootstrap fields, lost the moment
we relax it to ``extra='ignore'`` so subsystem keys don't trip the
loader.

This module reconstructs that safety net at a higher layer:

  * :data:`KNOWN_ENV_KEYS` is the union of every subsystem's contract —
    :data:`BOOTSTRAP_ENV_VARS` (mirrors :class:`BootstrapSettingsLoader`
    field aliases) plus each owning module's exported ``ENV_VARS``
    frozenset.
  * :func:`validate_env_file` parses ``.env`` and rejects any key not
    in the allowlist, surfacing :mod:`difflib` suggestions so the
    typo is visible at startup rather than as a silent default.

Adding a new env var: register it on the owning module's ``ENV_VARS``
frozenset, or — for bootstrap-tier keys — add a field on
:class:`snapper.config.bootstrap.BootstrapSettingsLoader` AND add the
alias here on :data:`BOOTSTRAP_ENV_VARS`. The
``test_bootstrap_env_vars_match_loader`` drift test fails loudly if
they diverge.

:data:`BOOTSTRAP_ENV_VARS` is duplicated here as a literal frozenset
rather than introspected from the loader so this module can sit below
``bootstrap.py`` in the import graph — pydantic's ``BaseSettings``
runs the ``model_validator`` hook on every instantiation, which is the
chokepoint we need, and that hook lives in ``bootstrap.py`` and must
import this module.
"""

import difflib
from pathlib import Path

from snapper.application.db_stats.snapshotter import ENV_VARS as DB_STATS_ENV_VARS
from snapper.application.retention.policies import ENV_VARS as RETENTION_ENV_VARS
from snapper.application.system_metrics.snapshotter import ENV_VARS as SYSTEM_METRICS_ENV_VARS
from snapper.data.repository import ENV_VARS as DB_ENGINE_ENV_VARS
from snapper.messaging.infrastructure.tick_probe import ENV_VARS as TICK_PROBE_ENV_VARS
from snapper.messaging.infrastructure.trade_probe import ENV_VARS as TRADE_PROBE_ENV_VARS

__all__ = [
    "BOOTSTRAP_ENV_VARS",
    "KNOWN_ENV_KEYS",
    "UnknownEnvKeyError",
    "parse_env_file",
    "validate_env_file",
    "validate_env_keys",
]

DEFAULT_ENV_FILE: str = ".env"
SUGGESTION_CUTOFF: float = 0.7

BOOTSTRAP_ENV_VARS: frozenset[str] = frozenset(
    {
        "DB_URL",
        "MASTER_PASSWORD",
        "SERVER_HOST",
        "SERVER_PORT",
        "SERVER_RELOAD",
        "SERVER_API_ONLY",
        "PROCESS_AUTOSTART_PROFILE",
        "SERVER_PROXY_HEADERS",
        "SERVER_FORWARDED_ALLOW_IPS",
        "ZMQ_BROKER_XSUB",
        "ZMQ_BROKER_XPUB",
        "ZMQ_BROKER_BIND_XSUB",
        "ZMQ_BROKER_BIND_XPUB",
        "TELEMETRY_RECORDING_ENABLED",
        "SNAPPER_COORDINATOR_INSTANCE_ID",
        "SNAPPER_COORDINATOR_INSTANCE_COUNT",
        "SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS",
        "PAIRED_EXECUTION_GUARD_ENABLED",
    }
)
"""Aliases of every field on :class:`BootstrapSettingsLoader`.

Mirrored manually to keep this module free of a back-import. The
``test_bootstrap_env_vars_match_loader`` drift test guarantees they
stay aligned with the actual loader fields.
"""


KNOWN_ENV_KEYS: frozenset[str] = (
    BOOTSTRAP_ENV_VARS
    | DB_ENGINE_ENV_VARS
    | DB_STATS_ENV_VARS
    | RETENTION_ENV_VARS
    | SYSTEM_METRICS_ENV_VARS
    | TICK_PROBE_ENV_VARS
    | TRADE_PROBE_ENV_VARS
)
"""Union of every subsystem's env-var contract."""


class UnknownEnvKeyError(ValueError):
    """Raised when ``.env`` contains keys outside :data:`KNOWN_ENV_KEYS`.

    Attributes:
        unknown_keys: Offending keys, in first-seen order.
        suggestions: Mapping of ``unknown_key -> closest allowlist match``
            from :func:`difflib.get_close_matches`. Empty when no key
            scored above :data:`SUGGESTION_CUTOFF`.
    """

    def __init__(self, unknown_keys: list[str], suggestions: dict[str, str]) -> None:
        """Build a human-readable error listing each unknown key.

        Args:
            unknown_keys: Offending keys, in first-seen order.
            suggestions: ``unknown_key -> closest allowlist match`` map.
        """
        self.unknown_keys: list[str] = list(unknown_keys)
        self.suggestions: dict[str, str] = dict(suggestions)
        lines: list[str] = ["Unknown env keys in .env (not in shared allowlist):"]
        for key in self.unknown_keys:
            hint = suggestions.get(key)
            lines.append(f"  - {key}  (did you mean {hint}?)" if hint else f"  - {key}")
        lines.append(
            "Register new keys on the owning subsystem's ENV_VARS "
            "frozenset, or as a field on BootstrapSettingsLoader."
        )
        super().__init__("\n".join(lines))


def parse_env_file(path: Path | str) -> list[str]:
    """Parse a ``.env`` file and return its KEY names (in declaration order).

    Skips blank lines, ``#`` comment lines, and lines without ``=``.
    Empty keys (``=value`` style) are dropped. Keys are upper-cased so
    matches honour pydantic's ``case_sensitive=False`` contract.

    Args:
        path: Path to the ``.env`` file.

    Returns:
        Ordered list of upper-cased env var names. Empty list when the
        file does not exist.
    """
    keys: list[str] = []
    p = Path(path)
    if not p.exists():
        return keys
    for raw_line in p.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key = line.split("=", 1)[0].strip()
        if key:
            keys.append(key.upper())
    return keys


def validate_env_keys(keys: list[str], allowed: frozenset[str] = KNOWN_ENV_KEYS) -> None:
    """Validate a list of env var names against the allowlist.

    Args:
        keys: Env var names to validate (case is normalised by
            :func:`parse_env_file`; callers passing raw input should
            upper-case themselves).
        allowed: Allowlist of permitted keys. Defaults to
            :data:`KNOWN_ENV_KEYS`.

    Raises:
        UnknownEnvKeyError: If any key falls outside ``allowed``. The
            exception lists every offender with a :mod:`difflib`
            suggestion when a close match (>= :data:`SUGGESTION_CUTOFF`)
            exists.
    """
    unknown: list[str] = []
    seen: set[str] = set()
    for key in keys:
        if key in allowed or key in seen:
            continue
        seen.add(key)
        unknown.append(key)
    if not unknown:
        return
    allowed_list: list[str] = sorted(allowed)
    suggestions: dict[str, str] = {}
    for key in unknown:
        matches = difflib.get_close_matches(key, allowed_list, n=1, cutoff=SUGGESTION_CUTOFF)
        if matches:
            suggestions[key] = matches[0]
    raise UnknownEnvKeyError(unknown, suggestions)


def validate_env_file(path: Path | str = DEFAULT_ENV_FILE) -> None:
    """Validate every key in ``path`` against :data:`KNOWN_ENV_KEYS`.

    No-op when ``path`` does not exist (e.g. fresh clone before
    ``cp .env.example .env``, or containers running purely on
    process-injected env vars).

    Args:
        path: Path to the ``.env`` file to validate. Defaults to
            :data:`DEFAULT_ENV_FILE` (``.env``) — the same file pydantic
            reads via :class:`pydantic_settings.SettingsConfigDict`.

    Raises:
        UnknownEnvKeyError: If the file contains keys outside the
            shared allowlist.
    """
    keys = parse_env_file(path)
    if keys:
        validate_env_keys(keys)
