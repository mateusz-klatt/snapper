"""Tests for the shared ``.env`` contract validator.

Verifies the safety net that replaces ``extra='forbid'`` on
:class:`BootstrapSettingsLoader` — see
``src/snapper/config/env_contract.py`` for the rationale.

Coverage matrix:

  * :data:`KNOWN_ENV_KEYS` composition (every owner contributes).
  * :func:`parse_env_file` skips noise (blanks, comments, no-equals).
  * :func:`validate_env_keys` catches typos with :mod:`difflib`
    suggestions, deduplicates repeats, and accepts the empty path.
  * :func:`validate_env_file` is a no-op on missing files.
  * ``.env.example`` round-trips through validation (the regression
    bait that motivated this contract — the operator-facing template
    must not contain unregistered keys).
  * :class:`BootstrapSettingsLoader` instantiation triggers validation
    via the ``model_validator(mode='before')`` hook.
"""

from pathlib import Path

import pytest
from pydantic import ValidationError

from snapper.application.ai_research.trigger import ENV_VARS as AI_RESEARCH_TRIGGER_ENV_VARS
from snapper.application.ai_review.maintenance import ENV_VARS as AI_REVIEW_MAINTENANCE_ENV_VARS
from snapper.application.data_quality.trade_integrity import ENV_VARS as TRADE_INTEGRITY_ENV_VARS
from snapper.application.db_stats.snapshotter import ENV_VARS as DB_STATS_ENV_VARS
from snapper.application.notify.portfolio_drift_recovery import (
    ENV_VARS as PORTFOLIO_DRIFT_RECOVERY_ENV_VARS,
)
from snapper.application.retention.policies import ENV_VARS as RETENTION_ENV_VARS
from snapper.application.system_metrics.snapshotter import ENV_VARS as SYSTEM_METRICS_ENV_VARS
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.config.delegate_profile import ENV_VARS as DELEGATE_PROFILE_ENV_VARS
from snapper.config.env_contract import BOOTSTRAP_ENV_VARS
from snapper.config.env_contract import KNOWN_ENV_KEYS
from snapper.config.env_contract import UnknownEnvKeyError
from snapper.config.env_contract import parse_env_file
from snapper.config.env_contract import validate_env_file
from snapper.config.env_contract import validate_env_keys
from snapper.data.repository import ENV_VARS as DB_ENGINE_ENV_VARS


class TestKnownEnvKeys:
    """Allowlist composition: every subsystem contributes its own keys."""

    def test_bootstrap_env_vars_match_loader(self) -> None:
        """``BOOTSTRAP_ENV_VARS`` mirrors every alias on the loader.

        The mirror is duplicated manually in ``env_contract`` to avoid
        a back-import (``env_contract`` must sit below ``bootstrap`` in
        the import graph). This drift test guarantees the two stay in
        sync; the moment a new bootstrap field is added, this test
        fails and the contributor is told exactly which key to add to
        ``BOOTSTRAP_ENV_VARS``.
        """
        actual: set[str] = set()
        for name, field in BootstrapSettingsLoader.model_fields.items():
            alias = field.alias
            actual.add(alias.upper() if alias else name.upper())
        assert actual == BOOTSTRAP_ENV_VARS

    def test_includes_bootstrap_aliases(self) -> None:
        """Every aliased field on bootstrap appears in the allowlist."""
        assert "DB_URL" in KNOWN_ENV_KEYS
        assert "SNAPPER_ENV" in KNOWN_ENV_KEYS
        assert "MASTER_PASSWORD" in KNOWN_ENV_KEYS
        assert "SERVER_PORT" in KNOWN_ENV_KEYS
        assert "SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS" in KNOWN_ENV_KEYS
        assert "MCP_OAUTH_ENABLED" in KNOWN_ENV_KEYS
        assert "MCP_PUBLIC_RESOURCE_URL" in KNOWN_ENV_KEYS

    def test_includes_delegate_profile_keys(self) -> None:
        """The optional Compose profile contributes only its host inputs."""
        assert DELEGATE_PROFILE_ENV_VARS.issubset(KNOWN_ENV_KEYS)
        assert {
            "SNAPPER_DELEGATE_GEMINI_BASE_URL",
            "SNAPPER_DELEGATE_GEMINI_MODEL",
            "SNAPPER_DELEGATE_KIMI_BASE_URL",
            "SNAPPER_DELEGATE_KIMI_MODEL",
        }.issubset(DELEGATE_PROFILE_ENV_VARS)

    def test_includes_system_metrics_keys(self) -> None:
        """The system-metrics subsystem contributes its ENV_VARS."""
        assert SYSTEM_METRICS_ENV_VARS.issubset(KNOWN_ENV_KEYS)
        assert "SYSTEM_METRICS_INTERVAL_SECONDS" in KNOWN_ENV_KEYS

    def test_includes_ai_research_trigger_key(self) -> None:
        """The AI-research trigger contributes its cadence key."""
        assert AI_RESEARCH_TRIGGER_ENV_VARS.issubset(KNOWN_ENV_KEYS)
        assert "AI_RESEARCH_TRIGGER_INTERVAL_SECONDS" in KNOWN_ENV_KEYS

    def test_includes_ai_review_maintenance_key(self) -> None:
        """The AI-review maintenance driver contributes its cadence key."""
        assert AI_REVIEW_MAINTENANCE_ENV_VARS.issubset(KNOWN_ENV_KEYS)
        assert "AI_REVIEW_MAINTENANCE_INTERVAL_SECONDS" in KNOWN_ENV_KEYS

    def test_includes_trade_integrity_monitor_key(self) -> None:
        """The trade-integrity watchdog contributes only its cadence key."""
        assert TRADE_INTEGRITY_ENV_VARS.issubset(KNOWN_ENV_KEYS)
        assert frozenset({"TRADE_INTEGRITY_MONITOR_INTERVAL_SECONDS"}) == TRADE_INTEGRITY_ENV_VARS

    def test_includes_retention_keys(self) -> None:
        """The retention subsystem contributes its ENV_VARS."""
        assert RETENTION_ENV_VARS.issubset(KNOWN_ENV_KEYS)
        assert "RETENTION_DISABLED" in KNOWN_ENV_KEYS

    def test_includes_portfolio_drift_recovery_key(self) -> None:
        """The drift recovery subsystem contributes its interval key."""
        assert PORTFOLIO_DRIFT_RECOVERY_ENV_VARS.issubset(KNOWN_ENV_KEYS)
        assert "PORTFOLIO_DRIFT_RECOVERY_INTERVAL_SECONDS" in KNOWN_ENV_KEYS

    def test_includes_db_stats_keys(self) -> None:
        """The db_stats subsystem contributes its ENV_VARS."""
        assert DB_STATS_ENV_VARS.issubset(KNOWN_ENV_KEYS)
        assert "DB_METRICS_DISABLED" in KNOWN_ENV_KEYS

    def test_includes_db_engine_pool_keys(self) -> None:
        """The repository contributes its per-process pool-clamp keys."""
        assert DB_ENGINE_ENV_VARS.issubset(KNOWN_ENV_KEYS)
        assert "DB_POOL_SIZE" in KNOWN_ENV_KEYS
        assert "DB_MAX_OVERFLOW" in KNOWN_ENV_KEYS


class TestParseEnvFile:
    """Parser semantics: skip noise, normalise case, preserve order."""

    def test_returns_empty_for_missing_file(self, tmp_path: Path) -> None:
        """A missing path yields an empty list rather than raising."""
        assert parse_env_file(tmp_path / "missing.env") == []

    def test_skips_blank_and_comment_lines(self, tmp_path: Path) -> None:
        """Blanks and ``#``-comments are ignored."""
        env = tmp_path / ".env"
        env.write_text("# header comment\n\nDB_URL=sqlite\n   \n# trailing\n")
        assert parse_env_file(env) == ["DB_URL"]

    def test_skips_lines_without_equals(self, tmp_path: Path) -> None:
        """Lines without ``=`` are dropped (not key candidates)."""
        env = tmp_path / ".env"
        env.write_text("DB_URL=sqlite\nNOT_A_KEY_LINE\nSERVER_PORT=8000\n")
        assert parse_env_file(env) == ["DB_URL", "SERVER_PORT"]

    def test_skips_empty_keys(self, tmp_path: Path) -> None:
        """Lines like ``=value`` produce no key."""
        env = tmp_path / ".env"
        env.write_text("=orphan_value\nDB_URL=sqlite\n")
        assert parse_env_file(env) == ["DB_URL"]

    def test_uppercases_keys(self, tmp_path: Path) -> None:
        """Keys are returned uppercase to honour case_sensitive=False."""
        env = tmp_path / ".env"
        env.write_text("db_url=sqlite\nServer_Port=8000\n")
        assert parse_env_file(env) == ["DB_URL", "SERVER_PORT"]

    def test_accepts_string_path(self, tmp_path: Path) -> None:
        """A ``str`` path works the same as a :class:`Path`."""
        env = tmp_path / ".env"
        env.write_text("DB_URL=sqlite\n")
        assert parse_env_file(str(env)) == ["DB_URL"]


class TestValidateEnvKeys:
    """Allowlist enforcement with :mod:`difflib` suggestions."""

    def test_accepts_empty_list(self) -> None:
        """An empty list never raises."""
        validate_env_keys([])

    def test_accepts_all_known_keys(self) -> None:
        """Every key in the allowlist passes."""
        validate_env_keys(sorted(KNOWN_ENV_KEYS))

    def test_raises_on_typo_with_suggestion(self) -> None:
        """A close typo surfaces the intended key as a suggestion."""
        with pytest.raises(UnknownEnvKeyError) as exc_info:
            validate_env_keys(["MASTER_PASWORD"])
        err = exc_info.value
        assert err.unknown_keys == ["MASTER_PASWORD"]
        assert err.suggestions.get("MASTER_PASWORD") == "MASTER_PASSWORD"
        assert "did you mean MASTER_PASSWORD" in str(err)

    def test_raises_on_unknown_key_without_close_match(self) -> None:
        """A wildly unrelated key still raises, with no suggestion line."""
        with pytest.raises(UnknownEnvKeyError) as exc_info:
            validate_env_keys(["TOTAL_GARBAGE_XYZ"])
        err = exc_info.value
        assert err.unknown_keys == ["TOTAL_GARBAGE_XYZ"]
        assert "TOTAL_GARBAGE_XYZ" not in err.suggestions
        assert "TOTAL_GARBAGE_XYZ" in str(err)
        assert "did you mean" not in str(err)

    def test_deduplicates_repeated_unknowns(self) -> None:
        """Repeated occurrences of the same unknown key collapse."""
        with pytest.raises(UnknownEnvKeyError) as exc_info:
            validate_env_keys(["FOO_BAR", "FOO_BAR", "FOO_BAR"])
        assert exc_info.value.unknown_keys == ["FOO_BAR"]

    def test_accepts_custom_allowlist(self) -> None:
        """A caller-supplied allowlist overrides the default."""
        validate_env_keys(["X", "Y"], allowed=frozenset({"X", "Y"}))

    def test_reports_every_offender(self) -> None:
        """All unknown keys are reported in a single error."""
        with pytest.raises(UnknownEnvKeyError) as exc_info:
            validate_env_keys(["ALPHA_KEY", "BETA_KEY"])
        assert set(exc_info.value.unknown_keys) == {"ALPHA_KEY", "BETA_KEY"}


class TestValidateEnvFile:
    """End-to-end: parse + validate against the shared allowlist."""

    def test_noop_when_file_missing(self, tmp_path: Path) -> None:
        """A missing path is silently skipped (no raise)."""
        validate_env_file(tmp_path / "absent.env")

    def test_accepts_env_with_only_known_keys(self, tmp_path: Path) -> None:
        """A file containing only allowlisted keys validates."""
        env = tmp_path / ".env"
        env.write_text("DB_URL=sqlite\nSERVER_PORT=8000\nRETENTION_DISABLED=false\n")
        validate_env_file(env)

    def test_rejects_typo_via_file(self, tmp_path: Path) -> None:
        """A typo in the file raises ``UnknownEnvKeyError`` with suggestion."""
        env = tmp_path / ".env"
        env.write_text("MASTER_PASWORD=oops\n")
        with pytest.raises(UnknownEnvKeyError) as exc_info:
            validate_env_file(env)
        assert exc_info.value.suggestions.get("MASTER_PASWORD") == "MASTER_PASSWORD"


class TestRealEnvExample:
    """The shipped ``.env.example`` must round-trip through the contract.

    Regression bait: the original Docker-healthcheck failure happened
    because ``.env.example`` shipped keys that bootstrap's
    ``extra='forbid'`` rejected. This test fails the moment somebody
    adds a new key to ``.env.example`` without registering it on the
    owning subsystem's ``ENV_VARS`` frozenset.
    """

    def test_env_example_validates_against_contract(self) -> None:
        """``.env.example`` at the repo root contains only allowlisted keys."""
        repo_root = Path(__file__).resolve().parents[2]
        env_example = repo_root / ".env.example"
        assert env_example.exists(), "Expected .env.example at repo root"
        validate_env_file(env_example)


class TestBootstrapTriggersValidation:
    """``BootstrapSettingsLoader()`` invokes the validator via model_validator."""

    def test_loader_raises_when_env_file_has_typo(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pointing the loader at a typo'd ``.env`` raises before parsing.

        Pydantic wraps the underlying :class:`UnknownEnvKeyError` in a
        :class:`pydantic.ValidationError`; the offending key and the
        :mod:`difflib` suggestion both survive into the error message
        so operators see the typo at startup.
        """
        bad_env = tmp_path / ".env"
        bad_env.write_text("MASTER_PASWORD=oops\n")
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ValidationError) as exc_info:
            BootstrapSettingsLoader()
        message = str(exc_info.value)
        assert "MASTER_PASWORD" in message
        assert "did you mean MASTER_PASSWORD" in message

    def test_loader_succeeds_when_env_file_is_clean(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A clean ``.env`` lets the loader build without raising.

        The conftest seeds ``DB_URL`` in :data:`os.environ`, which beats
        any value in the ``.env`` file, so this test only asserts that
        construction succeeds rather than reading a field back.
        """
        good_env = tmp_path / ".env"
        good_env.write_text("SERVER_PORT=8000\nRETENTION_DISABLED=false\n")
        monkeypatch.chdir(tmp_path)
        loader = BootstrapSettingsLoader()
        assert isinstance(loader, BootstrapSettingsLoader)

    def test_loader_succeeds_when_env_file_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No ``.env`` on disk skips validation and falls back to defaults."""
        monkeypatch.chdir(tmp_path)
        loader = BootstrapSettingsLoader()
        assert isinstance(loader, BootstrapSettingsLoader)
