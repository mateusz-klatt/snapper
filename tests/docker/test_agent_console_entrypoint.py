"""Contract pins for the agent-console entrypoint, codex policy, and provenance.

These tests defend the v10/v11 review outcomes mechanically: the delegate mode
must map to exactly one strict PID1 exec with no shell preflight and no user
config rewriting, the baked codex requirements policy must pin the update
check to a literal false, and every vendored license text must match the
digest recorded in the provenance table. Reintroducing a second configuration
load, a TOML rewriter, or silently editing a licensed text fails these pins.
"""

import hashlib
import re
import tomllib
from pathlib import Path
from typing import Final

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent
_CONSOLE_DIR: Final[Path] = _REPO_ROOT / "integrations" / "snapper-agent-console"
_ENTRYPOINT: Final[Path] = _CONSOLE_DIR / "entrypoint.sh"
_REQUIREMENTS: Final[Path] = _CONSOLE_DIR / "codex-requirements.toml"
_LICENSES_DIR: Final[Path] = _CONSOLE_DIR / "licenses"
_PROVENANCE: Final[Path] = _LICENSES_DIR / "PROVENANCE.md"
_LOCAL_DOCS: Final[frozenset[str]] = frozenset({"PROVENANCE.md", "THIRD-PARTY-INVENTORY.md"})
_PROVENANCE_ROW_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^\| `(?P<name>[^`]+)` \| .* \| `(?P<digest>[0-9a-f]{64})` \|$",
    re.MULTILINE,
)
_STRICT_BLOCK: Final[str] = (
    'if [ "${AGENT_CONSOLE_MODE:-idle}" = "delegate" ]; then\n'
    "    export SNAPPER_PID1_STRICT=1\n"
    "    exec python -m snapper_delegate.pid1\n"
    "fi\n"
)


def test_delegate_mode_maps_to_exactly_one_strict_pid1_exec() -> None:
    """The delegate branch is the exact strict-flag-plus-exec pair.

    Given: The committed entrypoint script,
    When: Its delegate branch is compared against the pinned block,
    Then: The block appears verbatim exactly once, so the strict flag and the
        exec cannot drift apart or gain an intermediate step.
    """
    script = _ENTRYPOINT.read_text(encoding="utf-8")
    assert script.count(_STRICT_BLOCK) == 1
    assert script.count("SNAPPER_PID1_STRICT") == 1
    assert script.count("snapper_delegate.pid1") == 1


def test_entrypoint_has_no_preflight_and_no_user_config_rewriter() -> None:
    """The entrypoint never re-validates configuration or touches user config.

    Given: The committed entrypoint script,
    When: It is scanned for the removed preflight and rewriter mechanisms,
    Then: No canonical-loader invocation, TOML parsing, or codex user-config
        path is present, so the single in-process load stays the only load.
    """
    script = _ENTRYPOINT.read_text(encoding="utf-8")
    forbidden = (
        "load_runner_configuration",
        "tomllib",
        ".codex/config.toml",
        "check_for_update_on_startup",
    )
    assert all(token not in script for token in forbidden)


def test_codex_requirements_policy_pins_update_check_to_literal_false() -> None:
    """The baked managed policy holds exactly the enforced root key.

    Given: The committed codex requirements file,
    When: It is parsed as TOML,
    Then: The update-check root key is the boolean False and is the only key,
        so the policy cannot silently widen or weaken.
    """
    parsed = tomllib.loads(_REQUIREMENTS.read_text(encoding="utf-8"))
    assert parsed == {"check_for_update_on_startup": False}


def test_every_vendored_license_text_matches_its_recorded_digest() -> None:
    """The provenance table and the committed texts agree byte-for-byte.

    Given: The provenance table rows naming each vendored text and digest,
    When: Every committed license file is hashed,
    Then: The recorded and recomputed SHA-256 values match in both directions
        (no undocumented file, no stale table row).
    """
    table = {
        match.group("name"): match.group("digest")
        for match in _PROVENANCE_ROW_PATTERN.finditer(_PROVENANCE.read_text(encoding="utf-8"))
    }
    committed = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(_LICENSES_DIR.iterdir())
        if path.name not in _LOCAL_DOCS
    }
    assert table == committed
