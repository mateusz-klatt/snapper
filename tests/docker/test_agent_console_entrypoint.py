"""Contract pins for the agent-console entrypoint, codex policy, and provenance.

These tests defend the v10/v11 review outcomes mechanically: the delegate mode
must map to exactly one strict PID1 exec with no shell preflight and no user
config rewriting, the baked codex requirements policy must pin the update
check to a literal false, and every vendored license text must match the
digest recorded in the provenance table. Reintroducing a second configuration
load, a TOML rewriter, or silently editing a licensed text fails these pins.

The token blacklists below are a cheap first line only; they are trivially
bypassed by renaming a helper or by assembling the codex user-config path from
fragments. The authoritative pin is therefore behavioural: the real
``entrypoint.sh`` is executed against a throwaway ``$HOME`` and a fake ``PATH``
whose ``python``/``sleep`` shims append one audit row per invocation to a
registry file (a file, not a pipe, because the script ``exec``s and so replaces
itself). Byte-comparing the codex user config across that run kills any
rewriter regardless of what it is called or how it spells the path, and
comparing the whole registry against an exact expected tuple kills any extra
preflight process and any drift in the strict-mode flag.
"""

import hashlib
import re
import shlex
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Final

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent
_CONSOLE_DIR: Final[Path] = _REPO_ROOT / "integrations" / "snapper-agent-console"
_ENTRYPOINT: Final[Path] = _CONSOLE_DIR / "entrypoint.sh"
_DOCKERFILE: Final[Path] = _CONSOLE_DIR / "Dockerfile"
_REQUIREMENTS: Final[Path] = _CONSOLE_DIR / "codex-requirements.toml"
_LICENSES_DIR: Final[Path] = _CONSOLE_DIR / "licenses"
_PROVENANCE: Final[Path] = _LICENSES_DIR / "PROVENANCE.md"
_LOCAL_DOCS: Final[frozenset[str]] = frozenset({"PROVENANCE.md", "THIRD-PARTY-INVENTORY.md"})
_PROVENANCE_ROW_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^\| `(?P<name>[^`]+)` \| .* \| `(?P<digest>[0-9a-f]{64})` \|$",
    re.MULTILINE,
)
_REQUIREMENTS_COPY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^COPY[ \t]+snapper-agent-console/codex-requirements\.toml[ \t]+"
    r"/etc/codex/requirements\.toml[ \t]*$",
    re.MULTILINE,
)
_STRICT_BLOCK: Final[str] = (
    'if [ "${AGENT_CONSOLE_MODE:-idle}" = "delegate" ]; then\n'
    "    export SNAPPER_PID1_STRICT=1\n"
    "    exec python -m snapper_delegate.pid1\n"
    "fi\n"
)

_SYSTEM_PATH: Final[str] = "/usr/local/bin:/usr/bin:/bin"
_SHIMMED_PROGRAMS: Final[tuple[str, ...]] = ("python", "python3", "sleep")
_RUN_TIMEOUT_SECONDS: Final[float] = 15.0
_USER_CONFIG_RELATIVE: Final[str] = ".codex/config.toml"
_USER_CONFIG_TEXT: Final[str] = '''check_for_update_on_startup = true

[console]
banner = """
check_for_update_on_startup = true
The line above is DATA inside a multiline basic string, not a root key.
"""
profiles = [
    "alpha",
    "beta",
]
'''


@dataclass(frozen=True, slots=True)
class _Invocation:
    """One recorded execution of a shimmed interpreter or sleep binary."""

    program: str
    arguments: str
    strict_flag: str
    console_mode: str


@dataclass(frozen=True, slots=True)
class _HarnessRun:
    """The observable outcome of executing the entrypoint under the harness."""

    returncode: int
    invocations: tuple[_Invocation, ...]
    config_before: bytes
    config_after: bytes


def _recorder_source(registry: Path) -> str:
    """Build a shim that appends one tab-separated audit row and exits cleanly.

    The shim reports its own basename, its joined argv, and the two environment
    values the entrypoint contract is responsible for, so a single template
    serves ``python``, ``python3``, and ``sleep`` alike.
    """
    fields = '"${0##*/}" "$*" "${SNAPPER_PID1_STRICT-}" "${AGENT_CONSOLE_MODE-}"'
    return (
        "#!/bin/sh\n"
        f"printf '%s\\t%s\\t%s\\t%s\\n' {fields} >> {shlex.quote(str(registry))}\n"
        "exit 0\n"
    )


def _parse_invocation(row: str) -> _Invocation:
    """Turn one registry row back into a structured invocation record."""
    program, arguments, strict_flag, console_mode = row.split("\t")
    return _Invocation(
        program=program,
        arguments=arguments,
        strict_flag=strict_flag,
        console_mode=console_mode,
    )


def _run_entrypoint(script: Path, workspace: Path, console_mode: str | None) -> _HarnessRun:
    """Execute one entrypoint script against an isolated HOME and shimmed PATH.

    A non-trivial codex user config is planted and snapshotted first, then the
    script runs with only HOME, PATH, and the optional console mode in its
    environment. Shims for ``python``/``python3``/``sleep`` sit ahead of the
    system path so every exec target is recorded and returns immediately,
    which keeps the idle branch deterministic instead of racing a timeout.
    """
    home = workspace / "home"
    user_config = home / _USER_CONFIG_RELATIVE
    user_config.parent.mkdir(parents=True)
    user_config.write_text(_USER_CONFIG_TEXT, encoding="utf-8")
    config_before = user_config.read_bytes()

    shim_dir = workspace / "shims"
    shim_dir.mkdir()
    registry = workspace / "invocations.tsv"
    registry.touch()
    source = _recorder_source(registry)
    for program in _SHIMMED_PROGRAMS:
        shim = shim_dir / program
        shim.write_text(source, encoding="utf-8")
        shim.chmod(0o755)

    environment = {"HOME": str(home), "PATH": f"{shim_dir}:{_SYSTEM_PATH}"}
    if console_mode is not None:
        environment["AGENT_CONSOLE_MODE"] = console_mode
    completed = subprocess.run(
        ["/bin/sh", str(script)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=_RUN_TIMEOUT_SECONDS,
        check=False,
    )
    return _HarnessRun(
        returncode=completed.returncode,
        invocations=tuple(
            _parse_invocation(row)
            for row in registry.read_text(encoding="utf-8").splitlines()
            if row
        ),
        config_before=config_before,
        config_after=user_config.read_bytes(),
    )


def test_delegate_mode_execs_strict_pid1_and_never_rewrites_user_config(tmp_path: Path) -> None:
    """Running the real script in delegate mode yields one strict PID1 exec.

    Given: The committed entrypoint, an isolated HOME holding a non-trivial
        codex user config, and PATH shims recording every exec,
    When: The script is executed with AGENT_CONSOLE_MODE=delegate,
    Then: The user config is byte-identical afterwards and the recorded
        process list is exactly one strict-flagged `python -m
        snapper_delegate.pid1`, so no rewriter and no preflight can hide behind
        a renamed helper or a reassembled path.
    """
    run = _run_entrypoint(_ENTRYPOINT, tmp_path, "delegate")
    assert run.returncode == 0
    assert run.config_after == run.config_before
    assert run.invocations == (
        _Invocation(
            program="python",
            arguments="-m snapper_delegate.pid1",
            strict_flag="1",
            console_mode="delegate",
        ),
    )


def test_idle_mode_never_invokes_python_and_never_rewrites_user_config(tmp_path: Path) -> None:
    """Running the real script without a mode parks it without any interpreter.

    Given: The committed entrypoint, an isolated HOME holding a non-trivial
        codex user config, and PATH shims recording every exec,
    When: The script is executed with AGENT_CONSOLE_MODE unset,
    Then: The user config is byte-identical afterwards and the only recorded
        process is the parking `sleep infinity`, so the idle branch neither
        starts a delegate nor performs configuration work.
    """
    run = _run_entrypoint(_ENTRYPOINT, tmp_path, None)
    assert run.returncode == 0
    assert run.config_after == run.config_before
    assert run.invocations == (
        _Invocation(
            program="sleep",
            arguments="infinity",
            strict_flag="",
            console_mode="",
        ),
    )
    assert all(invocation.program not in {"python", "python3"} for invocation in run.invocations)


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


def test_dockerfile_bakes_the_requirements_policy_at_the_managed_path() -> None:
    """The parsed policy file actually reaches the codex managed location.

    Given: The committed agent-console Dockerfile,
    When: Its COPY instructions are matched against the requirements pin,
    Then: Exactly one instruction copies the source policy to
        /etc/codex/requirements.toml, so deleting or retargeting that COPY
        cannot leave the policy test green against an unbaked file.
    """
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    assert len(_REQUIREMENTS_COPY_PATTERN.findall(dockerfile)) == 1
    assert dockerfile.count("/etc/codex/requirements.toml") == 1


def test_every_vendored_license_text_matches_its_recorded_digest() -> None:
    """The provenance table and the committed texts agree byte-for-byte.

    Given: The provenance table rows naming each vendored text and digest,
    When: Every committed license file is hashed,
    Then: The recorded and recomputed SHA-256 values match in both directions
        (no undocumented file, no stale table row).
    """
    rows = [
        (match.group("name"), match.group("digest"))
        for match in _PROVENANCE_ROW_PATTERN.finditer(_PROVENANCE.read_text(encoding="utf-8"))
    ]
    names = [name for name, _ in rows]
    assert len(names) == len(set(names)), f"duplicate provenance rows: {sorted(names)}"
    committed = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(_LICENSES_DIR.iterdir())
        if path.name not in _LOCAL_DOCS
    }
    assert dict(rows) == committed
