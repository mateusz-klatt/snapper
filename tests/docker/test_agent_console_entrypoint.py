"""Contract pins for the agent-console entrypoint, codex policy, and provenance.

These tests defend the v10/v11 review outcomes mechanically: the delegate mode
must map to exactly one strict PID1 exec with no shell preflight and no user
config rewriting, the baked codex requirements policy must pin the update
check to a literal false, and every vendored license text must match the
digest recorded in the provenance table. Reintroducing a second configuration
load, a TOML rewriter, or silently editing a licensed text fails these pins.

The token blacklists below are a cheap first line only; they are trivially
bypassed by renaming a helper or by assembling the codex user-config path from
fragments. Two behavioural harnesses back them up. They prove different things,
and neither claim may be stretched into the other.

The PATH harness runs the real ``entrypoint.sh`` against a throwaway ``$HOME``
and a fake ``PATH`` whose ``python``/``python3``/``sleep`` shims append one
audit row per invocation to a registry file (a file, not a pipe, because the
script ``exec``s and so replaces itself). What it proves is narrow but sharp:
of the programs reached *through a PATH lookup under those three names*,
exactly one runs, and it is the strict-flagged delegate. It is blind by
construction to anything started by absolute path, such as
``/usr/local/bin/python -c ...``, ``/bin/true``, or ``/bin/sh -c ...``, so on
its own it cannot support a claim about the exact process list.

The strace harness closes exactly that hole. It runs the same script under
``strace -f -e trace=execve`` with the transcript written to a file (again
because the script ``exec``s), follows every descendant, and compares the
*complete* list of successfully executed programs, resolved path plus argv,
against an exact expected tuple. Since ``execve`` is the only way a POSIX
process can start a program, an absolute-path preflight is recorded there
whether or not PATH was consulted and whether it runs before, between, or after
the mode branches.

Neither harness sees work that never reaches ``execve``: a rewriter written
purely with shell builtins and a redirection performs no exec at all. That case
is covered instead by byte-comparing the codex user config across every run,
which both harnesses assert and which is indifferent to how the write was
spelled. Neither harness observes the image; they pin the script's behaviour
under the host ``/bin/sh``, and the Dockerfile pins are what tie that script to
the image.
"""

import hashlib
import re
import shlex
import shutil
import subprocess
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pytest

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
_SHELL: Final[str] = "/bin/sh"
_SHIMMED_PROGRAMS: Final[tuple[str, ...]] = ("python", "python3", "sleep")
_KIMI_STATE_RELATIVE: Final[str] = ".kimi-code"
_RUN_TIMEOUT_SECONDS: Final[float] = 15.0
_STRACE_PATH: Final[str | None] = shutil.which("strace")
_TRACE_STRING_LIMIT: Final[str] = "4096"
_EXECVE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r'^(?:\d+\s+)?execve\("(?P<path>[^"]*)", \[(?P<argv>.*)\], 0x[0-9a-f]+[^)]*\) = 0$',
    re.MULTILINE,
)
_TRACE_STRING_PATTERN: Final[re.Pattern[str]] = re.compile(r'"((?:[^"\\]|\\.)*)"')
_TRACE_ESCAPE_PATTERN: Final[re.Pattern[str]] = re.compile(r"\\(.)")
_TRACE_ESCAPES: Final[dict[str, str]] = {
    "\\": "\\",
    '"': '"',
    "n": "\n",
    "r": "\r",
    "t": "\t",
}
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


@dataclass(frozen=True, slots=True)
class _Execution:
    """One program the kernel actually started, as reported by ``execve``."""

    program: str
    argv: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _Sandbox:
    """The throwaway HOME, shim directory, and PATH one entrypoint run uses."""

    home: Path
    user_config: Path
    config_before: bytes
    shim_dir: Path
    registry: Path
    search_path: str


@dataclass(frozen=True, slots=True)
class _TracedRun:
    """The observable outcome of executing the entrypoint under strace."""

    returncode: int
    executions: tuple[_Execution, ...]
    trace_text: str
    sandbox: _Sandbox
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


def _build_sandbox(workspace: Path) -> _Sandbox:
    """Plant a throwaway HOME, a codex user config, and the recording shims.

    A non-trivial codex user config is written and snapshotted so any rewriter
    shows up as a byte difference. Shims for ``python``/``python3``/``sleep``
    sit ahead of the system path so those exec targets are recorded and return
    immediately, which keeps the idle branch deterministic instead of racing a
    timeout.
    """
    home = workspace / "home"
    user_config = home / _USER_CONFIG_RELATIVE
    user_config.parent.mkdir(parents=True)
    user_config.write_text(_USER_CONFIG_TEXT, encoding="utf-8")

    shim_dir = workspace / "shims"
    shim_dir.mkdir()
    registry = workspace / "invocations.tsv"
    registry.touch()
    source = _recorder_source(registry)
    for program in _SHIMMED_PROGRAMS:
        shim = shim_dir / program
        shim.write_text(source, encoding="utf-8")
        shim.chmod(0o755)

    return _Sandbox(
        home=home,
        user_config=user_config,
        config_before=user_config.read_bytes(),
        shim_dir=shim_dir,
        registry=registry,
        search_path=f"{shim_dir}:{_SYSTEM_PATH}",
    )


def _sandbox_environment(sandbox: _Sandbox, console_mode: str | None) -> dict[str, str]:
    """Build the entire environment the traced script is allowed to observe."""
    environment = {"HOME": str(sandbox.home), "PATH": sandbox.search_path}
    if console_mode is not None:
        environment["AGENT_CONSOLE_MODE"] = console_mode
    return environment


def _resolve_on_path(program: str, sandbox: _Sandbox) -> str:
    """Resolve a program the way the sandboxed shell resolves it.

    Expected executions name absolute paths because that is what ``execve``
    reports, and the absolute path of a system tool such as ``mkdir`` differs
    between distributions. Resolving through the sandbox PATH keeps the pin
    exact without hard-coding one host's layout.
    """
    located = shutil.which(program, path=sandbox.search_path)
    assert located is not None, f"{program} is not reachable on the sandbox PATH"
    return located


def _run_entrypoint(script: Path, workspace: Path, console_mode: str | None) -> _HarnessRun:
    """Execute one entrypoint script against an isolated HOME and shimmed PATH.

    Only programs found by a PATH lookup under a shimmed name are recorded, so
    this run answers "which of python/python3/sleep ran, with what argv"; it
    deliberately does not answer "which processes ran". The strace runner below
    answers the second question.
    """
    sandbox = _build_sandbox(workspace)
    completed = subprocess.run(
        [_SHELL, str(script)],
        env=_sandbox_environment(sandbox, console_mode),
        capture_output=True,
        text=True,
        timeout=_RUN_TIMEOUT_SECONDS,
        check=False,
    )
    return _HarnessRun(
        returncode=completed.returncode,
        invocations=tuple(
            _parse_invocation(row)
            for row in sandbox.registry.read_text(encoding="utf-8").splitlines()
            if row
        ),
        config_before=sandbox.config_before,
        config_after=sandbox.user_config.read_bytes(),
    )


def _unescape_trace_string(text: str) -> str:
    """Undo the C-style quoting strace applies to the strings it prints.

    Only the escapes an argv can realistically carry are decoded; anything else
    is left verbatim rather than guessed at. Expected values contain no escapes
    at all, so this decoding is the identity on them and cannot loosen a
    comparison, but it keeps a quoted mutant argv readable in the diff that
    reports it.
    """
    return _TRACE_ESCAPE_PATTERN.sub(
        lambda match: _TRACE_ESCAPES.get(match.group(1), match.group(0)),
        text,
    )


def _parse_executions(trace_text: str) -> tuple[_Execution, ...]:
    """Extract every program the kernel actually started from an strace log.

    Only records ending in ``= 0`` are kept, because a failed ``execve`` starts
    no program; counting those would report the shell's PATH probing rather
    than the script's behaviour. Nothing else is dropped. In particular the
    shims are ``#!/bin/sh`` scripts, yet on Linux the kernel's script handling
    is transparent to ``execve``, so running one costs a single record naming
    the shim and no separate interpreter record. Verified on this host: a shim
    run appears once, as its own path. A ``/bin/sh`` record is therefore always
    a real shell start, never shim overhead, and is matched by argv rather than
    by program path so that an absolute ``/bin/sh -c ...`` preflight stays
    distinguishable from the harness starting the script.

    Tracing ``execve`` alone is enough even though Linux also offers
    ``execveat``: reaching the second syscall requires a process that is
    already running, and that process can only have been started by an
    ``execve`` recorded here.
    """
    return tuple(
        _Execution(
            program=_unescape_trace_string(match.group("path")),
            argv=tuple(
                _unescape_trace_string(argument)
                for argument in _TRACE_STRING_PATTERN.findall(match.group("argv"))
            ),
        )
        for match in _EXECVE_PATTERN.finditer(trace_text)
    )


def _trace_completeness_defects(trace_text: str) -> tuple[str, ...]:
    """Name strace artefacts that would drop or shorten a record unnoticed.

    An interleaved syscall is split across an ``<unfinished ...>`` and a
    ``<... resumed>`` line, and an over-long string or argument array is cut
    with an ellipsis. Either would let a record parse as absent or as different
    from what really ran, so the tests refuse to draw conclusions from a
    transcript containing them instead of silently under-reporting.
    """
    defects = []
    if "<unfinished" in trace_text:
        defects.append("interleaved syscall lines")
    if "..." in trace_text:
        defects.append("truncated strings or argument arrays")
    return tuple(defects)


def _probe_strace_support() -> str:
    """Return an empty string when strace can trace a child here, else why not.

    Presence of the binary is not enough: a hardened ``ptrace_scope``, a
    missing capability, or a container without ``CAP_SYS_PTRACE`` makes strace
    start and then report nothing. The probe therefore traces ``/bin/true`` and
    insists on a parsed record, so an environment where the harness would
    measure nothing produces a loud skip rather than a green test.
    """
    if _STRACE_PATH is None:
        return "strace is not installed on this host"
    with tempfile.TemporaryDirectory() as scratch:
        trace_file = Path(scratch) / "probe.trace"
        try:
            probe = subprocess.run(
                [_STRACE_PATH, "-f", "-e", "trace=execve", "-o", str(trace_file), "/bin/true"],
                capture_output=True,
                text=True,
                timeout=_RUN_TIMEOUT_SECONDS,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as error:
            return f"strace could not be started: {error}"
        if probe.returncode != 0:
            return f"strace exited {probe.returncode}: {probe.stderr.strip()}"
        if not _parse_executions(trace_file.read_text(encoding="utf-8")):
            return "strace recorded no execve; ptrace is restricted in this environment"
    return ""


_STRACE_SKIP_REASON: Final[str] = _probe_strace_support()


def _run_traced_entrypoint(script: Path, workspace: Path, console_mode: str | None) -> _TracedRun:
    """Execute one entrypoint script under ``strace -f -e trace=execve``.

    The sandbox is the PATH harness's sandbox, so both runners observe the same
    HOME, the same planted codex config, and the same shimmed PATH. ``-f``
    follows children, which is required because the script forks for
    ``seed_kimi_region`` and then ``exec``s itself. The transcript goes to a
    file rather than a pipe for the same reason the registry does: the traced
    process replaces itself, and buffered pipe output could lose the tail.
    """
    assert _STRACE_PATH is not None, _STRACE_SKIP_REASON
    sandbox = _build_sandbox(workspace)
    trace_file = workspace / "execve.trace"
    completed = subprocess.run(
        [
            _STRACE_PATH,
            "-f",
            "-s",
            _TRACE_STRING_LIMIT,
            "-e",
            "trace=execve",
            "-o",
            str(trace_file),
            _SHELL,
            str(script),
        ],
        env=_sandbox_environment(sandbox, console_mode),
        capture_output=True,
        text=True,
        timeout=_RUN_TIMEOUT_SECONDS,
        check=False,
    )
    trace_text = trace_file.read_text(encoding="utf-8")
    return _TracedRun(
        returncode=completed.returncode,
        executions=_parse_executions(trace_text),
        trace_text=trace_text,
        sandbox=sandbox,
        config_before=sandbox.config_before,
        config_after=sandbox.user_config.read_bytes(),
    )


def _expected_prologue(sandbox: _Sandbox, script: Path) -> tuple[_Execution, ...]:
    """List the executions every entrypoint run performs before its branch.

    The first is the harness starting the script under ``/bin/sh``, which is
    apparatus but is pinned rather than filtered so that a second shell start
    cannot hide behind it. The second is ``seed_kimi_region`` creating the kimi
    state directory: that is genuine entrypoint behaviour, so it belongs in the
    expected list rather than in a filter.
    """
    return (
        _Execution(program=_SHELL, argv=(_SHELL, str(script))),
        _Execution(
            program=_resolve_on_path("mkdir", sandbox),
            argv=("mkdir", "-p", str(sandbox.home / _KIMI_STATE_RELATIVE)),
        ),
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


@pytest.mark.skipif(bool(_STRACE_SKIP_REASON), reason=_STRACE_SKIP_REASON or "strace is usable")
def test_delegate_mode_execve_trace_holds_no_preflight_by_any_path(tmp_path: Path) -> None:
    """Every program delegate mode starts is accounted for, PATH or not.

    Given: The committed entrypoint, an isolated HOME holding a non-trivial
        codex user config, and strace following every descendant's execve,
    When: The script is executed with AGENT_CONSOLE_MODE=delegate,
    Then: The complete list of started programs is exactly the shell that runs
        the script, the kimi state mkdir, and the strict-flagged delegate exec,
        and the user config is byte-identical, so a preflight invoked by
        absolute path such as `/usr/local/bin/python -c ...`, `/bin/true`, or
        `/bin/sh -c ...` is recorded as a surplus entry and fails here even
        though a PATH-shim registry could never see it.
    """
    run = _run_traced_entrypoint(_ENTRYPOINT, tmp_path, "delegate")
    assert _trace_completeness_defects(run.trace_text) == ()
    assert run.returncode == 0
    assert run.config_after == run.config_before
    assert run.executions == (
        *_expected_prologue(run.sandbox, _ENTRYPOINT),
        _Execution(
            program=str(run.sandbox.shim_dir / "python"),
            argv=("python", "-m", "snapper_delegate.pid1"),
        ),
    )


@pytest.mark.skipif(bool(_STRACE_SKIP_REASON), reason=_STRACE_SKIP_REASON or "strace is usable")
def test_idle_mode_execve_trace_holds_no_interpreter_by_any_path(tmp_path: Path) -> None:
    """Every program idle mode starts is accounted for, PATH or not.

    Given: The committed entrypoint, an isolated HOME holding a non-trivial
        codex user config, and strace following every descendant's execve,
    When: The script is executed with AGENT_CONSOLE_MODE unset,
    Then: The complete list of started programs is exactly the shell that runs
        the script, the kimi state mkdir, and the parking sleep, and the user
        config is byte-identical, so the idle branch cannot smuggle an
        interpreter in under an absolute path either.
    """
    run = _run_traced_entrypoint(_ENTRYPOINT, tmp_path, None)
    assert _trace_completeness_defects(run.trace_text) == ()
    assert run.returncode == 0
    assert run.config_after == run.config_before
    assert run.executions == (
        *_expected_prologue(run.sandbox, _ENTRYPOINT),
        _Execution(program=str(run.sandbox.shim_dir / "sleep"), argv=("sleep", "infinity")),
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
