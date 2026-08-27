r"""Contract pins for the agent-console entrypoint, codex policy, and provenance.

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
``strace -f -e trace=execve,execveat`` with the transcript written to a file
(again because the script ``exec``s), follows every descendant, and compares
the *complete* list of successfully executed programs, resolved path plus
argv, against an exact expected tuple. An absolute-path preflight is recorded
there whether or not PATH was consulted and whether it runs before, between,
or after the mode branches.

Both exec syscalls are traced because ``execveat`` is an equal way to start a
program, and an untraced syscall is worse than a mis-parsed one: it leaves no
line at all. Measured on this host, ``os.execve(fd, argv, env)`` emits
``execveat(3, "", ["/bin/true", "marker"], 0x... /* 1 var */, AT_EMPTY_PATH)``
under ``trace=execve,execveat`` and emits *nothing whatsoever* under
``trace=execve``. No amount of parsing care recovers a syscall that was never
asked for, so the trace set has to be right first.

Two further properties turn the comparison into a proof rather than a hope.

First, the transcript is decoded, not pattern-matched. strace prints paths and
argv as C strings, so a program named ``silent"helper`` appears as
``execve("/dir/silent\"helper", [...]) = 0``. A path regex of the shape
``"[^"]*"`` stops at the backslash, fails to match the whole record, and
silently drops it: the transcript then contains a successful exec that the
expected tuple never has to account for, and the mutant passes green. The
decoder here walks each quoted string character by character and understands
``\\``, ``\"``, the single-character escapes, and octal and hexadecimal byte
escapes, so a hostile name cannot pick a spelling the reader skips over.

Second — and this is what makes the first property enforceable rather than
merely intended — every run is reconciled. The raw transcript is scanned for
*every* line naming an exec syscall, and each is classified into exactly one
of three buckets: a successful record the decoder fully understood, a failed
exec (``= -1 ENOENT`` and friends, which start no program and are just the
shell probing PATH), or a line the parser could not account for. Anything in
that third bucket fails the test and is printed verbatim. Without the
reconciliation, "the parser did not understand this line" and "there was no
such line" are the same observation — an empty result either way — and that is
exactly the state a mutant wants the harness to be in. With it, a decoder that
ever loses a record becomes a loud failure naming the record it lost.
``<unfinished ...>``/``<... resumed>`` splits and ellipsis truncation are
refused for the same reason: each would let a record read as absent or as
shorter than what really ran.

An absent ``strace`` is treated as the same kind of hole. The proof layer does
not skip by default: with no usable ``strace`` the two tracing tests call
``pytest.fail``, so a run that never executed the proof cannot end up calling
itself successful. A developer on a machine without ``strace`` waives the
layer by exporting the opt-out variable to its exact literal value, which
turns the hard failure into a skip — a decision that someone had to make, that
is greppable, and that a typo cannot produce by accident. A guard test refuses
that waiver on any host where ``strace`` does work, so it cannot be left set
in CI and quietly disarm the layer there.

Neither harness sees work that never reaches ``execve``: a rewriter written
purely with shell builtins and a redirection performs no exec at all. That case
is covered instead by byte-comparing the codex user config across every run,
which both harnesses assert and which is indifferent to how the write was
spelled. Neither harness observes the image; they pin the script's behaviour
under the host ``/bin/sh``, and the Dockerfile pins are what tie that script to
the image.
"""

import hashlib
import os
import re
import shlex
import shutil
import string
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

_ENTRYPOINT_EXACT_BODY: Final[str] = (
    '#!/bin/sh\nset -eu\n\nseed_kimi_region() {\n    if [ ! -f "$HOME/.kimi-code/region" ]; then\n        mkdir -p "$HOME/.kimi-code"\n        printf \'global\\n\' > "$HOME/.kimi-code/region"\n    fi\n}\n\nseed_kimi_region\n\nif [ "${AGENT_CONSOLE_MODE:-idle}" = "delegate" ]; then\n    export SNAPPER_PID1_STRICT=1\n    exec python -m snapper_delegate.pid1\nfi\n\nexec sleep infinity\n'
)

_SYSTEM_PATH: Final[str] = "/usr/local/bin:/usr/bin:/bin"
_SHELL: Final[str] = "/bin/sh"
_SHIMMED_PROGRAMS: Final[tuple[str, ...]] = ("python", "python3", "sleep")
_KIMI_STATE_RELATIVE: Final[str] = ".kimi-code"
_RUN_TIMEOUT_SECONDS: Final[float] = 15.0
_STRACE_PATH: Final[str | None] = shutil.which("strace")
_TRACE_STRING_LIMIT: Final[str] = "4096"
_TRACE_EXPRESSION: Final[str] = "trace=execve,execveat"
_EXEC_MENTION_PATTERN: Final[re.Pattern[str]] = re.compile(r"\bexecve(?:at)?\(")
_EXEC_SUCCESS_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^(?:\d+\s+)?(?P<syscall>execve|execveat)\((?P<body>.*)\)\s+=\s+0(?:\s.*)?$"
)
_EXEC_FAILURE_PATTERN: Final[re.Pattern[str]] = re.compile(r"\)\s+=\s+-1\s+[A-Z][A-Z0-9_]*\b")
_EXECVEAT_DIRFD_PATTERN: Final[re.Pattern[str]] = re.compile(r"^(?P<dirfd>AT_FDCWD|-?\d+), ")
_ENVP_TAIL_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?:0x[0-9a-fA-F]+ /\* \d+ vars? \*/|NULL)(?:, [A-Z][A-Z0-9_|]*)?\Z"
)
_C_ESCAPES: Final[dict[str, str]] = {
    "a": "\a",
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "v": "\v",
    "\\": "\\",
    '"': '"',
    "'": "'",
    "?": "?",
}
_OCTAL_DIGITS: Final[str] = "01234567"
_MAX_OCTAL_DIGITS: Final[int] = 3
_MAX_HEX_DIGITS: Final[int] = 2
_TRACE_PROOF_WAIVER_VARIABLE: Final[str] = "SNAPPER_ALLOW_UNPROVEN_ENTRYPOINT_EXEC"
_TRACE_PROOF_WAIVER_VALUE: Final[str] = "i-accept-an-unproven-entrypoint"
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
    """One program the kernel actually started, as reported by an exec syscall.

    ``syscall`` and ``directory_fd`` carry defaults matching a plain ``execve``
    so the expected tuples stay readable, while an ``execveat`` record still
    compares unequal to every one of them and prints the descriptor it resolved
    against. A correct run of this entrypoint contains no ``execveat`` at all,
    so any such record is by construction a surplus entry.
    """

    program: str
    argv: tuple[str, ...]
    syscall: str = "execve"
    directory_fd: str = ""


@dataclass(frozen=True, slots=True)
class _TraceParse:
    """A reconciled reading of one strace transcript.

    ``understood`` are the records the decoder fully consumed, ``ignored`` is
    the count of exec attempts that failed and therefore started no program,
    and ``unaccounted`` holds, verbatim, every line naming an exec syscall that
    fell into neither group. The last field is the point of the type: it is
    what separates "the parser understood nothing because nothing happened"
    from "the parser understood nothing because it could not read the line".
    """

    understood: tuple[_Execution, ...]
    ignored: int
    unaccounted: tuple[str, ...]


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
    parse: _TraceParse
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


def _take_digits(text: str, index: int, alphabet: str, limit: int) -> str:
    """Return the run of ``alphabet`` characters at ``index``, at most ``limit``."""
    end = index
    while end < len(text) and end - index < limit and text[end] in alphabet:
        end += 1
    return text[index:end]


def _decode_escape(text: str, index: int) -> tuple[bytes, int] | None:
    """Decode the escape sequence whose backslash sits just before ``index``.

    Returns the bytes it stands for and the index just past it, or ``None``
    when the sequence is not one this decoder claims to understand. Returning
    ``None`` rather than guessing is deliberate: an unread escape must surface
    as an unaccounted record, never as a silently mangled program name.
    """
    if index >= len(text):
        return None
    marker = text[index]
    simple = _C_ESCAPES.get(marker)
    if simple is not None:
        return simple.encode("utf-8"), index + 1
    if marker == "x":
        digits = _take_digits(text, index + 1, string.hexdigits, _MAX_HEX_DIGITS)
        if not digits:
            return None
        return bytes((int(digits, 16),)), index + 1 + len(digits)
    if marker in _OCTAL_DIGITS:
        digits = _take_digits(text, index, _OCTAL_DIGITS, _MAX_OCTAL_DIGITS)
        value = int(digits, 8)
        if value > 0xFF:
            return None
        return bytes((value,)), index + len(digits)
    return None


def _decode_c_string(text: str, start: int) -> tuple[str, int] | None:
    r"""Decode one C-style quoted string starting at ``start``.

    strace quotes every path and argv element this way, so a program named
    ``silent"helper`` is printed as ``"silent\"helper"``. A regex of the shape
    ``"[^"]*"`` stops at the backslash and loses the record; scanning character
    by character does not. Bytes are accumulated rather than characters so that
    numeric escapes reassemble into real UTF-8 instead of one codepoint per
    byte, and the result is surrogate-escaped so an undecodable name still
    round-trips into the failure message. Truncated output (``"abc"...``) is
    rejected downstream, because the closing quote is followed by an ellipsis
    the record grammar does not allow.
    """
    if start >= len(text) or text[start] != '"':
        return None
    buffer = bytearray()
    index = start + 1
    while index < len(text):
        character = text[index]
        if character == '"':
            return buffer.decode("utf-8", errors="surrogateescape"), index + 1
        if character != "\\":
            buffer.extend(character.encode("utf-8"))
            index += 1
            continue
        decoded = _decode_escape(text, index + 1)
        if decoded is None:
            return None
        piece, index = decoded
        buffer.extend(piece)
    return None


def _decode_c_string_array(text: str, start: int) -> tuple[tuple[str, ...], int] | None:
    """Decode one bracketed argv array of C strings starting at ``start``.

    Anything the grammar does not allow between elements — most importantly the
    ``...`` strace writes when it truncates a long array — ends the decode with
    ``None`` so the record is reported as unaccounted rather than as a shorter
    argv that happens to differ from the expected one in a confusing way.
    """
    if start >= len(text) or text[start] != "[":
        return None
    index = start + 1
    if index < len(text) and text[index] == "]":
        return (), index + 1
    values: list[str] = []
    while True:
        decoded = _decode_c_string(text, index)
        if decoded is None:
            return None
        value, index = decoded
        values.append(value)
        if index < len(text) and text[index] == "]":
            return tuple(values), index + 1
        if not text.startswith(", ", index):
            return None
        index += 2


def _decode_exec_record(syscall: str, body: str) -> _Execution | None:
    """Decode the argument list of one successful ``execve``/``execveat`` line.

    The whole body must be consumed: path, argv, and an envp tail that is
    either strace's ``0x... /* N vars */`` summary or ``NULL``, plus the flag
    word ``execveat`` adds. Insisting on the tail keeps the decode honest — if
    a future strace changes the format, the record stops being understood and
    the reconciliation fails loudly instead of the harness quietly measuring a
    subset of what ran.
    """
    index = 0
    directory_fd = ""
    if syscall == "execveat":
        prefix = _EXECVEAT_DIRFD_PATTERN.match(body)
        if prefix is None:
            return None
        directory_fd = prefix.group("dirfd")
        index = prefix.end()
    decoded_path = _decode_c_string(body, index)
    if decoded_path is None:
        return None
    program, index = decoded_path
    if not body.startswith(", ", index):
        return None
    decoded_argv = _decode_c_string_array(body, index + 2)
    if decoded_argv is None:
        return None
    argv, index = decoded_argv
    if not body.startswith(", ", index):
        return None
    if _ENVP_TAIL_PATTERN.match(body, index + 2) is None:
        return None
    return _Execution(program=program, argv=argv, syscall=syscall, directory_fd=directory_fd)


def _parse_trace(trace_text: str) -> _TraceParse:
    """Reconcile an strace transcript into understood, ignored, and unaccounted.

    Every line naming an exec syscall must land in exactly one bucket. Records
    ending in ``= 0`` are decoded, because those are the ones that started a
    program; records ending in ``= -1 ENOENT`` and the like are ignored,
    because a failed exec starts nothing and counting them would report the
    shell's PATH probing rather than the script's behaviour. Anything else —
    an escape the decoder refused, a format it does not know, half of an
    interleaved call — is kept verbatim so the caller can fail with it.

    The shims are ``#!/bin/sh`` scripts, yet on Linux the kernel's script
    handling is transparent to ``execve``, so running one costs a single record
    naming the shim and no separate interpreter record. Verified on this host:
    a shim run appears once, as its own path. A ``/bin/sh`` record is therefore
    always a real shell start, never shim overhead, and is matched by argv
    rather than by program path so that an absolute ``/bin/sh -c ...``
    preflight stays distinguishable from the harness starting the script.
    """
    understood: list[_Execution] = []
    unaccounted: list[str] = []
    ignored = 0
    for line in trace_text.splitlines():
        if not _EXEC_MENTION_PATTERN.search(line):
            continue
        success = _EXEC_SUCCESS_PATTERN.match(line)
        if success is None:
            if _EXEC_FAILURE_PATTERN.search(line):
                ignored += 1
            else:
                unaccounted.append(line)
            continue
        record = _decode_exec_record(success.group("syscall"), success.group("body"))
        if record is None:
            unaccounted.append(line)
            continue
        understood.append(record)
    return _TraceParse(tuple(understood), ignored, tuple(unaccounted))


def _trace_completeness_defects(parse: _TraceParse, trace_text: str) -> tuple[str, ...]:
    """Name every reason this transcript may not be read as a complete record.

    The reconciliation is the load-bearing one: an exec line the decoder could
    not account for is reported verbatim, because otherwise "the parser lost a
    record" and "there was no record" are indistinguishable — both produce an
    empty result — and a mutant that renames a helper into a spelling the
    parser skips passes on exactly that ambiguity. The strace-level artefacts
    are refused for the same reason: an interleaved syscall is split across
    ``<unfinished ...>`` and ``<... resumed>`` lines, and an over-long string
    or array is cut with an ellipsis, so either would let a record read as
    absent or as different from what really ran.
    """
    defects = [f"exec line the parser could not account for: {line}" for line in parse.unaccounted]
    if "<unfinished" in trace_text or "resumed>" in trace_text:
        defects.append("interleaved syscall lines")
    if "..." in trace_text:
        defects.append("truncated strings or argument arrays")
    return tuple(defects)


def _probe_strace_support() -> str:
    """Return an empty string when strace can trace a child here, else why not.

    Presence of the binary is not enough: a hardened ``ptrace_scope``, a
    missing capability, or a container without ``CAP_SYS_PTRACE`` makes strace
    start and then report nothing. The probe therefore traces ``/bin/true``
    with the same syscall expression the harness uses and insists on a decoded
    record, so an environment where the harness would silently measure nothing
    is detected here rather than passing as an empty expected list. The probe
    also proves the running strace accepts ``execveat``, which an older build
    would reject outright.
    """
    if _STRACE_PATH is None:
        return "strace is not installed on this host"
    with tempfile.TemporaryDirectory() as scratch:
        trace_file = Path(scratch) / "probe.trace"
        try:
            probe = subprocess.run(
                [_STRACE_PATH, "-f", "-e", _TRACE_EXPRESSION, "-o", str(trace_file), "/bin/true"],
                capture_output=True,
                text=True,
                timeout=_RUN_TIMEOUT_SECONDS,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as error:
            return f"strace could not be started: {error}"
        if probe.returncode != 0:
            return f"strace exited {probe.returncode}: {probe.stderr.strip()}"
        if not _parse_trace(trace_file.read_text(encoding="utf-8")).understood:
            return "strace recorded no exec syscall; ptrace is restricted in this environment"
    return ""


_STRACE_UNUSABLE_REASON: Final[str] = _probe_strace_support()


def _require_execve_trace_proof() -> None:
    """Refuse to continue unless the exec trace proof can actually be executed.

    A missing ``strace`` used to skip, which meant a host without it reported
    ``passed`` for a run in which the two tests that carry the whole
    process-list claim never executed — a green result whose greenness came
    from not looking. The default is therefore a hard failure. The waiver is
    an environment variable that must equal one exact literal, so it cannot be
    produced by a stray ``=1`` or an unrelated truthy value, it is greppable in
    CI configuration, and it is refused outright on hosts where the proof would
    have run (see the guard test below). ``pytest.fail`` is preferred over a
    session-level guard test because it attaches the refusal to the specific
    claim that went unproven, and it survives a ``-k`` selection that would
    deselect a separate guard.
    """
    if not _STRACE_UNUSABLE_REASON:
        return
    if os.environ.get(_TRACE_PROOF_WAIVER_VARIABLE) == _TRACE_PROOF_WAIVER_VALUE:
        pytest.skip(
            f"exec trace proof explicitly waived via "
            f"{_TRACE_PROOF_WAIVER_VARIABLE}={_TRACE_PROOF_WAIVER_VALUE} "
            f"({_STRACE_UNUSABLE_REASON})"
        )
    pytest.fail(
        f"the entrypoint exec trace proof could not run: {_STRACE_UNUSABLE_REASON}. "
        f"Install strace, or accept an unproven entrypoint deliberately by exporting "
        f"{_TRACE_PROOF_WAIVER_VARIABLE}={_TRACE_PROOF_WAIVER_VALUE}. "
        f"This never skips by default: a run that did not execute the proof must not "
        f"be reportable as a successful one."
    )


def _run_traced_entrypoint(script: Path, workspace: Path, console_mode: str | None) -> _TracedRun:
    """Execute one entrypoint script under ``strace -f -e trace=execve,execveat``.

    The sandbox is the PATH harness's sandbox, so both runners observe the same
    HOME, the same planted codex config, and the same shimmed PATH. ``-f``
    follows children, which is required because the script forks for
    ``seed_kimi_region`` and then ``exec``s itself. The transcript goes to a
    file rather than a pipe for the same reason the registry does: the traced
    process replaces itself, and buffered pipe output could lose the tail.
    """
    _require_execve_trace_proof()
    assert _STRACE_PATH is not None, _STRACE_UNUSABLE_REASON
    sandbox = _build_sandbox(workspace)
    trace_file = workspace / "execve.trace"
    completed = subprocess.run(
        [
            _STRACE_PATH,
            "-f",
            "-s",
            _TRACE_STRING_LIMIT,
            "-e",
            _TRACE_EXPRESSION,
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
        parse=_parse_trace(trace_text),
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


def test_exec_trace_proof_is_never_waived_where_it_could_run() -> None:
    """A host that can run the exec trace proof is not allowed to opt out of it.

    Given: The probe result for this host and the opt-out environment variable,
    When: Both are read together,
    Then: The waiver is absent whenever strace is usable, so the escape hatch
        that keeps a strace-less developer machine workable cannot be left set
        in CI and silently disarm the layer on a host that would have executed
        it.
    """
    waived = os.environ.get(_TRACE_PROOF_WAIVER_VARIABLE) == _TRACE_PROOF_WAIVER_VALUE
    assert not (waived and not _STRACE_UNUSABLE_REASON), (
        f"{_TRACE_PROOF_WAIVER_VARIABLE} is set on a host where strace works; "
        f"unset it so the exec trace proof actually runs"
    )


def test_delegate_mode_execve_trace_holds_no_preflight_by_any_path(tmp_path: Path) -> None:
    """Every program delegate mode starts is accounted for, PATH or not.

    Given: The committed entrypoint, an isolated HOME holding a non-trivial
        codex user config, and strace following every descendant's execve and
        execveat,
    When: The script is executed with AGENT_CONSOLE_MODE=delegate,
    Then: Every exec line in the transcript is accounted for and the complete
        list of started programs is exactly the shell that runs the script, the
        kimi state mkdir, and the strict-flagged delegate exec, and the user
        config is byte-identical, so a preflight invoked by absolute path such
        as `/usr/local/bin/python -c ...`, `/bin/true`, or `/bin/sh -c ...` is
        recorded as a surplus entry and fails here even though a PATH-shim
        registry could never see it, and a preflight named so that the decoder
        cannot read its record fails as an unaccounted line rather than
        vanishing.
    """
    run = _run_traced_entrypoint(_ENTRYPOINT, tmp_path, "delegate")
    assert _trace_completeness_defects(run.parse, run.trace_text) == ()
    assert run.returncode == 0
    assert run.config_after == run.config_before
    assert run.parse.understood == (
        *_expected_prologue(run.sandbox, _ENTRYPOINT),
        _Execution(
            program=str(run.sandbox.shim_dir / "python"),
            argv=("python", "-m", "snapper_delegate.pid1"),
        ),
    )


def test_idle_mode_execve_trace_holds_no_interpreter_by_any_path(tmp_path: Path) -> None:
    """Every program idle mode starts is accounted for, PATH or not.

    Given: The committed entrypoint, an isolated HOME holding a non-trivial
        codex user config, and strace following every descendant's execve and
        execveat,
    When: The script is executed with AGENT_CONSOLE_MODE unset,
    Then: Every exec line in the transcript is accounted for and the complete
        list of started programs is exactly the shell that runs the script, the
        kimi state mkdir, and the parking sleep, and the user config is
        byte-identical, so the idle branch cannot smuggle an interpreter in
        under an absolute path or under a name the decoder would drop.
    """
    run = _run_traced_entrypoint(_ENTRYPOINT, tmp_path, None)
    assert _trace_completeness_defects(run.parse, run.trace_text) == ()
    assert run.returncode == 0
    assert run.config_after == run.config_before
    assert run.parse.understood == (
        *_expected_prologue(run.sandbox, _ENTRYPOINT),
        _Execution(program=str(run.sandbox.shim_dir / "sleep"), argv=("sleep", "infinity")),
    )


def test_entrypoint_body_is_pinned_exactly() -> None:
    """The whole script is pinned, so ANY edit must be a deliberate one.

    Given: The committed entrypoint script,
    When: Its full text is compared against the pinned body,
    Then: They are byte-identical.

    This is the outermost of three layers and the only one that is complete.
    The behavioural harnesses below observe processes: the PATH shims see
    programs resolved through PATH, and the strace harness sees every execve
    whatever path invoked it. Neither can see work done entirely by SHELL
    BUILTINS — a mutant that reads the codex config with a redirect and
    exports a hostile PYTHONPATH performs no exec at all, leaves the config
    byte-identical, and passes every process-level and token-level check while
    breaking the delegate import in the real image. Pinning the exact body
    closes that class outright: any change, by any mechanism, fails here first.
    Updating this constant is therefore part of changing the entrypoint, and
    the diff of this test is the review surface for it.
    """
    assert _ENTRYPOINT.read_text(encoding="utf-8") == _ENTRYPOINT_EXACT_BODY


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
