"""Regression tests for refreshing quality hooks without losing Iris guards."""

import runpy
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import install_pre_commit as installer


def _installed_iris(name: str) -> str:
    """Resolve only the expected Iris command for guarded installer tests."""
    assert name == "iris"
    return "test-iris"


def _hook_path(monkeypatch: pytest.MonkeyPatch, root: Path) -> Path:
    """Make Git resolve a private hook path without touching the real checkout."""
    hook = root / "custom hooks" / "pre-commit"
    hook.parent.mkdir()

    def git_output(command: list[str], **options: object) -> str:
        """Return the actual absolute hook path a Git worktree may report."""
        assert command == ["git", "rev-parse", "--git-path", "hooks/pre-commit"]
        assert options == {"cwd": root, "encoding": "utf-8"}
        return str(hook) + "\n"

    monkeypatch.setattr(installer.subprocess, "check_output", git_output)
    return hook


@pytest.mark.parametrize("existing", (False, True))
@pytest.mark.parametrize("exit_code", (0, 17))
def test_ordinary_installs_keep_the_installers_exit_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, existing: bool, exit_code: int
) -> None:
    """Refresh ordinary hooks without requiring Iris.

    Given: An absent or ordinary quality hook and the installer's exit status,
    When: Hook setup runs,
    Then: It invokes only pre-commit and returns that exact status.
    """
    hook = _hook_path(monkeypatch, tmp_path)
    if existing:
        hook.write_text("#!/bin/sh\npre-commit\n", encoding="utf-8")
    calls: list[list[str]] = []

    def run(command: list[str], **options: object) -> subprocess.CompletedProcess[bytes]:
        """Record the command and preserve a real nonzero installer result."""
        assert options == {"cwd": tmp_path, "check": False}
        calls.append(command)
        return subprocess.CompletedProcess(command, exit_code)

    monkeypatch.setattr(installer.subprocess, "run", run)
    assert installer.install_pre_commit(tmp_path) == exit_code
    assert calls == [[sys.executable, "-m", "pre_commit", "install"]]


@pytest.mark.parametrize("outcome", ("success", "nonzero", "exception"))
def test_guard_is_restored_even_when_quality_hook_installation_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    """Restore the guard around both normal and unsuccessful pre-commit refreshes.

    Given: An Iris wrapper around the quality hook,
    When: The quality installer succeeds, fails or cannot start,
    Then: The wrapper is detached first and restored last without hiding a failure.
    """
    hook = _hook_path(monkeypatch, tmp_path)
    hook.write_text("#!/bin/sh\n# Iris cooperative guard v2\n", encoding="utf-8")
    monkeypatch.setattr(installer, "which", _installed_iris)
    calls: list[list[str]] = []

    def run(command: list[str], **options: object) -> subprocess.CompletedProcess[bytes]:
        """Record guard ordering and simulate actual installer failure modes."""
        calls.append(command)
        assert options["cwd"] == tmp_path
        if command[0] == sys.executable:
            assert options["check"] is False
            if outcome == "exception":
                raise OSError("synthetic startup failure")
            return subprocess.CompletedProcess(command, 17 if outcome == "nonzero" else 0)
        assert options["check"] is True
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(installer.subprocess, "run", run)
    if outcome == "exception":
        with pytest.raises(OSError, match="startup failure"):
            installer.install_pre_commit(tmp_path)
    else:
        assert installer.install_pre_commit(tmp_path) == (17 if outcome == "nonzero" else 0)
    assert calls == [
        ["test-iris", "guard", "remove"],
        [sys.executable, "-m", "pre_commit", "install"],
        ["test-iris", "guard", "install"],
    ]


def test_missing_iris_refuses_before_changing_an_existing_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the existing guard intact when its CLI cannot be found.

    Given: An installed Iris wrapper without a CLI on PATH,
    When: Hook refresh is requested,
    Then: Setup refuses before removing or replacing the wrapper.
    """
    hook = _hook_path(monkeypatch, tmp_path)
    original = b"#!/bin/sh\n# Iris cooperative guard v2\n"
    hook.write_bytes(original)

    def missing_iris(name: str) -> None:
        """Report that the expected Iris CLI cannot be found."""
        assert name == "iris"

    monkeypatch.setattr(installer, "which", missing_iris)
    with pytest.raises(RuntimeError, match="not on PATH"):
        installer.install_pre_commit(tmp_path)
    assert hook.read_bytes() == original


@pytest.mark.parametrize("failed_operation", ("remove", "install"))
def test_guard_failures_are_not_reported_as_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_operation: str
) -> None:
    """Surface failures to detach or restore the guard.

    Given: An existing Iris guard and an unsuccessful Iris operation,
    When: The hook installer reaches that operation,
    Then: Setup fails rather than claiming that the guard is installed.
    """
    hook = _hook_path(monkeypatch, tmp_path)
    hook.write_text("#!/bin/sh\n# Iris cooperative guard v2\n", encoding="utf-8")
    monkeypatch.setattr(installer, "which", _installed_iris)
    calls: list[list[str]] = []

    def run(command: list[str], **options: object) -> subprocess.CompletedProcess[bytes]:
        """Fail exactly the requested guard operation with subprocess semantics."""
        calls.append(command)
        if command == ["test-iris", "guard", failed_operation]:
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(installer.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError):
        installer.install_pre_commit(tmp_path)
    assert len(calls) == (1 if failed_operation == "remove" else 3)


def test_main_refreshes_the_current_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Use the script's checkout rather than an unrelated caller directory.

    Given: The script entry point and a failed installer result,
    When: Main runs,
    Then: It targets the script's repository and returns the failure unchanged.
    """
    roots: list[Path] = []

    def install(root: Path) -> int:
        """Record the repository selected by main."""
        roots.append(root)
        return 17

    monkeypatch.setattr(installer, "install_pre_commit", install)
    assert installer.main() == 17
    assert roots == [Path(installer.__file__).resolve().parents[1]]


def test_standalone_entry_point_propagates_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Execute the actual main guard without installing hooks in the real checkout.

    Given: Git resolves an absent hook and the quality installer succeeds,
    When: The script executes as a standalone program,
    Then: It exits successfully through the public installer flow.
    """
    hook = tmp_path / "absent-hook"

    def git_output(command: list[str], **options: object) -> str:
        """Resolve the private absent hook for the standalone module instance."""
        return str(hook)

    def run(command: list[str], **options: object) -> subprocess.CompletedProcess[bytes]:
        """Avoid modifying the live checkout during entry-point verification."""
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(installer.subprocess, "check_output", git_output)
    monkeypatch.setattr(installer.subprocess, "run", run)
    with pytest.raises(SystemExit) as result:
        runpy.run_path(str(installer.__file__), run_name="__main__")
    assert result.value.code == 0
