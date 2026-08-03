"""Tests for the delegate boundary checker script."""

from pathlib import Path

import pytest

import scripts.check_delegate_boundary as boundary


def _write(tmp_path: Path, name: str, source: str) -> Path:
    package = tmp_path / boundary.SCAN_ROOT
    package.mkdir(parents=True, exist_ok=True)
    module = package / name
    module.write_text(source, encoding="utf-8")
    return module


class TestAllowlistFor:
    """Test suite for per-file allowlist selection."""

    def test_registration_uses_the_wide_process_manager_allowlist(self) -> None:
        """Only the managed registration may reach process-manager modules.

        Given: The registration module name,
        When: The allowlist is resolved,
        Then: It includes the process-manager models module and core types.
        """
        allowed = boundary.allowlist_for("registration.py")
        assert "snapper.application.process_manager.models" in allowed
        assert "snapper.core.types" in allowed

    @pytest.mark.parametrize("filename", ["runner.py", "chat_completions.py", "pid1.py"])
    def test_agent_lifecycle_modules_get_no_parent_imports(self, filename: str) -> None:
        """Self-contained agent modules cannot import the parent package.

        Given: A non-registration module name,
        When: The allowlist is resolved,
        Then: Its first-party import allowlist is empty.
        """
        allowed = boundary.allowlist_for(filename)
        assert allowed == frozenset()


class TestForbiddenImports:
    """Test suite for message-bus and data-layer rejection."""

    def test_zmq_import_is_rejected(self, tmp_path: Path) -> None:
        """A direct zmq import breaches the boundary.

        Given: An agent-plane file importing zmq,
        When: The file is checked,
        Then: A violation is reported for the zmq module.
        """
        module = _write(tmp_path, "runner.py", "import zmq\n")
        findings = boundary.check_delegate_boundary(module, boundary.allowlist_for("runner.py"))
        assert findings == [(1, "zmq")]

    def test_messaging_import_is_rejected(self, tmp_path: Path) -> None:
        """Importing the message bus breaches the boundary.

        Given: An agent-plane file importing snapper.messaging,
        When: The file is checked,
        Then: A violation is reported for the messaging module.
        """
        module = _write(tmp_path, "runner.py", "from snapper.messaging.topics import validation\n")
        findings = boundary.check_delegate_boundary(module, boundary.allowlist_for("runner.py"))
        assert (1, "snapper.messaging.topics") in findings

    def test_data_import_is_rejected(self, tmp_path: Path) -> None:
        """Importing the ORM data layer breaches the boundary.

        Given: An agent-plane file importing snapper.data,
        When: The file is checked,
        Then: A violation is reported for the data module.
        """
        module = _write(tmp_path, "runner.py", "from snapper.data import repository\n")
        findings = boundary.check_delegate_boundary(module, boundary.allowlist_for("runner.py"))
        assert (1, "snapper.data") in findings

    def test_from_snapper_import_data_submodule_is_rejected(self, tmp_path: Path) -> None:
        """A submodule import through the top package is still caught.

        Given: An agent-plane file doing ``from snapper import data``,
        When: The file is checked,
        Then: The synthesized snapper.data path is reported as a violation.
        """
        module = _write(tmp_path, "runner.py", "from snapper import data\n")
        findings = boundary.check_delegate_boundary(module, boundary.allowlist_for("runner.py"))
        assert (1, "snapper.data") in findings

    def test_aliased_data_import_is_rejected(self, tmp_path: Path) -> None:
        """An aliased data import cannot evade the check.

        Given: An agent-plane file doing ``import snapper.data as d``,
        When: The file is checked,
        Then: The snapper.data path is reported as a violation.
        """
        module = _write(tmp_path, "runner.py", "import snapper.data as d\n")
        findings = boundary.check_delegate_boundary(module, boundary.allowlist_for("runner.py"))
        assert (1, "snapper.data") in findings

    def test_data_descendant_import_is_rejected(self, tmp_path: Path) -> None:
        """A deep descendant of the data layer is a violation.

        Given: An agent-plane file importing snapper.data.repository,
        When: The file is checked,
        Then: The descendant path is reported as a violation.
        """
        module = _write(tmp_path, "runner.py", "import snapper.data.repository\n")
        findings = boundary.check_delegate_boundary(module, boundary.allowlist_for("runner.py"))
        assert (1, "snapper.data.repository") in findings


class TestLookalikeAndAllowlist:
    """Test suite for lookalike prefixes and non-allowlisted imports."""

    def test_lookalike_prefix_is_not_forbidden(self, tmp_path: Path) -> None:
        """A module whose name merely starts with the letters ``data`` is safe.

        Given: An agent-plane file importing snapper.database (a lookalike),
        When: The file is checked with a permissive allowlist,
        Then: The lookalike is not reported as a forbidden data import.
        """
        module = _write(tmp_path, "runner.py", "import snapper.database\n")
        allowlist = frozenset({"snapper.database"})
        findings = boundary.check_delegate_boundary(module, allowlist)
        assert findings == []

    def test_non_allowlisted_first_party_is_rejected(self, tmp_path: Path) -> None:
        """A first-party import outside the file allowlist is a violation.

        Given: An agent-plane file importing snapper.server,
        When: The file is checked,
        Then: The non-allowlisted module is reported as a violation.
        """
        module = _write(tmp_path, "runner.py", "from snapper.server import app\n")
        findings = boundary.check_delegate_boundary(module, boundary.allowlist_for("runner.py"))
        assert (1, "snapper.server") in findings

    def test_allowlisted_first_party_passes(self, tmp_path: Path) -> None:
        """An allowlisted first-party import produces no violation.

        Given: The registration file importing an allowlisted core module,
        When: The file is checked,
        Then: No violation is reported.
        """
        module = _write(
            tmp_path,
            "registration.py",
            "from snapper.core.types import ProcessRoleEnum\n",
        )
        findings = boundary.check_delegate_boundary(
            module, boundary.allowlist_for("registration.py")
        )
        assert findings == []

    def test_relative_import_is_not_a_first_party_violation(self, tmp_path: Path) -> None:
        """A package-relative import carries no module path to police.

        Given: An agent-plane file doing ``from . import helper``,
        When: The file is checked,
        Then: No violation is reported because the module path is empty.
        """
        module = _write(tmp_path, "runner.py", "from . import helper\n")
        findings = boundary.check_delegate_boundary(module, boundary.allowlist_for("runner.py"))
        assert findings == []

    def test_third_party_imports_are_unrestricted(self, tmp_path: Path) -> None:
        """Stdlib and third-party imports are never restricted.

        Given: An agent-plane file importing httpx and asyncio,
        When: The file is checked,
        Then: No violation is reported.
        """
        module = _write(tmp_path, "chat_completions.py", "import asyncio\nimport httpx\n")
        findings = boundary.check_delegate_boundary(
            module, boundary.allowlist_for("chat_completions.py")
        )
        assert findings == []


class TestFailClosed:
    """Test suite for fail-closed behavior."""

    def test_syntax_error_fails_closed(self, tmp_path: Path) -> None:
        """An unparseable file is reported rather than skipped.

        Given: An agent-plane file that does not parse,
        When: The file is checked,
        Then: A synthetic fail-closed violation is returned.
        """
        module = _write(tmp_path, "runner.py", "def broken(:\n")
        findings = boundary.check_delegate_boundary(module, boundary.allowlist_for("runner.py"))
        assert findings == [(0, "unparseable source (fail closed)")]

    def test_unreadable_file_fails_closed(self, tmp_path: Path) -> None:
        """A path that cannot be read is reported rather than skipped.

        Given: A path pointing at a directory instead of a file,
        When: The file is checked,
        Then: A synthetic fail-closed violation is returned.
        """
        package = tmp_path / boundary.SCAN_ROOT
        package.mkdir(parents=True, exist_ok=True)
        findings = boundary.check_delegate_boundary(package, boundary.allowlist_for("runner.py"))
        assert findings == [(0, "unreadable source (fail closed)")]

    def test_missing_source_root_fails_closed(self, tmp_path: Path) -> None:
        """A missing source root is a fail-closed scan result.

        Given: A project root without the delegate source package,
        When: The tree is scanned,
        Then: A single synthetic missing-root violation is returned.
        """
        results = boundary.scan_files(tmp_path)
        assert len(results) == 1
        (findings,) = results.values()
        assert findings == [(0, "missing source root (fail closed)")]


class TestScanAndRun:
    """Test suite for whole-tree scanning and the strict exit code."""

    def test_scan_skips_pycache(self, tmp_path: Path) -> None:
        """Compiled cache files are not scanned.

        Given: A package containing a clean module and a __pycache__ artifact,
        When: The tree is scanned,
        Then: Only the real module is considered and it is clean.
        """
        _write(tmp_path, "chat_completions.py", "import httpx\n")
        cache = tmp_path / boundary.SCAN_ROOT / "__pycache__"
        cache.mkdir(parents=True, exist_ok=True)
        (cache / "runner.py").write_text("import zmq\n", encoding="utf-8")
        results = boundary.scan_files(tmp_path)
        assert results == {}

    def test_run_scan_strict_returns_one_on_violation(self, tmp_path: Path) -> None:
        """Strict mode fails when a violation exists.

        Given: A package with a forbidden import,
        When: run_scan executes in strict mode,
        Then: It returns exit code 1.
        """
        _write(tmp_path, "runner.py", "import zmq\n")
        assert boundary.run_scan(tmp_path, strict_mode=True) == 1

    def test_run_scan_reports_only_without_strict(self, tmp_path: Path) -> None:
        """Report-only mode returns success even with violations.

        Given: A package with a forbidden import,
        When: run_scan executes without strict mode,
        Then: It returns exit code 0.
        """
        _write(tmp_path, "runner.py", "import zmq\n")
        assert boundary.run_scan(tmp_path, strict_mode=False) == 0

    def test_run_scan_clean_package_returns_zero(self, tmp_path: Path) -> None:
        """A clean package passes strict mode.

        Given: A package whose only import is allowlisted,
        When: run_scan executes in strict mode,
        Then: It returns exit code 0.
        """
        _write(tmp_path, "registration.py", "from snapper.core.types import ProcessRoleEnum\n")
        assert boundary.run_scan(tmp_path, strict_mode=True) == 0

    def test_real_delegate_package_is_clean(self) -> None:
        """The shipped delegate package passes its own boundary.

        Given: The real repository root,
        When: The delegate package is scanned in strict mode,
        Then: It reports no violations.
        """
        root = Path(boundary.__file__).resolve().parent.parent
        assert boundary.run_scan(root, strict_mode=True) == 0

    def test_main_strict_passes_on_the_real_repository(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The strict CLI entry point returns success on the shipped tree.

        Given: The process invoked with the --strict flag,
        When: main resolves the repository root and scans it,
        Then: It returns exit code 0 because the real package is clean.
        """
        monkeypatch.setattr(boundary.sys, "argv", ["check_delegate_boundary.py", "--strict"])
        assert boundary.main() == 0
