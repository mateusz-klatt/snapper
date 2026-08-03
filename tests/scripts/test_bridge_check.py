"""Tests for the standalone scripts/bridge_check/ scripts.

The drift + OSS-prose scripts are the verification gate that
``make bridge-check`` runs. They are tested at the function level
(``compute_drift`` / ``scan_forbidden_tokens``) and at the CLI level
(``main`` returning the documented exit codes 0/1/2).
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.bridge_check import check_drift
from scripts.bridge_check import check_oss_prose
from scripts.generate_types import generate_bridge_wire_contract


@pytest.fixture
def fresh_wire_contract(tmp_path: Path) -> Path:
    """Generate a fresh wire-contract file for the drift script to compare against."""
    target = tmp_path / "wire-contract.ts"
    generate_bridge_wire_contract(target, tmp_path)
    return target


class TestComputeDrift:
    """Unit tests for ``check_drift.compute_drift``."""

    def test_match_returns_true_and_empty_diff(self, fresh_wire_contract: Path) -> None:
        """A freshly-generated file matches itself byte-for-byte."""
        matches, diff = check_drift.compute_drift(
            fresh_wire_contract,
            fresh_wire_contract.parent,
        )
        assert matches is True
        assert diff == ""

    def test_drift_returns_false_with_unified_diff(self, fresh_wire_contract: Path) -> None:
        """A modified working-tree file produces a unified diff."""
        original = fresh_wire_contract.read_text(encoding="utf-8")
        fresh_wire_contract.write_text(original + "\n// stray manual edit\n", encoding="utf-8")
        matches, diff = check_drift.compute_drift(
            fresh_wire_contract,
            fresh_wire_contract.parent,
        )
        assert matches is False
        assert "stray manual edit" in diff
        assert "<regenerated>" in diff

    def test_missing_target_raises_file_not_found(self, tmp_path: Path) -> None:
        """A missing working-tree file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="not found"):
            check_drift.compute_drift(tmp_path / "missing.ts", tmp_path)


class TestCheckDriftCli:
    """CLI-level tests for ``check_drift.main`` exit codes."""

    def test_exit_zero_when_target_matches(
        self, fresh_wire_contract: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Exit 0 prints OK + the target path."""
        with patch.object(check_drift, "_project_root", return_value=fresh_wire_contract.parent):
            exit_code = check_drift.main(["--target", str(fresh_wire_contract)])
        captured = capsys.readouterr()
        assert exit_code == 0
        assert "OK" in captured.out
        assert str(fresh_wire_contract) in captured.out

    def test_exit_one_when_target_drifts(
        self, fresh_wire_contract: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Exit 1 prints FAIL + the unified diff to stderr."""
        original = fresh_wire_contract.read_text(encoding="utf-8")
        fresh_wire_contract.write_text(
            original + "\nexport interface ManuallyAdded {}\n", encoding="utf-8"
        )
        with patch.object(check_drift, "_project_root", return_value=fresh_wire_contract.parent):
            exit_code = check_drift.main(["--target", str(fresh_wire_contract)])
        captured = capsys.readouterr()
        assert exit_code == 1
        assert "FAIL" in captured.err
        assert "ManuallyAdded" in captured.err

    def test_exit_two_when_target_missing(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Exit 2 surfaces the FileNotFoundError to stderr."""
        target = tmp_path / "missing.ts"
        with patch.object(check_drift, "_project_root", return_value=tmp_path):
            exit_code = check_drift.main(["--target", str(target)])
        captured = capsys.readouterr()
        assert exit_code == 2
        assert "not found" in captured.err

    def test_default_target_resolves_to_project_root(self, tmp_path: Path) -> None:
        """Without ``--target``, the default project-root path is used."""
        with (
            patch.object(check_drift, "_project_root", return_value=tmp_path),
            patch.object(
                check_drift,
                "compute_drift",
                return_value=(True, ""),
            ) as mock_compute,
        ):
            exit_code = check_drift.main([])
        assert exit_code == 0
        called_with = mock_compute.call_args[0][0]
        assert called_with.is_absolute()
        assert called_with.parts[-3:] == ("src", "generated", "wire-contract.ts")

    def test_relative_target_resolves_against_project_root(self, tmp_path: Path) -> None:
        """A relative ``--target`` resolves against the project root."""
        with (
            patch.object(check_drift, "_project_root", return_value=tmp_path),
            patch.object(
                check_drift,
                "compute_drift",
                return_value=(True, ""),
            ) as mock_compute,
        ):
            check_drift.main(["--target", "relative/wire.ts"])
        called_with = mock_compute.call_args[0][0]
        assert called_with == (tmp_path / "relative/wire.ts").resolve()

    def test_exit_two_when_target_escapes_project(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """An explicit target outside the active project fails closed."""
        project_root = tmp_path / "project"
        project_root.mkdir()
        with patch.object(check_drift, "_project_root", return_value=project_root):
            exit_code = check_drift.main(["--target", str(tmp_path / "outside.ts")])
        captured = capsys.readouterr()
        assert exit_code == 2
        assert "escapes trusted directory" in captured.err


class TestScanForbiddenTokens:
    """Unit tests for ``check_oss_prose.scan_forbidden_tokens``."""

    def test_clean_file_returns_empty_findings(self, fresh_wire_contract: Path) -> None:
        """The autogenerator's output contains no forbidden tokens by construction."""
        findings = check_oss_prose.scan_forbidden_tokens(
            fresh_wire_contract,
            fresh_wire_contract.parent,
        )
        assert findings == []

    @pytest.mark.parametrize(
        "snippet,pattern_name",
        [
            ("// see Plan A for context", "plan-letter"),
            ("// per §3.5 of the design", "paragraph-anchor"),
            ("// see Q15 for the routing rule", "q-reference"),
            ("// emitted in Phase 2", "phase-reference"),
            ("// from src/snapper/messaging/schemas.py", "snapper-src-path"),
            ("// matches frontend/src/types/ws.generated.ts", "frontend-src-path"),
            ("// see proprietary/plans/foo.md", "proprietary-path"),
            ("// per memory/feedback.md", "memory-path"),
            ("// see session_resume_2026_01_01.md", "session-resume"),
            ("// indexed in MEMORY.md", "memory-index"),
            ("// holzera private remote", "internal-handle"),
        ],
    )
    def test_each_forbidden_token_is_detected(
        self,
        tmp_path: Path,
        snippet: str,
        pattern_name: str,
    ) -> None:
        """Every forbidden pattern surfaces as a finding when present."""
        target = tmp_path / "wire-contract.ts"
        target.write_text(f"// header\n{snippet}\nexport interface X {{}}\n", encoding="utf-8")
        findings = check_oss_prose.scan_forbidden_tokens(target, tmp_path)
        assert any(
            name == pattern_name for _, name, _ in findings
        ), f"expected pattern {pattern_name} to fire on {snippet!r}; got {findings}"

    def test_missing_target_raises_file_not_found(self, tmp_path: Path) -> None:
        """A missing target file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="not found"):
            check_oss_prose.scan_forbidden_tokens(tmp_path / "missing.ts", tmp_path)


class TestCheckOssProseCli:
    """CLI-level tests for ``check_oss_prose.main`` exit codes."""

    def test_exit_zero_on_clean_file(
        self, fresh_wire_contract: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Exit 0 prints OK on a clean working-tree file."""
        with patch.object(
            check_oss_prose,
            "_project_root",
            return_value=fresh_wire_contract.parent,
        ):
            exit_code = check_oss_prose.main(["--target", str(fresh_wire_contract)])
        captured = capsys.readouterr()
        assert exit_code == 0
        assert "OK" in captured.out

    def test_exit_one_on_forbidden_token(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Exit 1 prints FAIL + per-finding lines to stderr."""
        target = tmp_path / "wire-contract.ts"
        target.write_text("// header\n// Plan A reference here\n", encoding="utf-8")
        with patch.object(check_oss_prose, "_project_root", return_value=tmp_path):
            exit_code = check_oss_prose.main(["--target", str(target)])
        captured = capsys.readouterr()
        assert exit_code == 1
        assert "FAIL" in captured.err
        assert "plan-letter" in captured.err
        assert "Plan A reference" in captured.err

    def test_exit_two_when_target_missing(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Exit 2 surfaces the FileNotFoundError to stderr."""
        with patch.object(check_oss_prose, "_project_root", return_value=tmp_path):
            exit_code = check_oss_prose.main(["--target", str(tmp_path / "missing.ts")])
        captured = capsys.readouterr()
        assert exit_code == 2
        assert "not found" in captured.err

    def test_default_target_resolves_to_project_root(self, tmp_path: Path) -> None:
        """Without ``--target``, the default project-root path is used."""
        with (
            patch.object(check_oss_prose, "_project_root", return_value=tmp_path),
            patch.object(
                check_oss_prose,
                "scan_forbidden_tokens",
                return_value=[],
            ) as mock_scan,
        ):
            exit_code = check_oss_prose.main([])
        assert exit_code == 0
        called_with = mock_scan.call_args[0][0]
        assert called_with.parts[-3:] == ("src", "generated", "wire-contract.ts")

    def test_relative_target_resolves_against_project_root(self, tmp_path: Path) -> None:
        """A relative ``--target`` resolves against the project root."""
        with (
            patch.object(check_oss_prose, "_project_root", return_value=tmp_path),
            patch.object(
                check_oss_prose,
                "scan_forbidden_tokens",
                return_value=[],
            ) as mock_scan,
        ):
            check_oss_prose.main(["--target", "relative/wire.ts"])
        called_with = mock_scan.call_args[0][0]
        assert called_with == (tmp_path / "relative/wire.ts").resolve()


class TestProjectRoot:
    """Sanity check: both scripts agree on the repo root location."""

    def test_drift_root_matches_oss_prose_root(self) -> None:
        """``_project_root`` returns the same path from both modules."""
        assert check_drift._project_root() == check_oss_prose._project_root()
        assert (check_drift._project_root() / "scripts" / "generate_types.py").is_file()
