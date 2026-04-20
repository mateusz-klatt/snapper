"""Tests for the vendor-neutrality scanner script (plan §3.11)."""

from pathlib import Path
from unittest.mock import patch

import pytest

import scripts.check_vendor_neutral as checker


class TestIterPythonFiles:
    """Test suite for iter_python_files functionality."""

    def test_collects_files_under_scan_root(self, tmp_path: Path) -> None:
        """Verify iter_python_files collects *.py under src/snapper/.

        Given: Python files under ``src/snapper/``,
        When: ``iter_python_files`` is called,
        Then: It returns the files in sorted order.
        """
        src_dir = tmp_path / "src" / "snapper" / "services"
        src_dir.mkdir(parents=True)
        (src_dir / "a.py").write_text('"""A."""\n')
        (src_dir / "b.py").write_text('"""B."""\n')

        files = checker.iter_python_files(tmp_path)

        assert tmp_path / "src" / "snapper" / "services" / "a.py" in files
        assert tmp_path / "src" / "snapper" / "services" / "b.py" in files

    def test_skips_pycache(self, tmp_path: Path) -> None:
        """Verify __pycache__ entries are excluded.

        Given: A compiled cache directory under the scan root,
        When: ``iter_python_files`` is called,
        Then: The cache file is omitted.
        """
        cache_dir = tmp_path / "src" / "snapper" / "__pycache__"
        cache_dir.mkdir(parents=True)
        (cache_dir / "m.py").write_text('"""cached."""\n')

        assert checker.iter_python_files(tmp_path) == []

    def test_skips_migrations(self, tmp_path: Path) -> None:
        """Verify migrations directory is excluded.

        Given: A migration file under the scan root,
        When: ``iter_python_files`` is called,
        Then: The migration file is omitted.
        """
        mig_dir = tmp_path / "src" / "snapper" / "migrations"
        mig_dir.mkdir(parents=True)
        (mig_dir / "0001_init.py").write_text('"""mig."""\n')

        assert checker.iter_python_files(tmp_path) == []

    def test_returns_empty_when_scan_root_missing(self, tmp_path: Path) -> None:
        """Verify an empty list is returned when the scan root is absent.

        Given: A project root without ``src/snapper``,
        When: ``iter_python_files`` is called,
        Then: It returns ``[]``.
        """
        assert checker.iter_python_files(tmp_path) == []


class TestCheckFileShouldFail:
    """Regression cases from plan §3.11 that must trip the scanner."""

    def test_import_anthropic_flagged(self, tmp_path: Path) -> None:
        """Verify ``import anthropic`` is flagged.

        Given: A file that imports the anthropic SDK,
        When: ``check_file`` runs,
        Then: One violation is returned.
        """
        f = tmp_path / "x.py"
        f.write_text("import anthropic\n")

        findings = checker.check_file(f)

        assert len(findings) == 1
        assert findings[0][1].lower() == "anthropic"

    def test_openai_api_key_flagged(self, tmp_path: Path) -> None:
        """Verify OpenAI API key constants are flagged.

        Given: A file declaring OPENAI_API_KEY,
        When: ``check_file`` runs,
        Then: One violation is returned.
        """
        f = tmp_path / "x.py"
        f.write_text('OPENAI_API_KEY = "sk-..."\n')

        findings = checker.check_file(f)

        assert len(findings) == 1
        assert findings[0][1].lower() == "openai"

    def test_claude_code_integration_flagged(self, tmp_path: Path) -> None:
        """Verify ``claude-code`` vendor references are flagged.

        Given: A file mentioning claude-code integration,
        When: ``check_file`` runs,
        Then: One violation is returned.
        """
        f = tmp_path / "x.py"
        f.write_text('DESCRIPTION = "claude-code integration"\n')

        findings = checker.check_file(f)

        assert len(findings) == 1

    def test_claude_desktop_flagged(self, tmp_path: Path) -> None:
        """Verify ``claude desktop`` references without allowlist are flagged.

        Given: A file mentioning Claude Desktop with no allowlist marker,
        When: ``check_file`` runs,
        Then: One violation is returned.
        """
        f = tmp_path / "x.py"
        f.write_text('NAME = "Claude Desktop client"\n')

        findings = checker.check_file(f)

        assert len(findings) == 1

    def test_gemini_api_flagged(self, tmp_path: Path) -> None:
        """Verify ``gemini api`` is flagged.

        Given: A file mentioning the Gemini API surface,
        When: ``check_file`` runs,
        Then: One violation is returned.
        """
        f = tmp_path / "x.py"
        f.write_text("USE_GEMINI_API = True\n")

        findings = checker.check_file(f)

        assert len(findings) == 1

    def test_copilot_api_flagged(self, tmp_path: Path) -> None:
        """Verify ``copilot api`` is flagged.

        Given: A file mentioning the Copilot API,
        When: ``check_file`` runs,
        Then: One violation is returned.
        """
        f = tmp_path / "x.py"
        f.write_text('COPILOT_API_ENDPOINT = "..."\n')

        findings = checker.check_file(f)

        assert len(findings) == 1


class TestCheckFileShouldPass:
    """Safe forms that must NOT trip the scanner."""

    def test_allowlist_marker_exempts_line(self, tmp_path: Path) -> None:
        """Verify a trailing ``vendor-neutral-ok`` exempts the line.

        Given: A vendor-named line with the allowlist marker,
        When: ``check_file`` runs,
        Then: No violations are returned.
        """
        f = tmp_path / "x.py"
        f.write_text('NAME = "Compatible with Claude Desktop"  # vendor-neutral-ok\n')

        assert checker.check_file(f) == []

    def test_allowlist_marker_in_string_literal_does_not_exempt(self, tmp_path: Path) -> None:
        """Marker inside a string literal MUST NOT bypass the scanner.

        Given: a vendor-named line whose only ``vendor-neutral-ok``
            occurrence is inside a string literal (no ``#`` comment),
        When: ``check_file`` runs,
        Then: one violation is returned — closes the R1 finding where
            substring matching the marker would let authors bypass the
            gate with a plain string.
        """
        f = tmp_path / "x.py"
        f.write_text('NAME = "Claude Desktop is not vendor-neutral-ok here"\n')

        findings = checker.check_file(f)

        assert len(findings) == 1

    def test_allowlist_marker_in_string_with_hash_does_not_exempt(self, tmp_path: Path) -> None:
        """A ``#`` *inside* a string literal MUST NOT qualify as a comment.

        Given: a vendor-named line where the ``#`` sits inside a
            string literal (``"Claude Desktop # vendor-neutral-ok"``),
        When: ``check_file`` runs,
        Then: one violation is returned — closes the R2 finding that
            a regex-based ``#`` anchor could not distinguish a real
            Python comment from a ``#`` embedded in a string. The
            tokenizer-based allowlist correctly classifies the ``#``
            as part of the STRING token rather than a COMMENT token.
        """
        f = tmp_path / "x.py"
        f.write_text('NAME = "Claude Desktop # vendor-neutral-ok"\n')

        findings = checker.check_file(f)

        assert len(findings) == 1

    def test_allowlist_marker_in_fstring_does_not_exempt(self, tmp_path: Path) -> None:
        """An f-string carrying the marker MUST NOT exempt the line.

        Given: a vendor-named line where the marker sits inside an
            ``f"..."`` literal,
        When: ``check_file`` runs,
        Then: one violation is returned — f-strings tokenize as a
            ``FSTRING_*`` / ``STRING`` span, never as ``COMMENT``.
        """
        f = tmp_path / "x.py"
        f.write_text('MSG = f"Claude Desktop # vendor-neutral-ok"\n')

        findings = checker.check_file(f)

        assert len(findings) == 1

    def test_tokenizer_error_does_not_false_exempt(self, tmp_path: Path) -> None:
        """An unparseable line MUST NOT silently exempt a vendor reference.

        Given: a syntactically-broken line (dangling string + vendor
            name) that the tokenizer cannot fully decode,
        When: ``check_file`` runs,
        Then: the regex still fires and one violation is returned —
            the allowlist fails closed, not open, on tokenizer errors.
        """
        f = tmp_path / "x.py"
        f.write_text('NAME = "unterminated  anthropic\n')

        findings = checker.check_file(f)

        assert len(findings) == 1

    def test_generic_mcp_phrasing_is_safe(self, tmp_path: Path) -> None:
        """Verify vendor-neutral phrasing is NOT flagged.

        Given: A file describing support for any MCP-compatible client,
        When: ``check_file`` runs,
        Then: No violations are returned.
        """
        f = tmp_path / "x.py"
        f.write_text('"""Supports any MCP-compatible client."""\n')

        assert checker.check_file(f) == []

    def test_asyncio_channel_is_safe(self, tmp_path: Path) -> None:
        """Verify ``asyncio.Channel`` does not false-positive on ``channel[._ -]?plugin``.

        Given: A file referencing ``asyncio.Channel``,
        When: ``check_file`` runs,
        Then: No violations are returned.
        """
        f = tmp_path / "x.py"
        f.write_text("channel = asyncio.Channel()\n")

        assert checker.check_file(f) == []

    def test_unreadable_file_returns_empty(self, tmp_path: Path) -> None:
        """Verify unreadable files return no violations instead of raising.

        Given: A path that cannot be read,
        When: ``check_file`` runs,
        Then: It returns ``[]`` without raising.
        """
        missing = tmp_path / "ghost.py"

        assert checker.check_file(missing) == []


class TestRunScan:
    """End-to-end scan behaviour."""

    def test_strict_mode_passes_on_clean_tree(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify strict mode returns 0 on a clean tree.

        Given: A project root with only vendor-neutral source,
        When: ``run_scan`` is executed in strict mode,
        Then: It returns 0 and prints a success banner.
        """
        src_dir = tmp_path / "src" / "snapper"
        src_dir.mkdir(parents=True)
        (src_dir / "ok.py").write_text('"""Vendor-neutral module."""\n')

        result = checker.run_scan(tmp_path, strict_mode=True)

        assert result == 0
        assert "Core stays vendor-neutral." in capsys.readouterr().out

    def test_strict_mode_fails_on_violation(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify strict mode returns 1 when a violation is found.

        Given: A project root with a vendor-specific import,
        When: ``run_scan`` is executed in strict mode,
        Then: It returns 1 and surfaces the offending file.
        """
        src_dir = tmp_path / "src" / "snapper" / "services"
        src_dir.mkdir(parents=True)
        (src_dir / "bad.py").write_text("import anthropic\n")

        result = checker.run_scan(tmp_path, strict_mode=True)

        captured = capsys.readouterr()
        assert result == 1
        assert "services/bad.py" in captured.out
        assert "STRICT MODE" in captured.out

    def test_report_mode_returns_zero_with_violations(self, tmp_path: Path) -> None:
        """Verify report mode returns 0 even when violations exist.

        Given: A project root with a violation,
        When: ``run_scan`` is executed without strict mode,
        Then: It returns 0.
        """
        src_dir = tmp_path / "src" / "snapper" / "services"
        src_dir.mkdir(parents=True)
        (src_dir / "bad.py").write_text("import anthropic\n")

        assert checker.run_scan(tmp_path, strict_mode=False) == 0


class TestMain:
    """Test suite for the ``main`` entry point."""

    def test_passes_strict_flag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify ``main`` forwards ``--strict`` to run_scan.

        Given: ``sys.argv`` contains ``--strict``,
        When: ``main`` is called,
        Then: ``run_scan`` is invoked with ``strict_mode=True``.
        """
        monkeypatch.setattr(checker.sys, "argv", ["prog", "--strict"])
        with patch("scripts.check_vendor_neutral.run_scan", return_value=0) as mock_run:
            result = checker.main()

        assert result == 0
        assert mock_run.call_args.kwargs.get("strict_mode", mock_run.call_args.args[1]) is True

    def test_defaults_to_report_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify ``main`` defaults to report mode without ``--strict``.

        Given: ``sys.argv`` without ``--strict``,
        When: ``main`` is called,
        Then: ``run_scan`` is invoked with ``strict_mode=False``.
        """
        monkeypatch.setattr(checker.sys, "argv", ["prog"])
        with patch("scripts.check_vendor_neutral.run_scan", return_value=0) as mock_run:
            result = checker.main()

        assert result == 0
        assert mock_run.call_args.kwargs.get("strict_mode", mock_run.call_args.args[1]) is False
