"""Tests for temporal mutation checker script."""

from pathlib import Path
from unittest.mock import patch

import pytest

import scripts.check_temporal_mutations as checker


class TestShouldSkipPath:
    """Test suite for should_skip_path functionality."""

    def test_skips_whitelisted_file(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns True for whitelisted file names.

        Given: A file path with a whitelisted filename like models.py,
        When: should_skip_path is called,
        Then: It returns True.
        """
        python_file = tmp_path / "src" / "snapper" / "data" / "models.py"
        assert checker.should_skip_path(python_file) is True

    def test_skips_repository_file(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns True for repository.py.

        Given: A file path named repository.py,
        When: should_skip_path is called,
        Then: It returns True.
        """
        python_file = tmp_path / "src" / "snapper" / "data" / "repository.py"
        assert checker.should_skip_path(python_file) is True

    def test_skips_migrations_directory(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns True for migration files.

        Given: A file path inside a migrations directory,
        When: should_skip_path is called,
        Then: It returns True.
        """
        python_file = tmp_path / "src" / "snapper" / "data" / "migrations" / "0001_init.py"
        assert checker.should_skip_path(python_file) is True

    def test_skips_pycache_directory(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns True for __pycache__ directories.

        Given: A file path inside __pycache__,
        When: should_skip_path is called,
        Then: It returns True.
        """
        python_file = tmp_path / "__pycache__" / "module.pyc"
        assert checker.should_skip_path(python_file) is True

    def test_does_not_skip_symbol_updater_base(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns False for symbol updater base.

        Given: A file path for the symbol updater base module,
        When: should_skip_path is called,
        Then: It returns False (scanned at line level for violations).
        """
        python_file = (
            tmp_path / "src" / "snapper" / "application" / "updaters" / "symbols" / "base.py"
        )
        assert checker.should_skip_path(python_file) is False

    def test_does_not_skip_normal_file(self, tmp_path: Path) -> None:
        """Verify should_skip_path returns False for normal source files.

        Given: A file path that is not whitelisted or in a skipped directory,
        When: should_skip_path is called,
        Then: It returns False.
        """
        python_file = tmp_path / "src" / "snapper" / "services" / "settings.py"
        assert checker.should_skip_path(python_file) is False


class TestIterPythonFiles:
    """Test suite for iter_python_files functionality."""

    def test_collects_files_under_scan_root(self, tmp_path: Path) -> None:
        """Verify iter_python_files collects files under src/snapper/.

        Given: Python files under src/snapper/ and a whitelisted file,
        When: iter_python_files is called,
        Then: It returns only the non-whitelisted files in sorted order.
        """
        src_dir = tmp_path / "src" / "snapper" / "services"
        src_dir.mkdir(parents=True)
        (src_dir / "a.py").write_text('"""Module A."""\n')
        (src_dir / "b.py").write_text('"""Module B."""\n')
        models_dir = tmp_path / "src" / "snapper" / "data"
        models_dir.mkdir(parents=True)
        (models_dir / "models.py").write_text('"""Models."""\n')

        files = checker.iter_python_files(tmp_path)

        assert tmp_path / "src" / "snapper" / "services" / "a.py" in files
        assert tmp_path / "src" / "snapper" / "services" / "b.py" in files
        assert tmp_path / "src" / "snapper" / "data" / "models.py" not in files

    def test_returns_empty_when_scan_root_missing(self, tmp_path: Path) -> None:
        """Verify iter_python_files returns empty list when scan root is absent.

        Given: A project root with no src/snapper directory,
        When: iter_python_files is called,
        Then: It returns an empty list.
        """
        files = checker.iter_python_files(tmp_path)

        assert files == []


class TestIsWhitelistedLine:
    """Test suite for _is_whitelisted_line functionality."""

    def test_mapped_column_is_whitelisted(self) -> None:
        """Verify lines with Mapped[ are whitelisted.

        Given: A line containing a Mapped type annotation,
        When: _is_whitelisted_line is called,
        Then: It returns True.
        """
        assert (
            checker._is_whitelisted_line("    value: Mapped[str] = mapped_column(String(1024))")
            is True
        )

    def test_dict_assignment_is_whitelisted(self) -> None:
        """Verify dict-style assignments are whitelisted.

        Given: A line with a dict key assignment,
        When: _is_whitelisted_line is called,
        Then: It returns True.
        """
        assert checker._is_whitelisted_line('    new_values["value"] = "test"') is True

    def test_known_to_is_whitelisted(self) -> None:
        """Verify lines containing known_to are whitelisted.

        Given: A line referencing known_to,
        When: _is_whitelisted_line is called,
        Then: It returns True.
        """
        assert checker._is_whitelisted_line("    row.known_to = KNOWN_TO_MAX") is True

    def test_default_assignment_is_whitelisted(self) -> None:
        """Verify lines with default= are whitelisted.

        Given: A line containing default= keyword,
        When: _is_whitelisted_line is called,
        Then: It returns True.
        """
        assert (
            checker._is_whitelisted_line(
                "    is_active: Mapped[bool] = mapped_column(default=True)"
            )
            is True
        )

    def test_def_line_is_whitelisted(self) -> None:
        """Verify function definition lines are whitelisted.

        Given: A line with a function definition,
        When: _is_whitelisted_line is called,
        Then: It returns True.
        """
        assert checker._is_whitelisted_line("    def set_value(self, value: str) -> None:") is True

    def test_class_line_is_whitelisted(self) -> None:
        """Verify class definition lines are whitelisted.

        Given: A line with a class definition,
        When: _is_whitelisted_line is called,
        Then: It returns True.
        """
        assert checker._is_whitelisted_line("class SymbolExchangeCapability(Base):") is True

    def test_docstring_line_is_whitelisted(self) -> None:
        """Verify docstring lines are whitelisted.

        Given: A line containing triple quotes,
        When: _is_whitelisted_line is called,
        Then: It returns True.
        """
        assert checker._is_whitelisted_line('    """Set the value attribute."""') is True

    def test_server_default_is_whitelisted(self) -> None:
        """Verify server_default= is whitelisted.

        Given: A line containing server_default=,
        When: _is_whitelisted_line is called,
        Then: It returns True.
        """
        line = '    asset_type: Mapped[str] = mapped_column(String(16), server_default="crypto")'
        assert checker._is_whitelisted_line(line) is True

    def test_mutation_line_is_not_whitelisted(self) -> None:
        """Verify direct attribute mutation lines are not whitelisted.

        Given: A line with direct attribute mutation on a temporal field,
        When: _is_whitelisted_line is called,
        Then: It returns False.
        """
        assert checker._is_whitelisted_line("    setting.value = new_val") is False


class TestCheckFile:
    """Test suite for check_file functionality."""

    def test_detects_value_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects Setting.value mutation.

        Given: A file containing a direct .value = assignment,
        When: check_file is called,
        Then: It returns a violation for Setting.value.
        """
        python_file = tmp_path / "service.py"
        python_file.write_text("setting.value = new_val\n")

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert findings[0][0] == 1
        assert findings[0][1] == "Setting.value"

    def test_detects_password_hash_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects User.password_hash mutation.

        Given: A file containing a direct .password_hash = assignment,
        When: check_file is called,
        Then: It returns a violation for User.password_hash.
        """
        python_file = tmp_path / "auth.py"
        python_file.write_text("user.password_hash = hashed\n")

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert findings[0][1] == "User.password_hash"

    def test_detects_email_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects User.email mutation.

        Given: A file with .email = assignment,
        When: check_file is called,
        Then: It returns a violation for User.email.
        """
        python_file = tmp_path / "user_svc.py"
        python_file.write_text("user.email = new_email\n")

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert findings[0][1] == "User.email"

    def test_detects_role_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects User.role mutation.

        Given: A file with .role = assignment,
        When: check_file is called,
        Then: It returns a violation for User.role.
        """
        python_file = tmp_path / "user_svc.py"
        python_file.write_text("user.role = new_role\n")

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert findings[0][1] == "User.role"

    def test_detects_is_active_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects User.is_active mutation.

        Given: A file with .is_active = assignment,
        When: check_file is called,
        Then: It returns a violation for User.is_active.
        """
        python_file = tmp_path / "user_svc.py"
        python_file.write_text("user.is_active = False\n")

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert findings[0][1] == "User.is_active"

    def test_detects_exchange_symbol_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects SymbolAlias.exchange_symbol mutation.

        Given: A file with .exchange_symbol = assignment,
        When: check_file is called,
        Then: It returns a violation for SymbolAlias.exchange_symbol.
        """
        python_file = tmp_path / "symbol_svc.py"
        python_file.write_text("alias.exchange_symbol = new_sym\n")

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert findings[0][1] == "SymbolAlias.exchange_symbol"

    def test_detects_can_trade_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects SymbolExchangeCapability.can_trade mutation.

        Given: A file with .can_trade = assignment,
        When: check_file is called,
        Then: It returns a violation for SymbolExchangeCapability.can_trade.
        """
        python_file = tmp_path / "cap_svc.py"
        python_file.write_text("cap.can_trade = True\n")

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert findings[0][1] == "SymbolExchangeCapability.can_trade"

    def test_detects_can_market_data_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects SymbolExchangeCapability.can_market_data mutation.

        Given: A file with .can_market_data = assignment,
        When: check_file is called,
        Then: It returns a violation for SymbolExchangeCapability.can_market_data.
        """
        python_file = tmp_path / "cap_svc.py"
        python_file.write_text("cap.can_market_data = False\n")

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert findings[0][1] == "SymbolExchangeCapability.can_market_data"

    def test_detects_source_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects source attribute mutation.

        Given: A file with .source = assignment,
        When: check_file is called,
        Then: It returns a violation for SymbolExchangeCapability.source.
        """
        python_file = tmp_path / "cap_svc.py"
        python_file.write_text('cap.source = "updater"\n')

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert findings[0][1] == "SymbolExchangeCapability.source"

    def test_detects_base_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects base attribute mutation.

        Given: A file with .base = assignment,
        When: check_file is called,
        Then: It returns a violation for Instrument.base / SymbolVersion.base.
        """
        python_file = tmp_path / "inst_svc.py"
        python_file.write_text('instrument.base = "BTC"\n')

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert "base" in findings[0][1]

    def test_detects_quote_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects quote attribute mutation.

        Given: A file with .quote = assignment,
        When: check_file is called,
        Then: It returns a violation for Instrument.quote / SymbolVersion.quote.
        """
        python_file = tmp_path / "inst_svc.py"
        python_file.write_text('catalog.quote = "USD"\n')

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert "quote" in findings[0][1]

    def test_detects_asset_type_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects SymbolVersion.asset_type mutation.

        Given: A file with .asset_type = assignment,
        When: check_file is called,
        Then: It returns a violation for SymbolVersion.asset_type.
        """
        python_file = tmp_path / "catalog_svc.py"
        python_file.write_text('catalog.asset_type = "equity"\n')

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert findings[0][1] == "SymbolVersion.asset_type"

    def test_detects_reason_mutation(self, tmp_path: Path) -> None:
        """Verify check_file detects reason attribute mutation.

        Given: A file with .reason = assignment,
        When: check_file is called,
        Then: It returns a violation for reason attributes.
        """
        python_file = tmp_path / "svc.py"
        python_file.write_text('cap.reason = "stale"\n')

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert "reason" in findings[0][1]

    def test_detects_session_delete(self, tmp_path: Path) -> None:
        """Verify check_file detects session.delete() calls.

        Given: A file containing session.delete(),
        When: check_file is called,
        Then: It returns a violation for session.delete().
        """
        python_file = tmp_path / "service.py"
        python_file.write_text("session.delete(record)\n")

        findings = checker.check_file(python_file)

        assert len(findings) == 1
        assert findings[0][1] == "session.delete()"

    def test_ignores_whitelisted_lines(self, tmp_path: Path) -> None:
        """Verify check_file ignores lines matching whitelist patterns.

        Given: A file with Mapped type annotations and dict assignments,
        When: check_file is called,
        Then: It returns no violations.
        """
        python_file = tmp_path / "clean.py"
        python_file.write_text(
            "value: Mapped[str] = mapped_column(String(1024))\n"
            'new_values["value"] = "test"\n'
            "row.known_to = KNOWN_TO_MAX\n"
        )

        findings = checker.check_file(python_file)

        assert findings == []

    def test_ignores_equality_comparison(self, tmp_path: Path) -> None:
        """Verify check_file ignores == comparison operators.

        Given: A file with .value == comparison,
        When: check_file is called,
        Then: It returns no violations because == is not an assignment.
        """
        python_file = tmp_path / "query.py"
        python_file.write_text('if setting.value == "expected":\n')

        findings = checker.check_file(python_file)

        assert findings == []

    def test_returns_empty_for_missing_file(self, tmp_path: Path) -> None:
        """Verify missing file is handled gracefully.

        Given: A file path that does not exist,
        When: check_file is called,
        Then: It returns an empty list.
        """
        missing = tmp_path / "missing.py"
        assert checker.check_file(missing) == []

    def test_multiple_violations_in_one_file(self, tmp_path: Path) -> None:
        """Verify check_file detects multiple violations in a single file.

        Given: A file with two forbidden mutations,
        When: check_file is called,
        Then: It returns two violations with correct line numbers.
        """
        python_file = tmp_path / "bad.py"
        python_file.write_text("setting.value = new_val\nuser.password_hash = hashed\n")

        findings = checker.check_file(python_file)

        assert len(findings) == 2
        assert findings[0][0] == 1
        assert findings[1][0] == 2


class TestScanFiles:
    """Test suite for scan_files functionality."""

    def test_scans_src_snapper_directory(self, tmp_path: Path) -> None:
        """Verify scan_files scans the src/snapper directory.

        Given: A file with violations under src/snapper/,
        When: scan_files is called,
        Then: It reports the violations.
        """
        src_dir = tmp_path / "src" / "snapper" / "services"
        src_dir.mkdir(parents=True)
        bad_file = src_dir / "settings_service.py"
        bad_file.write_text("setting.value = new_val\n")

        results = checker.scan_files(tmp_path)

        assert bad_file in results

    def test_skips_whitelisted_files(self, tmp_path: Path) -> None:
        """Verify scan_files skips whitelisted files like models.py.

        Given: A models.py file with patterns that would normally be flagged,
        When: scan_files is called,
        Then: It does not report violations for the whitelisted file.
        """
        data_dir = tmp_path / "src" / "snapper" / "data"
        data_dir.mkdir(parents=True)
        (data_dir / "models.py").write_text("setting.value = new_val\n")

        results = checker.scan_files(tmp_path)

        assert not results

    def test_returns_empty_for_clean_codebase(self, tmp_path: Path) -> None:
        """Verify scan_files returns empty results for a clean codebase.

        Given: Source files with no forbidden patterns,
        When: scan_files is called,
        Then: It returns an empty dictionary.
        """
        src_dir = tmp_path / "src" / "snapper" / "services"
        src_dir.mkdir(parents=True)
        (src_dir / "clean.py").write_text('"""Clean module."""\n')

        results = checker.scan_files(tmp_path)

        assert results == {}


class TestPrintResults:
    """Test suite for print_results functionality."""

    def test_returns_zero_when_no_results(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify print_results returns 0 for empty results.

        Given: An empty results mapping,
        When: print_results is called,
        Then: It returns 0 and prints a clean message.
        """
        count = checker.print_results({}, tmp_path)

        assert count == 0
        captured = capsys.readouterr()
        assert "No temporal mutation violations found" in captured.out

    def test_counts_findings_and_truncates_long_lines(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Verify print_results counts findings and truncates long output.

        Given: A results mapping with violations including a long line,
        When: print_results is called,
        Then: It returns the total count and prints an ellipsis for long lines.
        """
        python_file = tmp_path / "src" / "snapper" / "svc.py"
        python_file.parent.mkdir(parents=True)
        long_line = "x" * 200
        results = {
            python_file: [
                (1, "Setting.value", "setting.value = x"),
                (2, "User.email", long_line),
            ]
        }

        count = checker.print_results(results, tmp_path)

        assert count == 2
        captured = capsys.readouterr()
        assert "L1: [Setting.value]" in captured.out
        assert "..." in captured.out


class TestRunScan:
    """Test suite for run_scan functionality."""

    def test_returns_zero_in_strict_mode_when_clean(self, tmp_path: Path) -> None:
        """Verify strict mode passes on a clean project tree.

        Given: A project root with no temporal mutation violations,
        When: run_scan is executed in strict mode,
        Then: It returns 0.
        """
        src_dir = tmp_path / "src" / "snapper"
        src_dir.mkdir(parents=True)
        (src_dir / "clean.py").write_text('"""Clean module."""\n')

        result = checker.run_scan(tmp_path, strict_mode=True)

        assert result == 0

    def test_returns_one_in_strict_mode_when_violations_found(self, tmp_path: Path) -> None:
        """Verify strict mode fails when violations are present.

        Given: A project root with a forbidden mutation,
        When: run_scan is executed in strict mode,
        Then: It returns 1.
        """
        src_dir = tmp_path / "src" / "snapper" / "services"
        src_dir.mkdir(parents=True)
        (src_dir / "bad.py").write_text("setting.value = new_val\n")

        result = checker.run_scan(tmp_path, strict_mode=True)

        assert result == 1

    def test_returns_zero_in_report_mode_when_violations_found(self, tmp_path: Path) -> None:
        """Verify report mode does not fail when violations are present.

        Given: A project root with a forbidden mutation,
        When: run_scan is executed without strict mode,
        Then: It returns 0.
        """
        src_dir = tmp_path / "src" / "snapper" / "services"
        src_dir.mkdir(parents=True)
        (src_dir / "bad.py").write_text("setting.value = new_val\n")

        result = checker.run_scan(tmp_path, strict_mode=False)

        assert result == 0


class TestMain:
    """Test suite for main entry point."""

    def test_passes_strict_flag_to_run_scan(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify main passes strict flag to run_scan.

        Given: sys.argv contains '--strict',
        When: main is called,
        Then: run_scan is invoked with strict_mode=True.
        """
        monkeypatch.setattr(checker.sys, "argv", ["prog", "--strict"])
        with patch("scripts.check_temporal_mutations.run_scan", return_value=0) as mock_run:
            result = checker.main()

        assert result == 0
        assert mock_run.call_args.args[1] is True

    def test_defaults_to_report_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify main defaults to report mode when no flag is provided.

        Given: sys.argv without '--strict',
        When: main is called,
        Then: run_scan is invoked with strict_mode=False.
        """
        monkeypatch.setattr(checker.sys, "argv", ["prog"])
        with patch("scripts.check_temporal_mutations.run_scan", return_value=0) as mock_run:
            result = checker.main()

        assert result == 0
        assert mock_run.call_args.args[1] is False


class TestPathSpecificWhitelist:
    """Test suite for path-specific line whitelist functionality."""

    def test_mutation_flagged_in_any_file(self, tmp_path: Path) -> None:
        """Verify attribute mutations are flagged in all files (no path-specific whitelist).

        Given: A file with .base = assignment,
        When: check_file is called,
        Then: Violation reported.
        """
        other_file = tmp_path / "other_service.py"
        other_file.write_text("existing.base = base\n")

        findings = checker.check_file(other_file)

        assert len(findings) == 1
        assert "base" in findings[0][1]

    def test_setting_mutation_flagged_in_base_py(self, tmp_path: Path) -> None:
        """Verify Setting mutations are flagged in updater base file.

        Given: A file at updaters/symbols/base.py with .value = assignment,
        When: check_file is called,
        Then: Violation reported for Setting.value.
        """
        base_dir = tmp_path / "application" / "updaters" / "symbols"
        base_dir.mkdir(parents=True)
        base_file = base_dir / "base.py"
        base_file.write_text("setting.value = new_val\n")

        findings = checker.check_file(base_file)

        assert len(findings) == 1
        assert findings[0][1] == "Setting.value"


class TestKnownToMaxSelectDetection:
    """Test suite for KNOWN_TO_MAX SELECT/WHERE detection."""

    def test_detects_equality_comparison(self, tmp_path: Path) -> None:
        """Verify check_file detects KNOWN_TO_MAX equality in SELECT context.

        Given: A file with == KNOWN_TO_MAX comparison,
        When: check_file is called,
        Then: Violation reported for KNOWN_TO_MAX in SELECT/WHERE.
        """
        python_file = tmp_path / "query.py"
        python_file.write_text("User.known_to == KNOWN_TO_MAX,\n")

        findings = checker.check_file(python_file)

        known_to_findings = [f for f in findings if "KNOWN_TO_MAX" in f[1]]
        assert len(known_to_findings) == 1
        assert known_to_findings[0][1] == "KNOWN_TO_MAX in SELECT/WHERE"

    def test_detects_inequality_comparison(self, tmp_path: Path) -> None:
        """Verify check_file detects KNOWN_TO_MAX inequality comparison.

        Given: A file with != KNOWN_TO_MAX comparison,
        When: check_file is called,
        Then: Violation reported for KNOWN_TO_MAX in SELECT/WHERE.
        """
        python_file = tmp_path / "query.py"
        python_file.write_text("cap.known_to != KNOWN_TO_MAX\n")

        findings = checker.check_file(python_file)

        known_to_findings = [f for f in findings if "KNOWN_TO_MAX" in f[1]]
        assert len(known_to_findings) == 1
        assert known_to_findings[0][1] == "KNOWN_TO_MAX in SELECT/WHERE"

    def test_whitelists_dict_insert_default(self, tmp_path: Path) -> None:
        """Verify dict assignment with KNOWN_TO_MAX is not flagged.

        Given: A file with dict-style known_to assignment for INSERT,
        When: check_file is called,
        Then: No KNOWN_TO_MAX violation reported.
        """
        python_file = tmp_path / "repo.py"
        python_file.write_text('r["known_to"] = KNOWN_TO_MAX\n')

        findings = checker.check_file(python_file)

        known_to_findings = [f for f in findings if "KNOWN_TO_MAX" in f[1]]
        assert known_to_findings == []

    def test_whitelists_new_values_insert_default(self, tmp_path: Path) -> None:
        """Verify new_values dict assignment with KNOWN_TO_MAX is not flagged.

        Given: A file with new_values known_to assignment for INSERT,
        When: check_file is called,
        Then: No KNOWN_TO_MAX violation reported.
        """
        python_file = tmp_path / "repo.py"
        python_file.write_text('new_values["known_to"] = KNOWN_TO_MAX\n')

        findings = checker.check_file(python_file)

        known_to_findings = [f for f in findings if "KNOWN_TO_MAX" in f[1]]
        assert known_to_findings == []

    def test_detects_reversed_equality(self, tmp_path: Path) -> None:
        """Verify detection works with KNOWN_TO_MAX on the left side.

        Given: A file with KNOWN_TO_MAX == comparison (reversed order),
        When: check_file is called,
        Then: Violation reported.
        """
        python_file = tmp_path / "query.py"
        python_file.write_text("KNOWN_TO_MAX == cap.known_to\n")

        findings = checker.check_file(python_file)

        known_to_findings = [f for f in findings if "KNOWN_TO_MAX" in f[1]]
        assert len(known_to_findings) == 1


class TestIsKnownToMaxSelectWhitelisted:
    """Test suite for _is_known_to_max_select_whitelisted."""

    def test_dict_assignment_whitelisted(self) -> None:
        """Verify dict-style known_to INSERT default is whitelisted.

        Given: A line assigning KNOWN_TO_MAX to dict key,
        When: _is_known_to_max_select_whitelisted is called,
        Then: Returns True.
        """
        assert checker._is_known_to_max_select_whitelisted('r["known_to"] = KNOWN_TO_MAX')

    def test_new_values_assignment_whitelisted(self) -> None:
        """Verify new_values INSERT default is whitelisted.

        Given: A line assigning KNOWN_TO_MAX to new_values dict,
        When: _is_known_to_max_select_whitelisted is called,
        Then: Returns True.
        """
        assert checker._is_known_to_max_select_whitelisted('new_values["known_to"] = KNOWN_TO_MAX')

    def test_select_comparison_not_whitelisted(self) -> None:
        """Verify SELECT equality comparison is not whitelisted.

        Given: A line with == KNOWN_TO_MAX in a WHERE clause,
        When: _is_known_to_max_select_whitelisted is called,
        Then: Returns False.
        """
        assert not checker._is_known_to_max_select_whitelisted("User.known_to == KNOWN_TO_MAX,")


class TestCheckLine:
    """Test suite for _check_line helper function."""

    def test_skips_whitelisted_line(self, tmp_path: Path) -> None:
        """Verify _check_line skips globally whitelisted patterns.

        Given: A line with a function definition containing a forbidden word,
        When: _check_line is called,
        Then: No violation added.
        """
        violations: list[checker.Violation] = []
        filepath = tmp_path / "test.py"
        checker._check_line(1, "    def set_value(self) -> None:", filepath, violations)
        assert violations == []

    def test_detects_mutation(self, tmp_path: Path) -> None:
        """Verify _check_line detects forbidden mutations.

        Given: A line with a forbidden attribute assignment,
        When: _check_line is called,
        Then: Violation added to the list.
        """
        violations: list[checker.Violation] = []
        filepath = tmp_path / "test.py"
        checker._check_line(5, "setting.value = new_val", filepath, violations)
        assert len(violations) == 1
        assert violations[0][0] == 5

    def test_detects_delete(self, tmp_path: Path) -> None:
        """Verify _check_line detects session.delete calls.

        Given: A line with session.delete(),
        When: _check_line is called,
        Then: Violation added for session.delete().
        """
        violations: list[checker.Violation] = []
        filepath = tmp_path / "test.py"
        checker._check_line(3, "session.delete(obj)", filepath, violations)
        assert len(violations) == 1
        assert violations[0][1] == "session.delete()"

    def test_detects_known_to_max_select(self, tmp_path: Path) -> None:
        """Verify _check_line detects KNOWN_TO_MAX in SELECT context.

        Given: A line with == KNOWN_TO_MAX comparison,
        When: _check_line is called,
        Then: Violation added for KNOWN_TO_MAX in SELECT/WHERE.
        """
        violations: list[checker.Violation] = []
        filepath = tmp_path / "test.py"
        checker._check_line(7, "User.known_to == KNOWN_TO_MAX,", filepath, violations)
        known_to_violations = [v for v in violations if "KNOWN_TO_MAX" in v[1]]
        assert len(known_to_violations) == 1


class TestPathSuffix:
    """Test suite for _path_suffix helper."""

    def test_returns_posix_path(self, tmp_path: Path) -> None:
        """Verify _path_suffix returns POSIX-formatted path string.

        Given: A Path object,
        When: _path_suffix is called,
        Then: Returns the POSIX string representation.
        """
        filepath = tmp_path / "src" / "snapper" / "base.py"
        result = checker._path_suffix(filepath)
        assert result.endswith("src/snapper/base.py")


class TestSuccessMessage:
    """Test suite for updated success message."""

    def test_success_message_text(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify the success message uses updated wording.

        Given: A clean project tree with no violations,
        When: run_scan is called,
        Then: Success message reads 'No forbidden temporal mutations found.'
        """
        src_dir = tmp_path / "src" / "snapper"
        src_dir.mkdir(parents=True)
        (src_dir / "clean.py").write_text('"""Clean module."""\n')

        checker.run_scan(tmp_path, strict_mode=False)

        captured = capsys.readouterr()
        assert "No forbidden temporal mutations found." in captured.out
        assert "Bitemporal integrity enforced" not in captured.out
