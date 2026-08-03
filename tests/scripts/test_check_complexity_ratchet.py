"""Tests for the Ruff function-complexity debt ratchet."""

import json
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest

import scripts.check_complexity_ratchet as ratchet


def _baseline_document() -> dict[str, object]:
    """Return a minimal valid baseline document."""
    return {
        "schema_version": 1,
        "limits": {
            "C901": 10,
            "PLR0912": 12,
            "PLR0913": 5,
            "PLR0915": 50,
        },
        "violations": {
            "src/example.py": {
                "Service.run": {
                    "C901": 11,
                    "PLR0913": 6,
                }
            }
        },
    }


def _write_baseline(tmp_path: Path, document: object | None = None) -> Path:
    """Write one baseline fixture and return its path."""
    baseline = tmp_path / ratchet.BASELINE_PATH
    baseline.parent.mkdir(parents=True, exist_ok=True)
    baseline.write_text(
        json.dumps(_baseline_document() if document is None else document),
        encoding="utf-8",
    )
    return baseline


def _valid_config_text() -> str:
    """Return a minimal valid Ruff ratchet configuration."""
    return """
[tool.ruff.lint]
select = ["C901", "PLR0912", "PLR0913", "PLR0915"]
ignore = ["E501"]

[tool.ruff.lint.mccabe]
max-complexity = 10

[tool.ruff.lint.pylint]
max-branches = 12
max-args = 5
max-statements = 50

[tool.ruff.lint.per-file-ignores]
"src/other.py" = ["E402"]

[tool.ruff.lint.extend-per-file-ignores]
"src/example.py" = ["C901", "PLR0913"]
"""


def _write_config(tmp_path: Path, content: str | None = None) -> Path:
    """Write one pyproject fixture and return its path."""
    config = tmp_path / ratchet.CONFIG_PATH
    config.write_text(_valid_config_text() if content is None else content, encoding="utf-8")
    return config


def _make_scan_roots(tmp_path: Path) -> None:
    """Create every required source root."""
    for relative, required in ratchet.SCAN_ROOTS:
        if required:
            (tmp_path / relative).mkdir(parents=True, exist_ok=True)
    (tmp_path / "src" / "_scan_sentinel.py").write_text(
        '"""Scan sentinel."""\n',
        encoding="utf-8",
    )


def _diagnostic(
    filename: str,
    row: int,
    code: str,
    message: str,
) -> dict[str, object]:
    """Build a minimal Ruff JSON diagnostic."""
    return {
        "filename": filename,
        "location": {"row": row},
        "code": code,
        "message": message,
    }


class TestPrimitiveValidation:
    """Exercise strict JSON and scalar validation."""

    def test_unique_json_object_rejects_duplicates(self) -> None:
        """Duplicate JSON keys fail closed."""
        with pytest.raises(ratchet.RatchetError, match="duplicate JSON key"):
            ratchet._unique_json_object([("key", 1), ("key", 2)])

    def test_unique_json_object_preserves_values(self) -> None:
        """Unique JSON pairs become a normal object."""
        assert ratchet._unique_json_object([("key", 1)]) == {"key": 1}

    def test_load_json_rejects_missing_invalid_and_duplicate_content(self, tmp_path: Path) -> None:
        """Unreadable, malformed, and duplicate-key JSON all fail closed."""
        with pytest.raises(ratchet.RatchetError, match="cannot read"):
            ratchet._load_json(tmp_path / "missing.json")
        malformed = tmp_path / "malformed.json"
        malformed.write_text("{", encoding="utf-8")
        with pytest.raises(ratchet.RatchetError, match="invalid JSON"):
            ratchet._load_json(malformed)
        duplicate = tmp_path / "duplicate.json"
        duplicate.write_text('{"key": 1, "key": 2}', encoding="utf-8")
        with pytest.raises(ratchet.RatchetError, match="invalid JSON"):
            ratchet._load_json(duplicate)

    def test_load_json_accepts_valid_content(self, tmp_path: Path) -> None:
        """Valid JSON returns its parsed object."""
        path = tmp_path / "valid.json"
        path.write_text('{"key": 1}', encoding="utf-8")
        assert ratchet._load_json(path) == {"key": 1}

    @pytest.mark.parametrize(
        ("call", "message"),
        [
            (lambda: ratchet._object_dict([], "value"), "must be an object"),
            (lambda: ratchet._object_dict({1: "value"}, "value"), "string keys"),
            (lambda: ratchet._string(1, "value"), "must be a string"),
            (lambda: ratchet._integer(True, "value"), "must be an integer"),
            (lambda: ratchet._integer("1", "value"), "must be an integer"),
            (lambda: ratchet._string_list("value", "value"), "string list"),
            (lambda: ratchet._string_list([1], "value"), "string list"),
        ],
    )
    def test_scalar_validators_fail_closed(
        self,
        call: object,
        message: str,
    ) -> None:
        """Invalid scalar shapes are rejected."""
        callable_value = cast(Callable[[], object], call)
        with pytest.raises(ratchet.RatchetError, match=message):
            callable_value()

    def test_scalar_validators_accept_expected_shapes(self) -> None:
        """Valid scalar shapes are returned unchanged."""
        assert ratchet._object_dict({"key": 1}, "value") == {"key": 1}
        assert ratchet._string("value", "value") == "value"
        assert ratchet._integer(1, "value") == 1
        assert ratchet._string_list(["value"], "value") == ["value"]
        assert ratchet._rule_coverage("ALL") == frozenset(ratchet.RULE_LIMITS)
        assert ratchet._rule_coverage("E501") == frozenset()

    @pytest.mark.parametrize(
        "path",
        [
            "/src/example.py",
            "src\\example.py",
            "src/../example.py",
            "src/example.txt",
            "outside/example.py",
        ],
    )
    def test_source_path_rejects_unsafe_or_out_of_scope_values(self, path: str) -> None:
        """Only normalized Python paths below a scan root are accepted."""
        with pytest.raises(ratchet.RatchetError, match="normalized|outside"):
            ratchet._normalized_source_path(path, "path")

    def test_source_path_accepts_normalized_scan_path(self) -> None:
        """A normalized Python path below src is accepted."""
        assert ratchet._normalized_source_path("src/example.py", "path") == "src/example.py"


class TestBaseline:
    """Exercise deterministic baseline loading."""

    def test_loads_sorted_baseline(self, tmp_path: Path) -> None:
        """A valid document becomes sorted violation records."""
        baseline = ratchet.load_baseline(_write_baseline(tmp_path))
        assert baseline == (
            ratchet.Violation("src/example.py", "Service.run", "C901", 11),
            ratchet.Violation("src/example.py", "Service.run", "PLR0913", 6),
        )
        assert baseline[0].key == ("src/example.py", "Service.run", "C901")

    @pytest.mark.parametrize(
        ("field", "value", "message"),
        [
            ("schema_version", 2, "unsupported baseline schema"),
            ("limits", {"C901": 10}, "must equal"),
            (
                "limits",
                {"PLR0915": 50, "C901": 10, "PLR0912": 12, "PLR0913": 5},
                "keys must be sorted",
            ),
            ("violations", {}, "at least one"),
            (
                "violations",
                {
                    "tests/z.py": {"test_z": {"PLR0913": 6}},
                    "src/a.py": {"a": {"C901": 11}},
                },
                "file keys must be sorted",
            ),
            (
                "violations",
                {
                    "src/example.py": {
                        "z": {"C901": 11},
                        "a": {"C901": 11},
                    }
                },
                "symbols must be sorted",
            ),
            (
                "violations",
                {"src/example.py": {"": {"C901": 11}}},
                "must not be empty",
            ),
            (
                "violations",
                {
                    "src/example.py": {
                        "run": {
                            "PLR0913": 6,
                            "C901": 11,
                        }
                    }
                },
                "rules must be sorted",
            ),
            (
                "violations",
                {"src/example.py": {"run": {"PLR0999": 6}}},
                "unsupported baseline rule",
            ),
            (
                "violations",
                {"src/example.py": {"run": {"C901": 10}}},
                "not a violation",
            ),
        ],
    )
    def test_rejects_stale_or_nondeterministic_baseline(
        self,
        tmp_path: Path,
        field: str,
        value: object,
        message: str,
    ) -> None:
        """Malformed and non-canonical baseline documents fail closed."""
        document = _baseline_document()
        document[field] = value
        baseline_path = _write_baseline(tmp_path, document)
        with pytest.raises(ratchet.RatchetError, match=message):
            ratchet.load_baseline(baseline_path)


class TestInventoryCollection:
    """Exercise isolated Ruff collection and symbol resolution."""

    def test_collects_qualified_sync_and_async_symbols(self, tmp_path: Path) -> None:
        """Ruff rows map to stable qualified symbols and pinned command settings."""
        _make_scan_roots(tmp_path)
        module = tmp_path / "src" / "example.py"
        module.write_text(
            "def top():\n"
            "    return None\n"
            "\n"
            "class Service:\n"
            "    def run(self):\n"
            "        return None\n"
            "\n"
            "    async def wait(self):\n"
            "        return None\n",
            encoding="utf-8",
        )
        diagnostics = [
            _diagnostic(str(module), 1, "C901", "`top` is too complex (11 > 10)"),
            _diagnostic(str(module), 5, "PLR0913", "Too many arguments (6 > 5)"),
            _diagnostic(str(module), 8, "PLR0915", "Too many statements (51 > 50)"),
        ]
        completed = subprocess.CompletedProcess([], 0, json.dumps(diagnostics), "")
        with patch(
            "scripts.check_complexity_ratchet.subprocess.run", return_value=completed
        ) as run:
            inventory = ratchet.collect_inventory(tmp_path, Path("/tools/ruff"))
        assert inventory.violations == (
            ratchet.Violation("src/example.py", "Service.run", "PLR0913", 6),
            ratchet.Violation("src/example.py", "Service.wait", "PLR0915", 51),
            ratchet.Violation("src/example.py", "top", "C901", 11),
        )
        assert "proprietary/src" not in inventory.active_roots
        command = run.call_args.args[0]
        assert command[0] == str(Path("/tools/ruff"))
        assert "--isolated" in command
        assert "--ignore-noqa" in command
        assert "--no-respect-gitignore" in command
        assert "--no-force-exclude" in command
        assert "lint.pylint.max-statements = 50" in command
        assert str(module) in command
        assert str(tmp_path / "src") not in command
        assert all(
            not argument.startswith(str(tmp_path)) or argument.endswith(".py")
            for argument in command
        )
        assert run.call_args.kwargs["cwd"] == tmp_path

    def test_collects_optional_root_and_relative_filename(self, tmp_path: Path) -> None:
        """Present optional roots are scanned and relative Ruff paths resolve."""
        _make_scan_roots(tmp_path)
        module = tmp_path / "proprietary" / "src" / "feature.py"
        module.parent.mkdir(parents=True)
        (tmp_path / "proprietary" / "tests").mkdir()
        module.write_text("def run():\n    return None\n", encoding="utf-8")
        diagnostic = _diagnostic(
            "proprietary/src/feature.py",
            1,
            "PLR0912",
            "Too many branches (13 > 12)",
        )
        completed = subprocess.CompletedProcess([], 0, json.dumps([diagnostic]), "")
        with patch("scripts.check_complexity_ratchet.subprocess.run", return_value=completed):
            inventory = ratchet.collect_inventory(tmp_path, Path("ruff"))
        assert "proprietary/src" in inventory.active_roots
        assert inventory.violations[0].path == "proprietary/src/feature.py"

    def test_collects_explicit_sources_in_bounded_batches(self, tmp_path: Path) -> None:
        """Large inventories cannot exceed a platform command-line limit."""
        _make_scan_roots(tmp_path)
        module = tmp_path / "src" / "example.py"
        module.write_text("def run():\n    return None\n", encoding="utf-8")
        diagnostic = _diagnostic(str(module), 1, "C901", "`run` is too complex (11 > 10)")
        completed = (
            subprocess.CompletedProcess([], 0, "[]", ""),
            subprocess.CompletedProcess([], 0, json.dumps([diagnostic]), ""),
        )
        with (
            patch("scripts.check_complexity_ratchet._RUFF_SOURCE_BATCH_SIZE", 1),
            patch(
                "scripts.check_complexity_ratchet.subprocess.run",
                side_effect=completed,
            ) as run,
        ):
            inventory = ratchet.collect_inventory(tmp_path, Path("ruff"))
        assert inventory.violations == (ratchet.Violation("src/example.py", "run", "C901", 11),)
        assert run.call_count == 2

    def test_missing_required_root_fails_closed(self, tmp_path: Path) -> None:
        """A missing required source root cannot silently shrink inventory."""
        with pytest.raises(ratchet.RatchetError, match="required scan root"):
            ratchet.collect_inventory(tmp_path, Path("ruff"))

    def test_partial_optional_checkout_fails_closed(self, tmp_path: Path) -> None:
        """A present proprietary checkout must expose every configured root."""
        _make_scan_roots(tmp_path)
        (tmp_path / "proprietary" / "src").mkdir(parents=True)
        with pytest.raises(ratchet.RatchetError, match="proprietary/tests"):
            ratchet.collect_inventory(tmp_path, Path("ruff"))

    def test_empty_required_roots_fail_closed(self, tmp_path: Path) -> None:
        """An empty source topology cannot make Ruff scan the repository default."""
        for relative, required in ratchet.SCAN_ROOTS:
            if required:
                (tmp_path / relative).mkdir(parents=True)
        with pytest.raises(ratchet.RatchetError, match="no Python sources"):
            ratchet.collect_inventory(tmp_path, Path("ruff"))

    def test_scan_root_symlink_fails_closed(self, tmp_path: Path) -> None:
        """A required root cannot redirect inventory outside the repository.

        The symlink is simulated by patching ``Path.is_symlink`` because
        creating a real one needs elevated privileges on Windows; the guard
        under test only consults that predicate, so the patch exercises the
        identical fail-closed branch on every platform.
        """
        _make_scan_roots(tmp_path)
        link = tmp_path / "src"

        def fake_is_symlink(self: Path) -> bool:
            """Report only the redirected scan root as a symlink."""
            return self == link

        with (
            patch.object(Path, "is_symlink", fake_is_symlink),
            pytest.raises(ratchet.RatchetError, match="scan root must not be a symlink"),
        ):
            ratchet.collect_inventory(tmp_path, Path("ruff"))

    def test_nested_symlink_fails_closed_before_ruff(self, tmp_path: Path) -> None:
        """A nested alias cannot evade identity or leave the scan boundary.

        The symlink is simulated by patching ``Path.is_symlink`` (real links
        need elevated privileges on Windows); no subprocess patch exists, so
        reaching Ruff would fail loudly and the raise proves the guard fires
        first.
        """
        _make_scan_roots(tmp_path)
        linked = tmp_path / "src" / "linked.py"
        linked.write_text("def clean():\n    return None\n", encoding="utf-8")

        def fake_is_symlink(self: Path) -> bool:
            """Report only the nested alias as a symlink."""
            return self == linked

        with (
            patch.object(Path, "is_symlink", fake_is_symlink),
            pytest.raises(ratchet.RatchetError, match="must not contain symlinks"),
        ):
            ratchet.collect_inventory(tmp_path, Path("ruff"))

    def test_explicit_source_listing_rejects_outside_root(self, tmp_path: Path) -> None:
        """A caller cannot supply an otherwise valid directory outside its root."""
        repository = tmp_path / "repository"
        repository.mkdir()
        outside = tmp_path / "outside"
        outside.mkdir()
        with pytest.raises(ratchet.RatchetError, match="cannot enumerate scan root"):
            ratchet._explicit_python_sources(repository, (outside,))

    def test_explicit_source_listing_skips_non_python_files(self, tmp_path: Path) -> None:
        """Only Python files become explicit Ruff command operands."""
        source_root = tmp_path / "src"
        source_root.mkdir()
        python_source = source_root / "module.py"
        python_source.write_text("def clean():\n    return None\n", encoding="utf-8")
        (source_root / "notes.txt").write_text("not Python\n", encoding="utf-8")
        assert ratchet._explicit_python_sources(tmp_path, (source_root,)) == (python_source,)

    def test_explicit_source_listing_rechecks_file_containment(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A source resolving outside after enumeration fails closed."""
        source_root = tmp_path / "src"
        source_root.mkdir()
        python_source = source_root / "module.py"
        python_source.write_text("def clean():\n    return None\n", encoding="utf-8")
        outside = tmp_path.parent / "outside.py"
        original_resolve = Path.resolve

        def _resolve(path: Path, strict: bool = False) -> Path:
            if path == python_source:
                return outside
            return original_resolve(path, strict=strict)

        monkeypatch.setattr(Path, "resolve", _resolve)
        with pytest.raises(ratchet.RatchetError, match="Python source escapes"):
            ratchet._explicit_python_sources(tmp_path, (source_root,))

    @pytest.mark.parametrize(
        ("stderr", "stdout", "message"),
        [
            ("ruff error", "", "ruff error"),
            ("", "stdout error", "stdout error"),
            ("", "", "no diagnostic output"),
        ],
    )
    def test_ruff_process_failure_fails_closed(
        self,
        tmp_path: Path,
        stderr: str,
        stdout: str,
        message: str,
    ) -> None:
        """Every nonzero Ruff process outcome is reported."""
        _make_scan_roots(tmp_path)
        completed = subprocess.CompletedProcess([], 2, stdout, stderr)
        with (
            patch("scripts.check_complexity_ratchet.subprocess.run", return_value=completed),
            pytest.raises(ratchet.RatchetError, match=message),
        ):
            ratchet.collect_inventory(tmp_path, Path("ruff"))

    @pytest.mark.parametrize(
        ("stdout", "message"),
        [
            ("{", "invalid JSON"),
            ("{}", "must be a list"),
        ],
    )
    def test_invalid_ruff_json_fails_closed(
        self,
        tmp_path: Path,
        stdout: str,
        message: str,
    ) -> None:
        """Malformed Ruff output cannot become an empty inventory."""
        _make_scan_roots(tmp_path)
        completed = subprocess.CompletedProcess([], 0, stdout, "")
        with (
            patch("scripts.check_complexity_ratchet.subprocess.run", return_value=completed),
            pytest.raises(ratchet.RatchetError, match=message),
        ):
            ratchet.collect_inventory(tmp_path, Path("ruff"))

    def test_duplicate_symbol_diagnostic_fails_closed(self, tmp_path: Path) -> None:
        """Ruff cannot report the same symbol and rule twice."""
        _make_scan_roots(tmp_path)
        module = tmp_path / "src" / "example.py"
        module.write_text("def run():\n    return None\n", encoding="utf-8")
        diagnostic = _diagnostic(str(module), 1, "C901", "`run` is too complex (11 > 10)")
        completed = subprocess.CompletedProcess([], 0, json.dumps([diagnostic, diagnostic]), "")
        with (
            patch("scripts.check_complexity_ratchet.subprocess.run", return_value=completed),
            pytest.raises(ratchet.RatchetError, match="duplicate symbol"),
        ):
            ratchet.collect_inventory(tmp_path, Path("ruff"))


class TestDiagnosticParsing:
    """Exercise fail-closed Ruff diagnostic parsing."""

    def test_reuses_cached_symbol_map(self, tmp_path: Path) -> None:
        """Multiple rule diagnostics on one row share the parsed symbol map."""
        module = tmp_path / "src" / "example.py"
        module.parent.mkdir(parents=True)
        module.write_text("def run():\n    return None\n", encoding="utf-8")
        cache: dict[Path, dict[int, list[str]]] = {}
        first = ratchet._parse_diagnostic(
            tmp_path,
            _diagnostic(str(module), 1, "C901", "`run` is too complex (11 > 10)"),
            cache,
        )
        second = ratchet._parse_diagnostic(
            tmp_path,
            _diagnostic(str(module), 1, "PLR0913", "Too many arguments (6 > 5)"),
            cache,
        )
        assert first.symbol == second.symbol == "run"
        assert list(cache) == [module]

    @pytest.mark.parametrize(
        ("diagnostic", "message"),
        [
            (
                {"filename": "src/example.py", "location": {"row": 1}, "code": 1, "message": "x"},
                "code must be a string",
            ),
            (
                _diagnostic("src/example.py", 1, "PLR0999", "Metric (6 > 5)"),
                "unexpected rule",
            ),
            (
                _diagnostic("../outside.py", 1, "C901", "Metric (11 > 10)"),
                "outside the repository",
            ),
            (
                _diagnostic("src/example.py", 1, "C901", "metric missing"),
                "has no metric",
            ),
            (
                _diagnostic("src/example.py", 1, "C901", "Metric (11 > 9)"),
                "threshold drift",
            ),
            (
                _diagnostic("src/example.py", 1, "C901", "Metric (10 > 10)"),
                "threshold drift",
            ),
        ],
    )
    def test_malformed_diagnostic_fails_closed(
        self,
        tmp_path: Path,
        diagnostic: dict[str, object],
        message: str,
    ) -> None:
        """Unexpected Ruff diagnostic shapes and metrics are rejected."""
        module = tmp_path / "src" / "example.py"
        module.parent.mkdir(parents=True)
        module.write_text("def run():\n    return None\n", encoding="utf-8")
        with pytest.raises(ratchet.RatchetError, match=message):
            ratchet._parse_diagnostic(tmp_path, diagnostic, {})

    def test_unparseable_source_fails_closed(self, tmp_path: Path) -> None:
        """A diagnostic source with invalid Python cannot be baselined."""
        module = tmp_path / "src" / "example.py"
        module.parent.mkdir(parents=True)
        module.write_text("def broken(:\n", encoding="utf-8")
        diagnostic = _diagnostic(str(module), 1, "C901", "Metric (11 > 10)")
        with pytest.raises(ratchet.RatchetError, match="cannot parse Ruff source"):
            ratchet._parse_diagnostic(tmp_path, diagnostic, {})

    @pytest.mark.parametrize("symbols", [[], ["one", "two"]])
    def test_non_unique_symbol_mapping_fails_closed(
        self,
        tmp_path: Path,
        symbols: list[str],
    ) -> None:
        """A row must identify exactly one definition."""
        module = tmp_path / "src" / "example.py"
        module.parent.mkdir(parents=True)
        module.write_text("def run():\n    return None\n", encoding="utf-8")
        diagnostic = _diagnostic(str(module), 1, "C901", "Metric (11 > 10)")
        cache = {module: {1: symbols}}
        with pytest.raises(ratchet.RatchetError, match="cannot resolve one symbol"):
            ratchet._parse_diagnostic(tmp_path, diagnostic, cache)


class TestConfiguration:
    """Exercise Ruff policy and grandfather-list validation."""

    def test_valid_configuration_has_no_findings(self, tmp_path: Path) -> None:
        """Exact thresholds, selections, and per-file pairs pass."""
        _write_config(tmp_path)
        baseline = tuple(ratchet.load_baseline(_write_baseline(tmp_path)))
        assert ratchet.configuration_problems(tmp_path, baseline) == []

    def test_reports_selection_ignore_threshold_and_pair_drift(self, tmp_path: Path) -> None:
        """All independently actionable configuration drift is reported together."""
        config = """
[tool.ruff.lint]
select = ["C901", "PLR0912"]
ignore = ["PLR0913"]
extend-ignore = ["C"]

[tool.ruff.lint.mccabe]
max-complexity = 11

[tool.ruff.lint.pylint]
max-branches = 12
max-args = 5
max-statements = 50

[tool.ruff.lint.per-file-ignores]
"src/misplaced.py" = ["C901"]

[tool.ruff.lint.extend-per-file-ignores]
"tests/**/*.py" = ["PLR0913"]
"src/example.py" = ["PLR"]
"src/no_complexity.py" = ["E402"]
"src/stale.py" = ["PLR0913", "C901", "C901", "E402"]
"""
        _write_config(tmp_path, config)
        baseline = tuple(ratchet.load_baseline(_write_baseline(tmp_path)))
        findings = ratchet.configuration_problems(tmp_path, baseline)
        assert "complexity rule is not globally selected: PLR0913" in findings
        assert "complexity rule is not globally selected: PLR0915" in findings
        assert "global ignore disables complexity rules: ignore=PLR0913" in findings
        assert "global ignore disables complexity rules: extend-ignore=C" in findings
        assert any("configured complexity limits" in finding for finding in findings)
        assert "blanket complexity ignore is forbidden: src/example.py:PLR" in findings
        assert "complexity ignore path must be exact: tests/**/*.py:PLR0913" in findings
        assert "complexity ignore must use the generated section: src/misplaced.py" in findings
        assert "generated complexity entry has no complexity rule: src/no_complexity.py" in findings
        assert "generated complexity entry has unrelated rules: src/stale.py" in findings
        assert "duplicate per-file complexity ignore: src/stale.py" in findings
        assert "complexity ignore codes must be sorted: src/stale.py" in findings
        assert "complexity grandfather paths must be sorted" in findings
        assert "missing per-file grandfather: src/example.py:C901" in findings
        assert "missing per-file grandfather: src/example.py:PLR0913" in findings
        assert "stale per-file grandfather: src/stale.py:C901" in findings

    def test_non_complexity_per_file_ignore_is_accepted(self, tmp_path: Path) -> None:
        """Unrelated Ruff grandfather entries are outside this policy."""
        _write_config(tmp_path)
        baseline = tuple(ratchet.load_baseline(_write_baseline(tmp_path)))
        assert ratchet.configuration_problems(tmp_path, baseline) == []

    def test_invalid_toml_and_missing_tables_fail_closed(self, tmp_path: Path) -> None:
        """Malformed TOML and absent required tables cannot pass."""
        _write_config(tmp_path, "[")
        with pytest.raises(ratchet.RatchetError, match="invalid TOML"):
            ratchet.configuration_problems(tmp_path, ())
        _write_config(tmp_path, "[tool]\n")
        with pytest.raises(ratchet.RatchetError, match="tool.ruff"):
            ratchet.configuration_problems(tmp_path, ())


class TestInventoryComparison:
    """Exercise the symbol-level debt comparison."""

    def test_reports_new_stale_increased_and_improved_metrics(self) -> None:
        """Every debt movement has a distinct actionable finding."""
        baseline = (
            ratchet.Violation("src/a.py", "stale", "C901", 11),
            ratchet.Violation("src/a.py", "increased", "C901", 11),
            ratchet.Violation("src/a.py", "improved", "PLR0913", 8),
            ratchet.Violation("proprietary/src/optional.py", "hidden", "C901", 11),
        )
        inventory = ratchet.Inventory(
            (
                ratchet.Violation("src/a.py", "new", "PLR0913", 6),
                ratchet.Violation("src/a.py", "increased", "C901", 12),
                ratchet.Violation("src/a.py", "improved", "PLR0913", 7),
            ),
            ("src",),
        )
        findings = ratchet.inventory_problems(inventory, baseline)
        assert "new complexity violation: src/a.py:new:PLR0913=6" in findings
        assert "stale baseline violation: src/a.py:stale:C901=11" in findings
        assert "complexity increased: src/a.py:increased:C901 11->12" in findings
        assert "baseline can be lowered: src/a.py:improved:PLR0913 8->7" in findings
        assert not any("optional.py" in finding for finding in findings)

    def test_exact_inventory_passes(self) -> None:
        """An exact current inventory matches its baseline."""
        violation = ratchet.Violation("src/a.py", "run", "C901", 11)
        inventory = ratchet.Inventory((violation,), ("src",))
        assert ratchet.inventory_problems(inventory, (violation,)) == []


class TestEntrypoint:
    """Exercise executable discovery and command outcomes."""

    def test_find_ruff_prefers_interpreter_sibling(self, tmp_path: Path) -> None:
        """The active environment's Ruff executable takes precedence."""
        executable = tmp_path / "python"
        ruff = tmp_path / "ruff"
        ruff.touch()
        with patch("scripts.check_complexity_ratchet.sys.executable", str(executable)):
            assert ratchet._find_ruff() == ruff

    def test_find_ruff_uses_windows_executable_suffix(self, tmp_path: Path) -> None:
        """A Windows interpreter resolves its adjacent Ruff executable."""
        scripts = tmp_path / "Scripts"
        scripts.mkdir()
        executable = scripts / "python.exe"
        ruff = scripts / "ruff.exe"
        ruff.touch()
        with patch("scripts.check_complexity_ratchet.sys.executable", str(executable)):
            assert ratchet._find_ruff() == ruff

    def test_find_ruff_ignores_posix_version_suffix(self, tmp_path: Path) -> None:
        """A versioned POSIX interpreter still resolves an extensionless Ruff."""
        executable = tmp_path / "python3.13"
        ruff = tmp_path / "ruff"
        ruff.touch()
        with patch("scripts.check_complexity_ratchet.sys.executable", str(executable)):
            assert ratchet._find_ruff() == ruff

    def test_find_ruff_uses_path_or_fails(self, tmp_path: Path) -> None:
        """PATH is the fallback and a missing executable fails closed."""
        executable = tmp_path / "python"
        with (
            patch("scripts.check_complexity_ratchet.sys.executable", str(executable)),
            patch("scripts.check_complexity_ratchet.shutil.which", return_value="/tools/ruff"),
        ):
            assert ratchet._find_ruff() == Path("/tools/ruff")
        with (
            patch("scripts.check_complexity_ratchet.sys.executable", str(executable)),
            patch("scripts.check_complexity_ratchet.shutil.which", return_value=None),
            pytest.raises(ratchet.RatchetError, match="cannot locate"),
        ):
            ratchet._find_ruff()

    def test_run_check_handles_clean_findings_and_fail_closed(self, tmp_path: Path) -> None:
        """The runner returns zero only for a complete clean comparison."""
        violation = ratchet.Violation("src/a.py", "run", "C901", 11)
        inventory = ratchet.Inventory((violation,), ("src",))
        with (
            patch("scripts.check_complexity_ratchet.load_baseline", return_value=(violation,)),
            patch("scripts.check_complexity_ratchet.configuration_problems", return_value=[]),
            patch("scripts.check_complexity_ratchet.collect_inventory", return_value=inventory),
            patch("scripts.check_complexity_ratchet.inventory_problems", return_value=[]),
        ):
            assert ratchet.run_check(tmp_path, Path("ruff")) == 0
        with (
            patch("scripts.check_complexity_ratchet.load_baseline", return_value=(violation,)),
            patch(
                "scripts.check_complexity_ratchet.configuration_problems",
                return_value=["finding"],
            ),
            patch("scripts.check_complexity_ratchet.collect_inventory", return_value=inventory),
            patch("scripts.check_complexity_ratchet.inventory_problems", return_value=[]),
        ):
            assert ratchet.run_check(tmp_path, Path("ruff")) == 1
        with patch(
            "scripts.check_complexity_ratchet.load_baseline",
            side_effect=ratchet.RatchetError("broken"),
        ):
            assert ratchet.run_check(tmp_path, Path("ruff")) == 1

    def test_run_check_discovers_ruff_and_main_uses_repository_root(self, tmp_path: Path) -> None:
        """Implicit Ruff discovery and the module entry point delegate correctly."""
        violation = ratchet.Violation("src/a.py", "run", "C901", 11)
        inventory = ratchet.Inventory((violation,), ("src",))
        with (
            patch("scripts.check_complexity_ratchet.load_baseline", return_value=(violation,)),
            patch("scripts.check_complexity_ratchet.configuration_problems", return_value=[]),
            patch("scripts.check_complexity_ratchet._find_ruff", return_value=Path("ruff")) as find,
            patch("scripts.check_complexity_ratchet.collect_inventory", return_value=inventory),
            patch("scripts.check_complexity_ratchet.inventory_problems", return_value=[]),
        ):
            assert ratchet.run_check(tmp_path) == 0
            find.assert_called_once_with()
        with (
            patch("scripts.check_complexity_ratchet.sys.argv", ["checker"]),
            patch("scripts.check_complexity_ratchet.run_check", return_value=7) as run,
        ):
            assert ratchet.main() == 7
            assert run.call_args.args[0] == Path(ratchet.__file__).resolve().parent.parent

    @pytest.mark.parametrize("argument", ["--bootstrap-adoption", "--unknown"])
    def test_main_rejects_every_argument(self, argument: str) -> None:
        """The shipped checker exposes no debt re-baselining path."""
        with patch("scripts.check_complexity_ratchet.sys.argv", ["checker", argument]):
            assert ratchet.main() == 2
