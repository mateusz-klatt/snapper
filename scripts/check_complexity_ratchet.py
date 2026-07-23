"""Enforce the Ruff function-complexity debt ratchet.

Ruff's per-file ignores let the existing codebase adopt strict complexity
thresholds without a flag day, but an ignore also hides new offenders in the
same file. This checker runs Ruff without repository ignores and compares every
diagnostic with a symbol-level baseline. Existing functions may only improve;
new offenders and increased metrics fail the gate. Configuration validation
keeps the per-file grandfather list exact and removable.
"""

import ast
import json
import re
import shutil
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from typing import cast

RULE_LIMITS: Final[dict[str, int]] = {
    "C901": 10,
    "PLR0912": 12,
    "PLR0913": 5,
    "PLR0915": 50,
}
SCAN_ROOTS: Final[tuple[tuple[str, bool], ...]] = (
    ("src", True),
    ("tests", True),
    ("scripts", True),
    ("proprietary/src", False),
    ("proprietary/tests", False),
    ("integrations/snapper-delegate/src", True),
    ("integrations/snapper-delegate/tests", True),
)
BASELINE_PATH: Final[Path] = Path("scripts/complexity_baseline.json")
CONFIG_PATH: Final[Path] = Path("pyproject.toml")
_BASELINE_SCHEMA_VERSION: Final[int] = 1
_RUFF_SOURCE_BATCH_SIZE: Final[int] = 64
_VALUE_PATTERN: Final[re.Pattern[str]] = re.compile(r"\((\d+) > (\d+)\)$")
_GLOB_CHARACTERS: Final[frozenset[str]] = frozenset("*?[")


class RatchetError(RuntimeError):
    """Report an unreadable or internally inconsistent ratchet input."""


@dataclass(frozen=True, order=True)
class Violation:
    """Represent one Ruff diagnostic pinned to a qualified function symbol."""

    path: str
    symbol: str
    code: str
    value: int

    @property
    def key(self) -> tuple[str, str, str]:
        """Return the stable identity independent of the measured value.

        Returns:
            Repository path, qualified symbol, and Ruff rule code.
        """
        return (self.path, self.symbol, self.code)


@dataclass(frozen=True)
class Inventory:
    """Hold current diagnostics and the source roots Ruff actually scanned."""

    violations: tuple[Violation, ...]
    active_roots: tuple[str, ...]


class _SymbolVisitor(ast.NodeVisitor):
    """Map definition lines to qualified class and function names."""

    def __init__(self) -> None:
        self.symbols: dict[int, list[str]] = {}
        self._stack: list[str] = []

    def _visit_named(
        self,
        node: ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        """Record a named definition and visit definitions nested inside it."""
        symbol = ".".join((*self._stack, node.name))
        self.symbols.setdefault(node.lineno, []).append(symbol)
        self._stack.append(node.name)
        self.generic_visit(node)
        self._stack.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Visit a class definition."""
        self._visit_named(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Visit a synchronous function definition."""
        self._visit_named(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Visit an asynchronous function definition."""
        self._visit_named(node)


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Build a JSON object while rejecting duplicate keys."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise RatchetError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _load_json(path: Path) -> object:
    """Load strict JSON from a file and reject malformed content."""
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RatchetError(f"cannot read {path}: {exc}") from exc
    try:
        return cast(
            object,
            json.loads(content, object_pairs_hook=_unique_json_object),
        )
    except (json.JSONDecodeError, RatchetError) as exc:
        raise RatchetError(f"invalid JSON in {path}: {exc}") from exc


def _object_dict(value: object, label: str) -> dict[str, object]:
    """Return a string-keyed object mapping or fail closed."""
    if not isinstance(value, dict):
        raise RatchetError(f"{label} must be an object")
    if not all(isinstance(key, str) for key in value):
        raise RatchetError(f"{label} must use string keys")
    return cast(dict[str, object], value)


def _string(value: object, label: str) -> str:
    """Return a string value or fail closed."""
    if not isinstance(value, str):
        raise RatchetError(f"{label} must be a string")
    return value


def _integer(value: object, label: str) -> int:
    """Return a non-boolean integer value or fail closed."""
    if not isinstance(value, int) or isinstance(value, bool):
        raise RatchetError(f"{label} must be an integer")
    return value


def _string_list(value: object, label: str) -> list[str]:
    """Return a list containing only strings or fail closed."""
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise RatchetError(f"{label} must be a string list")
    return cast(list[str], value)


def _normalized_source_path(value: object, label: str) -> str:
    """Validate and normalize a repository-relative Python source path."""
    path = _string(value, label)
    candidate = Path(path)
    if (
        candidate.is_absolute()
        or "\\" in path
        or path != candidate.as_posix()
        or ".." in candidate.parts
        or candidate.suffix != ".py"
    ):
        raise RatchetError(f"{label} is not a normalized Python path: {path}")
    if not any(path == root or path.startswith(f"{root}/") for root, _required in SCAN_ROOTS):
        raise RatchetError(f"{label} is outside the configured scan roots: {path}")
    return path


def _parse_limits(value: object, label: str) -> dict[str, int]:
    """Parse and validate the exact rule-limit mapping."""
    raw_limits = _object_dict(value, label)
    limits = {
        code: _integer(raw_value, f"{label}.{code}") for code, raw_value in raw_limits.items()
    }
    if limits != RULE_LIMITS:
        raise RatchetError(f"{label} must equal {RULE_LIMITS}")
    if list(raw_limits) != sorted(raw_limits):
        raise RatchetError(f"{label} keys must be sorted")
    return limits


def _baseline_file_violations(raw_path: str, raw_symbols: object) -> list[Violation]:
    """Parse all baseline violations recorded for one source file."""
    path = _normalized_source_path(raw_path, "baseline path")
    symbols = _object_dict(raw_symbols, f"baseline.violations.{path}")
    if list(symbols) != sorted(symbols):
        raise RatchetError(f"baseline symbols must be sorted for {path}")
    violations: list[Violation] = []
    for symbol, raw_rules in symbols.items():
        if not symbol:
            raise RatchetError(f"baseline symbol must not be empty in {path}")
        rules = _object_dict(raw_rules, f"baseline.violations.{path}.{symbol}")
        if list(rules) != sorted(rules):
            raise RatchetError(f"baseline rules must be sorted for {path}:{symbol}")
        for code, raw_value in rules.items():
            if code not in RULE_LIMITS:
                raise RatchetError(f"unsupported baseline rule: {code}")
            measured = _integer(raw_value, f"baseline value for {path}:{symbol}:{code}")
            if measured <= RULE_LIMITS[code]:
                raise RatchetError(f"baseline value is not a violation: {path}:{symbol}:{code}")
            violations.append(Violation(path, symbol, code, measured))
    return violations


def load_baseline(path: Path) -> tuple[Violation, ...]:
    """Load the deterministic symbol-level grandfather snapshot.

    Args:
        path: Baseline JSON path.

    Returns:
        Sorted baseline violations.
    """
    document = _object_dict(_load_json(path), "baseline")
    schema_version = _integer(document.get("schema_version"), "baseline.schema_version")
    if schema_version != _BASELINE_SCHEMA_VERSION:
        raise RatchetError(f"unsupported baseline schema version: {schema_version}")
    _parse_limits(document.get("limits"), "baseline.limits")
    files = _object_dict(document.get("violations"), "baseline.violations")
    if list(files) != sorted(files):
        raise RatchetError("baseline file keys must be sorted")
    violations: list[Violation] = []
    for raw_path, raw_symbols in files.items():
        violations.extend(_baseline_file_violations(raw_path, raw_symbols))
    if not violations:
        raise RatchetError("baseline must contain at least one violation")
    return tuple(sorted(violations))


def _active_scan_paths(root: Path) -> tuple[tuple[Path, ...], tuple[str, ...]]:
    """Resolve required and optional source roots for the current checkout."""
    paths: list[Path] = []
    names: list[str] = []
    optional_roots = tuple(relative for relative, required in SCAN_ROOTS if not required)
    optional_checkout_present = (root / "proprietary" / ".git").exists() or any(
        (root / relative).exists() for relative in optional_roots
    )
    for relative, required in SCAN_ROOTS:
        candidate = root / relative
        if candidate.is_symlink():
            raise RatchetError(f"scan root must not be a symlink: {relative}")
        if candidate.is_dir():
            paths.append(candidate)
            names.append(relative)
        elif required or (not required and optional_checkout_present):
            raise RatchetError(f"required scan root is missing: {relative}")
    return tuple(paths), tuple(names)


def _explicit_python_sources(root: Path, scan_paths: tuple[Path, ...]) -> tuple[Path, ...]:
    """List every in-repository Python source without ignore-file filtering."""
    repository = root.resolve()
    sources: list[Path] = []
    for scan_path in scan_paths:
        try:
            scan_path.resolve(strict=True).relative_to(repository)
            descendants = sorted(scan_path.rglob("*"))
        except (OSError, ValueError) as exc:
            raise RatchetError(f"cannot enumerate scan root {scan_path}: {exc}") from exc
        for descendant in descendants:
            if descendant.is_symlink():
                raise RatchetError(f"scan roots must not contain symlinks: {descendant}")
            if not descendant.is_file() or descendant.suffix != ".py":
                continue
            try:
                descendant.resolve(strict=True).relative_to(repository)
            except (OSError, ValueError) as exc:
                raise RatchetError(f"Python source escapes the repository: {descendant}") from exc
            sources.append(descendant)
    if not sources:
        raise RatchetError("configured scan roots contain no Python sources")
    return tuple(sources)


def _ruff_command(ruff_executable: Path, source_files: tuple[Path, ...]) -> list[str]:
    """Build the isolated Ruff inventory command with pinned thresholds."""
    command = [
        str(ruff_executable),
        "check",
        "--isolated",
        "--no-cache",
        "--ignore-noqa",
        "--no-respect-gitignore",
        "--no-force-exclude",
        "--exit-zero",
        "--target-version",
        "py314",
        "--select",
        ",".join(RULE_LIMITS),
        "--output-format",
        "json",
    ]
    command.extend(
        [
            "--config",
            f"lint.mccabe.max-complexity = {RULE_LIMITS['C901']}",
            "--config",
            f"lint.pylint.max-branches = {RULE_LIMITS['PLR0912']}",
            "--config",
            f"lint.pylint.max-args = {RULE_LIMITS['PLR0913']}",
            "--config",
            f"lint.pylint.max-statements = {RULE_LIMITS['PLR0915']}",
        ]
    )
    command.extend(str(path) for path in source_files)
    return command


def _symbol_lines(path: Path) -> dict[int, list[str]]:
    """Parse one Python file and map definition rows to qualified symbols."""
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
    except (OSError, SyntaxError) as exc:
        raise RatchetError(f"cannot parse Ruff source {path}: {exc}") from exc
    visitor = _SymbolVisitor()
    visitor.visit(tree)
    return visitor.symbols


def _diagnostic_path(root: Path, filename: str) -> tuple[Path, str]:
    """Resolve a Ruff filename and prove it stays inside the repository."""
    candidate = Path(filename)
    absolute = candidate if candidate.is_absolute() else root / candidate
    try:
        relative = absolute.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise RatchetError(f"Ruff reported a path outside the repository: {filename}") from exc
    normalized = _normalized_source_path(relative.as_posix(), "Ruff path")
    return absolute, normalized


def _parse_diagnostic(
    root: Path,
    raw_value: object,
    symbols_by_path: dict[Path, dict[int, list[str]]],
) -> Violation:
    """Convert one Ruff JSON diagnostic into a stable violation."""
    raw = _object_dict(raw_value, "Ruff diagnostic")
    code = _string(raw.get("code"), "Ruff diagnostic code")
    if code not in RULE_LIMITS:
        raise RatchetError(f"Ruff returned an unexpected rule: {code}")
    filename = _string(raw.get("filename"), "Ruff diagnostic filename")
    absolute_path, path = _diagnostic_path(root, filename)
    location = _object_dict(raw.get("location"), "Ruff diagnostic location")
    row = _integer(location.get("row"), "Ruff diagnostic row")
    message = _string(raw.get("message"), "Ruff diagnostic message")
    value_match = _VALUE_PATTERN.search(message)
    if value_match is None:
        raise RatchetError(f"Ruff diagnostic has no metric: {path}:{row}:{code}")
    measured, threshold = (int(part) for part in value_match.groups())
    if threshold != RULE_LIMITS[code] or measured <= threshold:
        raise RatchetError(f"Ruff threshold drift for {path}:{row}:{code}: {message}")
    symbol_lines = symbols_by_path.get(absolute_path)
    if symbol_lines is None:
        symbol_lines = _symbol_lines(absolute_path)
        symbols_by_path[absolute_path] = symbol_lines
    symbols = symbol_lines.get(row, [])
    if len(symbols) != 1:
        raise RatchetError(f"cannot resolve one symbol for {path}:{row}:{code}")
    return Violation(path, symbols[0], code, measured)


def collect_inventory(root: Path, ruff_executable: Path) -> Inventory:
    """Run isolated Ruff and collect every current complexity violation.

    Args:
        root: Repository root to scan.
        ruff_executable: Ruff executable path.

    Returns:
        Current violations and active scan roots.
    """
    scan_paths, active_roots = _active_scan_paths(root)
    source_files = _explicit_python_sources(root, scan_paths)
    raw_diagnostics: list[object] = []
    for offset in range(0, len(source_files), _RUFF_SOURCE_BATCH_SIZE):
        source_batch = source_files[offset : offset + _RUFF_SOURCE_BATCH_SIZE]
        result = subprocess.run(
            _ruff_command(ruff_executable, source_batch),
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "no diagnostic output"
            raise RatchetError(f"Ruff inventory failed: {detail}")
        try:
            raw_batch = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RatchetError(f"Ruff returned invalid JSON: {exc}") from exc
        if not isinstance(raw_batch, list):
            raise RatchetError("Ruff JSON output must be a list")
        raw_diagnostics.extend(cast(list[object], raw_batch))
    symbols_by_path: dict[Path, dict[int, list[str]]] = {}
    violations = [_parse_diagnostic(root, raw, symbols_by_path) for raw in raw_diagnostics]
    ordered = tuple(sorted(violations))
    if len({violation.key for violation in ordered}) != len(ordered):
        raise RatchetError("Ruff returned duplicate symbol-level diagnostics")
    return Inventory(ordered, active_roots)


def _rule_coverage(token: str) -> frozenset[str]:
    """Return complexity rules covered by one Ruff selector token."""
    if token == "ALL":
        return frozenset(RULE_LIMITS)
    return frozenset(rule for rule in RULE_LIMITS if rule.startswith(token))


def _table_field(parent: dict[str, object], key: str, label: str) -> dict[str, object]:
    """Read a required TOML table."""
    return _object_dict(parent.get(key), f"{label}.{key}")


def _load_toml(path: Path) -> dict[str, object]:
    """Load a TOML document or fail closed."""
    try:
        with path.open("rb") as stream:
            raw = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise RatchetError(f"invalid TOML in {path}: {exc}") from exc
    return _object_dict(raw, "configuration")


def _configured_limits(lint: dict[str, object]) -> dict[str, int]:
    """Extract complexity thresholds from Ruff's plugin tables."""
    mccabe = _table_field(lint, "mccabe", "tool.ruff.lint")
    pylint = _table_field(lint, "pylint", "tool.ruff.lint")
    return {
        "C901": _integer(mccabe.get("max-complexity"), "lint.mccabe.max-complexity"),
        "PLR0912": _integer(pylint.get("max-branches"), "lint.pylint.max-branches"),
        "PLR0913": _integer(pylint.get("max-args"), "lint.pylint.max-args"),
        "PLR0915": _integer(pylint.get("max-statements"), "lint.pylint.max-statements"),
    }


def _generated_ignore_entry(
    raw_path: str,
    raw_codes: object,
) -> tuple[set[tuple[str, str]], list[str], bool]:
    """Validate one generated path entry and return its exact rule pairs."""
    codes = _string_list(raw_codes, f"extend-per-file-ignores.{raw_path}")
    covered_tokens = [token for token in codes if _rule_coverage(token)]
    problems: list[str] = []
    if not covered_tokens:
        return set(), [f"generated complexity entry has no complexity rule: {raw_path}"], False
    if len(covered_tokens) != len(codes):
        problems.append(f"generated complexity entry has unrelated rules: {raw_path}")
    if len(covered_tokens) != len(set(covered_tokens)):
        problems.append(f"duplicate per-file complexity ignore: {raw_path}")
    if covered_tokens != sorted(covered_tokens):
        problems.append(f"complexity ignore codes must be sorted: {raw_path}")
    pairs: set[tuple[str, str]] = set()
    for token in covered_tokens:
        if token not in RULE_LIMITS:
            problems.append(f"blanket complexity ignore is forbidden: {raw_path}:{token}")
        elif any(character in raw_path for character in _GLOB_CHARACTERS):
            problems.append(f"complexity ignore path must be exact: {raw_path}:{token}")
        else:
            path = _normalized_source_path(raw_path, "complexity ignore path")
            pairs.add((path, token))
    return pairs, problems, True


def _ignored_complexity_pairs(
    lint: dict[str, object],
) -> tuple[frozenset[tuple[str, str]], list[str]]:
    """Extract exact per-file complexity ignores and report blanket selectors."""
    primary_ignores = _table_field(lint, "per-file-ignores", "tool.ruff.lint")
    raw_ignores = _table_field(lint, "extend-per-file-ignores", "tool.ruff.lint")
    pairs: set[tuple[str, str]] = set()
    problems: list[str] = []
    for raw_path, raw_codes in primary_ignores.items():
        codes = _string_list(raw_codes, f"per-file-ignores.{raw_path}")
        if any(_rule_coverage(token) for token in codes):
            problems.append(f"complexity ignore must use the generated section: {raw_path}")
    ordered_paths: list[str] = []
    for raw_path, raw_codes in raw_ignores.items():
        entry_pairs, entry_problems, is_complexity_entry = _generated_ignore_entry(
            raw_path,
            raw_codes,
        )
        pairs.update(entry_pairs)
        problems.extend(entry_problems)
        if is_complexity_entry:
            ordered_paths.append(raw_path)
    if ordered_paths != sorted(ordered_paths):
        problems.append("complexity grandfather paths must be sorted")
    return frozenset(pairs), problems


def configuration_problems(
    root: Path,
    baseline: tuple[Violation, ...],
) -> list[str]:
    """Validate Ruff thresholds and the minimal per-file grandfather list.

    Args:
        root: Repository root containing pyproject.toml.
        baseline: Canonical symbol-level baseline.

    Returns:
        Actionable configuration policy findings.
    """
    document = _load_toml(root / CONFIG_PATH)
    tool = _table_field(document, "tool", "configuration")
    ruff = _table_field(tool, "ruff", "tool")
    lint = _table_field(ruff, "lint", "tool.ruff")
    selected = set(_string_list(lint.get("select"), "tool.ruff.lint.select"))
    selected.update(_string_list(lint.get("extend-select", []), "tool.ruff.lint.extend-select"))
    missing_rules = RULE_LIMITS.keys() - selected
    problems = [f"complexity rule is not globally selected: {rule}" for rule in missing_rules]
    for field in ("ignore", "extend-ignore"):
        for token in _string_list(lint.get(field, []), f"tool.ruff.lint.{field}"):
            covered = _rule_coverage(token)
            if covered:
                problems.append(f"global ignore disables complexity rules: {field}={token}")
    configured_limits = _configured_limits(lint)
    if configured_limits != RULE_LIMITS:
        problems.append(
            f"configured complexity limits {configured_limits} do not equal {RULE_LIMITS}"
        )
    pairs, ignore_problems = _ignored_complexity_pairs(lint)
    problems.extend(ignore_problems)
    expected_pairs = frozenset((item.path, item.code) for item in baseline)
    for path, code in sorted(expected_pairs - pairs):
        problems.append(f"missing per-file grandfather: {path}:{code}")
    for path, code in sorted(pairs - expected_pairs):
        problems.append(f"stale per-file grandfather: {path}:{code}")
    return problems


def _path_is_active(path: str, active_roots: tuple[str, ...]) -> bool:
    """Return whether a baseline path belongs to a source root in this checkout."""
    return any(path == root or path.startswith(f"{root}/") for root in active_roots)


def inventory_problems(
    inventory: Inventory,
    baseline: tuple[Violation, ...],
) -> list[str]:
    """Compare current metrics with the active portion of the baseline.

    Args:
        inventory: Current Ruff diagnostics and active roots.
        baseline: Canonical symbol-level baseline.

    Returns:
        New, stale, increased, and improved debt findings.
    """
    current_by_key = {item.key: item for item in inventory.violations}
    active_baseline = tuple(
        item for item in baseline if _path_is_active(item.path, inventory.active_roots)
    )
    baseline_by_key = {item.key: item for item in active_baseline}
    problems: list[str] = []
    for key in sorted(current_by_key.keys() - baseline_by_key.keys()):
        current = current_by_key[key]
        problems.append(
            f"new complexity violation: {current.path}:{current.symbol}:{current.code}={current.value}"
        )
    for key in sorted(baseline_by_key.keys() - current_by_key.keys()):
        stale = baseline_by_key[key]
        problems.append(
            f"stale baseline violation: {stale.path}:{stale.symbol}:{stale.code}={stale.value}"
        )
    for key in sorted(current_by_key.keys() & baseline_by_key.keys()):
        current = current_by_key[key]
        grandfathered = baseline_by_key[key]
        if current.value > grandfathered.value:
            problems.append(
                "complexity increased: "
                f"{current.path}:{current.symbol}:{current.code} "
                f"{grandfathered.value}->{current.value}"
            )
        elif current.value < grandfathered.value:
            problems.append(
                "baseline can be lowered: "
                f"{current.path}:{current.symbol}:{current.code} "
                f"{grandfathered.value}->{current.value}"
            )
    return problems


def _find_ruff() -> Path:
    """Locate Ruff beside the active Python interpreter or on PATH."""
    adjacent = Path(sys.executable).with_name("ruff")
    if adjacent.is_file():
        return adjacent
    discovered = shutil.which("ruff")
    if discovered is None:
        raise RatchetError("cannot locate the Ruff executable")
    return Path(discovered)


def run_check(root: Path, ruff_executable: Path | None = None) -> int:
    """Run configuration and inventory validation and return an exit code.

    Args:
        root: Repository root to validate.
        ruff_executable: Optional explicit Ruff executable path.

    Returns:
        Zero when the ratchet is clean, otherwise one.
    """
    try:
        baseline = load_baseline(root / BASELINE_PATH)
        config_findings = configuration_problems(root, baseline)
        inventory = collect_inventory(root, ruff_executable or _find_ruff())
        findings = config_findings + inventory_problems(inventory, baseline)
    except RatchetError as exc:
        print(f"Complexity ratchet failed closed: {exc}")
        return 1
    if findings:
        print("Complexity ratchet findings:")
        for finding in findings:
            print(f"  {finding}")
        return 1
    print(f"Complexity ratchet clean: {len(inventory.violations)} grandfathered diagnostics")
    return 0


def main() -> int:
    """Run the checker from the repository root.

    Returns:
        Process exit code.
    """
    root = Path(__file__).resolve().parent.parent
    arguments = sys.argv[1:]
    if not arguments:
        return run_check(root)
    print("Usage: check_complexity_ratchet.py")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
