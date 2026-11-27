"""Fence research snapshot access away from live market pricing."""

import ast
import re
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src" / "snapper"
_ALLOWED_FILES = {
    "cli/app.py",
    "data/archiver.py",
    "data/models.py",
    "data/repository_types.py",
}
_ALLOWED_DIRECTORIES = (
    "application/market_snapshot_refresh/",
    "data/migrations/",
    "infrastructure/market_data/",
)
_REFERENCE = re.compile(
    r"\b(?:get_market_snapshots|iter_market_snapshots|get_active_market_snapshot_price|"
    r"get_active_usd_fx_snapshot_candidates|get_active_market_snapshot_freshness|"
    r"MarketSnapshot|market_snapshots)\b"
)


_REPOSITORY_FILE = "data/repository.py"
_REPOSITORY_FUNCTIONS = {"_market_snapshot_freshness_query"}
_REPOSITORY_METHODS = {
    "Repository": {
        "get_active_market_snapshot_freshness",
        "get_market_snapshots",
        "iter_market_snapshots",
    },
    "SQLAlchemyRepository": {
        "get_active_market_snapshot_freshness",
        "get_market_snapshots",
        "iter_market_snapshots",
        "_upsert_snapshot_row",
    },
    "DatabaseRepository": {"get_active_market_snapshot_freshness"},
}
_RETIRED_READERS = {"get_active_market_snapshot_price", "get_active_usd_fx_snapshot_candidates"}


def _repository_snapshot_lines(source: str) -> set[int]:
    """Allow snapshot references only in named repository methods and the model import."""
    lines: set[int] = set()
    for node in ast.parse(source).body:
        if isinstance(node, ast.ImportFrom) and node.module == "snapper.data.models":
            if any(name.name == "MarketSnapshot" for name in node.names):
                lines.add(node.lineno)
        if isinstance(node, ast.FunctionDef) and node.name in _REPOSITORY_FUNCTIONS:
            lines.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
        if isinstance(node, ast.ClassDef):
            allowed = _REPOSITORY_METHODS.get(node.name, set())
            for method in node.body:
                if (
                    isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and method.name in allowed
                ):
                    lines.update(range(method.lineno, (method.end_lineno or method.lineno) + 1))
    return lines


def test_market_snapshots_stay_inside_research_boundary() -> None:
    """Keep live pricing modules off the research snapshot table.

    Given: the explicit list of repository, research and maintenance modules,
    When: production source files are scanned for snapshot identifiers,
    Then: every reference stays inside the allowed boundary.
    """
    violations: list[str] = []
    for file in sorted(_SRC.rglob("*.py")):
        relative = file.relative_to(_SRC).as_posix()
        if relative in _ALLOWED_FILES or relative.startswith(_ALLOWED_DIRECTORIES):
            continue
        source = file.read_text(encoding="utf-8")
        allowed_lines = (
            _repository_snapshot_lines(source) if relative == _REPOSITORY_FILE else set()
        )
        for line_number, line in enumerate(source.splitlines(), 1):
            for match in _REFERENCE.finditer(line):
                identifier = match.group()
                if line_number not in allowed_lines or identifier in _RETIRED_READERS:
                    violations.append(f"{relative}:{line_number}: {identifier}")
    assert not violations, "Research snapshot references outside the boundary:\n" + "\n".join(
        violations
    )
