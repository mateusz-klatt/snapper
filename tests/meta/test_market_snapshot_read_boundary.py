"""Fence research snapshot access away from live market pricing."""

import re
from pathlib import Path

_SRC = Path(__file__).resolve().parents[2] / "src" / "snapper"
_ALLOWED_FILES = {
    "cli/app.py",
    "data/archiver.py",
    "data/models.py",
    "data/repository.py",
    "data/repository_types.py",
    "server/metrics_routes.py",
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
        for line_number, line in enumerate(file.read_text(encoding="utf-8").splitlines(), 1):
            if match := _REFERENCE.search(line):
                violations.append(f"{relative}:{line_number}: {match.group()}")
    assert not violations, "Research snapshot references outside the boundary:\n" + "\n".join(
        violations
    )
