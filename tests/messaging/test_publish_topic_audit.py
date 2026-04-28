"""Audit test: forbid future ZMQ-publish bypass paths in production backend.

The chokepoint contract introduced in Phase 2 is that every
production publish call site emitting a ``StrictDataSchema``-derived
payload routes serialization through ``StrictDataSchema.publish_to``
so the payload's ``topic`` field is always populated on the wire.

This audit grep-scans the production backend (``src/snapper/**``)
for the antipattern of calling ``.to_json().encode(...)`` OR
``.model_dump_json().encode(...)`` near ``send_multipart(...)``.
The replay publisher historically used the ``model_dump_json``
variant (verified at master HEAD); this audit covers BOTH forms so
a regression in either style fails the test.

Allow-list (in scan scope):
    - The ``StrictDataSchema.publish_to`` source line itself.
    - Module-level docstring examples in messaging/__init__.py.

Out of scope (NOT scanned):
    - ``scripts/uat_*.py`` — UAT spike scripts hand-roll JSON via
      ``json.dumps`` for manual integration testing; they don't go
      through ``StrictDataSchema`` and therefore don't need to use
      ``publish_to``. If a future UAT script migrates to
      schema-derived publishing, it will join the production scan
      scope automatically and the audit will catch a missing
      ``publish_to``.
    - Tests under ``tests/`` — they often serialize fixtures
      directly; production code is what the contract binds.
"""

import re
from pathlib import Path

import pytest

_BYPASS_TO_JSON = re.compile(r"\.to_json\(\)\.encode\b")
_BYPASS_MODEL_DUMP_JSON = re.compile(r"\.model_dump_json\(\)\.encode\b")
_SEND_MULTIPART = re.compile(r"\bsend_multipart\b")

_PROXIMITY_LINES = 5

_SCAN_ROOT = Path(__file__).resolve().parent.parent.parent / "src" / "snapper"

_ALLOWLIST: frozenset[Path] = frozenset(
    {
        _SCAN_ROOT / "api" / "schemas" / "base.py",
        _SCAN_ROOT / "messaging" / "__init__.py",
    }
)


def _find_bypass_findings(file_path: Path) -> list[tuple[int, str]]:
    """Scan a single .py file for the bypass antipattern.

    Returns:
        List of ``(line_number, line)`` tuples where a bypass call
        is followed by ``send_multipart`` within
        ``_PROXIMITY_LINES`` lines, or the inverse (a
        ``send_multipart`` preceded by a bypass within proximity).
    """
    if file_path in _ALLOWLIST:
        return []

    text = file_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    findings: list[tuple[int, str]] = []
    for index, line in enumerate(lines):
        if not (_BYPASS_TO_JSON.search(line) or _BYPASS_MODEL_DUMP_JSON.search(line)):
            continue
        window_start = max(0, index - _PROXIMITY_LINES)
        window_end = min(len(lines), index + _PROXIMITY_LINES + 1)
        nearby = "\n".join(lines[window_start:window_end])
        if _SEND_MULTIPART.search(nearby):
            findings.append((index + 1, line.rstrip()))
    return findings


def _iter_production_python_files() -> list[Path]:
    """Yield every production .py file under src/snapper/ (recursively)."""
    return sorted(_SCAN_ROOT.rglob("*.py"))


class TestNoSerializationBypassNearSendMultipart:
    """Production code MUST NOT bypass the publish_to chokepoint."""

    def test_scan_root_exists(self) -> None:
        """Sanity check: the scan root resolves to the production source tree."""
        assert _SCAN_ROOT.is_dir(), f"Audit scan root not found: {_SCAN_ROOT}"

    def test_no_bypass_in_production_backend(self) -> None:
        """Every StrictDataSchema-derived publish in src/snapper/** uses publish_to.

        The audit scans for ``.to_json().encode(...)`` AND
        ``.model_dump_json().encode(...)`` patterns within
        ``_PROXIMITY_LINES`` lines of ``send_multipart(``. Allow-list
        carves out the ``publish_to`` source line + module docstring
        example; everything else must route through the helper.
        """
        all_findings: list[tuple[Path, int, str]] = []
        for file_path in _iter_production_python_files():
            for line_number, line in _find_bypass_findings(file_path):
                all_findings.append((file_path, line_number, line))

        if all_findings:
            formatted = "\n".join(
                f"  {path.relative_to(_SCAN_ROOT.parent.parent.parent)}:{line_number} -> {line}"
                for path, line_number, line in all_findings
            )
            pytest.fail(
                "publish-bypass antipattern detected in production code.\n"
                "Every ZMQ publish of a StrictDataSchema-derived payload MUST "
                "route through StrictDataSchema.publish_to(topic) instead of "
                ".to_json().encode(...) / .model_dump_json().encode(...).\n"
                f"Findings:\n{formatted}"
            )

    def test_publish_to_is_present_at_chokepoint(self) -> None:
        """The audit only matters when publish_to exists; verify the helper is in place."""
        base_path = _SCAN_ROOT / "api" / "schemas" / "base.py"
        text = base_path.read_text(encoding="utf-8")
        assert "def publish_to(self, topic: str) -> bytes:" in text, (
            "StrictDataSchema.publish_to chokepoint helper is missing; "
            "the audit cannot enforce its use without the helper itself."
        )
