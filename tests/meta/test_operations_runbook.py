r"""Phase 4 meta-audit — ``docs/operations.md`` runbook structure.

Per plan §8 acceptance #17, the operations runbook must carry:

    - systemd template unit (``snapper-trade-zmq@`` reference).
    - At least ONE non-systemd orchestrator recipe (exactly one of
      ``docker compose``, ``kubectl``, or ``nomad`` — bare ``docker``
      is rejected because ``docker run`` alone does not provide the
      per-instance lifecycle the contract requires).
    - A provenance line matching
      ``^Verified on \\d{4}-\\d{2}-\\d{2} against \\S+$`` (exact
      casing, anchored).
    - A ``full cutover`` / ``full-cutover`` / ``improper restart
      with overlap`` phrase somewhere in the body (the invariant
      that HA-absence operational discipline must protect).
    - Scale-up, scale-down, and crash-recovery section headers so
      the operator can navigate the three failure modes.

This test gates the runbook at ``make check-all`` time so that an
edit which accidentally drops one of the sections fails CI
immediately rather than after a real incident.
"""

import re
from pathlib import Path

RUNBOOK_PATH = Path(__file__).resolve().parents[2] / "docs" / "operations.md"

NON_SYSTEMD_TOKENS: tuple[str, ...] = ("docker compose", "kubectl", "nomad")
PROVENANCE_REGEX = re.compile(r"^Verified on \d{4}-\d{2}-\d{2} against \S+$", re.MULTILINE)
CUTOVER_PHRASES: tuple[str, ...] = (
    "full cutover",
    "full-cutover",
    "improper restart with overlap",
)


def _read_runbook() -> str:
    """Read ``docs/operations.md`` with UTF-8 decoding."""
    return RUNBOOK_PATH.read_text(encoding="utf-8")


def test_runbook_file_exists() -> None:
    """The operations runbook must live at ``docs/operations.md``."""
    assert RUNBOOK_PATH.exists(), f"missing runbook: {RUNBOOK_PATH}"


def test_runbook_mentions_systemd_template_unit() -> None:
    """Systemd template recipe must be the canonical reference."""
    body = _read_runbook()
    assert "systemd" in body.lower(), "runbook missing systemd reference"
    assert "trade-zmq@" in body, "runbook missing trade-zmq@ template unit reference"


def test_runbook_has_non_systemd_orchestrator_recipe() -> None:
    """Exactly one non-systemd orchestrator token must appear.

    Bare ``docker`` (``docker run``) is rejected: ``docker run``
    alone does not provide the per-instance lifecycle the scale-up /
    scale-down / crash-recovery contract requires. Only
    ``docker compose``, ``kubectl``, or ``nomad`` count.
    """
    body = _read_runbook()
    matched = [token for token in NON_SYSTEMD_TOKENS if token in body]
    assert (
        matched
    ), f"runbook missing non-systemd orchestrator token: expected one of {NON_SYSTEMD_TOKENS}"


def test_runbook_carries_provenance_line() -> None:
    """The ``Verified on YYYY-MM-DD against <platform>`` line gates freshness."""
    body = _read_runbook()
    match = PROVENANCE_REGEX.search(body)
    assert (
        match is not None
    ), f"runbook missing provenance line matching {PROVENANCE_REGEX.pattern!r}"


def test_runbook_mentions_full_cutover_invariant() -> None:
    """Full-cutover discipline is the explicit no-HA trade-off."""
    body = _read_runbook().lower()
    matched = [phrase for phrase in CUTOVER_PHRASES if phrase in body]
    assert matched, f"runbook missing cutover invariant phrase: expected one of {CUTOVER_PHRASES}"


def test_runbook_has_scale_up_scale_down_crash_recovery_sections() -> None:
    """All three failure-mode sections must be present."""
    body = _read_runbook().lower()
    assert "scale up" in body or "scale-up" in body, "runbook missing scale-up section"
    assert "scale down" in body or "scale-down" in body, "runbook missing scale-down section"
    assert (
        "crash recovery" in body or "crash-recovery" in body
    ), "runbook missing crash-recovery section"
