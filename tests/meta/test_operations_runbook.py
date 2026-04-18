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
    """The operations runbook must live at ``docs/operations.md``.

    Given: the repository root resolved relative to this meta-test,
    When: the meta-test checks ``docs/operations.md`` existence,
    Then: the file must exist — a missing runbook fails CI so an
        accidental delete never reaches main silently.
    """
    assert RUNBOOK_PATH.exists(), f"missing runbook: {RUNBOOK_PATH}"


def test_runbook_mentions_systemd_template_unit() -> None:
    """Systemd template recipe must be the canonical reference.

    Given: a full read of ``docs/operations.md``,
    When: the meta-test greps for ``systemd`` + ``trade-zmq@``,
    Then: both markers must be present — systemd template units
        (``snapper-trade-zmq@0.service``, ``@1.service``) are the
        stable recipe across 28 plan-review rounds.
    """
    body = _read_runbook()
    assert "systemd" in body.lower(), "runbook missing systemd reference"
    assert "trade-zmq@" in body, "runbook missing trade-zmq@ template unit reference"


def test_runbook_has_non_systemd_orchestrator_recipe() -> None:
    """Exactly one non-systemd orchestrator token must appear.

    Given: the runbook body + :data:`NON_SYSTEMD_TOKENS` tuple
        (``docker compose``, ``kubectl``, ``nomad``),
    When: the meta-test scans the body for any of the tokens,
    Then: at least one must match — bare ``docker`` (``docker run``
        alone) is rejected because it does not provide the
        per-instance lifecycle the scale-up / scale-down /
        crash-recovery contract requires.
    """
    body = _read_runbook()
    matched = [token for token in NON_SYSTEMD_TOKENS if token in body]
    assert (
        matched
    ), f"runbook missing non-systemd orchestrator token: expected one of {NON_SYSTEMD_TOKENS}"


def test_runbook_carries_provenance_line() -> None:
    """The ``Verified on YYYY-MM-DD against <platform>`` line gates freshness.

    Given: the runbook body,
    When: the meta-test matches
        :data:`PROVENANCE_REGEX` (multiline, anchored),
    Then: at least one line must satisfy the regex — operators
        need to see when the non-systemd recipe was last
        empirically verified against a specific platform version.
    """
    body = _read_runbook()
    match = PROVENANCE_REGEX.search(body)
    assert (
        match is not None
    ), f"runbook missing provenance line matching {PROVENANCE_REGEX.pattern!r}"


def test_runbook_mentions_full_cutover_invariant() -> None:
    """Full-cutover discipline is the explicit no-HA trade-off.

    Given: the lowercased runbook body,
    When: the meta-test checks for any of :data:`CUTOVER_PHRASES`,
    Then: at least one phrase (``full cutover`` / ``full-cutover``
        / ``improper restart with overlap``) must appear — the
        full-cutover invariant IS the operational discipline that
        compensates for the absence of HA in Phase 4 scope.
    """
    body = _read_runbook().lower()
    matched = [phrase for phrase in CUTOVER_PHRASES if phrase in body]
    assert matched, f"runbook missing cutover invariant phrase: expected one of {CUTOVER_PHRASES}"


def test_runbook_has_scale_up_scale_down_crash_recovery_sections() -> None:
    """All three failure-mode sections must be present.

    Given: the lowercased runbook body,
    When: the meta-test greps for scale-up / scale-down /
        crash-recovery markers,
    Then: all three markers must appear so operators can
        navigate directly to each Phase 4 failure-mode recipe
        without reading the full document.
    """
    body = _read_runbook().lower()
    assert "scale up" in body or "scale-up" in body, "runbook missing scale-up section"
    assert "scale down" in body or "scale-down" in body, "runbook missing scale-down section"
    assert (
        "crash recovery" in body or "crash-recovery" in body
    ), "runbook missing crash-recovery section"
