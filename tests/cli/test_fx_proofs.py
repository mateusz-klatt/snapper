"""CLI post-state rendering, locking, reset, and exit-code witnesses."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from typer.testing import CliRunner

from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinMetrics
from snapper.application.portfolio.fx_proof_backfill import FX_PROOF_BACKFILL_CHECKPOINT
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillResult
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillSemanticRefusal
from snapper.cli.fx_proofs import _anchored_checkpoint
from snapper.cli.fx_proofs import _render
from snapper.cli.fx_proofs import _run
from snapper.cli.fx_proofs import _run_locked
from snapper.cli.fx_proofs import fx_proofs_app
from snapper.config.settings import AppSettings
from snapper.data.repository import Repository
from tests.application.portfolio.test_fx_proof_backfill import _requirement


def _result(
    adverse: bool = False,
    semantic: bool = True,
) -> FxProofBackfillResult:
    """Build one post-apply result with proof, refusal, and optional failure."""
    proof = replace(_requirement(), pinned=True)
    refusal = replace(
        _requirement(),
        evaluation=replace(_requirement().evaluation, selected_plane=None, rows=()),
        pinned=False,
        refusal_audited=True,
    )
    semantic_refusals = (
        (
            FxProofBackfillSemanticRefusal(
                wallet_public_id=proof.wallet_public_id,
                valuation_ccy="USD",
                calculation_version="5B.2",
                instrument_public_id="lost-instrument",
                reason="execution_price_provenance_unproven",
                lost_requirements=("execution:e1@minute",),
            ),
        )
        if semantic
        else ()
    )
    return FxProofBackfillResult(
        requirements=(proof, refusal),
        semantic_refusals=semantic_refusals,
        metrics=FxShadowPinMetrics(creation=2, reuse=1, failure=int(adverse)),
        processed_consumers=1,
        proof_creations=1,
        refusal_audit_creations=1,
        aborted=adverse,
        fully_verified=not adverse,
    )


def test_command_renders_now_state_classified_writes_and_adverse_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The operator sees post-state and Typer's nonzero exit is not swallowed.

    Given proof-bearing and refusal-audit creations plus an adverse apply result
    When report and apply commands render
    Then NOW states and classified counts print, failure is a refusal, and apply exits one
    """
    healthy = _result()
    adverse = _result(adverse=True)

    def run_locked(apply: bool, checkpoint: Path, reset_checkpoint: bool) -> FxProofBackfillResult:
        """Return the result selected by mutation mode."""
        return adverse if apply else healthy

    monkeypatch.setattr("snapper.cli.fx_proofs._run_locked", run_locked)
    report = CliRunner().invoke(fx_proofs_app, [])
    applied = CliRunner().invoke(
        fx_proofs_app,
        ["--apply", "--checkpoint", str(tmp_path / "cursor.json")],
    )
    assert report.exit_code == 0
    assert "consumer-horizon state" in report.stdout
    assert "proof creations | 1" in report.stdout
    assert "refusal audit creations | 1" in report.stdout
    assert "lost=execution:e1@minute" in report.stdout
    assert applied.exit_code == 1
    assert "persistence | aborted=true | failure=1" in applied.stdout
    assert "refused:" not in applied.stdout


def test_command_validation_error_and_checkpoint_anchoring(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Relative paths anchor to the repo and validated errors remain stable.

    Given relative and absolute cursor paths plus a reset without apply
    When path normalization and the command validation execute
    Then paths are deterministic and the invalid reset exits nonzero

    The absolute case takes its path from ``tmp_path`` rather than a POSIX
    literal: on Windows a rootless path such as ``/tmp/cursor.json`` carries no
    drive, so ``Path.is_absolute`` is False and the anchoring under test would
    be asked the wrong question.
    """
    already_absolute = tmp_path / "cursor.json"
    relative = _anchored_checkpoint(Path("custom/cursor.json"))
    absolute = _anchored_checkpoint(already_absolute)
    assert relative == FX_PROOF_BACKFILL_CHECKPOINT.parents[1] / "custom/cursor.json"
    assert absolute == already_absolute
    invoked = CliRunner().invoke(fx_proofs_app, ["--reset-checkpoint"])
    assert invoked.exit_code == 1
    assert "--reset-checkpoint requires --apply" in invoked.output


@pytest.mark.asyncio
async def test_run_opens_configured_repository_and_empty_report_renders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The async seam uses configured storage and an empty report remains explicit.

    Given a configured database URL and an empty successful report
    When the async runner and renderer execute
    Then the repository is forwarded and the refusal section states none
    """
    repository = cast(Repository, object())
    expected = FxProofBackfillResult((), (), FxShadowPinMetrics(), 0, 0, 0, False, True)
    monkeypatch.setattr(
        "snapper.cli.fx_proofs.get_settings",
        lambda: cast(AppSettings, SimpleNamespace(db_url="sqlite+aiosqlite:///unused.db")),
    )
    monkeypatch.setattr("snapper.cli.fx_proofs.get_repository", lambda url: repository)

    async def run_backfill(
        selected: Repository, apply: bool, checkpoint: Path
    ) -> FxProofBackfillResult:
        """Verify and return the fixed empty result."""
        assert selected is repository
        return expected

    monkeypatch.setattr("snapper.cli.fx_proofs.run_fx_proof_backfill", run_backfill)
    assert await _run(False, tmp_path / "cursor.json") is expected
    _render(expected, False)
    assert "Refusals\nnone" in capsys.readouterr().out


def test_locked_runner_resets_only_under_apply_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reset and apply occur inside the same single-writer critical section.

    Given a cursor file and a stub async runner
    When locked apply requests reset
    Then the cursor is absent before the runner starts and the result returns
    """
    checkpoint = tmp_path / "cursor.json"
    checkpoint.write_text("stale", encoding="utf-8")
    expected = _result(semantic=False)

    async def run(apply: bool, selected: Path) -> FxProofBackfillResult:
        """Assert reset completed before repository work."""
        assert not selected.exists()
        return expected

    monkeypatch.setattr("snapper.cli.fx_proofs._run", run)
    assert _run_locked(True, checkpoint, True) is expected
    assert _run_locked(True, checkpoint, False) is expected
    assert _run_locked(False, checkpoint, False) is expected
