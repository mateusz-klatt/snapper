"""CLI rendering and operator refusal witnesses for FX proof backfill."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from typer.testing import CliRunner

from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinMetrics
from snapper.application.portfolio.fx_proof_backfill import FxProofBackfillResult
from snapper.cli.fx_proofs import _render
from snapper.cli.fx_proofs import _run
from snapper.cli.fx_proofs import fx_proofs_app
from snapper.config.settings import AppSettings
from snapper.data.repository import Repository
from tests.application.portfolio.test_fx_proof_backfill import _requirement


def test_backfill_command_renders_groups_refusals_and_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The operator receives grouped gaps, explicit refusals, and all counters.

    Given one provable and one refused unpinned requirement
    When the default report command renders its result
    Then grouping, semantic refusal, mode, and seven counters are printed
    """
    requirement = _requirement()
    refusal = replace(
        requirement,
        consumer_public_id="00000000-0000-7000-8000-000000000899",
        evaluation=replace(requirement.evaluation, selected_plane=None, rows=()),
    )
    result = FxProofBackfillResult(
        requirements=(requirement, refusal),
        semantic_refusals=("wallet | USD | 5B.2 | execution identity unproven",),
        metrics=FxShadowPinMetrics(creation=1, reuse=2),
        processed=0,
    )

    async def run(apply: bool, checkpoint: Path) -> FxProofBackfillResult:
        """Return the fixed report without opening a database."""
        return result

    monkeypatch.setattr("snapper.cli.fx_proofs._run", run)
    invoked = CliRunner().invoke(fx_proofs_app, [])
    assert invoked.exit_code == 0
    assert "Unpinned requirements" in invoked.stdout
    assert "fx_conversion_unproven" in invoked.stdout
    assert "creation | 1" in invoked.stdout
    assert "reuse | 2" in invoked.stdout
    assert "mode | report" in invoked.stdout
    applied = CliRunner().invoke(fx_proofs_app, ["--apply"])
    assert applied.exit_code == 1


def test_backfill_command_reports_operator_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Validated discovery failures become stable nonzero operator refusals.

    Given discovery cannot prove a durable requirement identity
    When the apply command is invoked
    Then the semantic error is emitted and the command exits nonzero
    """

    async def run(apply: bool, checkpoint: Path) -> FxProofBackfillResult:
        """Raise the same validated failure produced by discovery."""
        raise ValueError("identity cannot be proven")

    monkeypatch.setattr("snapper.cli.fx_proofs._run", run)
    invoked = CliRunner().invoke(fx_proofs_app, ["--apply"])
    assert invoked.exit_code == 1
    assert "refused: identity cannot be proven" in invoked.output


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
    expected = FxProofBackfillResult((), (), FxShadowPinMetrics(), 0)
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
