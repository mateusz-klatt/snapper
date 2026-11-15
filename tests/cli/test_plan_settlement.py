"""Tests for the operator execution-plan settlement CLI."""

import json
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import typer
from typer.testing import CliRunner

from snapper.application.plans.settlement import PlanChildOutcome
from snapper.application.plans.settlement import PlanSettlementCandidate
from snapper.application.plans.settlement import PlanSettlementDriftError
from snapper.application.plans.settlement import PlanSettlementPreview
from snapper.application.plans.settlement import PlanSettlementReceipt
from snapper.application.plans.settlement import PlanSettlementSkipped
from snapper.cli import plan_settlement
from snapper.cli.plan_settlement import main
from snapper.cli.plan_settlement import settle_plans
from snapper.core.json_types import JsonObject
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 11, 15, 12, 0, 0, tzinfo=UTC)
_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64
_TARGET_DB: JsonObject = {
    "dialect": "postgresql",
    "host": "prod-db",
    "port": 5432,
    "database": "snapper_prod",
}
_CODE_REV: JsonObject = {"snapper/application/plans/settlement.py": "c" * 64}


def _build_cli() -> typer.Typer:
    """Create a minimal Typer app mounting `settle-plans` for CLI tests.

    Returns:
        Configured `typer.Typer` application.
    """
    cli = typer.Typer(add_completion=False)
    cli.command(name="settle-plans")(settle_plans)
    return cli


def _sample_preview(
    *,
    mode: str | None = "live",
    digest: str = _DIGEST_A,
    with_candidates: bool = True,
) -> PlanSettlementPreview:
    """Construct a sample `PlanSettlementPreview` for CLI tests.

    Args:
        mode: Optional execution mode filter.
        digest: 64-character hex SHA-256 preview digest.
        with_candidates: Whether to include one candidate row.

    Returns:
        Frozen `PlanSettlementPreview`.
    """
    candidates = (
        (
            PlanSettlementCandidate(
                plan_public_id="01960000-0000-7000-8000-000000000010",
                plan_type="manual_once",
                exchange="kraken",
                mode="live",
                shard_key="kraken.XRP-EUR.live.w1",
                current_status="active",
                current_session_id="s1",
                current_sequence_id=1,
                current_filled_quantity=0.0,
                target_status="completed",
                filled_quantity=10.0,
                last_error=None,
                children=(
                    PlanChildOutcome(
                        client_order_id="cid-1",
                        command_public_id="01960000-0000-7000-8000-000000000020",
                        command_status="filled",
                        terminal_venue_event_id=101,
                        fill_venue_event_id=100,
                        cumulative_fill_size=10.0,
                        last_error=None,
                    ),
                ),
            ),
        )
        if with_candidates
        else ()
    )
    skipped = (
        PlanSettlementSkipped(
            plan_public_id="01960000-0000-7000-8000-000000000011",
            plan_type="manual_once",
            exchange="kraken",
            mode="live",
            current_status="cancel_requested",
            skip_code="non_terminal_child",
            detail="child cid-2 is in status accepted",
        ),
    )
    return PlanSettlementPreview(
        mode=mode,
        target_database=_TARGET_DB,
        code_revision=_CODE_REV,
        candidates=candidates,
        skipped=skipped,
        digest=digest,
    )


@pytest.fixture
def patched_cli_env(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch bootstrap settings and repository lifecycle for CLI tests.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        Mocked `SQLAlchemyRepository` instance.
    """
    repo = MagicMock(spec=SQLAlchemyRepository)
    monkeypatch.setattr(
        plan_settlement,
        "configured_repository",
        lambda db_url: repo,
    )
    monkeypatch.setattr(
        plan_settlement,
        "dispose_repositories",
        AsyncMock(),
    )
    monkeypatch.setattr(
        plan_settlement,
        "get_bootstrap_settings",
        lambda: MagicMock(db_url="postgresql+asyncpg://operator:secret@prod-db:5432/snapper_prod"),
    )
    return repo


def test_dry_run_prints_safe_db_identity_preview_and_digest(
    patched_cli_env: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run read-only preview by default without exposing DB credentials.

    Given: One eligible settlement candidate, one skipped plan, and a
        credential-bearing PostgreSQL URL with explicit host and port.
    When: `settle-plans` runs without `--confirm`.
    Then: It prints the sanitized database identity, operational disclosures,
        candidate child evidence ids, `preview_sha256`, and never invokes
        `apply_plan_settlements`.
    """
    preview_mock = AsyncMock(return_value=_sample_preview())
    apply_mock = AsyncMock()
    monkeypatch.setattr(plan_settlement, "preview_plan_settlements", preview_mock)
    monkeypatch.setattr(plan_settlement, "apply_plan_settlements", apply_mock)

    result = CliRunner().invoke(_build_cli(), [])

    assert result.exit_code == 0, result.stderr
    assert "host=prod-db database=snapper_prod" in result.stdout
    assert "operator" not in result.stdout
    assert "secret" not in result.stdout
    assert "mode scope: live" in result.stdout
    assert "run order requirement:" in result.stdout
    assert "residual concurrency window:" in result.stdout
    assert "settlement candidates: 1" in result.stdout
    assert "status=active->completed" in result.stdout
    assert "term_ev=101:fill_ev=100" in result.stdout
    assert "skipped active plans: 1" in result.stdout
    assert f"preview_sha256: {_DIGEST_A}" in result.stdout
    assert "DRY RUN: nothing was written" in result.stdout
    apply_mock.assert_not_awaited()


def test_confirm_with_matching_digest_applies_and_prints_json_receipt(
    patched_cli_env: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Apply previewed settlements when `--confirm` and `--expect-digest` match.

    Given: A `--mode all` preview with digest `_DIGEST_A`.
    When: `settle-plans --mode all --confirm --expect-digest` supplies the
        uppercase spelling of `_DIGEST_A`.
    Then: `apply_plan_settlements` is awaited once and stdout ends with a
        valid sorted JSON receipt including child evidence ids, endpoint, and
        code revision.
    """
    preview = _sample_preview(mode=None, digest=_DIGEST_A)
    monkeypatch.setattr(
        plan_settlement,
        "preview_plan_settlements",
        AsyncMock(return_value=preview),
    )
    apply_mock = AsyncMock(
        return_value=PlanSettlementReceipt(
            mode=None,
            target_database=_TARGET_DB,
            code_revision=_CODE_REV,
            digest=_DIGEST_A,
            settled_public_ids=("01960000-0000-7000-8000-000000000010",),
            settled_candidates=preview.candidates,
            settled_at=_NOW,
            session_id="cli-settle-plans-fixed",
            count=1,
        )
    )
    monkeypatch.setattr(plan_settlement, "apply_plan_settlements", apply_mock)

    result = CliRunner().invoke(
        _build_cli(),
        ["--mode", "all", "--confirm", "--expect-digest", _DIGEST_A.upper()],
    )

    assert result.exit_code == 0, result.stderr
    assert "mode scope: all" in result.stdout
    assert f"settled 1 execution plan(s) at {_NOW.isoformat()}" in result.stdout
    last_line = result.stdout.strip().splitlines()[-1]
    receipt_json = json.loads(last_line)
    assert receipt_json["count"] == 1
    assert receipt_json["preview_sha256"] == _DIGEST_A
    assert receipt_json["target_database"] == _TARGET_DB
    assert receipt_json["code_revision"] == _CODE_REV
    assert receipt_json["settled_plans"][0]["children"][0]["terminal_venue_event_id"] == 101
    apply_mock.assert_awaited_once()


def test_validation_refuses_invalid_mode_digest_or_unsafe_database_url(
    patched_cli_env: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse bad CLI arguments or unsafe DB URLs before opening the repository.

    Given: Invocations with an unknown `--mode`, `--confirm` without
        `--expect-digest`, a non-sha256 `--expect-digest`, and an in-memory
        SQLite `db_url`.
    When: Typer dispatches `settle-plans`.
    Then: Each exits with status 1 and never awaits `preview_plan_settlements`.
    """
    preview_mock = AsyncMock()
    monkeypatch.setattr(plan_settlement, "preview_plan_settlements", preview_mock)
    cli = _build_cli()

    bad_mode = CliRunner().invoke(cli, ["--mode", "staging"])
    no_digest = CliRunner().invoke(cli, ["--confirm"])
    bad_digest = CliRunner().invoke(cli, ["--expect-digest", "not-hex"])
    monkeypatch.setattr(
        plan_settlement,
        "get_bootstrap_settings",
        lambda: MagicMock(db_url="sqlite+aiosqlite:///:memory:"),
    )
    bad_db = CliRunner().invoke(cli, [])

    assert bad_mode.exit_code == 1
    assert "--mode must be one of" in bad_mode.stderr
    assert no_digest.exit_code == 1
    assert "--confirm requires --expect-digest" in no_digest.stderr
    assert bad_digest.exit_code == 1
    assert "64-character hexadecimal sha256" in bad_digest.stderr
    assert bad_db.exit_code == 1
    assert "refused: a SQLite target must be a database file" in bad_db.stderr
    preview_mock.assert_not_awaited()


def test_digest_mismatch_empty_candidates_and_apply_drift_are_handled_safely(
    patched_cli_env: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Handle digest mismatch, zero-candidate no-ops, and concurrent drift.

    Given: Mocked previews covering mismatched digest, empty candidate list, and
        `PlanSettlementDriftError` during apply.
    When: `settle-plans` runs against each condition.
    Then: Mismatch and drift exit 1 on stderr, and zero candidates exits 0
        without calling `apply_plan_settlements`.
    """
    cli = _build_cli()
    apply_mock = AsyncMock()
    monkeypatch.setattr(plan_settlement, "apply_plan_settlements", apply_mock)

    monkeypatch.setattr(
        plan_settlement,
        "preview_plan_settlements",
        AsyncMock(return_value=_sample_preview(digest=_DIGEST_A)),
    )
    mismatch = CliRunner().invoke(cli, ["--confirm", "--expect-digest", _DIGEST_B])
    assert mismatch.exit_code == 1
    assert "differs from current preview_sha256" in mismatch.stderr
    apply_mock.assert_not_awaited()

    monkeypatch.setattr(
        plan_settlement,
        "preview_plan_settlements",
        AsyncMock(return_value=_sample_preview(digest=_DIGEST_A, with_candidates=False)),
    )
    empty = CliRunner().invoke(cli, ["--confirm", "--expect-digest", _DIGEST_A])
    assert empty.exit_code == 0, empty.stderr
    assert "nothing to do; settled 0 execution plans" in empty.stdout
    apply_mock.assert_not_awaited()

    monkeypatch.setattr(
        plan_settlement,
        "preview_plan_settlements",
        AsyncMock(return_value=_sample_preview(digest=_DIGEST_A)),
    )
    monkeypatch.setattr(
        plan_settlement,
        "apply_plan_settlements",
        AsyncMock(side_effect=PlanSettlementDriftError("concurrent plan update")),
    )
    drift = CliRunner().invoke(cli, ["--confirm", "--expect-digest", _DIGEST_A])
    assert drift.exit_code == 1
    assert "refused: concurrent plan update" in drift.stderr


def test_main_entrypoint_returns_zero_on_dry_run_and_one_on_refusal(
    patched_cli_env: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Support standalone `python -m snapper.cli.plan_settlement` invocation.

    Given: Patched bootstrap settings and a mocked settlement preview.
    When: `main` runs with dry-run arguments and with unconfirmed `--confirm`.
    Then: Dry run returns exit code 0 and refusal returns exit code 1.
    """
    monkeypatch.setattr(
        plan_settlement,
        "preview_plan_settlements",
        AsyncMock(return_value=_sample_preview()),
    )
    assert main(["--mode", "live"]) == 0
    assert main(["--confirm"]) == 1
