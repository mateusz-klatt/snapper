"""Tests for the trade-projection checkpoint retirement CLI."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from snapper.cli import trade_projection_checkpoints
from snapper.cli.app import app

_NOW = datetime(2026, 7, 28, 1, 30, tzinfo=UTC)


def _checkpoint(shard_key: str, public_id: str) -> dict[str, object]:
    """Build one complete checkpoint result for CLI tests."""
    return {
        "public_id": public_id,
        "shard_key": shard_key,
        "position_qty": 21.04,
        "entry_price": 4.3,
        "position_opened_at": _NOW,
        "cash": 100.0,
        "peak_equity": 110.0,
        "realized_pnl": 0.0,
        "turnover": 92.24084400000001,
        "last_venue_event_id": 2,
        "last_venue_event_at": _NOW,
        "open_command_ids": None,
        "seen_exec_ids": "[]",
        "checkpoint_at": _NOW,
        "session_id": "session",
        "wallet_public_id": "00000000-0000-7000-8000-000000000001",
        "operator_public_id": None,
    }


@pytest.fixture
def repository(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Provide a mocked repository without opening a database connection."""
    mocked = MagicMock()
    mocked.get_trade_projection_checkpoint_retirement_candidates = AsyncMock(
        return_value=[
            _checkpoint("paper.BTC-USD.paper.example", "cp-paper"),
            _checkpoint("walutomat.EUR-PLN.live.example", "cp-live"),
        ]
    )
    mocked.retire_trade_projection_checkpoints = AsyncMock(return_value=2)
    monkeypatch.setattr(trade_projection_checkpoints, "_repository", lambda db_url: mocked)
    monkeypatch.setattr(
        trade_projection_checkpoints,
        "dispose_repositories",
        AsyncMock(),
    )
    monkeypatch.setattr(
        trade_projection_checkpoints,
        "get_bootstrap_settings",
        lambda: MagicMock(db_url="postgresql+asyncpg://operator:secret@prod-db/snapper_prod"),
    )
    return mocked


def test_dry_run_previews_every_value_and_writes_nothing(repository: MagicMock) -> None:
    """The default path is a complete read-only preview.

    Given: Two active checkpoints and a credential-bearing production URL.
    When: The all-checkpoints command runs without ``--confirm``.
    Then: It prints the safe database identity and every required economic
        field, never exposes credentials, and never calls the writer.
    """
    result = CliRunner().invoke(app, ["retire-trade-projection-checkpoints", "--all"])

    assert result.exit_code == 0, result.stderr
    assert "host=prod-db database=snapper_prod" in result.stdout
    assert "operator" not in result.stdout
    assert "secret" not in result.stdout
    assert "candidate checkpoints: 2" in result.stdout
    assert "shard_key=walutomat.EUR-PLN.live.example position_qty=21.04" in result.stdout
    assert "realized_pnl=0.0 turnover=92.24084400000001" in result.stdout
    assert f"checkpoint_at={_NOW.isoformat()}" in result.stdout
    assert "DRY RUN: nothing was written" in result.stdout
    repository.retire_trade_projection_checkpoints.assert_not_awaited()


def test_confirm_closes_exactly_the_previewed_single_shard(repository: MagicMock) -> None:
    """Confirmation remains separate from selection and preserves its scope.

    Given: Two active sibling checkpoints.
    When: One exact shard is selected and separately confirmed.
    Then: The writer receives only that previewed public id and reports one
        closed row.
    """
    repository.retire_trade_projection_checkpoints.return_value = 1
    repository.get_trade_projection_checkpoint_retirement_candidates.return_value = [
        _checkpoint("walutomat.EUR-PLN.live.example", "cp-live")
    ]

    result = CliRunner().invoke(
        app,
        [
            "retire-trade-projection-checkpoints",
            "--shard-key",
            "walutomat.EUR-PLN.live.example",
            "--confirm",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "candidate checkpoints: 1" in result.stdout
    assert "closed 1 checkpoint rows" in result.stdout
    args = repository.retire_trade_projection_checkpoints.await_args.args
    assert args[0] == "walutomat.EUR-PLN.live.example"
    assert args[1] == ["cp-live"]


def test_selection_must_be_explicit_and_separate_from_confirmation(
    repository: MagicMock,
) -> None:
    """A confirmation flag cannot select a destructive target.

    Given: A command invocation containing only ``--confirm``.
    When: Typer dispatches the command.
    Then: It refuses before database resolution or any write.
    """
    result = CliRunner().invoke(app, ["retire-trade-projection-checkpoints", "--confirm"])

    assert result.exit_code == 1
    assert "supply exactly one of --shard-key or --all" in result.stderr
    repository.get_trade_projection_checkpoint_retirement_candidates.assert_not_awaited()
    repository.retire_trade_projection_checkpoints.assert_not_awaited()


def test_all_and_single_selection_cannot_be_combined(repository: MagicMock) -> None:
    """Ambiguous destructive scope is rejected.

    Given: Both supported selectors in one invocation.
    When: The command validates the selection.
    Then: It refuses before reading or writing the database.
    """
    result = CliRunner().invoke(
        app,
        [
            "retire-trade-projection-checkpoints",
            "--all",
            "--shard-key",
            "walutomat.EUR-PLN.live.example",
        ],
    )

    assert result.exit_code == 1
    repository.get_trade_projection_checkpoint_retirement_candidates.assert_not_awaited()
    repository.retire_trade_projection_checkpoints.assert_not_awaited()


def test_rerun_with_no_active_candidate_is_an_honest_no_op(repository: MagicMock) -> None:
    """A completed retirement cannot be applied twice.

    Given: No currently active checkpoint for the selected shard.
    When: The confirmed command is rerun.
    Then: It reports zero candidates and zero closes without calling the writer.
    """
    repository.get_trade_projection_checkpoint_retirement_candidates.return_value = []

    result = CliRunner().invoke(
        app,
        [
            "retire-trade-projection-checkpoints",
            "--shard-key",
            "walutomat.EUR-PLN.live.example",
            "--confirm",
        ],
    )

    assert result.exit_code == 0, result.stderr
    assert "candidate checkpoints: 0" in result.stdout
    assert "nothing to do; closed 0 checkpoint rows" in result.stdout
    repository.retire_trade_projection_checkpoints.assert_not_awaited()


def test_writer_count_mismatch_fails_loudly(repository: MagicMock) -> None:
    """A database count disagreement is never reported as success.

    Given: Two previewed rows and a writer reporting one close.
    When: The all-checkpoints command is confirmed.
    Then: It exits non-zero with both counts.
    """
    repository.retire_trade_projection_checkpoints.return_value = 1

    result = CliRunner().invoke(app, ["retire-trade-projection-checkpoints", "--all", "--confirm"])

    assert result.exit_code == 1
    assert "close count mismatch: previewed=2 closed=1" in result.stderr


def test_repository_resolution_uses_the_configured_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repository resolution passes through the exact configured URL.

    Given: A patched repository factory and one database URL.
    When: The CLI repository helper resolves it.
    Then: It returns the factory result and passes the URL unchanged.
    """
    resolved = MagicMock()
    factory = MagicMock(return_value=resolved)
    monkeypatch.setattr(trade_projection_checkpoints, "get_repository", factory)

    result = trade_projection_checkpoints._repository("sqlite+aiosqlite:///fixture.db")

    assert result is resolved
    factory.assert_called_once_with("sqlite+aiosqlite:///fixture.db")


def test_writer_refusal_is_printed_and_exits_nonzero(repository: MagicMock) -> None:
    """A guarded-writer refusal cannot be mistaken for success.

    Given: A repository writer detecting that the previewed set changed.
    When: The confirmed command reaches the writer.
    Then: It exits non-zero and prints the refusal.
    """
    repository.retire_trade_projection_checkpoints.side_effect = ValueError(
        "checkpoint set changed after preview; nothing was written"
    )

    result = CliRunner().invoke(app, ["retire-trade-projection-checkpoints", "--all", "--confirm"])

    assert result.exit_code == 1
    assert "refused: checkpoint set changed after preview" in result.stderr
