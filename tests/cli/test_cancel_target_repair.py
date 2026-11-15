"""Tests of the ``snapper repair-cancel-targets`` operator command (G3-C)."""

import asyncio
import json
import re
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import typer
from click.testing import Result
from typer.testing import CliRunner

from snapper.application.trade.cancel_target_repair import CancelTargetRepairRefusedError
from snapper.cli import cancel_target_repair as command_module
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.cancel_target_repair import SeededPair
from tests.helpers.cancel_target_repair import seed_kraken_pair

_DIGEST = re.compile(r"digest: ([0-9a-f]{64})")


def _app() -> typer.Typer:
    """Mount the command on its own app, as ``cli/app.py`` will."""
    app = typer.Typer()
    app.command(name="repair-cancel-targets")(command_module.repair_cancel_targets)
    app.command(name="noop")(lambda: None)
    return app


@pytest.fixture
def database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[str, SeededPair]:
    """Seed a SQLite database with one Kraken pair and point the command at it."""
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'cli.db'}"

    async def seed() -> SeededPair:
        repository = SQLAlchemyRepository(db_url)
        await repository.create_all()
        try:
            return await seed_kraken_pair(repository)
        finally:
            await repository.engine.dispose()

    pair = asyncio.run(seed())
    monkeypatch.setattr(command_module, "get_bootstrap_settings", lambda: MagicMock(db_url=db_url))
    return db_url, pair


def _invoke(*arguments: str) -> Result:
    """Run the command with the given options."""
    return CliRunner().invoke(_app(), ["repair-cancel-targets", *arguments])


def test_dry_run_then_confirm_records_the_terminal(
    database: tuple[str, SeededPair], tmp_path: Path
) -> None:
    """The dry run writes the vote files and the confirmed run applies them.

    Given: A database with one repairable Kraken pair,
    When: The dry run runs, then the write with its directory and digest,
    Then: Both exit 0, the files exist, the receipt says recorded, and no URL is printed.
    """
    db_url, pair = database
    voted = tmp_path / "voted"
    dry = _invoke("--cancel", pair.cancel_id, "--output-dir", str(voted))
    assert dry.exit_code == 0, dry.output
    assert "DRY RUN" in dry.output
    assert db_url not in dry.output
    match = _DIGEST.search(dry.output)
    assert match is not None
    assert (voted / "preview.json").exists() and (voted / "summary.json").exists()

    written = tmp_path / "written"
    write = _invoke(
        "--cancel",
        pair.cancel_id,
        "--output-dir",
        str(written),
        "--confirm",
        "--voted-preview",
        str(voted),
        "--expect-digest",
        match.group(1).upper(),
    )
    assert write.exit_code == 0, write.output
    receipt = json.loads((written / "receipt.json").read_text())
    assert receipt["results"] == [{"cancel_public_id": pair.cancel_id, "result": "recorded"}]
    assert db_url not in write.output


def test_a_write_that_skips_a_line_exits_one(
    database: tuple[str, SeededPair], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A write whose line was not applied exits 1.

    Given: A write that reports changed_before_write,
    When: The confirmed run finishes,
    Then: It exits 1 and still writes the receipt.
    """
    _, pair = database
    plan = MagicMock(preview=[], summary={}, digest="d" * 64)
    monkeypatch.setattr(
        command_module,
        "write_repair",
        AsyncMock(return_value=(plan, [{"result": "changed_before_write"}])),
    )
    voted = tmp_path / "voted"
    voted.mkdir()
    result = _invoke(
        "--cancel",
        pair.cancel_id,
        "--output-dir",
        str(tmp_path / "out"),
        "--confirm",
        "--voted-preview",
        str(voted),
        "--expect-digest",
        "d" * 64,
    )
    assert result.exit_code == 1, result.output
    assert (tmp_path / "out" / "receipt.json").exists()


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (("--cancel", "not-a-uuid"), "--cancel is not a UUID"),
        (
            (
                "--cancel",
                "00000000-0000-7000-8000-0000000001ca",
                "--cancel",
                "00000000-0000-7000-8000-0000000001ca",
            ),
            "twice",
        ),
        (
            ("--cancel", "00000000-0000-7000-8000-0000000001ca", "--expect-digest", "0" * 64),
            "only apply with --confirm",
        ),
        (
            ("--cancel", "00000000-0000-7000-8000-0000000001ca", "--confirm"),
            "--confirm needs --voted-preview",
        ),
    ],
)
def test_argument_refusals_exit_one(
    database: tuple[str, SeededPair],
    tmp_path: Path,
    arguments: tuple[str, ...],
    message: str,
) -> None:
    """Malformed, repeated or mismatched options refuse before any read.

    Given: Options of the stated shape,
    When: The command runs,
    Then: It exits 1 with the refusal and writes nothing.
    """
    result = _invoke(*arguments, "--output-dir", str(tmp_path / "out"))
    assert result.exit_code == 1
    assert message in result.output
    assert not (tmp_path / "out").exists()


def test_a_non_empty_output_directory_refuses(
    database: tuple[str, SeededPair], tmp_path: Path
) -> None:
    """An output directory that already holds files refuses.

    Given: A directory with a file in it,
    When: The dry run is pointed at it,
    Then: It exits 1 with the refusal.
    """
    _, pair = database
    busy = tmp_path / "busy"
    busy.mkdir()
    (busy / "old.json").write_text("{}")
    result = _invoke("--cancel", pair.cancel_id, "--output-dir", str(busy))
    assert result.exit_code == 1
    assert "refused: --output-dir must be empty" in result.output


def test_an_unexpected_failure_prints_its_class_only(
    database: tuple[str, SeededPair], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unexpected error prints its class name and no text.

    Given: A plan that raises an error carrying secret text,
    When: The dry run runs,
    Then: It exits 1 naming the class only.
    """
    _, pair = database
    monkeypatch.setattr(
        command_module, "plan_repair", AsyncMock(side_effect=ValueError("secret text"))
    )
    result = _invoke("--cancel", pair.cancel_id, "--output-dir", str(tmp_path / "out"))
    assert result.exit_code == 1
    assert "failed: ValueError" in result.output
    assert "secret text" not in result.output


def test_canonical_cancel_ids_keeps_the_order_in_canonical_spelling() -> None:
    """Valid ids keep their order and are spelled canonically.

    Given: Two ids, one in upper case,
    When: They are validated,
    Then: Both come back lower-case, in the given order.
    """
    first = "00000000-0000-7000-8000-0000000000AA"
    second = "00000000-0000-7000-8000-000000000001"
    assert command_module.canonical_cancel_ids([first, second]) == [first.lower(), second]


def test_write_target_returns_the_vote_for_a_confirmed_run(tmp_path: Path) -> None:
    """A confirmed run with both write options returns them, digest lowered.

    Given: --confirm with a voted directory and an upper-case digest,
    When: The write options are cross-checked,
    Then: The directory and the lower-case digest are returned; a bad digest refuses.
    """
    assert command_module.write_target(True, tmp_path, "A" * 64) == (tmp_path, "a" * 64)
    assert command_module.write_target(False, None, None) is None
    with pytest.raises(CancelTargetRepairRefusedError):
        command_module.write_target(True, tmp_path, "not-a-digest")
