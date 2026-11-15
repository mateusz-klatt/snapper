"""Tests of the ``snapper repair-cancel-targets`` operator command (G3-C)."""

import asyncio
import json
import re
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import typer
from click.testing import Result
from typer.testing import CliRunner

from snapper.application.trade.cancel_target_repair import CancelTargetRepairRefusedError
from snapper.application.trade.cancel_target_repair import WriteSession
from snapper.cli import cancel_target_repair as command_module
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.cancel_target_repair import SeededPair
from tests.helpers.cancel_target_repair import seed_kraken_pair

_DIGEST = re.compile(r"digest: ([0-9a-f]{64})")
_SESSION = re.compile(r"write session: ([0-9a-f-]{36})")
_STUB_SESSION_ID = "00000000-0000-7000-8000-0000000000ab"
_STUB_WRITE_TIME = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)


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


def _stub_session() -> WriteSession:
    """Build a prepared write over a stub plan, with no line applied."""
    plan = MagicMock(preview=[], summary={}, digest="d" * 64)
    return WriteSession(
        plan=plan, session_id=_STUB_SESSION_ID, write_time=_STUB_WRITE_TIME, results=[]
    )


def _confirmed(pair: SeededPair, tmp_path: Path) -> Result:
    """Run a confirmed write into ``tmp_path / "out"`` against a stub vote directory."""
    voted = tmp_path / "voted"
    voted.mkdir()
    return _invoke(
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
    session = _SESSION.search(write.output)
    assert session is not None
    receipt = json.loads((written / "receipt.json").read_text())
    assert receipt["session_id"] == session.group(1)
    assert receipt["digest"] == match.group(1)
    assert datetime.fromisoformat(receipt["write_time"]).tzinfo is not None
    [line] = receipt["results"]
    assert (line["cancel_public_id"], line["result"], line["sequence_id"]) == (
        pair.cancel_id,
        "recorded",
        1,
    )
    assert isinstance(line["venue_event_id"], int) and isinstance(
        line["venue_event_public_id"], str
    )
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
    session = _stub_session()

    async def skip(repository: SQLAlchemyRepository, prepared: WriteSession) -> None:
        prepared.results.append(
            {"cancel_public_id": pair.cancel_id, "result": "changed_before_write"}
        )

    monkeypatch.setattr(command_module, "prepare_write", AsyncMock(return_value=session))
    monkeypatch.setattr(command_module, "apply_write", skip)
    result = _confirmed(pair, tmp_path)
    assert result.exit_code == 1, result.output
    receipt = json.loads((tmp_path / "out" / "receipt.json").read_text())
    assert receipt["results"] == [
        {"cancel_public_id": pair.cancel_id, "result": "changed_before_write"}
    ]


def test_a_write_that_raises_still_leaves_its_receipt(
    database: tuple[str, SeededPair], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A write interrupted after a committed line keeps a receipt of what it applied.

    Given: A write that applies one line and then raises with secret text,
    When: The confirmed run fails,
    Then: It exits 1 naming the class only, the session id was printed first, and the
        receipt holds the session id, the write time and the applied line.
    """
    _, pair = database
    session = _stub_session()

    async def fail_after_one(repository: SQLAlchemyRepository, prepared: WriteSession) -> None:
        prepared.results.append({"cancel_public_id": pair.cancel_id, "result": "recorded"})
        raise RuntimeError("secret text")

    monkeypatch.setattr(command_module, "prepare_write", AsyncMock(return_value=session))
    monkeypatch.setattr(command_module, "apply_write", fail_after_one)
    result = _confirmed(pair, tmp_path)
    assert result.exit_code == 1, result.output
    assert "failed: RuntimeError" in result.output and "secret text" not in result.output
    assert result.output.index(f"write session: {_STUB_SESSION_ID}") < result.output.index(
        "failed: RuntimeError"
    )
    assert f"the rows carrying session {_STUB_SESSION_ID} are the authority" in result.output
    receipt = json.loads((tmp_path / "out" / "receipt.json").read_text())
    assert receipt == {
        "digest": "d" * 64,
        "session_id": _STUB_SESSION_ID,
        "write_time": _STUB_WRITE_TIME.isoformat(),
        "results": [{"cancel_public_id": pair.cancel_id, "result": "recorded"}],
    }
    assert (tmp_path / "out" / "preview.json").exists()


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


def test_the_receipt_is_written_before_the_plan_files(
    database: tuple[str, SeededPair], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plan file that cannot be written does not cost the receipt.

    Given: A write during which another process puts preview.json into the output directory,
    When: The run ends and its files are written,
    Then: It exits 1 naming the file error, and receipt.json is there with the applied line.
    """
    _, pair = database
    session = _stub_session()
    out = tmp_path / "out"

    async def collide(repository: SQLAlchemyRepository, prepared: WriteSession) -> None:
        prepared.results.append({"cancel_public_id": pair.cancel_id, "result": "recorded"})
        (out / "preview.json").write_text("[]")

    monkeypatch.setattr(command_module, "prepare_write", AsyncMock(return_value=session))
    monkeypatch.setattr(command_module, "apply_write", collide)
    result = _confirmed(pair, tmp_path)
    assert result.exit_code == 1, result.output
    assert "failed: FileExistsError" in result.output
    receipt = json.loads((out / "receipt.json").read_text())
    assert receipt["results"] == [{"cancel_public_id": pair.cancel_id, "result": "recorded"}]
