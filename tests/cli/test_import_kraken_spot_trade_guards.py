"""Exercise importer refusal paths and transaction setup without PostgreSQL."""

import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from typer.testing import CliRunner

from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    KrakenSpotTradeEvidence,
)
from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    KrakenSpotTradeEvidenceWindow,
)
from snapper.application.updaters.historical.kraken_spot_trade_reconciliation import (
    KrakenSpotTradeRecoveryPlan,
)
from snapper.cli import kraken_spot_trade_import as importer
from snapper.cli.app import app
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.infrastructure.publisher import SequenceTracker

_WHEN = datetime(2026, 1, 1, tzinfo=UTC)
_SHA = "a" * 64
_TRADE = KrakenSpotTradeEvidence(
    "BTC-USD",
    "dab3fb07-f663-484e-9890-52dfe7965c3f",
    "1",
    _WHEN,
    Decimal("10"),
    Decimal("2"),
    "buy",
)
_EVIDENCE = KrakenSpotTradeEvidenceWindow((_TRADE,), _WHEN, _WHEN + timedelta(seconds=1), _SHA)
_MISSING = KrakenSpotTradeRecoveryPlan((_TRADE,), 0)
_PRESENT = KrakenSpotTradeRecoveryPlan((), 1)
_INPUT_ARGS = [
    "--evidence",
    "capture.csv",
    "--evidence-sha256",
    _SHA,
    "--start",
    _EVIDENCE.start.isoformat(),
    "--end",
    _EVIDENCE.end.isoformat(),
]


def _repository_mock(dialect: str) -> tuple[MagicMock, AsyncMock]:
    """Return a repository and its context-managed session doubles."""
    repository = MagicMock(spec=SQLAlchemyRepository)
    repository.dialect_name = dialect
    session = AsyncMock(spec=AsyncSession)
    repository.session.return_value.__aenter__.return_value = session
    return repository, session


@pytest.mark.parametrize(
    ("flags", "message"),
    [
        (["--apply"], "requires a SHA-256 --expect-digest"),
        (["--apply", "--expect-digest", "bad"], "requires a SHA-256 --expect-digest"),
        (["--apply", "--expect-digest", _SHA], "requires --receipt"),
        (["--expect-digest", _SHA], "require --apply"),
        (["--receipt", "receipt.json"], "require --apply"),
        (
            ["--", "--apply", "--expect-digest", _SHA, "--receipt", "receipt.json"],
            "unrecognized arguments: -- --apply",
        ),
    ],
)
def test_apply_options_require_both_guards(flags: list[str], message: str) -> None:
    """Require approval and a new receipt path when applying.

    Given: Write options are incomplete, supplied without apply, or after ``--``.
    When: The operator runs the importer.
    Then: The parser refuses with usage status.
    """
    result = CliRunner().invoke(app, ["import-kraken-spot-trades", *_INPUT_ARGS, *flags])

    assert result.exit_code == 2
    assert message in result.stderr


@pytest.mark.asyncio
async def test_postgresql_read_plan_uses_read_only_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read one read-only PostgreSQL snapshot.

    Given: A PostgreSQL repository and session double.
    When: The importer compares evidence with stored rows.
    Then: The first statement establishes read-only repeatable-read isolation.
    """
    repository, session = _repository_mock("postgresql")
    catalog = repository.get_instrument_public_ids_by_symbols
    catalog.return_value = {_TRADE.symbol: _TRADE.instrument_public_id}
    compare = AsyncMock(return_value=_MISSING)
    monkeypatch.setattr(importer, "_compare", compare)

    assert await importer._read_plan(repository, _EVIDENCE) == _MISSING

    assert [str(call.args[0]) for call in session.execute.await_args_list] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    ]
    catalog.assert_awaited_once()
    assert catalog.await_args.args[:2] == ({_TRADE.symbol}, "kraken")
    assert catalog.await_args.kwargs == {"session": session}
    compare.assert_awaited_once_with(session, _EVIDENCE.rows)
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_read_plan_refuses_unsupported_dialect() -> None:
    """Refuse unsupported database dialects.

    Given: A repository for an unsupported dialect.
    When: The importer plans the live comparison.
    Then: It refuses before issuing SQL.
    """
    repository, session = _repository_mock("other")

    with pytest.raises(ValueError, match="supports PostgreSQL and SQLite"):
        await importer._read_plan(repository, _EVIDENCE)

    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_postgresql_chunk_skips_a_late_match_without_a_table_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recheck candidates on PostgreSQL with bounded timeouts and no table lock.

    Given: A matching trade appeared after the original plan.
    When: The importer rechecks the candidate inside the chunk transaction.
    Then: It counts the match as already present, skips the upsert, and never
        locks the trades table against live writers.
    """
    repository, session = _repository_mock("postgresql")
    statements: list[str] = []
    session.execute.side_effect = lambda statement: statements.append(str(statement))

    async def compare(
        active_session: AsyncSession, candidates: tuple[KrakenSpotTradeEvidence, ...]
    ) -> KrakenSpotTradeRecoveryPlan:
        """Check the transaction settings and return the exact stored match."""
        assert active_session is session
        assert candidates == _EVIDENCE.rows
        assert statements == [
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            "SET LOCAL lock_timeout = '2s'",
            "SET LOCAL statement_timeout = '15s'",
        ]
        return _PRESENT

    monkeypatch.setattr(importer, "_compare", compare)

    counts = await importer._write_chunk(repository, _EVIDENCE.rows, SequenceTracker())

    assert counts == {"inserted": 0, "already_present": 1}
    repository.upsert_trades.assert_not_awaited()
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(("after", "inserted"), [(_MISSING, 1), (_MISSING, 0), (_PRESENT, 2)])
async def test_chunk_refuses_to_commit_on_inconsistent_readback(
    after: KrakenSpotTradeRecoveryPlan, inserted: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse to commit a chunk whose insert and readback disagree.

    Given: The readback still misses a trade, or more rows were written than were missing.
    When: The importer attempts the chunk.
    Then: It refuses before committing.
    """
    repository, session = _repository_mock("sqlite")
    repository.upsert_trades.return_value = inserted
    monkeypatch.setattr(importer, "_compare", AsyncMock(side_effect=[_MISSING, after]))

    with pytest.raises(ValueError, match="did not match its live readback"):
        await importer._write_chunk(repository, _EVIDENCE.rows, SequenceTracker())

    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_chunk_counts_a_concurrently_stored_trade_as_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accept a trade that the live feed stored during the chunk.

    Given: A trade was missing at the recheck but the insert skipped it as a duplicate.
    When: The readback finds the trade present.
    Then: The chunk commits and counts the trade as already present.
    """
    repository, session = _repository_mock("sqlite")
    repository.upsert_trades.return_value = 0
    monkeypatch.setattr(importer, "_compare", AsyncMock(side_effect=[_MISSING, _PRESENT]))

    counts = await importer._write_chunk(repository, _EVIDENCE.rows, SequenceTracker())

    assert counts == {"inserted": 0, "already_present": 1}
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_apply_receipts_a_failed_final_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Record a failed final verification.

    Given: The final database snapshot still has a missing trade.
    When: The importer completes its verification.
    Then: It fails the apply and records the remaining count in the receipt.
    """
    repository, _ = _repository_mock("sqlite")
    receipt_path = tmp_path / "receipt.json"
    monkeypatch.setattr(importer, "_read_plan", AsyncMock(return_value=_MISSING))

    with pytest.raises(ValueError, match="verification found 1 missing trades"):
        await importer._apply(repository, _EVIDENCE, _PRESENT, _SHA, receipt_path)

    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["verified_missing"] == 1
    assert receipt["chunks"] == []


@pytest.mark.asyncio
async def test_apply_receipts_committed_chunks_before_a_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retain previously committed counts in a partial receipt.

    Given: One chunk succeeds and a later chunk fails.
    When: The importer handles the failure.
    Then: Its receipt includes the committed chunk and an unfinished verification.
    """
    repository, _ = _repository_mock("sqlite")
    receipt_path = tmp_path / "receipt.json"
    first = {"inserted": 1, "already_present": 0}
    read_plan = AsyncMock()
    monkeypatch.setattr(importer, "_CHUNK_SIZE", 1)
    monkeypatch.setattr(
        importer,
        "_write_chunk",
        AsyncMock(side_effect=[first, SQLAlchemyError("lock unavailable")]),
    )
    monkeypatch.setattr(importer, "_read_plan", read_plan)
    plan = KrakenSpotTradeRecoveryPlan((_TRADE, _TRADE), 0)

    with pytest.raises(SQLAlchemyError, match="lock unavailable"):
        await importer._apply(repository, _EVIDENCE, plan, _SHA, receipt_path)

    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["chunks"] == [first]
    assert receipt["total_inserted"] == 1
    assert receipt["verified_missing"] is None
    read_plan.assert_not_awaited()


def test_main_redacts_database_failures(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Keep database exception details out of the operator output.

    Given: A database exception with sensitive text.
    When: The command reports the failure.
    Then: Standard error includes the error class and omits the sensitive text.
    """
    monkeypatch.setattr(importer, "confined_input", MagicMock(return_value=Path("capture.csv")))
    monkeypatch.setattr(
        importer, "load_kraken_spot_trade_evidence", MagicMock(return_value=_EVIDENCE)
    )
    monkeypatch.setattr(
        importer, "_run", AsyncMock(side_effect=SQLAlchemyError("sensitive statement"))
    )

    assert importer.main(_INPUT_ARGS) == 1

    assert capsys.readouterr().err == "Trade import failed: SQLAlchemyError\n"


def test_import_command_exposes_argparse_help() -> None:
    """Expose the importer's argparse help through the Snapper CLI.

    Given: The operator invokes the import command with --help.
    When: Typer forwards the request to argparse.
    Then: The command explains where the receipt belongs and exits successfully.
    """
    result = CliRunner().invoke(app, ["import-kraken-spot-trades", "--help"])

    assert result.exit_code == 0
    assert result.stdout.startswith("usage: snapper import-kraken-spot-trades ")
    assert "--expect-digest" in result.stdout
    assert "New apply receipt file inside --input-root" in " ".join(result.stdout.split())
