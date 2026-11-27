"""Exercise the guarded Kraken Spot importer against isolated SQLite."""

import asyncio
import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import pytest
from sqlalchemy import select

from scripts import import_kraken_spot_trades as importer
from snapper.config.settings import get_bootstrap_settings
from snapper.core.json_types import JsonObject
from snapper.data.models import Trade
from snapper.data.models import TradeIntegrityWorkItem
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeUpsertRow

_BTC = "00000000-0000-4000-8000-000000000001"
_ETH = "00000000-0000-4000-8000-000000000002"
_START = "2026-01-01T00:00:00Z"
_END = "2026-01-01T00:01:00Z"
_BTC_EXECUTED_AT = datetime.fromisoformat("2026-01-01T00:00:00.123Z")
_ETH_EXECUTED_AT = datetime.fromisoformat("2026-01-01T00:00:01.123Z")
_EVIDENCE_HEADER = (
    "exchange,symbol,instrument_public_id,exchange_trade_id,executed_at_utc,"
    "exchange_unix_seconds,price,size,exchange_side,exchange_order_type,exchange_misc\n"
)
_BTC_EVIDENCE = (
    f"kraken,BTC-USD,{_BTC},999001,2026-01-01T00:00:00.123456Z,"
    "1767225600.1234561,12345.10000,0.01000000,b,l,\n"
)
_ETH_EVIDENCE = (
    f"kraken,ETH-USD,{_ETH},999001,2026-01-01T00:00:01.123456Z,"
    "1767225601.1234561,2345.10000,0.02000000,s,m,\n"
)
_STORED_BTC = TradeUpsertRow(
    public_id="00000000-0000-4000-8000-000000000003",
    instrument_public_id=_BTC,
    timestamp=datetime.fromisoformat(_START),
    executed_at=_BTC_EXECUTED_AT,
    price=12345.1,
    size=0.01,
    side="buy",
    trade_id="999001",
    session_id="00000000-0000-4000-8000-000000000004",
    sequence_id=20,
)


@pytest.fixture
def database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Use the repository's isolated SQLite template with a fresh file per test.

    Yields:
        The isolated repository URL also visible to the operator command.
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'import.db'}"

    async def create_database() -> None:
        """Initialize and release the repository used to prepare the fixture."""
        repository = SQLAlchemyRepository(url)
        try:
            await repository.create_all()
        finally:
            await repository.engine.dispose()

    asyncio.run(create_database())
    with monkeypatch.context() as patch:
        patch.setenv("DB_URL", url)
        get_bootstrap_settings.cache_clear()
        try:
            yield url
        finally:
            get_bootstrap_settings.cache_clear()


def _args(tmp_path: Path, evidence: str = _BTC_EVIDENCE + _ETH_EVIDENCE) -> list[str]:
    """Write evidence bytes and build the corresponding command arguments."""
    data = (_EVIDENCE_HEADER + evidence).encode()
    (tmp_path / "evidence.csv").write_bytes(data)
    return [
        "--input-root",
        str(tmp_path),
        "--evidence",
        "evidence.csv",
        "--evidence-sha256",
        hashlib.sha256(data).hexdigest().upper(),
        "--start",
        _START,
        "--end",
        _END,
    ]


def _dry_run(argv: list[str], capsys: pytest.CaptureFixture[str]) -> JsonObject:
    """Read the successful dry run's JSON report."""
    assert importer.main(argv) == 0
    return cast(JsonObject, json.loads(capsys.readouterr().out))


def _apply_args(argv: list[str], digest: str, receipt: Path) -> list[str]:
    """Add approval and receipt options to the preview's arguments."""
    return [*argv, "--apply", "--expect-digest", digest, "--receipt", str(receipt)]


async def _insert(url: str, row: TradeUpsertRow) -> None:
    """Seed an existing trade through the shared repository write path."""
    repository = SQLAlchemyRepository(url)
    try:
        assert await repository.upsert_trades([row]) == 1
    finally:
        await repository.engine.dispose()


async def _stored(url: str) -> tuple[list[Trade], list[TradeIntegrityWorkItem]]:
    """Read the stored trades and historical monitor obligations."""
    repository = SQLAlchemyRepository(url)
    try:
        async with repository.session() as session:
            trades = await session.scalars(select(Trade).order_by(Trade.id))
            work = await session.scalars(
                select(TradeIntegrityWorkItem).order_by(TradeIntegrityWorkItem.id)
            )
            return list(trades), list(work)
    finally:
        await repository.engine.dispose()


def test_dry_run_is_read_only_and_deterministic(
    tmp_path: Path, database: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Use the current database to plan without leaving writes or a receipt.

    Given: Two candidate instruments sharing a trade ID and one existing row.
    When: The same evidence is previewed twice.
    Then: The report is stable, the other instrument is missing, and nothing is written.
    """
    asyncio.run(_insert(database, _STORED_BTC))
    argv = _args(tmp_path)
    original_evidence = (tmp_path / "evidence.csv").read_bytes()

    report = _dry_run(argv, capsys)
    assert report == _dry_run(argv, capsys)
    assert report["total_missing"] == 1
    assert report["missing_by_symbol"] == {"BTC-USD": 0, "ETH-USD": 1}
    assert len(bytes.fromhex(cast(str, report["plan_digest"]))) == 32
    extended = [*argv[:-1], "2026-01-01T00:02:00Z"]
    assert _dry_run(extended, capsys)["plan_digest"] != report["plan_digest"]

    trades, obligations = asyncio.run(_stored(database))
    assert [trade.public_id for trade in trades] == [_STORED_BTC["public_id"]]
    assert obligations == []
    assert (tmp_path / "evidence.csv").read_bytes() == original_evidence


def test_dry_run_preserves_sqlite_journal_mode(
    tmp_path: Path,
    database: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Preserve an existing SQLite database's journal mode.

    Given: The configured database uses a SQLite URI and delete journal mode.
    When: The operator previews the import.
    Then: The preview succeeds and the journal mode stays unchanged.
    """
    path = tmp_path / "import.db"
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA journal_mode=DELETE").fetchone() == ("delete",)
    url = f"sqlite+aiosqlite:///{path.as_uri()}?uri=true"
    monkeypatch.setattr(importer, "get_bootstrap_settings", lambda: SimpleNamespace(db_url=url))

    assert _dry_run(_args(tmp_path), capsys)["total_missing"] == 2

    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)


def test_dry_run_does_not_create_a_missing_sqlite_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Refuse a missing SQLite database without creating a file.

    Given: The database URL points to an absent SQLite file.
    When: The operator previews the import.
    Then: The command reports the connection failure and the file stays absent.
    """
    path = tmp_path / "missing.db"
    monkeypatch.setattr(
        importer,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(db_url=f"sqlite+aiosqlite:///{path}"),
    )

    assert importer.main(_args(tmp_path)) == 1

    assert "OperationalError" in capsys.readouterr().err
    assert not path.exists()


def test_apply_refuses_a_stale_digest(
    tmp_path: Path, database: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Require approval of the plan as recomputed against the database.

    Given: One of the two pending rows arrives after the operator takes a preview.
    When: The operator applies the previous digest.
    Then: It exits with status two and the still-missing trade and receipt stay absent.
    """
    argv = _args(tmp_path)
    digest = cast(str, _dry_run(argv, capsys)["plan_digest"])
    asyncio.run(_insert(database, _STORED_BTC))
    receipt = tmp_path / "receipt.json"

    assert importer.main(_apply_args(argv, digest, receipt)) == 2
    assert not receipt.exists()
    assert "digest" in capsys.readouterr().err.lower()
    trades, obligations = asyncio.run(_stored(database))
    assert [trade.public_id for trade in trades] == [_STORED_BTC["public_id"]]
    assert obligations == []


def test_apply_writes_only_missing_trades_and_their_obligations(
    tmp_path: Path, database: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Commit a verified receipt and make a fresh no-op plan repeatable.

    Given: Verified evidence with an existing trade and three missing trades on two streams.
    When: The operator applies the approved digest, then previews and applies again.
    Then: Only the first apply writes trades and their exact monitor obligations.
    """
    asyncio.run(_insert(database, _STORED_BTC))
    evidence = _BTC_EVIDENCE + _BTC_EVIDENCE.replace("999001", "999002")
    evidence += _ETH_EVIDENCE + _ETH_EVIDENCE.replace("999001", "999002")
    argv = _args(tmp_path, evidence)
    digest = cast(str, _dry_run(argv, capsys)["plan_digest"])
    receipt = tmp_path / "receipt.json"
    started = datetime.now(UTC)

    assert importer.main(_apply_args(argv, digest, receipt)) == 0
    finished = datetime.now(UTC)
    recorded = cast(JsonObject, json.loads(receipt.read_text(encoding="utf-8")))
    assert json.loads(capsys.readouterr().out) == recorded
    assert recorded["plan_digest"] == digest
    assert recorded["evidence_sha256"] == argv[argv.index("--evidence-sha256") + 1].lower()
    assert recorded["chunks"] == [{"inserted": 3, "already_present": 0}]
    assert recorded["total_inserted"] == 3
    assert recorded["total_already_present"] == 1
    assert recorded["verified_missing"] == 0
    window = cast(dict[str, str], recorded["window"])
    assert datetime.fromisoformat(window["start"]) == datetime.fromisoformat(_START)
    assert datetime.fromisoformat(window["end"]) == datetime.fromisoformat(_END)
    assert started <= datetime.fromisoformat(cast(str, recorded["started_at"])) <= finished
    assert started <= datetime.fromisoformat(cast(str, recorded["finished_at"])) <= finished
    trades, obligations = asyncio.run(_stored(database))
    existing, btc, first_eth, second_eth = trades
    assert existing.public_id == _STORED_BTC["public_id"]
    assert existing.session_id == _STORED_BTC["session_id"]
    assert existing.timestamp == _STORED_BTC["timestamp"]
    assert [(row.instrument_public_id, row.trade_id, row.sequence_id) for row in trades[1:]] == [
        (_BTC, "999002", 1),
        (_ETH, "999001", 1),
        (_ETH, "999002", 2),
    ]
    assert (btc.executed_at, btc.price, btc.size, btc.side) == (
        _BTC_EXECUTED_AT,
        12345.1,
        0.01,
        "buy",
    )
    for row in (first_eth, second_eth):
        assert (row.executed_at, row.price, row.size, row.side) == (
            _ETH_EXECUTED_AT,
            2345.1,
            0.02,
            "sell",
        )
    assert len({row.session_id for row in trades[1:]}) == 1
    assert UUID(btc.session_id).version == 7
    for row, obligation in zip(trades[1:], obligations, strict=True):
        assert UUID(row.public_id).version == 7
        assert started <= row.timestamp <= finished
        assert _identity(obligation) == _identity(row)
        assert obligation.m1_pending and obligation.m2_pending

    report = _dry_run(argv, capsys)
    assert report["total_missing"] == 0
    retry_receipt = tmp_path / "retry.json"
    assert importer.main(_apply_args(argv, cast(str, report["plan_digest"]), retry_receipt)) == 0
    retried = json.loads(retry_receipt.read_text(encoding="utf-8"))
    assert (retried["total_inserted"], retried["total_already_present"], retried["chunks"]) == (
        0,
        4,
        [],
    )
    trades_after_retry, obligations_after_retry = asyncio.run(_stored(database))
    assert [_identity(row) for row in trades_after_retry] == [_identity(row) for row in trades]
    assert [row.id for row in obligations_after_retry] == [row.id for row in obligations]


def _identity(row: Trade | TradeIntegrityWorkItem) -> tuple[str, str, str | None, datetime | None]:
    """Compare the fields each integrity obligation must back-reference exactly."""
    return row.public_id, row.instrument_public_id, row.trade_id, row.executed_at


@pytest.mark.parametrize("symlink", [False, True])
def test_apply_refuses_an_existing_receipt(
    tmp_path: Path, database: str, capsys: pytest.CaptureFixture[str], symlink: bool
) -> None:
    """Keep an existing receipt and the database untouched on refusal.

    Given: An approved plan and a receipt path occupied by a file or dangling symlink.
    When: The operator applies to that path.
    Then: The command refuses and preserves the receipt's original contents.
    """
    argv = _args(tmp_path)
    digest = cast(str, _dry_run(argv, capsys)["plan_digest"])
    receipt = tmp_path / "receipt.json"
    absent = tmp_path / "absent.json"
    if symlink:
        receipt.symlink_to(absent)
    else:
        receipt.write_text("existing receipt", encoding="utf-8")

    assert importer.main(_apply_args(argv, digest, receipt)) == 1
    assert "receipt" in capsys.readouterr().err.lower()
    if symlink:
        assert receipt.readlink() == absent
        assert not absent.exists()
    else:
        assert receipt.read_text(encoding="utf-8") == "existing receipt"
    assert asyncio.run(_stored(database)) == ([], [])


@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        ("--start", "2026-01-01T00:00:00", "millisecond-aligned"),
        ("--end", "2026-01-01T00:00:00.123Z", "outside the requested window"),
        ("--end", _START, "start must precede end"),
        ("--evidence", "../outside.csv", "inside the input root"),
        ("--evidence", "linked.csv", "inside the input root"),
        ("--evidence-sha256", "0" * 64, "Evidence SHA-256"),
    ],
)
def test_refuses_out_of_scope_inputs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], option: str, value: str, message: str
) -> None:
    """Check hash, path confinement and window bounds before planning.

    Given: A held evidence file and an input outside the approved scope.
    When: The operator previews the trade import.
    Then: The command reports the failure without printing a plan.
    """
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    argv = _args(inputs)
    outside = tmp_path / "outside.csv"
    outside.write_bytes((inputs / "evidence.csv").read_bytes())
    (inputs / "linked.csv").symlink_to(outside)
    argv[argv.index(option) + 1] = value

    assert importer.main(argv) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert message in output.err


def test_refuses_a_live_match_outside_the_window(
    tmp_path: Path, database: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Check matching venue IDs even when the stored execution time disagrees.

    Given: A stored Kraken trade has the candidate identity at another timestamp.
    When: The operator previews the import.
    Then: The command reports the conflict and leaves the stored trade intact.
    """
    stored = TradeUpsertRow(**_STORED_BTC)
    stored["executed_at"] = datetime(2025, 12, 31, tzinfo=UTC)
    asyncio.run(_insert(database, stored))

    assert importer.main(_args(tmp_path)) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "Conflicting stored Kraken Spot trade" in output.err
    trades, obligations = asyncio.run(_stored(database))
    assert [row.executed_at for row in trades] == [stored["executed_at"]]
    assert obligations == []


@pytest.mark.parametrize(("count", "chunk_sizes"), [(500, [500]), (501, [500, 1])])
def test_apply_respects_the_chunk_boundary(
    tmp_path: Path,
    database: str,
    capsys: pytest.CaptureFixture[str],
    count: int,
    chunk_sizes: list[int],
) -> None:
    """Read and write candidate sets at and over the write limit.

    Given: Verified evidence at the write limit or with one more candidate.
    When: The operator approves and applies the preview.
    Then: The receipt accounts for every row in chunks of at most five hundred.
    """
    evidence = "".join(_BTC_EVIDENCE.replace("999001", str(999001 + i)) for i in range(count))
    argv = _args(tmp_path, evidence)
    report = _dry_run(argv, capsys)
    assert report["total_missing"] == count
    receipt = tmp_path / "receipt.json"

    assert importer.main(_apply_args(argv, cast(str, report["plan_digest"]), receipt)) == 0
    recorded = json.loads(receipt.read_text(encoding="utf-8"))
    assert recorded["chunks"] == [{"inserted": size, "already_present": 0} for size in chunk_sizes]
    assert recorded["total_inserted"] == count
    assert recorded["verified_missing"] == 0
    trades, obligations = asyncio.run(_stored(database))
    assert len(trades) == len(obligations) == count
