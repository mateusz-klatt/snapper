"""Test bounded native archive and Futures repairs without external services."""

import argparse
import sys
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from io import BytesIO
from io import TextIOWrapper
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from zipfile import ZipFile

import pytest
from sqlalchemy.exc import OperationalError

from scripts import repair_kraken_candles as repair
from snapper.application.updaters.historical.repair_window import Window
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot

_START = datetime(2026, 9, 14, 13, 57, tzinfo=UTC)
_END = _START + timedelta(minutes=3)


def _args(**overrides: object) -> argparse.Namespace:
    """Build one bounded command namespace."""
    return argparse.Namespace(
        **(
            {
                "start": _START,
                "end": _END,
                "source": "archive",
                "archive": Path("unused.zip"),
                "symbol": None,
                "batch_size": 2,
                "dry_run": False,
                "allow_unmapped": False,
                "allow_invalid": False,
            }
            | overrides
        )
    )


def _repo() -> MagicMock:
    """Return a repository mock that echoes absent candidates."""
    repo = MagicMock(spec=Repository)
    repo.engine = MagicMock(dispose=AsyncMock())
    repo.insert_missing_native_candles = AsyncMock(side_effect=_echo)
    return repo


async def _echo(rows: list[CandleUpsertRow], *, dry_run: bool = False) -> list[CandleUpsertRow]:
    """Represent absent keys without a database write."""
    return rows


def _values(offset: int = 0) -> list[str]:
    """Return one official seven-column record."""
    return [str(int(_START.timestamp()) + offset * 60), "10", "12", "9", "11", "3", "7"]


def _snapshot(offset: int = 0) -> OhlcvSnapshot:
    """Return one public charts candle with no invented statistics."""
    return OhlcvSnapshot((_START + timedelta(minutes=offset)).timestamp(), 10, 12, 9, 11, 3)


@pytest.mark.parametrize(
    "bounds",
    [
        (_START.replace(tzinfo=None), _END),
        (_START + timedelta(seconds=1), _END),
        (_END, _START),
        (_START, datetime.now(UTC).replace(second=0, microsecond=0) + timedelta(days=1)),
        (_START.astimezone(timezone(timedelta(hours=1))), _END),
    ],
)
def test_window_rejects_unsafe_bounds(bounds: tuple[datetime, datetime]) -> None:
    """Validate ranges before any external access.

    Given naive, unaligned, reversed, future or non-UTC bounds,
    When a repair window is constructed,
    Then it rejects the range.
    """
    with pytest.raises(ValueError):
        Window(*bounds)


def test_window_is_half_open() -> None:
    """Preserve strict operator bounds.

    Given a closed historical range,
    When boundaries are tested,
    Then the start belongs and the end does not.
    """
    window = Window(_START, _END)
    assert window.contains(_START)
    assert not window.contains(_END)
    assert not window.contains(_START - timedelta(minutes=1))


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["--start", _START.isoformat(), "--end", _END.isoformat()],
        [
            "--source",
            "futures",
            "--archive",
            "x.zip",
            "--start",
            _START.isoformat(),
            "--end",
            _END.isoformat(),
        ],
        [
            "--source",
            "futures",
            "--start",
            _START.isoformat(),
            "--end",
            _END.isoformat(),
            "--resume",
        ],
        [
            "--source",
            "futures",
            "--start",
            _START.isoformat(),
            "--end",
            _END.isoformat(),
            "--batch-size",
            "101",
        ],
    ],
)
def test_parser_rejects_missing_or_inapplicable_options(argv: list[str]) -> None:
    """Require explicit bounded repair inputs.

    Given missing bounds, absent archive or inapplicable options,
    When the command is parsed,
    Then argparse rejects it before repository construction.
    """
    with pytest.raises(SystemExit):
        repair.parse_args(argv)


def test_parser_accepts_both_sources() -> None:
    """Accept the documented standalone invocations.

    Given explicit archive and Futures command arguments,
    When they are parsed,
    Then bounds and dry-run flags survive unchanged.
    """
    bounds = ["--start", _START.isoformat(), "--end", _END.isoformat()]
    assert repair.parse_args(["--archive", "q.zip", *bounds]).archive == Path("q.zip")
    assert repair.parse_args(["--source", "futures", "--dry-run", *bounds]).dry_run


@pytest.mark.parametrize("size", [0, 101])
def test_writer_rejects_unsafe_sizes(size: int) -> None:
    """Bound every persistence transaction.

    Given an empty or oversized batch limit,
    When the writer is constructed,
    Then it refuses the limit.
    """
    with pytest.raises(ValueError, match="batch-size"):
        repair.Writer(cast(Repository, _repo()), size, False)


def test_records_bound_untrusted_lines_and_reject_bad_csv() -> None:
    """Stream hostile CSV without extraction or unlimited record allocations.

    Given oversized lines, invalid quoted fields and a valid final record,
    When archive records are streamed,
    Then invalid records are marked and parsing resumes at the next line.
    """
    data = ("x" * 10000 + '\n"unterminated\n1,2,3\n' + "z" * 4097).encode()
    with TextIOWrapper(BytesIO(data)) as handle:
        assert list(repair.records(handle)) == [[], [], ["1", "2", "3"], []]


async def test_writer_counts_skips_and_dry_run_without_fabrication() -> None:
    """Preserve absent and present counts for bounded candidates.

    Given an absent record, a duplicate and an existing record,
    When the dry-run writer flushes,
    Then it records counts, writes nothing and preserves unknown VWAP.
    """
    repo = _repo()
    repo.insert_missing_native_candles.side_effect = [[{}], []]
    writer = repair.Writer(cast(Repository, repo), 2, True)
    row = writer.build(_values(), "instrument")
    assert row["vwap"] is None and row["trades"] == 7
    await writer.append(row)
    await writer.append(row)
    await writer.flush()
    await writer.flush()
    assert writer.totals.absent == writer.totals.already_present == 1
    assert writer.totals.written == 0
    assert all(call.kwargs["dry_run"] for call in repo.insert_missing_native_candles.call_args_list)
    assert writer.build(_values()[:6], "instrument")["trades"] is None
    with pytest.raises(ValueError, match="aligned"):
        writer.build([str(int(_START.timestamp()) + 1), *_values()[1:]], "instrument")


@pytest.mark.parametrize(
    "values",
    [
        [],
        ["bad", *_values()[1:]],
        [str(10**30), *_values()[1:]],
        [*_values()[:2], "8", *_values()[3:]],
        [*_values()[:5], "nan", "7"],
        [*_values()[:6], "1.5"],
    ],
)
async def test_invalid_records_are_reported_and_skipped(values: list[str]) -> None:
    """Do not let invalid native data reach persistence.

    Given malformed timestamps, counts, geometry or nonfinite values,
    When an archive record is consumed,
    Then the invalid counter increases and no row is queued.
    """
    repo = _repo()
    writer = repair.Writer(cast(Repository, repo), 2, False)
    await repair.consume_record(values, ("BTC-USD", "iid"), writer, Window(_START, _END))
    assert writer.totals.invalid == 1
    assert writer.pending == []
    repo.insert_missing_native_candles.assert_not_called()


async def test_archive_stream_reports_unmapped_and_enforces_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repair only exact mapped native minutes in the half-open window.

    Given a ZIP containing multiple intervals, unknown pairs and boundary rows,
    When a scoped archive repair runs,
    Then it reports unknown pairs and queues only valid selected in-window minutes.
    """
    archive = tmp_path / "q.zip"
    with ZipFile(archive, "w") as zipped:
        zipped.writestr(
            "XBTUSD_1.csv", "\n".join(",".join(_values(offset)) for offset in (-1, 0, 1, 3))
        )
        zipped.writestr("UNKNOWN_1.csv", ",".join(_values()))
        zipped.writestr("ETHEUR_1.csv", ",".join(_values()))
        zipped.writestr("XBTUSD_5.csv", ",".join(_values()))
        zipped.writestr("../../XBTUSD_1.csv", ",".join(_values()))
    mapping = {"XBTUSD": ("BTC-USD", "iid"), "ETHEUR": ("ETH-EUR", "eth")}
    monkeypatch.setattr(repair, "archive_instruments", AsyncMock(return_value=mapping))
    repo = _repo()
    writer = repair.Writer(cast(Repository, repo), 2, False)
    await repair.load_archive(
        _args(archive=archive, symbol="BTC-USD"), writer, Window(_START, _END)
    )
    assert writer.totals.in_window == 3
    assert writer.totals.unmapped == {"UNKNOWN"}
    assert writer.totals.written == 2
    inserted = repo.insert_missing_native_candles.call_args.args[0]
    assert [row["open_at"] for row in inserted] == [_START, _START + timedelta(minutes=1)]
    assert all(row["vwap"] is None and row["trades"] == 7 for row in inserted)


async def test_archive_mapping_is_exact_and_refuses_ambiguity() -> None:
    """Resolve direct REST names and normalize only legacy archive names.

    Given direct, legacy and colliding Kraken REST aliases,
    When archive identities are resolved,
    Then exact names win and conflicting normalized identities raise.
    """
    repo = _repo()
    session = repo.session.return_value.__aenter__.return_value
    result = MagicMock()
    session.execute = AsyncMock(return_value=result)
    result.all.return_value = [
        ("DOGE-USD", "XDGUSD", "doge"),
        ("BTC-USD", "XXBTZUSD", "btc"),
        ("ETH-BTC", "XETHXXBT", "eth"),
        ("OTHER", "UNUSED", "other"),
    ]
    pairs = {"XDGUSD", "XBTUSD", "ETHXBT"}
    assert await repair.archive_instruments(cast(Repository, repo), pairs) == {
        "XDGUSD": ("DOGE-USD", "doge"),
        "XBTUSD": ("BTC-USD", "btc"),
        "ETHXBT": ("ETH-BTC", "eth"),
    }
    result.all.return_value = [
        ("BTC-USD", "XBTUSD", "iid"),
        ("OTHER", "XXBTZUSD", "other"),
    ]
    with pytest.raises(ValueError, match="ambiguous"):
        await repair.archive_instruments(cast(Repository, repo), pairs)
    assert repair.archive_pair("XXBTZUSD", {"XXBTZUSD", "XBTUSD"}) == "XXBTZUSD"


async def test_futures_pages_across_empty_slices_and_filters_both_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Scan sparse charts windows independently of newest stored candles.

    Given an empty 500-minute slice followed by valid and out-of-range data,
    When Futures repair pages the explicit range,
    Then it reaches later slices and stores only valid in-page native minutes.
    """
    repo = _repo()
    writer = repair.Writer(cast(Repository, repo), 2, False)
    client = MagicMock(
        get_ohlcv=AsyncMock(
            side_effect=[
                [],
                [
                    _snapshot(-1),
                    _snapshot(500),
                    _snapshot(501),
                    _snapshot(502),
                    OhlcvSnapshot(_snapshot(501).timestamp + 0.5, 10, 12, 9, 11, 3),
                ],
            ]
        )
    )
    monkeypatch.setattr(repair, "native_to_ccxt", lambda symbol: "BTC/USD:USD")
    monkeypatch.setattr(repair.asyncio, "sleep", AsyncMock())
    window = Window(_START, _START + timedelta(minutes=502))
    await repair.futures_symbol(client, ("BTC-USD-PERP", "iid"), writer, window)
    assert writer.totals.written == 2
    assert writer.totals.invalid == 1
    assert [call.kwargs["limit"] for call in client.get_ohlcv.call_args_list] == [500, 2]
    assert client.get_ohlcv.call_args.kwargs["since"] == int(_snapshot(500).timestamp * 1000)


@pytest.mark.parametrize("symbol", [None, "MISSING"])
async def test_futures_resolves_active_instruments_and_disconnects(
    symbol: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Avoid catalog writes while resolving Futures repair targets.

    Given mapped and unmapped active symbol selections,
    When the bounded Futures loader runs,
    Then it repairs only mapped instruments and always closes its client.
    """
    repo = _repo()
    repo.get_exchange_instruments = AsyncMock(return_value=["BTC-USD-PERP", "MISSING"])
    repo.get_instrument_public_ids_by_symbols = AsyncMock(return_value={"BTC-USD-PERP": "iid"})
    client = MagicMock(connect=AsyncMock(), disconnect=AsyncMock())
    monkeypatch.setattr(repair, "KrakenFuturesExchangeClient", lambda: client)
    process = AsyncMock()
    monkeypatch.setattr(repair, "futures_symbol", process)
    writer = repair.Writer(cast(Repository, repo), 2, False)
    await repair.load_futures(_args(symbol=symbol), writer, Window(_START, _END))
    assert writer.totals.unmapped == {"MISSING"}
    assert process.await_count == (1 if symbol is None else 0)
    client.disconnect.assert_awaited_once()


@pytest.mark.parametrize("source", ["archive", "futures"])
async def test_run_prints_counts_and_disposes_repository(
    source: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Expose per-run counts for both operational modes.

    Given a valid bounded request with no returned data,
    When the command runs,
    Then it prints all required counters and disposes the repository.
    """
    repo = _repo()
    monkeypatch.setattr(repair, "get_repository", lambda url: repo)
    monkeypatch.setattr(repair, "load_archive", AsyncMock())
    monkeypatch.setattr(repair, "load_futures", AsyncMock())
    assert await repair.run(_args(source=source)) == repair.Totals()
    assert "rows_in_window=0 absent=0 already_present=0" in capsys.readouterr().out
    repo.engine.dispose.assert_awaited_once()


def test_main_runs_validated_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the host script entry point runnable.

    Given explicit source and UTC bounds,
    When main runs with an isolated mocked repair,
    Then it returns success after the coroutine completes.
    """
    monkeypatch.setattr(
        sys,
        "argv",
        ["repair", "--source", "futures", "--start", _START.isoformat(), "--end", _END.isoformat()],
    )
    process = AsyncMock(return_value=repair.Totals())
    monkeypatch.setattr(repair, "run", process)
    assert repair.main() == 0
    process.assert_awaited_once()


async def test_futures_short_pages_continue_from_last_returned_minute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not skip candles when charts return less than the requested page size.

    Given short responses followed by an empty tail inside a bounded request,
    When Futures repair advances its cursor,
    Then each short response resumes after its last minute and the empty tail terminates.
    """
    repo = _repo()
    writer = repair.Writer(cast(Repository, repo), 2, False)
    client = MagicMock(get_ohlcv=AsyncMock(side_effect=[[_snapshot(0)], [_snapshot(1)], []]))
    monkeypatch.setattr(repair, "native_to_ccxt", lambda symbol: "BTC/USD:USD")
    monkeypatch.setattr(repair.asyncio, "sleep", AsyncMock())
    await repair.futures_symbol(client, ("BTC-USD-PERP", "iid"), writer, Window(_START, _END))
    assert writer.totals.written == 2
    assert [call.kwargs["since"] for call in client.get_ohlcv.call_args_list] == [
        int(_snapshot(offset).timestamp * 1000) for offset in range(3)
    ]


@pytest.mark.parametrize(
    "case",
    [
        ({"members": 1, "in_window": 1}, {}, 0),
        ({"members": 0, "in_window": 1}, {"allow_invalid": True, "allow_unmapped": True}, 1),
        ({"members": 1, "in_window": 0}, {"allow_invalid": True, "allow_unmapped": True}, 1),
        ({"members": 1, "in_window": 2, "unmapped_rows": 1}, {}, 1),
        ({"members": 1, "in_window": 2, "unmapped_rows": 1}, {"allow_unmapped": True}, 0),
        ({"members": 1, "in_window": 1, "invalid": 1}, {}, 1),
        ({"members": 1, "in_window": 1, "invalid": 1}, {"allow_invalid": True}, 0),
        (
            {"members": 1, "in_window": 1, "errors": 1},
            {"allow_invalid": True, "allow_unmapped": True},
            1,
        ),
    ],
)
def test_archive_exit_status_requires_explicit_acknowledgement(
    case: tuple[dict[str, int], dict[str, bool], int],
) -> None:
    """Never report incomplete archive scans as unconditional success.

    Given empty selections, invalid data, unmapped rows or fatal failures,
    When final status is calculated,
    Then only explicitly acknowledged skipped data can return success.
    """
    counts, flags, expected = case
    assert repair.exit_status(_args(**flags), repair.Totals(**counts)) == expected


@pytest.mark.parametrize("name", ["XBTUSD_1.csv", "quarter/XBTUSD_1.csv"])
async def test_archive_accepts_one_directory_and_counts_unmapped_rows(
    name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Count unmapped in-window rows independently of whether they can be inserted.

    Given a supported archive member with an unmapped pair and bounded records,
    When the archive is scanned,
    Then only in-window records count and the pair name always appears in the summary state.
    """
    archive = tmp_path / "quarter.zip"
    with ZipFile(archive, "w") as zipped:
        zipped.writestr(name, "\n".join(",".join(_values(offset)) for offset in (-1, 0, 1, 3)))
    monkeypatch.setattr(repair, "archive_instruments", AsyncMock(return_value={}))
    writer = repair.Writer(cast(Repository, _repo()), 100, True)
    await repair.load_archive(_args(archive=archive), writer, Window(_START, _END))
    assert writer.totals.members == 1
    assert writer.totals.in_window == writer.totals.unmapped_rows == 2
    assert writer.totals.unmapped == {"XBTUSD"}
    assert repair.exit_status(_args(), writer.totals) == 1
    assert repair.exit_status(_args(allow_unmapped=True), writer.totals) == 0


@pytest.mark.parametrize("records", [None, _values(3)])
async def test_archive_empty_selection_is_failure_even_with_acknowledgements(
    records: list[str] | None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail archives without matching members or requested-window data.

    Given only another timeframe or a one-minute row at the exclusive end,
    When an acknowledged archive scan finishes,
    Then the empty selection still exits nonzero.
    """
    archive = tmp_path / "empty.zip"
    with ZipFile(archive, "w") as zipped:
        zipped.writestr(
            "XBTUSD_5.csv" if records is None else "XBTUSD_1.csv", ",".join(records or _values())
        )
    monkeypatch.setattr(repair, "archive_instruments", AsyncMock(return_value={}))
    writer = repair.Writer(cast(Repository, _repo()), 100, True)
    await repair.load_archive(_args(archive=archive), writer, Window(_START, _END))
    assert repair.exit_status(_args(allow_unmapped=True, allow_invalid=True), writer.totals) == 1


async def test_futures_missing_ccxt_alias_is_named_and_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never guess an alias for dated Futures contracts.

    Given a symbol without an exact CCXT alias,
    When its bounded repair is attempted,
    Then it is named as skipped without requesting any candles or failing the run.
    """
    writer = repair.Writer(cast(Repository, _repo()), 100, False)
    client = MagicMock(get_ohlcv=AsyncMock())
    monkeypatch.setattr(repair, "native_to_ccxt", MagicMock(side_effect=ValueError("no alias")))
    await repair.futures_symbol(client, ("BTC-USD-20261225", "iid"), writer, Window(_START, _END))
    assert writer.totals.skipped_ccxt == {"BTC-USD-20261225"}
    client.get_ohlcv.assert_not_awaited()
    assert repair.exit_status(_args(source="futures"), writer.totals) == 0


@pytest.mark.parametrize("database_failure", [False, True])
async def test_futures_fetch_failures_continue_but_database_failures_abort(
    database_failure: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Distinguish a bad symbol fetch from a failed repair transaction.

    Given a failed symbol followed by a healthy symbol,
    When Futures scanning runs,
    Then fetch failures are counted and scanning continues while database failures abort.
    """
    repo = _repo()
    repo.get_exchange_instruments = AsyncMock(return_value=["BAD", "GOOD"])
    repo.get_instrument_public_ids_by_symbols = AsyncMock(
        return_value={"BAD": "bad", "GOOD": "good"}
    )
    client = MagicMock(connect=AsyncMock(), disconnect=AsyncMock())
    monkeypatch.setattr(repair, "KrakenFuturesExchangeClient", lambda: client)
    failure = (
        OperationalError("write", None, Exception("locked"))
        if database_failure
        else RuntimeError("fetch failed")
    )
    process = AsyncMock(side_effect=[failure, None])
    monkeypatch.setattr(repair, "futures_symbol", process)
    writer = repair.Writer(cast(Repository, repo), 100, False)
    if database_failure:
        with pytest.raises(OperationalError):
            await repair.load_futures(_args(), writer, Window(_START, _END))
        assert process.await_count == 1
    else:
        await repair.load_futures(_args(), writer, Window(_START, _END))
        assert writer.totals.failed_symbols == {"BAD"}
        assert writer.totals.errors == 1 and process.await_count == 2
        assert repair.exit_status(_args(source="futures"), writer.totals) == 1
    client.disconnect.assert_awaited_once()


async def test_aborted_repair_reports_committed_progress_after_bounded_retries(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Retain acknowledged counts when a later database batch cannot finish.

    Given one committed batch followed by persistent lock timeouts,
    When the native run exhausts the retry budget,
    Then the summary reports the first commit and a nonzero final status.
    """
    repo = _repo()
    failure = OperationalError("write", None, Exception("locked"))
    repo.insert_missing_native_candles.side_effect = [[{}], failure, failure, failure, failure]
    monkeypatch.setattr(repair, "get_repository", lambda url: repo)
    monkeypatch.setattr(repair.asyncio, "sleep", AsyncMock())

    async def load(args: argparse.Namespace, writer: repair.Writer, window: Window) -> None:
        """Commit a first candidate before a second candidate fails."""
        writer.totals.members = 1
        writer.totals.in_window = 2
        await writer.append(writer.build(_values(0), "iid"))
        await writer.append(writer.build(_values(1), "iid"))

    monkeypatch.setattr(repair, "load_archive", load)
    totals = await repair.run(_args(batch_size=1))
    assert totals.written == totals.absent == 1
    assert totals.errors == 1
    assert repo.insert_missing_native_candles.await_count == 5
    output = capsys.readouterr().out
    assert "written=1" in output and "errors=1" in output
    assert repair.exit_status(_args(), totals) == 1
    repo.engine.dispose.assert_awaited_once()


async def test_unmapped_invalid_records_require_both_acknowledgements() -> None:
    """Validate all input payloads even when their identities are unavailable.

    Given an unmapped in-window record with invalid OHLC geometry,
    When the record is scanned,
    Then both counters increase and acknowledging only the missing mapping still fails.
    """
    writer = repair.Writer(cast(Repository, _repo()), 100, True)
    writer.totals.members = 1
    values = _values()
    values[2] = "8"
    await repair.consume_record(values, None, writer, Window(_START, _END))
    assert writer.totals.unmapped_rows == writer.totals.invalid == 1
    assert repair.exit_status(_args(allow_unmapped=True), writer.totals) == 1
    assert repair.exit_status(_args(allow_unmapped=True, allow_invalid=True), writer.totals) == 0


@pytest.mark.parametrize(
    "names",
    [
        ("XXBTZUSD", "XBTUSD"),
        ("XETHXXBT", "ETHXBT"),
        ("XXDGXXBT", "XDGXBT"),
        ("XXBTEUROP", "XBTEUROP"),
        ("XXBTUSDQ", "XBTUSDQ"),
        ("USDTZUSD", "USDTUSD"),
    ],
)
def test_legacy_rest_prefix_normalization_is_explicit(names: tuple[str, str]) -> None:
    """Normalize only Kraken's prefixed legacy asset-code form.

    Given a legacy REST name and its exact archive counterpart,
    When the direct REST name is absent from the archive,
    Then only the legacy X/Z asset prefixes are removed.
    """
    alias, pair = names
    assert repair.archive_pair(alias, {pair}) == pair


def test_legacy_rest_form_without_archive_pair_stays_unmapped() -> None:
    """Never turn a legacy-shaped alias into a pair the archive does not hold.

    Given a legacy-shaped REST name whose normalized form is not in the archive,
    When the pair is resolved,
    Then the alias is returned unchanged so the caller reports it as unmapped.
    """
    assert repair.archive_pair("XXBTZUSD", {"ETHUSD"}) == "XXBTZUSD"
