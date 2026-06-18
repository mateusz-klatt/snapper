"""Tests for the synthesized higher-timeframe candle backfill script."""

import argparse
from datetime import UTC
from datetime import datetime
from itertools import count
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from sqlalchemy.exc import IntegrityError

import scripts.backfill_synth_candles as backfill
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow


def _row(
    open_at: datetime,
    *,
    open_: float = 100.0,
    high: float = 100.0,
    low: float = 100.0,
    close: float = 100.0,
    volume: float = 1.0,
    vwap: float | None = 100.0,
    trades: int = 1,
) -> CandleRow:
    """Build a 1m CandleRow for tests."""
    return {
        "open_at": open_at,
        "timeframe": "1m",
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "vwap": vwap,
        "trades": trades,
        "source": "native",
        "complete": True,
        "public_id": "p",
        "timestamp": open_at,
        "session_id": "s",
        "sequence_id": 1,
    }


def _at(minute: int) -> datetime:
    """Return a fixed past UTC timestamp at the given minute offset."""
    return datetime(2026, 6, 1, 0, minute, 0, tzinfo=UTC)


def _urow(open_at: datetime | None = None) -> CandleUpsertRow:
    """Build a minimal upsert row; an open_at lets the fake mark it contended."""
    row: CandleUpsertRow = {"timeframe": "5m"}
    if open_at is not None:
        row["open_at"] = open_at
    return row


class _FakeRepo:
    """Minimal in-memory Repository stand-in for backfill tests."""

    def __init__(
        self,
        *,
        instruments: dict[str, list[str]] | None = None,
        ipids: dict[str, dict[str, str]] | None = None,
        candles: dict[str, list[CandleRow]] | None = None,
        has_engine: bool = True,
        raise_for: tuple[str, ...] = (),
        upsert_fail_times: int = 0,
        conflict_open_ats: set[datetime] | None = None,
    ) -> None:
        """Store the canned instrument/candle fixtures the backfill will read."""
        self._instruments = instruments or {}
        self._ipids = ipids or {}
        self._candles = candles or {}
        self._raise_for = set(raise_for)
        self._upsert_fail_times = upsert_fail_times
        self._conflict_open_ats = conflict_open_ats or set()
        self.upsert_calls = 0
        self.upserts: list[list[CandleUpsertRow]] = []
        if has_engine:
            self.engine = SimpleNamespace(dispose=AsyncMock())

    async def get_exchange_instruments(self, exchange: str, as_of: datetime) -> list[str]:
        """Return the canned native symbols for an exchange."""
        return self._instruments.get(exchange, [])

    async def get_instrument_public_ids_by_symbols(
        self, symbols: set[str], exchange: str, as_of: datetime
    ) -> dict[str, str]:
        """Resolve the subset of symbols that have a canned instrument id."""
        mapping = self._ipids.get(exchange, {})
        return {s: mapping[s] for s in symbols if s in mapping}

    async def get_candles(
        self,
        instrument: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        exchange: str,
        as_of: datetime,
        limit: int | None = None,
        order: str = "asc",
    ) -> list[CandleRow]:
        """Return canned 1m rows for an instrument, or raise if flagged."""
        if instrument in self._raise_for:
            raise RuntimeError("boom")
        return self._candles.get(instrument, [])

    async def upsert_candles(
        self, rows: list[CandleUpsertRow], session: object | None = None
    ) -> int:
        """Record an upsert batch, failing the first calls or any contended row."""
        self.upsert_calls += 1
        if self.upsert_calls <= self._upsert_fail_times:
            raise IntegrityError("INSERT", None, Exception("dup"))
        if any(r.get("open_at") in self._conflict_open_ats for r in rows):
            raise IntegrityError("INSERT", None, Exception("dup"))
        batch = list(rows)
        self.upserts.append(batch)
        return len(batch)


def _args(**overrides: object) -> argparse.Namespace:
    """Build a parsed-args namespace with test-friendly defaults."""
    base: dict[str, object] = {
        "exchanges": "kraken",
        "timeframes": "5m",
        "symbols": None,
        "since_days": 1,
        "settle_minutes": 0,
        "batch_size": 2000,
        "concurrency": 2,
        "limit_instruments": None,
        "dry_run": False,
        "progress_file": "",
        "db_host_rewrite": "",
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_parse_args_defaults() -> None:
    """Verify default arguments are populated.

    Given: An empty argument vector,
    When: _parse_args is called,
    Then: The documented defaults are returned.
    """
    args = backfill._parse_args([])
    assert args.since_days == 90
    assert args.settle_minutes == 120
    assert args.concurrency == 6
    assert args.dry_run is False
    assert args.db_host_rewrite == "172.17.0.1=127.0.0.1"


def test_parse_args_overrides() -> None:
    """Verify flags override the defaults.

    Given: An argument vector with explicit flags,
    When: _parse_args is called,
    Then: The parsed namespace reflects the overrides.
    """
    args = backfill._parse_args(
        ["--exchanges", "kraken", "--timeframes", "15m", "--since-days", "7", "--dry-run"]
    )
    assert args.timeframes == "15m"
    assert args.since_days == 7
    assert args.dry_run is True


def test_resolve_db_url_rewrites_host() -> None:
    """Verify the docker-bridge host is rewritten while the driver is kept.

    Given: A bootstrap DB URL on the docker bridge host,
    When: _resolve_db_url is called with a SRC=DST rewrite,
    Then: Only the host is swapped and +asyncpg is preserved.
    """
    settings = SimpleNamespace(db_url="postgresql+asyncpg://u:p@172.17.0.1:5432/db")
    with patch.object(backfill, "get_settings", return_value=settings):
        out = backfill._resolve_db_url("172.17.0.1=127.0.0.1")
    assert out == "postgresql+asyncpg://u:p@127.0.0.1:5432/db"


def test_resolve_db_url_empty_rewrite_noop() -> None:
    """Verify an empty rewrite leaves the URL untouched.

    Given: A bootstrap DB URL,
    When: _resolve_db_url is called with an empty rewrite,
    Then: The URL is returned verbatim.
    """
    settings = SimpleNamespace(db_url="postgresql+asyncpg://u:p@h:5432/db")
    with patch.object(backfill, "get_settings", return_value=settings):
        assert backfill._resolve_db_url("") == settings.db_url


def test_resolve_db_url_malformed_rewrite_noop() -> None:
    """Verify a rewrite without a delimiter is ignored.

    Given: A rewrite string with no '=' separator,
    When: _resolve_db_url is called,
    Then: The URL is returned unchanged.
    """
    settings = SimpleNamespace(db_url="postgresql+asyncpg://u:p@h:5432/db")
    with patch.object(backfill, "get_settings", return_value=settings):
        assert backfill._resolve_db_url("nodelimiter") == settings.db_url


def test_load_done_missing_file(tmp_path: Path) -> None:
    """Verify a missing progress file yields an empty set.

    Given: A path that does not exist,
    When: _load_done is called,
    Then: An empty set is returned.
    """
    assert backfill._load_done(tmp_path / "absent.jsonl") == set()


def test_load_done_reads_pairs_and_skips_malformed(tmp_path: Path) -> None:
    """Verify well-formed lines are loaded and malformed lines skipped.

    Given: A progress file with a valid line, a tab-less line, and a blank line,
    When: _load_done is called,
    Then: Only the valid (exchange, symbol) pair is returned.
    """
    progress = tmp_path / "p.jsonl"
    progress.write_text("kraken\tBTC-USD\nbogus\n\n", encoding="utf-8")
    assert backfill._load_done(progress) == {("kraken", "BTC-USD")}


def test_roll_up_folds_window_and_excludes_open() -> None:
    """Verify OHLCV folding and that the trailing open window is excluded.

    Given: Four 1m rows spanning a closed 5m window plus a later minute,
    When: _roll_up runs with a far-future settle cutoff,
    Then: One 5m bucket is returned with correct OHLCV and 15m stays empty.
    """
    rows = [
        _row(_at(0), open_=10, high=10, low=5, close=8, volume=1.0, vwap=10.0, trades=2),
        _row(_at(1), open_=8, high=12, low=6, close=9, volume=2.0, vwap=11.0, trades=3),
        _row(_at(2), open_=9, high=11, low=4, close=7, volume=3.0, vwap=12.0, trades=1),
        _row(_at(5), open_=7, high=7, low=7, close=7, volume=1.0, vwap=7.0, trades=1),
    ]
    closed = backfill._roll_up(rows, {"5m": 300, "15m": 900}, close_before_ts=4_000_000_000)
    assert len(closed["5m"]) == 1
    assert closed["15m"] == []
    begin_ts, bucket = closed["5m"][0]
    assert begin_ts == int(_at(0).timestamp())
    assert bucket.open == 10
    assert bucket.high == 12
    assert bucket.low == 4
    assert bucket.close == 7
    assert bucket.volume == 6.0
    assert bucket.trades == 6


def test_roll_up_vwap_none_uses_close() -> None:
    """Verify a missing 1m vwap folds the close instead.

    Given: A closed-window row whose vwap is None,
    When: _roll_up runs,
    Then: The bucket's vwap_sum uses the close price.
    """
    rows = [
        _row(_at(0), close=50.0, volume=2.0, vwap=None),
        _row(_at(5), close=50.0, volume=1.0, vwap=50.0),
    ]
    closed = backfill._roll_up(rows, {"5m": 300}, close_before_ts=4_000_000_000)
    _begin, bucket = closed["5m"][0]
    assert bucket.vwap_sum == 100.0


def test_roll_up_settle_cutoff_excludes_all() -> None:
    """Verify the settle cutoff suppresses windows that closed too recently.

    Given: Closed-window rows but a settle cutoff before any window end,
    When: _roll_up runs,
    Then: No buckets are returned.
    """
    rows = [_row(_at(0)), _row(_at(5))]
    closed = backfill._roll_up(rows, {"5m": 300}, close_before_ts=0)
    assert closed["5m"] == []


def test_build_rows_handles_zero_and_nonzero_volume() -> None:
    """Verify vwap is computed for traded windows and zeroed for empty ones.

    Given: Two closed buckets, one with volume and one with zero volume,
    When: _build_rows projects them,
    Then: The traded bar carries a weighted vwap and the empty bar carries 0.0.
    """
    traded = backfill._Bucket(open=1, high=2, low=1, close=2, volume=4.0, trades=3, vwap_sum=8.0)
    empty = backfill._Bucket(open=2, high=2, low=2, close=2, volume=0.0, trades=0, vwap_sum=0.0)
    closed = {"5m": [(0, traded)], "15m": [(0, empty)]}
    out = backfill._build_rows(closed, "iid", _at(0), count(1), "sess")
    by_tf = {r["timeframe"]: r for r in out}
    assert by_tf["5m"]["vwap"] == 2.0
    assert by_tf["15m"]["vwap"] == 0.0
    assert by_tf["5m"]["source"] == "synthesized"
    assert by_tf["5m"]["complete"] is True
    assert by_tf["5m"]["instrument_public_id"] == "iid"


async def test_process_instrument_no_rows() -> None:
    """Verify an instrument with no 1m data is a no-op.

    Given: A repo returning no 1m candles,
    When: _process_instrument runs,
    Then: It reports zero 1m, windows, and writes.
    """
    repo = _FakeRepo(candles={})
    result = await backfill._process_instrument(
        repo, "kraken", "X", "iid", {"5m": 300}, _at(0), _at(10), 4_000_000_000, 2000, "sess", False
    )
    assert result == (0, 0, 0)


async def test_process_instrument_all_windows_open() -> None:
    """Verify rows with no closed window write nothing.

    Given: 1m rows but a settle cutoff that leaves every window open,
    When: _process_instrument runs,
    Then: It reports the 1m count but zero windows and writes.
    """
    repo = _FakeRepo(candles={"X": [_row(_at(0)), _row(_at(5))]})
    result = await backfill._process_instrument(
        repo, "kraken", "X", "iid", {"5m": 300}, _at(0), _at(10), 0, 2000, "sess", False
    )
    assert result == (2, 0, 0)


async def test_process_instrument_dry_run_skips_upsert() -> None:
    """Verify dry-run computes windows without writing.

    Given: A closed window and dry_run=True,
    When: _process_instrument runs,
    Then: It reports windows but performs no upsert.
    """
    repo = _FakeRepo(candles={"X": [_row(_at(0)), _row(_at(5))]})
    n_1m, n_windows, n_written = await backfill._process_instrument(
        repo, "kraken", "X", "iid", {"5m": 300}, _at(0), _at(10), 4_000_000_000, 2000, "sess", True
    )
    assert (n_1m, n_windows, n_written) == (2, 1, 0)
    assert repo.upserts == []


async def test_process_instrument_writes_in_batches() -> None:
    """Verify synthesized rows are written in batch-sized chunks.

    Given: Two closed windows and a batch size of one,
    When: _process_instrument runs in write mode,
    Then: It issues one upsert per row and sums the written count.
    """
    rows = [_row(_at(0)), _row(_at(5)), _row(_at(10)), _row(_at(15))]
    repo = _FakeRepo(candles={"X": rows})
    n_1m, n_windows, n_written = await backfill._process_instrument(
        repo, "kraken", "X", "iid", {"5m": 300}, _at(0), _at(20), 4_000_000_000, 1, "sess", False
    )
    assert n_windows == n_written
    assert len(repo.upserts) == n_windows
    assert all(len(batch) == 1 for batch in repo.upserts)


async def test_upsert_with_retry_first_try_succeeds() -> None:
    """Verify a clean batch needs no retry.

    Given: A repo whose upsert succeeds immediately,
    When: _upsert_with_retry runs,
    Then: It returns the written count after a single call.
    """
    repo = _FakeRepo()
    written = await backfill._upsert_with_retry(repo, [_urow()])
    assert written == 1
    assert repo.upsert_calls == 1


async def test_upsert_with_retry_recovers_after_conflict() -> None:
    """Verify a transient unique conflict is retried then succeeds.

    Given: A repo that raises IntegrityError once before succeeding,
    When: _upsert_with_retry runs with the backoff sleep patched out,
    Then: It returns the written count after a second attempt.
    """
    repo = _FakeRepo(upsert_fail_times=1)
    with patch.object(backfill.asyncio, "sleep", new=AsyncMock()) as sleep:
        written = await backfill._upsert_with_retry(repo, [_urow(), _urow()])
    assert written == 2
    assert repo.upsert_calls == 2
    sleep.assert_awaited_once()


async def test_upsert_with_retry_falls_back_to_per_row() -> None:
    """Verify exhausting the retry budget triggers the per-row fallback.

    Given: A repo that fails every batch attempt within the budget but accepts
        per-row upserts,
    When: _upsert_with_retry runs with the backoff sleep patched out,
    Then: It defers to the per-row pass and writes every row without raising.
    """
    repo = _FakeRepo(upsert_fail_times=backfill._UPSERT_RETRIES + 1)
    with patch.object(backfill.asyncio, "sleep", new=AsyncMock()) as sleep:
        written = await backfill._upsert_with_retry(repo, [_urow(), _urow()])
    assert written == 2
    assert sleep.await_count == backfill._UPSERT_RETRIES


async def test_upsert_skipping_contended_defers_hot_row() -> None:
    """Verify the per-row fallback skips a contended window and writes the rest.

    Given: A batch in which one window is contended by the live writer,
    When: _upsert_skipping_contended runs,
    Then: The contended row is skipped and the remaining rows are written.
    """
    repo = _FakeRepo(conflict_open_ats={_at(0)})
    written = await backfill._upsert_skipping_contended(repo, [_urow(_at(0)), _urow(_at(5))])
    assert written == 1
    assert len(repo.upserts) == 1
    assert repo.upserts[0][0]["open_at"] == _at(5)


async def test_run_write_mode_full(tmp_path: Path) -> None:
    """Verify a write run processes, skips, resumes, and records correctly.

    Given: Two exchanges with a done symbol, a good symbol, an empty symbol,
        and an unresolved symbol, in write mode,
    When: run executes,
    Then: Totals reflect each path, the progress file grows, and the engine is disposed.
    """
    progress = tmp_path / "done.jsonl"
    progress.write_text("kraken\tDONE-SYM\n", encoding="utf-8")
    good = [_row(_at(0)), _row(_at(1)), _row(_at(5))]
    repo = _FakeRepo(
        instruments={"kraken": ["DONE-SYM", "GOOD", "EMPTY", "NOIPID"], "walut": ["FX"]},
        ipids={
            "kraken": {"DONE-SYM": "id0", "GOOD": "id1", "EMPTY": "id2"},
            "walut": {"FX": "id3"},
        },
        candles={"GOOD": good, "FX": good, "EMPTY": []},
    )
    args = _args(
        exchanges="kraken,walut,",
        timeframes="5m,15m,",
        progress_file=str(progress),
        concurrency=2,
    )
    with (
        patch.object(backfill, "get_settings", return_value=SimpleNamespace(db_url="x")),
        patch.object(backfill, "get_repository", return_value=repo),
    ):
        totals = await backfill.run(args)
    assert totals.instruments == 3
    assert totals.skipped_no_instrument == 1
    assert totals.skipped_no_1m == 1
    assert totals.errors == 0
    assert totals.written == 2
    recorded = {tuple(line.split("\t")) for line in progress.read_text().splitlines()}
    assert ("kraken", "GOOD") in recorded
    assert ("walut", "FX") in recorded
    assert ("kraken", "EMPTY") in recorded
    repo.engine.dispose.assert_awaited_once()


async def test_run_dry_run_filters_and_error(tmp_path: Path) -> None:
    """Verify dry-run honours filters, isolates errors, and skips disposal.

    Given: A symbol filter, an instrument limit, a failing symbol, an empty
        exchange, dry-run mode, and a repo without an engine,
    When: run executes,
    Then: Only the good symbol succeeds, the failure is counted, nothing is
        written, and no progress file is created.
    """
    repo = _FakeRepo(
        instruments={"kraken": ["GOOD", "BAD", "OTHER"], "emptyex": []},
        ipids={"kraken": {"GOOD": "id1", "BAD": "id2"}},
        candles={"GOOD": [_row(_at(0)), _row(_at(5))]},
        has_engine=False,
        raise_for=("BAD",),
    )
    progress = tmp_path / "unused.jsonl"
    args = _args(
        exchanges="kraken,emptyex",
        timeframes="5m",
        symbols="GOOD,BAD,",
        limit_instruments=5,
        dry_run=True,
        concurrency=1,
        progress_file=str(progress),
    )
    with (
        patch.object(backfill, "get_settings", return_value=SimpleNamespace(db_url="x")),
        patch.object(backfill, "get_repository", return_value=repo),
    ):
        totals = await backfill.run(args)
    assert totals.instruments == 1
    assert totals.errors == 1
    assert repo.upserts == []
    assert not progress.exists()


async def test_run_unknown_timeframe_exits() -> None:
    """Verify an unsupported timeframe aborts the run.

    Given: A timeframe not in the supported synthesis set,
    When: run executes,
    Then: It raises SystemExit.
    """
    with pytest.raises(SystemExit):
        await backfill.run(_args(timeframes="5m,bogus"))


def test_main_returns_zero_on_success() -> None:
    """Verify main returns 0 when no instrument errored.

    Given: A patched run reporting zero errors,
    When: main is invoked,
    Then: It returns exit code 0.
    """
    totals = backfill._Totals(errors=0)
    with (
        patch.object(backfill, "run", new=AsyncMock(return_value=totals)),
        patch("sys.argv", ["backfill"]),
    ):
        assert backfill.main() == 0


def test_main_returns_one_on_errors() -> None:
    """Verify main returns 1 when any instrument errored.

    Given: A patched run reporting errors,
    When: main is invoked,
    Then: It returns exit code 1.
    """
    totals = backfill._Totals(errors=2)
    with (
        patch.object(backfill, "run", new=AsyncMock(return_value=totals)),
        patch("sys.argv", ["backfill"]),
    ):
        assert backfill.main() == 1
