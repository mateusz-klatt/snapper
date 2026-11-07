"""Tests for the synthesized higher-timeframe candle backfill script."""

import argparse
import asyncio
import threading
from datetime import UTC
from datetime import datetime
from io import StringIO
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

    async def insert_missing_synthesized_candles(
        self, rows: list[CandleUpsertRow], *, dry_run: bool = False
    ) -> list[CandleUpsertRow]:
        """Record only insert-only writes for bounded replay tests."""
        await self.upsert_historical_synthesized_candles(rows)
        return rows

    async def upsert_historical_synthesized_candles(
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
        "insert_only": False,
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


def test_load_done_rejects_traversal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a progress path that traverses outside the current directory.

    Given: A child working directory and a parent-relative progress path.
    When: The resume log loader resolves the operator-selected path.
    Then: Validation refuses the noncanonical target before reading it.
    """
    child = tmp_path / "child"
    child.mkdir()
    monkeypatch.chdir(child)
    with pytest.raises(ValueError, match="canonical child"):
        backfill._load_done(Path("../progress.jsonl"))


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


def test_roll_up_vwap_none_stays_unknown() -> None:
    """Verify missing native VWAP stays unknown.

    Given: A closed-window row whose vwap is None,
    When: _roll_up runs,
    Then: The bucket's vwap_sum remains unknown.
    """
    rows = [
        _row(_at(0), close=50.0, volume=2.0, vwap=None),
        _row(_at(5), close=50.0, volume=1.0, vwap=50.0),
    ]
    closed = backfill._roll_up(rows, {"5m": 300}, close_before_ts=4_000_000_000)
    _begin, bucket = closed["5m"][0]
    assert bucket.vwap_sum is None


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
    """Verify VWAP is weighted only when it is defined.

    Given: Two closed buckets, one with volume and one with zero volume,
    When: _build_rows projects them,
    Then: The traded bar carries a weighted vwap and the empty bar carries its
        own close, never 0.0 — its OHLC names a real price level, so a
        persisted zero would price a valid bar at nothing.

    This must match ``CandleAggregator._to_candle_update``: the script and the
    live publisher write to the same durable plane, so a reconstruction that
    disagreed with the live path would be worse than no reconstruction.
    """
    traded = backfill._Bucket(open=1, high=2, low=1, close=2, volume=4.0, trades=3, vwap_sum=8.0)
    empty = backfill._Bucket(open=2, high=2, low=2, close=2, volume=0.0, trades=0, vwap_sum=0.0)
    closed = {"5m": [(0, traded)], "15m": [(0, empty)]}
    out = backfill._build_rows(closed, "iid", _at(0), count(1), "sess")
    by_tf = {r["timeframe"]: r for r in out}
    assert by_tf["5m"]["vwap"] == 2.0
    assert by_tf["15m"]["vwap"] == 2.0
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
        repo,
        "kraken",
        "X",
        "iid",
        backfill._ReplayOptions({"5m": 300}, _at(0), _at(10), 4000000000, 2000, "sess", False),
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
        repo,
        "kraken",
        "X",
        "iid",
        backfill._ReplayOptions({"5m": 300}, _at(0), _at(10), 0, 2000, "sess", False),
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
        repo,
        "kraken",
        "X",
        "iid",
        backfill._ReplayOptions({"5m": 300}, _at(0), _at(10), 4000000000, 2000, "sess", True),
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
        repo,
        "kraken",
        "X",
        "iid",
        backfill._ReplayOptions({"5m": 300}, _at(0), _at(20), 4000000000, 1, "sess", False),
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


async def test_upsert_with_retry_exhaustion_is_an_error() -> None:
    """Reject silent row skipping after bounded conflict retries.

    Given a repository that keeps rejecting a batch,
    When all four attempts fail,
    Then the error propagates and no per-row fallback claims success.
    """
    repo = _FakeRepo(upsert_fail_times=4)
    with (
        patch.object(backfill.asyncio, "sleep", new=AsyncMock()) as sleep,
        pytest.raises(IntegrityError),
    ):
        await backfill._upsert_with_retry(repo, [_urow(), _urow()])
    assert repo.upsert_calls == 4
    assert sleep.await_count == 3
    assert repo.upserts == []


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
        since_days=365,
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


async def test_run_opens_progress_log_before_repository(tmp_path: Path) -> None:
    """Verify an unusable progress log aborts before repository construction.

    Given: A write run whose progress log cannot be opened,
    When: run performs its filesystem preflight,
    Then: The repository factory is never called.
    """
    args = _args(progress_file=str(tmp_path / "progress.tsv"))
    with (
        patch.object(backfill, "_open_progress_log", side_effect=OSError("read-only")),
        patch.object(backfill, "get_repository") as repository_factory,
        pytest.raises(OSError, match="read-only"),
    ):
        await backfill.run(args)
    repository_factory.assert_not_called()


@pytest.mark.parametrize("with_progress_handle", [False, True])
async def test_repository_failure_closes_optional_progress_handle(
    with_progress_handle: bool,
) -> None:
    """Verify repository construction failure preserves progress resources.

    Given: Repository construction fails with or without an open progress log,
    When: The post-preflight repository helper propagates the failure,
    Then: Any provided handle is closed and the original exception is retained.
    """
    progress_handle = StringIO() if with_progress_handle else None
    with (
        patch.object(backfill, "_resolve_db_url", return_value="postgresql://invalid"),
        patch.object(backfill, "get_repository", side_effect=RuntimeError("unavailable")),
        pytest.raises(RuntimeError, match="unavailable"),
    ):
        await backfill._repository_after_progress_preflight("", progress_handle)
    if progress_handle is not None:
        assert progress_handle.closed


async def test_run_waits_for_siblings_when_progress_append_fails(tmp_path: Path) -> None:
    """Verify progress-log failure cancels workers before resource teardown.

    Given: One worker whose progress append fails while a sibling is blocked,
    When: The exchange task group propagates the write failure,
    Then: The sibling is cancelled and awaited before the repository is disposed.
    """
    slow_started = asyncio.Event()
    slow_cancelled = asyncio.Event()
    blocker = asyncio.Event()

    async def process(*args: object) -> tuple[int, int, int]:
        """Complete the first worker and hold its sibling until cancellation."""
        symbol = str(args[2])
        if symbol == "FAIL":
            await slow_started.wait()
            return 1, 1, 1
        slow_started.set()
        try:
            await blocker.wait()
        except asyncio.CancelledError:
            slow_cancelled.set()
            raise
        return 1, 1, 1

    repo = _FakeRepo(
        instruments={"kraken": ["FAIL", "SLOW"]},
        ipids={"kraken": {"FAIL": "id1", "SLOW": "id2"}},
    )
    args = _args(progress_file=str(tmp_path / "progress.tsv"), concurrency=2)
    with (
        patch.object(backfill, "get_settings", return_value=SimpleNamespace(db_url="x")),
        patch.object(backfill, "get_repository", return_value=repo),
        patch.object(backfill, "_process_instrument", side_effect=process),
        patch.object(backfill, "_append_progress", side_effect=OSError("disk full")),
        pytest.raises(ExceptionGroup) as exc_info,
    ):
        await backfill.run(args)
    assert any(isinstance(exc, OSError) for exc in exc_info.value.exceptions)
    assert slow_cancelled.is_set()
    repo.engine.dispose.assert_awaited_once()


async def test_run_waits_for_inflight_progress_append_before_teardown(tmp_path: Path) -> None:
    """Verify cancellation cannot orphan an in-flight progress write.

    Given: A worker whose offloaded progress append is blocked in a real thread.
    When: The enclosing run is cancelled while that append is active.
    Then: Repository teardown waits until the append finishes before propagating cancellation.
    """
    append_started = threading.Event()
    append_release = threading.Event()
    original_append = backfill._append_progress

    def blocking_append(handle: StringIO, exchange: str, symbol: str) -> None:
        """Hold the append thread until the test permits safe teardown."""
        append_started.set()
        append_release.wait(5.0)
        original_append(handle, exchange, symbol)

    progress_handle = StringIO()
    repo = _FakeRepo(
        instruments={"kraken": ["BTC"]},
        ipids={"kraken": {"BTC": "id1"}},
    )
    args = _args(progress_file=str(tmp_path / "progress.tsv"))
    with (
        patch.object(backfill, "get_settings", return_value=SimpleNamespace(db_url="x")),
        patch.object(backfill, "get_repository", return_value=repo),
        patch.object(backfill, "_open_progress_log", return_value=progress_handle),
        patch.object(backfill, "_process_instrument", return_value=(1, 1, 1)),
        patch.object(backfill, "_append_progress", side_effect=blocking_append),
    ):
        run_task = asyncio.create_task(backfill.run(args))
        assert await asyncio.to_thread(append_started.wait, 2.0)
        run_task.cancel()
        await asyncio.sleep(0)
        completed_before_release = run_task.done()
        disposed_before_release = repo.engine.dispose.await_count
        append_release.set()
        with pytest.raises(asyncio.CancelledError):
            await run_task

    assert not completed_before_release
    assert disposed_before_release == 0
    repo.engine.dispose.assert_awaited_once()


async def test_append_progress_safely_rethrows_cancellation_after_write() -> None:
    """Verify the offloaded append itself finishes before cancellation escapes.

    Given: An append blocked inside its worker thread.
    When: The async append wrapper is cancelled and the thread is then released.
    Then: The write completes before the original cancellation is rethrown.
    """
    append_started = threading.Event()
    append_release = threading.Event()
    original_append = backfill._append_progress

    def blocking_append(handle: StringIO, exchange: str, symbol: str) -> None:
        """Hold the write until cancellation is known to have arrived."""
        append_started.set()
        append_release.wait(5.0)
        original_append(handle, exchange, symbol)

    progress_handle = StringIO()
    with patch.object(backfill, "_append_progress", side_effect=blocking_append):
        append_task = asyncio.create_task(
            backfill._append_progress_safely(progress_handle, "kraken", "BTC")
        )
        assert await asyncio.to_thread(append_started.wait, 2.0)
        append_task.cancel()
        await asyncio.sleep(0)
        completed_before_release = append_task.done()
        append_release.set()
        with pytest.raises(asyncio.CancelledError):
            await append_task

    assert not completed_before_release
    assert progress_handle.getvalue() == "kraken\tBTC\n"


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
    unsupported_timeframe_args = _args(timeframes="5m,bogus")

    with pytest.raises(SystemExit):
        await backfill.run(unsupported_timeframe_args)


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


def test_requested_window_rejects_unpaired_bounds_and_unsafe_limits() -> None:
    """Validate bounded synthesis before filesystem or database access.

    Given an unpaired bound, nonpositive batch or negative settle delay,
    When the request window is resolved,
    Then the invalid configuration raises.
    """
    for args in [
        _args(start=_at(0)),
        _args(end=_at(10)),
        _args(batch_size=0),
        _args(settle_minutes=-1),
    ]:
        with pytest.raises(ValueError):
            backfill._requested_window(args, _at(20))


def test_roll_up_drops_leading_partial_windows_and_unknown_statistics() -> None:
    """Exclude windows whose source range starts before the requested bound.

    Given an unaligned lower bound and minutes with unknown statistics,
    When bounded rollup runs,
    Then it emits only wholly enclosed windows and keeps statistics unknown.
    """
    unknown = _row(_at(6), vwap=None)
    unknown["trades"] = None
    rows = [_row(_at(1)), _row(_at(5)), unknown, _row(_at(9)), _row(_at(10))]
    closed = backfill._roll_up(rows, {"5m": 300}, int(_at(10).timestamp()), _at(1))
    assert [opening for opening, _ in closed["5m"]] == [int(_at(5).timestamp())]
    assert closed["5m"][0][1].vwap_sum is None
    assert closed["5m"][0][1].trades is None


async def test_explicit_window_ignores_progress_and_reads_at_acquisition(
    tmp_path: Path,
) -> None:
    """Make wider reruns independent of previous per-instrument progress.

    Given a completed legacy progress entry and explicit historical bounds,
    When bounded synthesis runs,
    Then it recomputes the range through the guarded writer and preserves the log.
    """
    progress = tmp_path / "done.jsonl"
    original = "kraken\tX\n"
    progress.write_text(original)
    repo = _FakeRepo(
        instruments={"kraken": ["X"]},
        ipids={"kraken": {"X": "iid"}},
        candles={
            "X": [
                _row(_at(0)),
                _row(_at(1)),
                _row(_at(4)),
                _row(_at(5)),
                _row(_at(9)),
                _row(_at(10)),
            ]
        },
    )
    args = _args(start=_at(1), end=_at(10), insert_only=True, progress_file=str(progress))
    with patch.object(backfill, "get_repository", return_value=repo):
        totals = await backfill.run(args)
    assert totals.instruments == 1
    assert totals.windows == 1
    assert repo.upserts[0][0]["open_at"] == _at(5)
    assert repo.upserts[0][0]["timestamp"] > _at(10)
    assert progress.read_text() == original


def test_bounded_synthesis_requires_insert_only_mode() -> None:
    """Make immutable repair behavior explicit at the operator boundary.

    Given explicit historical bounds without the insert-only flag,
    When the range is validated,
    Then the run is rejected before opening a repository.
    """
    with pytest.raises(ValueError, match="--insert-only"):
        backfill._requested_window(_args(start=_at(0), end=_at(10)), _at(20))
    parsed = backfill._parse_args(
        ["--start", _at(0).isoformat(), "--end", _at(10).isoformat(), "--insert-only"]
    )
    assert parsed.insert_only and parsed.batch_size == 100


async def test_bounded_synthesis_exhaustion_reports_error_and_committed_progress(
    tmp_path: Path,
) -> None:
    """Abort a failed instrument without losing previously acknowledged repair writes.

    Given two closed windows with the second batch repeatedly conflicting,
    When bounded insert-only synthesis runs,
    Then it reports the first commit and an error without reaching the revision writer.
    """
    repo = _FakeRepo(
        instruments={"kraken": ["X"]},
        ipids={"kraken": {"X": "iid"}},
        candles={"X": [_row(_at(0)), _row(_at(4)), _row(_at(5)), _row(_at(9))]},
    )
    conflict = IntegrityError("write", None, Exception("unique conflict"))
    insertion = AsyncMock(side_effect=[[{}], conflict, conflict, conflict, conflict])
    repo._instruments["kraken"].append("Y")
    repo._ipids["kraken"]["Y"] = "other"
    args = _args(
        start=_at(0),
        end=_at(10),
        insert_only=True,
        concurrency=1,
        exchanges="kraken,later",
        batch_size=1,
        progress_file=str(tmp_path / "unused.tsv"),
    )
    with (
        patch.object(repo, "insert_missing_synthesized_candles", insertion),
        patch.object(backfill, "get_repository", return_value=repo),
        patch.object(backfill.asyncio, "sleep", AsyncMock()),
    ):
        totals = await backfill.run(args)
    assert totals.written == 1 and totals.errors == 1
    assert insertion.await_count == 5
    assert repo.upsert_calls == 0


def test_zero_volume_known_statistics_keep_original_close_convention() -> None:
    """Preserve the original zero-volume fold while retaining unknown input statistics.

    Given zero-volume windows with known and unknown input VWAP,
    When synthesized rows are built,
    Then known statistics use the close convention and unknown VWAP remains null.
    """
    rows = [_row(_at(0), volume=0, vwap=100), _row(_at(4), volume=0, vwap=100)]
    closed = backfill._roll_up(rows, {"5m": 300}, int(_at(5).timestamp()), _at(0))
    built = backfill._build_rows(closed, "iid", _at(20), count(1), "session")
    assert built[0]["vwap"] == 100
    rows[0]["vwap"] = None
    closed = backfill._roll_up(rows, {"5m": 300}, int(_at(5).timestamp()), _at(0))
    built = backfill._build_rows(closed, "iid", _at(20), count(1), "session")
    assert built[0]["vwap"] is None
