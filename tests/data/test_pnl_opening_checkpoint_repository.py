"""Repository persistence, serialization, choke-point, and aggregate tests."""

import ast
import json
from collections.abc import AsyncIterator
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import PropertyMock
from unittest.mock import patch
from uuid import uuid7

import pytest
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.pnl_opening_checkpoint_certificate import MAX_DEFERRED_EXECUTIONS
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    checkpoint_minute_fenced,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    checkpoint_write_refusal,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    opening_checkpoint_fence,
)
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    opening_checkpoint_fence_source,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_wallet_alias
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccrualLedger
from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import PortfolioPnlCorrectionFrontier
from snapper.data.models import PortfolioPnlOpeningCheckpoint
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import Symbol
from snapper.data.pnl_correction_state import IdAggregate
from snapper.data.pnl_correction_state import PnlCorrectionChunk
from snapper.data.pnl_correction_state import PnlCorrectionEdit
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.pnl_correction_state import PnlCorrectionRequest
from snapper.data.pnl_correction_state import PnlOpeningCheckpointCertificate
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.pnl_correction_state import PnlOpeningCheckpointWrite
from snapper.data.pnl_correction_state import PortfolioPnlCorrectionConflictError
from snapper.data.pnl_correction_state import advance_correction_frontier
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _combine_id_aggregates
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlFxRateRow
from tests.data import test_pnl_timeline_sample_repository as samples
from tests.data.test_pnl_fx_rate_repository import _candle as _fx_candle
from tests.data.test_pnl_fx_rate_repository import _instrument as _fx_instrument
from tests.data.test_pnl_fx_rate_repository import _symbol as _fx_symbol

_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64
_SESSION = "00000000-0000-7000-8000-000000000701"
_INST_FX_A = "00000000-0000-7000-8000-000000000f01"
_REPOSITORY_PATH = (
    Path(__file__).resolve().parents[2] / "src" / "snapper" / "data" / "repository.py"
)


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated SQLite repository with checkpoint, frontier, and prefix tables.

    Args:
        tmp_path: Temporary directory owned by pytest.

    Yields:
        Configured ``SQLAlchemyRepository`` seeded with one active anchor row.
    """
    db_path = tmp_path / "opening-checkpoint.db"
    engine = create_engine(f"sqlite:///{db_path}")
    PortfolioPnlPoint.__table__.create(engine)
    PortfolioPnlCorrectionFrontier.__table__.create(engine)
    PortfolioPnlOpeningCheckpoint.__table__.create(engine)
    AccrualLedger.__table__.create(engine)
    Symbol.__table__.create(engine)
    Instrument.__table__.create(engine)
    Candle.__table__.create(engine)
    Execution.__table__.create(engine)
    engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    async with repo.session() as session:
        session.add(samples._anchor_orm())
        await session.commit()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def test_default_checkpoint_write_refusal_is_empty() -> None:
    """Keep the base repository compatible with callers that never stage checkpoints."""
    repository = cast(Repository, object())
    assert Repository.consume_last_pnl_checkpoint_write_refusal(repository) is None
    assert Repository.consume_last_pnl_checkpoint_invalidation_deleted(repository) == 0


def _key() -> PnlCorrectionKey:
    """Build the canonical correction key matching ``samples._scope()``.

    Returns:
        Five-component correction key bound to the active calculation version.
    """
    return PnlCorrectionKey(
        samples._WALLET,
        "live",
        "USD",
        samples._EPOCH,
        PNL_SAMPLE_CALC_VERSION,
    )


def _request(
    *,
    start: datetime = samples._M1,
    tip: datetime = samples._M3,
    as_of: datetime = samples._M5,
) -> PnlCorrectionRequest:
    """Build a valid correction request carrying complete execution-cut metadata.

    Args:
        start: Inclusive window start minute after anchor.
        tip: Target tip minute.
        as_of: Knowledge horizon timestamp.

    Returns:
        Populated ``PnlCorrectionRequest`` with pinned execution evidence.
    """
    return PnlCorrectionRequest(
        start=start,
        tip=tip,
        as_of=as_of,
        baseline_watermarks={"walutomat": 4, "kraken": 5},
        pending_watermarks={"walutomat": 4, "kraken": 5},
        prior_peak=None,
        raw_execution_watermarks={"walutomat": 4, "kraken": 5},
        execution_manifest_digest=_DIGEST_A,
        has_fill_gap=False,
        expected_venues=frozenset({"kraken", "walutomat"}),
        venue_event_watermarks={"kraken:spot": 9, "walutomat:spot": 2},
        fill_witness_digest=_DIGEST_B,
    )


def _empty_chunk(frontier: PnlCorrectionFrontier, end: datetime) -> PnlCorrectionChunk:
    """Construct a zero-edit chunk matched against ``frontier``'s next minute.

    Args:
        frontier: Current persisted frontier.
        end: Inclusive chunk end minute.

    Returns:
        Validated ``PnlCorrectionChunk`` ready to commit.
    """
    start = frontier.next_minute or frontier.window_start
    evidence = PnlCorrectionEvidence(
        key=frontier.key,
        start=start,
        end=end,
        versions=(),
        prior_peak=frontier.prior_peak,
    )
    return PnlCorrectionChunk(end=end, edits=(), prior_peak=None, evidence=evidence)


def _certificate_for(
    frontier: PnlCorrectionFrontier,
    **overrides: object,
) -> PnlOpeningCheckpointCertificate:
    """Build a valid opening-checkpoint certificate matching ``frontier``.

    Args:
        frontier: Settled frontier whose generation, tip, and evidence match.
        **overrides: Explicit field overrides for testing mismatches or shapes.

    Returns:
        Validated ``PnlOpeningCheckpointCertificate``.
    """
    kwargs: dict[str, object] = {
        "key": frontier.key,
        "timeline_calc_version": "5A.16",
        "fx_policy_version": "pnl-fiat-pointwise-v1",
        "schema_version": 1,
        "anchor_public_id": samples._ANCHOR_PUBLIC_ID,
        "anchor_digest": _DIGEST_B,
        "source_generation": frontier.generation,
        "checkpoint_minute": frontier.tip,
        "as_of": frontier.as_of,
        "cutoff_execution_watermarks": {"walutomat": 4, "kraken": 5},
        "execution_chain_digests": {"walutomat": _DIGEST_B, "kraken": _DIGEST_A},
        "cutoff_execution_counts": {"walutomat": 4, "kraken": 5},
        "applied_watermarks_by_exchange": {"walutomat": 4, "kraken": 3},
        "deferred_execution_sequences": {"kraken": [4, 5]},
        "prefix_has_idless_executions": False,
        "execution_manifest_digest": _DIGEST_A,
        "prefix_annulment_digest": _DIGEST_A,
        "accrual_prefix_fingerprint": IdAggregate(2, 15, 10, samples._M3),
        "prefix_lineage_digest": _DIGEST_A,
        "prefix_fill_witness_digest": _DIGEST_B,
        "prefix_venue_event_watermarks": {"walutomat:spot": 2, "kraken:spot": 9},
        "source_expected_venues": frozenset({"kraken", "walutomat"}),
        "source_venue_event_watermarks": {"walutomat:spot": 2, "kraken:spot": 9},
        "source_fill_witness_digest": _DIGEST_B,
        "symbol_ref_digest": _DIGEST_A,
        "consumed_fx_pairs": [("BTC", "USD"), ("EUR", "USD")],
        "consumed_fx_open_ats": [samples._M1, samples._M2],
        "event_fx_exchanges_digest": _DIGEST_A,
        "event_fx_candles_fingerprint": IdAggregate(3, (3 << 62) + 6, (1 << 62) + 3, samples._M2),
    }
    kwargs.update(overrides)
    return PnlOpeningCheckpointCertificate(**kwargs)


async def _seed_settled_checkpoint(
    repository: SQLAlchemyRepository,
    *,
    tip: datetime = samples._M3,
    as_of: datetime = samples._M5,
    state_json: str = '{"version":1,"pools":{}}',
) -> tuple[PnlCorrectionFrontier, PnlOpeningCheckpointWrite]:
    """Start generation 1 and settle a chunk writing one certified checkpoint.

    Args:
        repository: Isolated SQLite repository.
        tip: Generation tip and certified checkpoint minute.
        as_of: Generation knowledge horizon.
        state_json: Serialized engine state payload.

    Returns:
        Pair of committed settled frontier and written checkpoint payload.
    """
    started = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        _request(start=samples._M1, tip=tip, as_of=as_of),
        None,
    )
    chunk = _empty_chunk(started, tip)
    expected_after = advance_correction_frontier(started, chunk)
    write = PnlOpeningCheckpointWrite(_certificate_for(expected_after), state_json)
    settled = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(),
        started,
        chunk,
        checkpoint=write,
    )
    return settled, write


def _is_frontier_name(node: ast.AST) -> bool:
    """Return whether ``node`` names ``PortfolioPnlCorrectionFrontier``.

    Args:
        node: AST node to inspect.

    Returns:
        ``True`` when the expression refers to ``PortfolioPnlCorrectionFrontier``.
    """
    return (isinstance(node, ast.Name) and node.id == "PortfolioPnlCorrectionFrontier") or (
        isinstance(node, ast.Attribute) and node.attr == "PortfolioPnlCorrectionFrontier"
    )


def _is_mutating_dml_call(func: ast.AST) -> bool:
    """Return whether ``func`` invokes SQLAlchemy ``update`` or ``delete``.

    Args:
        func: Call target AST expression.

    Returns:
        ``True`` for ``update(...)`` or ``delete(...)`` calls.
    """
    return (isinstance(func, ast.Name) and func.id in {"update", "delete"}) or (
        isinstance(func, ast.Attribute) and func.attr in {"update", "delete"}
    )


def _target_argument(call: ast.Call) -> ast.AST | None:
    """Extract the target table AST argument from a SQLAlchemy DML call.

    Args:
        call: Parsed ``update(...)`` or ``delete(...)`` AST call.

    Returns:
        Positional or keyword ``table`` expression, if present.
    """
    if call.args:
        return call.args[0]
    return next((kw.value for kw in call.keywords if kw.arg == "table"), None)


def _call_violation_kind(call: ast.Call) -> str | None:
    """Classify whether ``call`` constructs or mutates the frontier ORM model.

    Args:
        call: AST call node to inspect.

    Returns:
        Violation tag or ``None`` for non-mutating expressions like ``select``.
    """
    if _is_frontier_name(call.func):
        return "constructor"
    if _is_mutating_dml_call(call.func):
        target = _target_argument(call)
        if target is not None and _is_frontier_name(target):
            return "dml_target"
    return None


class _FrontierWriteScanner(ast.NodeVisitor):
    """AST visitor detecting frontier writes outside ``_write_pnl_correction``."""

    def __init__(self) -> None:
        """Initialize visitor tracking outside any allowed choke-point method."""
        self._class_name: str | None = None
        self._in_choke_point = False
        self.violations: list[str] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Track enclosing class scope during traversal.

        Args:
            node: Class definition node.
        """
        previous = self._class_name
        self._class_name = node.name
        self.generic_visit(node)
        self._class_name = previous

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Allow writes only directly inside ``SQLAlchemyRepository._write_pnl_correction``.

        Args:
            node: Function or method definition node.
        """
        self._visit_callable(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Allow writes only directly inside ``SQLAlchemyRepository._write_pnl_correction``.

        Args:
            node: Async function or method definition node.
        """
        self._visit_callable(node)

    def _visit_callable(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """Visit a function body while maintaining choke-point scope state.

        Args:
            node: Sync or async function AST node.
        """
        previous = self._in_choke_point
        self._in_choke_point = (
            self._class_name == "SQLAlchemyRepository" and node.name == "_write_pnl_correction"
        )
        self.generic_visit(node)
        self._in_choke_point = previous

    def visit_Call(self, node: ast.Call) -> None:
        """Record any forbidden frontier construction or DML target outside choke point.

        Args:
            node: Call expression node.
        """
        if not self._in_choke_point:
            kind = _call_violation_kind(node)
            if kind is not None:
                self.violations.append(f"line {node.lineno}: {kind}")
        self.generic_visit(node)


def _find_frontier_write_violations(source: str) -> list[str]:
    """Scan Python source for frontier mutations outside ``_write_pnl_correction``.

    Args:
        source: Python source text to parse and verify.

    Returns:
        Human-readable descriptions of any disallowed call sites.
    """
    scanner = _FrontierWriteScanner()
    scanner.visit(ast.parse(source))
    return scanner.violations


def _accrual_orm(
    accrued_at: datetime,
    row_id: int,
    *,
    scope: tuple[str, str] = (samples._WALLET, "live"),
    timestamp: datetime = samples._M1,
    known_to: datetime = KNOWN_TO_MAX,
) -> AccrualLedger:
    """Create one ``AccrualLedger`` row with explicit validity bounds.

    Args:
        accrued_at: Accrual timestamp.
        row_id: Explicit primary key id.
        scope: ``(wallet_public_id, mode)`` ownership tuple.
        timestamp: Bitemporal bus creation time.
        known_to: Bitemporal retirement time.

    Returns:
        Unsaved ``AccrualLedger`` ORM object.
    """
    wallet, mode = scope
    return AccrualLedger(
        id=row_id,
        instrument_public_id=_INST_FX_A,
        wallet_public_id=wallet,
        operator_public_id=None,
        mode=mode,
        accrual_type="funding",
        accrued_at=accrued_at,
        amount=1.25,
        amount_asset="USD",
        rate=0.0001,
        notional=1000.0,
        position_quantity_at_accrual=1.0,
        exchange="kraken",
        timestamp=timestamp,
        known_to=known_to,
        session_id=_SESSION,
        sequence_id=1,
    )


def _fx_candle_seeded(
    instrument_public_id: str,
    open_at: datetime,
    row_id: int,
    *,
    window: tuple[datetime, datetime] = (samples._M1, KNOWN_TO_MAX),
    variant: tuple[str, bool] = ("1m", True),
) -> Candle:
    """Build one FX ``Candle`` via shared test fixtures with explicit id and validity.

    Args:
        instrument_public_id: Owning instrument id.
        open_at: Candle open minute.
        row_id: Explicit primary key id.
        window: ``(timestamp, known_to)`` bitemporal knowledge interval.
        variant: ``(timeframe, complete)`` bar variant tuple.

    Returns:
        Unsaved ``Candle`` ORM object.
    """
    timestamp, known_to = window
    timeframe, complete = variant
    candle = _fx_candle(
        instrument_public_id,
        open_at,
        1.25,
        timeframe=timeframe,
        complete=complete,
    )
    candle.id = row_id
    candle.timestamp = timestamp
    candle.known_to = known_to
    return candle


def _rows_id_aggregate(rows: Sequence[PnlFxRateRow]) -> IdAggregate:
    """Compute exact ``IdAggregate`` over replay rows preserving row multiplicity.

    Args:
        rows: Rows returned by ``get_pnl_fx_rate_candles_at``.

    Returns:
        Exact aggregate over ``(candle_id, candle_timestamp)`` for all rows.
    """
    return _python_id_aggregate([(row["candle_id"], row["candle_timestamp"]) for row in rows])


async def _assert_fx_fingerprint_matches_replay(
    repository: SQLAlchemyRepository,
    planes: Sequence[PnlFxRatePlane],
    opens: Sequence[datetime],
    as_of: datetime,
) -> tuple[IdAggregate, list[PnlFxRateRow]]:
    """Assert FX candle fingerprint matches Python aggregation of replay rows exactly.

    Args:
        repository: Repository under test.
        planes: Pinned FX planes.
        opens: Target 1-minute candle open timestamps.
        as_of: Knowledge horizon.

    Returns:
        Tuple of the matching aggregate and the underlying replay rows.
    """
    rows = await repository.get_pnl_fx_rate_candles_at(planes, opens, as_of)
    expected = _rows_id_aggregate(rows)
    actual = await repository.get_pnl_event_fx_candle_fingerprint(planes, opens, as_of)
    assert actual == expected
    return actual, rows


def _execution_orm(
    exchange: str,
    scope_sequence: int,
    *,
    scope: tuple[str, str] = (samples._WALLET, "live"),
    timestamp: datetime = samples._M1,
    known_to: datetime = KNOWN_TO_MAX,
) -> Execution:
    """Create one ``Execution`` ORM row with explicit sequence and visibility bounds.

    Args:
        exchange: Exchange venue name.
        scope_sequence: Monotonic per-scope execution sequence.
        scope: ``(wallet_public_id, mode)`` ownership tuple.
        timestamp: Bitemporal bus commit time.
        known_to: Bitemporal retirement time.

    Returns:
        Unsaved ``Execution`` ORM object.
    """
    wallet, mode = scope
    return Execution(
        public_id=str(uuid7()),
        order_public_id=str(uuid7()),
        wallet_public_id=wallet,
        operator_public_id=None,
        exchange=exchange,
        mode=mode,
        scope_sequence=scope_sequence,
        exec_id=f"{exchange}-{wallet[-4:]}-{mode}-{scope_sequence}",
        trade_id=None,
        side="buy",
        status="filled",
        price=100.0,
        size=1.0,
        fee=0.1,
        fee_asset="USD",
        liquidity_role="taker",
        session_id=_SESSION,
        sequence_id=scope_sequence,
        timestamp=timestamp,
        executed_at=timestamp,
        known_to=known_to,
    )


def _python_id_aggregate(rows: list[tuple[int, datetime]]) -> IdAggregate:
    """Recompute the expected ``IdAggregate`` independently in pure Python.

    Args:
        rows: Sequence of ``(id, timestamp)`` pairs for matching rows.

    Returns:
        Zero aggregate when empty, otherwise summed count, ids, max id, and max ts.
    """
    if not rows:
        return IdAggregate(0, 0, 0, None)
    return IdAggregate(
        count=len(rows),
        id_sum=sum(row_id for row_id, _ in rows),
        max_id=max(row_id for row_id, _ in rows),
        max_timestamp=max(ts for _, ts in rows),
    )


async def test_opening_checkpoint_canonical_serialization_and_round_trip(
    repository: SQLAlchemyRepository,
) -> None:
    """Persist canonical JSON, decimal id sums, pair lists, and UTC ISO strings cleanly.

    Given: A settled generation committed with an explicit opening-checkpoint write.
    When: Both the raw ORM row and ``get_portfolio_pnl_opening_checkpoint`` are read.
    Then: JSON columns use compact sorted formatting, ``consumed_fx_pairs`` stores lists
        of pairs, ``id_sum`` values are exact decimal strings, and the read reconstructs
        an identical certificate.
    """
    settled, write = await _seed_settled_checkpoint(repository)
    async with repository.session() as session:
        raw = (await session.execute(select(PortfolioPnlOpeningCheckpoint))).scalar_one()
    expected_agg_json = json.dumps(
        {
            "count": 2,
            "id_sum": "15",
            "max_id": 10,
            "max_timestamp": samples._M3.astimezone(UTC).isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    assert raw.accrual_prefix_fingerprint_json == expected_agg_json
    assert raw.consumed_fx_pairs_json == '[["BTC","USD"],["EUR","USD"]]'
    assert raw.consumed_fx_open_ats_json == json.dumps(
        [samples._M1.astimezone(UTC).isoformat(), samples._M2.astimezone(UTC).isoformat()],
        separators=(",", ":"),
    )
    assert raw.cutoff_execution_watermarks_json == '{"kraken":5,"walutomat":4}'
    assert raw.deferred_execution_sequences_json == '{"kraken":[4,5]}'
    assert raw.source_expected_venues_json == '["kraken","walutomat"]'
    assert raw.source_venue_event_watermarks_json == '{"kraken:spot":9,"walutomat:spot":2}'
    assert raw.source_fill_witness_digest == _DIGEST_B
    assert raw.event_fx_candle_id_sum == str((3 << 62) + 6)

    record = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert isinstance(record, PnlOpeningCheckpointRecord)
    assert record.certificate == write.certificate
    assert record.state_json == write.state_json
    assert record.updated_at.utcoffset() is not None
    assert settled.next_minute is None

    empty_cert = _certificate_for(
        settled,
        accrual_prefix_fingerprint=IdAggregate(0, 0, 0, None),
        event_fx_candles_fingerprint=IdAggregate(0, 0, 0, None),
        applied_watermarks_by_exchange={},
        deferred_execution_sequences={},
        consumed_fx_pairs=(),
        consumed_fx_open_ats=(),
    )
    next_g = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        _request(start=samples._M4, tip=samples._M5, as_of=samples._M5),
        settled,
    )
    chunk_g2 = _empty_chunk(next_g, samples._M5)
    after_g2 = advance_correction_frontier(next_g, chunk_g2)
    zero_write = PnlOpeningCheckpointWrite(
        replace(empty_cert, source_generation=next_g.generation, checkpoint_minute=after_g2.tip),
        '{"empty":true}',
    )
    await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(),
        next_g,
        chunk_g2,
        checkpoint=zero_write,
    )
    zero_record = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert zero_record is not None and zero_record.certificate == zero_write.certificate


@pytest.mark.parametrize(
    ("column_name", "bad_value"),
    [
        ("cutoff_execution_watermarks_json", "[]"),
        ("cutoff_execution_watermarks_json", '{"kraken":true}'),
        ("cutoff_execution_watermarks_json", '{"kraken":1,"kraken":2}'),
        ("execution_chain_digests_json", '{"kraken":123}'),
        ("deferred_execution_sequences_json", '{"kraken":"bad"}'),
        ("deferred_execution_sequences_json", '{"kraken":[true]}'),
        ("accrual_prefix_fingerprint_json", '{"count":0}'),
        (
            "accrual_prefix_fingerprint_json",
            '{"count":true,"id_sum":"0","max_id":0,"max_timestamp":null}',
        ),
        (
            "accrual_prefix_fingerprint_json",
            '{"count":1,"id_sum":1,"max_id":1,"max_timestamp":null}',
        ),
        (
            "accrual_prefix_fingerprint_json",
            '{"count":1,"id_sum":"1","max_id":1,"max_timestamp":"2026-07-20T12:03:00"}',
        ),
        ("accrual_prefix_fingerprint_json", "[]"),
        ("consumed_fx_pairs_json", '{"BTC":"USD"}'),
        ("consumed_fx_pairs_json", '[["BTC"]]'),
        ("consumed_fx_pairs_json", "[[123,456]]"),
        ("consumed_fx_open_ats_json", '["not-a-datetime"]'),
        ("consumed_fx_open_ats_json", "[123]"),
        ("event_fx_candle_id_sum", "not-decimal"),
        ("event_fx_candle_id_sum", "-1"),
        ("state_json", ""),
        ("anchor_digest", "short"),
        ("source_expected_venues_json", "[]"),
        ("source_expected_venues_json", '["Kraken"]'),
        ("source_expected_venues_json", '["kraken","kraken"]'),
        ("source_expected_venues_json", "{}"),
        ("source_venue_event_watermarks_json", "[]"),
        ("source_venue_event_watermarks_json", '{"kraken:spot":-1}'),
        ("source_fill_witness_digest", "short"),
    ],
)
async def test_corrupt_persisted_opening_checkpoint_raises_value_error(
    repository: SQLAlchemyRepository,
    column_name: str,
    bad_value: str,
) -> None:
    """Re-validate every persisted checkpoint field on read and refuse corruption.

    Given: One persisted checkpoint row mutated in SQLite to hold a malformed value.
    When: ``get_portfolio_pnl_opening_checkpoint`` reads that scope.
    Then: ``ValueError`` is raised instead of silently returning bad data.
    """
    await _seed_settled_checkpoint(repository)
    async with repository.session() as session:
        await session.execute(
            text(f"UPDATE portfolio_pnl_opening_checkpoints SET {column_name} = :value"),
            {"value": bad_value},
        )
        await session.commit()
    with pytest.raises(ValueError):
        await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())


async def test_opening_checkpoint_lookup_isolates_calc_version_and_pk_components(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat ``calc_version`` as an opaque isolated key part.

    Given: A persisted checkpoint row stored under the current calculation version
        and another row under the never-current foreign ``calc_version="5C.0"``.
    When: ``get_portfolio_pnl_opening_checkpoint(samples._scope())`` is called.
    Then: Neither the current-keyed nor foreign-keyed row is returned for the other key.
    """
    _, write = await _seed_settled_checkpoint(repository)
    other_version = "5C.0"
    with monkeypatch.context() as patcher:
        patcher.setattr("snapper.data.repository.PNL_SAMPLE_CALC_VERSION", other_version)
        assert await repository.get_portfolio_pnl_opening_checkpoint(samples._scope()) is None

    other_cert = replace(
        write.certificate,
        key=replace(write.certificate.key, calc_version=other_version),
    )
    async with repository.session() as session:
        await session.execute(delete(PortfolioPnlOpeningCheckpoint))
        session.add(
            PortfolioPnlOpeningCheckpoint(
                **SQLAlchemyRepository._pnl_opening_checkpoint_values(
                    PnlOpeningCheckpointWrite(other_cert, write.state_json),
                    datetime.now(UTC),
                )
            )
        )
        await session.commit()
    assert await repository.get_portfolio_pnl_opening_checkpoint(samples._scope()) is None


async def test_fenced_delete_respects_source_generation_and_acquires_write_fence(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delete an opening checkpoint only inside the writer fence and matching generation.

    Given: A persisted generation-1 opening checkpoint.
    When: ``delete_portfolio_pnl_opening_checkpoint`` is called with a stale
        generation first and the exact generation second.
    Then: The stale generation returns ``False`` and keeps the row, the matching
        generation returns ``True`` and removes it, and both enter the per-scope fence.
    """
    await _seed_settled_checkpoint(repository)
    original_begin = repository._begin_portfolio_pnl_sample_write_transaction
    fenced_calls: list[bool] = []

    async def spy_begin(*args: object, **kwargs: object) -> None:
        """Record write-fence acquisition while delegating to the real transaction fence."""
        fenced_calls.append(True)
        await cast(AsyncMock, original_begin)(*args, **kwargs)

    monkeypatch.setattr(repository, "_begin_portfolio_pnl_sample_write_transaction", spy_begin)
    assert not await repository.delete_portfolio_pnl_opening_checkpoint(samples._scope(), 99)
    assert await repository.get_portfolio_pnl_opening_checkpoint(samples._scope()) is not None
    assert await repository.delete_portfolio_pnl_opening_checkpoint(samples._scope(), 1)
    assert await repository.get_portfolio_pnl_opening_checkpoint(samples._scope()) is None
    assert not await repository.delete_portfolio_pnl_opening_checkpoint(samples._scope(), 1)
    assert len(fenced_calls) == 3
    with pytest.raises(ValueError, match="expected_generation"):
        await repository.delete_portfolio_pnl_opening_checkpoint(
            samples._scope(),
            cast(int, True),
        )


async def test_choke_point_invalidates_or_retains_checkpoint_for_both_writers(
    repository: SQLAlchemyRepository,
) -> None:
    """Enforce ``checkpoint_minute >= earliest_open`` deletion on every frontier write.

    Given: Settled checkpoints at ``C = M3`` and ``C = M4`` exercised across
        invalidate rewind, forward invalidate, mid-generation forward commit,
        and repair-from fallback.
    When: Frontier writers execute through ``_write_pnl_correction``.
    Then: Invalidate with ``start <= C`` deletes, invalidate with ``start = C + 1m``
        keeps, mid-gen forward commit with ``next_minute > C`` keeps, and a commit
        whose resulting ``repair_from <= C`` deletes.
    """
    m6 = samples._M5 + timedelta(minutes=1)
    as_of_later = m6 + timedelta(minutes=2)
    settled_g1, _ = await _seed_settled_checkpoint(repository, tip=samples._M3)
    messages: list[str] = []
    sink_id = logger.add(messages.append, level="INFO", format="{message}")
    try:
        rewound = await repository.invalidate_portfolio_pnl_correction(
            samples._scope(),
            _request(start=samples._M3, tip=samples._M4, as_of=as_of_later),
            settled_g1,
        )
    finally:
        logger.remove(sink_id)
    assert checkpoint_minute_fenced(samples._M3, opening_checkpoint_fence(rewound))
    assert repository.consume_last_pnl_checkpoint_invalidation_deleted() == 1
    fence_lines = [
        message.strip() for message in messages if "invalidated by frontier fence" in message
    ]
    assert len(fence_lines) == 1
    assert f"wallet={checkpoint_wallet_alias(samples._WALLET)} " in fence_lines[0]
    assert f"frontier_generation={rewound.generation} " in fence_lines[0]
    assert f"fence={opening_checkpoint_fence(rewound).isoformat()} " in fence_lines[0]
    assert f"fence_source={opening_checkpoint_fence_source(rewound)} " in fence_lines[0]
    assert "checkpoint_minute=" in fence_lines[0]
    assert fence_lines[0].endswith(f"checkpoint_generation={settled_g1.generation}")
    assert repository.consume_last_pnl_checkpoint_invalidation_deleted() == 0
    assert await repository.get_portfolio_pnl_opening_checkpoint(samples._scope()) is None

    chunk_m4 = _empty_chunk(rewound, samples._M4)
    write_m4 = PnlOpeningCheckpointWrite(
        _certificate_for(advance_correction_frontier(rewound, chunk_m4)),
        '{"g":2}',
    )
    settled_g2 = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(),
        rewound,
        chunk_m4,
        checkpoint=write_m4,
    )
    assert repository.consume_last_pnl_checkpoint_invalidation_deleted() == 0

    forward_g3 = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        _request(start=samples._M4 + timedelta(minutes=1), tip=m6, as_of=as_of_later),
        settled_g2,
    )
    kept = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert kept is not None and kept.certificate == write_m4.certificate
    assert not checkpoint_minute_fenced(
        kept.certificate.checkpoint_minute,
        opening_checkpoint_fence(forward_g3),
    )
    assert repository.consume_last_pnl_checkpoint_invalidation_deleted() == 0

    mid_chunk = _empty_chunk(forward_g3, samples._M5)
    mid_frontier = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(),
        forward_g3,
        mid_chunk,
    )
    assert mid_frontier.next_minute == m6
    assert await repository.get_portfolio_pnl_opening_checkpoint(samples._scope()) is not None

    async with repository.session() as session:
        stored_frontier = (
            await session.execute(select(PortfolioPnlCorrectionFrontier))
        ).scalar_one()
        stored_frontier.window_start = samples._M2
        stored_frontier.repair_from = samples._M2
        stored_frontier.next_minute = m6
        await session.commit()
    patched_frontier = await repository.get_portfolio_pnl_correction_frontier(samples._scope())
    assert patched_frontier is not None
    repair_chunk = PnlCorrectionChunk(
        end=m6,
        edits=(),
        prior_peak=None,
        evidence=PnlCorrectionEvidence(
            patched_frontier.key,
            m6,
            m6,
            (),
            patched_frontier.prior_peak,
        ),
        withheld_minutes=(m6,),
        reconciles_repair=False,
    )
    committed = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(),
        patched_frontier,
        repair_chunk,
    )
    assert committed.repair_from is not None and committed.repair_from <= samples._M4
    assert repository.consume_last_pnl_checkpoint_invalidation_deleted() == 1
    assert await repository.get_portfolio_pnl_opening_checkpoint(samples._scope()) is None


async def test_choke_point_insert_branch_retains_after_c_and_deletes_at_or_before_c(
    repository: SQLAlchemyRepository,
) -> None:
    """Verify the ``actual is None`` insert branch of ``_write_pnl_correction``.

    Given: An opening checkpoint persisted at ``C = M3`` with the frontier row
        removed so the next invalidation takes the insert branch.
    When: Initial invalidation runs first with ``start = C + 1m`` and again after
        resetting with ``start = C``.
    Then: Starting strictly after ``C`` retains the checkpoint, while starting at
        or before ``C`` deletes it on insert.
    """
    _, write_m3 = await _seed_settled_checkpoint(repository, tip=samples._M3)
    async with repository.session() as session:
        await session.execute(delete(PortfolioPnlCorrectionFrontier))
        await session.commit()

    inserted_after = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        _request(start=samples._M3 + timedelta(minutes=1), tip=samples._M5),
        None,
    )
    assert inserted_after.generation == 1
    kept = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert kept is not None and kept.certificate == write_m3.certificate

    async with repository.session() as session:
        await session.execute(delete(PortfolioPnlCorrectionFrontier))
        await session.commit()
    await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        _request(start=samples._M3, tip=samples._M5),
        None,
    )
    assert await repository.get_portfolio_pnl_opening_checkpoint(samples._scope()) is None


def test_ast_guard_forbids_frontier_writes_outside_choke_point() -> None:
    """Inspect ``repository.py`` AST so all frontier writes pass through the choke point.

    Given: The real ``src/snapper/data/repository.py`` module plus synthetic source
        snippets containing planted violations and allowed ``select`` reads.
    When: ``_find_frontier_write_violations`` parses each source.
    Then: ``repository.py`` produces zero violations, ``select`` reads are allowed,
        and every planted constructor, ``update``, and ``delete`` violation is caught.
    """
    repo_source = _REPOSITORY_PATH.read_text(encoding="utf-8")
    assert _find_frontier_write_violations(repo_source) == []

    planted = """
class SQLAlchemyRepository:
    async def _write_pnl_correction(self):
        s.add(PortfolioPnlCorrectionFrontier())
        await s.execute(update(PortfolioPnlCorrectionFrontier))

    async def rogue_writer(self):
        row = PortfolioPnlCorrectionFrontier()
        await s.execute(update(PortfolioPnlCorrectionFrontier))
        await s.execute(sa.delete(PortfolioPnlCorrectionFrontier))
        await s.execute(select(PortfolioPnlCorrectionFrontier))
        return row
"""
    violations = _find_frontier_write_violations(planted)
    assert len(violations) == 3
    assert any("constructor" in item for item in violations)
    assert violations.count(next(item for item in violations if "dml_target" in item)) >= 0
    assert sum("dml_target" in item for item in violations) == 2


async def test_fenced_commit_settled_and_forward_replace_and_cas_loss(
    repository: SQLAlchemyRepository,
) -> None:
    """Commit frontier and checkpoint together, replace forward, and roll back on CAS loss.

    Given: A settled checkpoint at ``M3``, then a forward generation at ``M4..M5``,
        and a simulated CAS failure on ``UPDATE portfolio_pnl_correction_frontiers``.
    When: The forward generation fails CAS first and succeeds after removing the guard.
    Then: The CAS loss preserves both the prior frontier and checkpoint ``M3``, and
        the succeeding forward settle replaces ``M3`` with ``M5``.
    """
    settled_g1, write_g1 = await _seed_settled_checkpoint(repository, tip=samples._M3)
    g2 = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        _request(start=samples._M4, tip=samples._M5, as_of=samples._M5),
        settled_g1,
    )
    chunk_g2 = _empty_chunk(g2, samples._M5)
    after_g2 = advance_correction_frontier(g2, chunk_g2)
    write_g2 = PnlOpeningCheckpointWrite(_certificate_for(after_g2), '{"version":2}')

    async with repository.session() as session:
        await session.execute(
            text(
                "CREATE TRIGGER reject_frontier_cas BEFORE UPDATE ON "
                "portfolio_pnl_correction_frontiers BEGIN SELECT RAISE(IGNORE); END"
            )
        )
        await session.commit()
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="final checkpoint CAS"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(),
            g2,
            chunk_g2,
            checkpoint=write_g2,
        )
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == g2
    prior = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert prior is not None and prior.certificate == write_g1.certificate

    async with repository.session() as session:
        await session.execute(text("DROP TRIGGER reject_frontier_cas"))
        await session.commit()
    settled_g2 = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(),
        g2,
        chunk_g2,
        checkpoint=write_g2,
    )
    assert settled_g2.next_minute is None
    replaced = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert replaced is not None
    assert replaced.certificate == write_g2.certificate
    assert replaced.state_json == '{"version":2}'


async def test_lost_cas_neither_writes_nor_deletes_existing_checkpoint(
    repository: SQLAlchemyRepository,
) -> None:
    """Leave existing checkpoints intact and write nothing when CAS loses after delete intent.

    Given: A persisted opening checkpoint at ``C = M3`` and an active generation whose
        resulting frontier window would satisfy ``checkpoint_minute >= earliest_open``.
    When: Both an application stale-revision rewind/commit AND a SQL-level CAS refusal
        are attempted while staging a replacement checkpoint write.
    Then: Each attempt raises ``PortfolioPnlCorrectionConflictError``, writes no new
        checkpoint, and leaves the seeded ``C = M3`` checkpoint intact.
    """
    m6 = samples._M5 + timedelta(minutes=1)
    as_of_later = m6 + timedelta(minutes=2)
    settled_g1, write_m3 = await _seed_settled_checkpoint(repository, tip=samples._M3)

    async with repository.session() as session:
        stored_frontier = (
            await session.execute(select(PortfolioPnlCorrectionFrontier))
        ).scalar_one()
        stored_frontier.window_start = samples._M2
        stored_frontier.repair_from = samples._M2
        stored_frontier.next_minute = m6
        stored_frontier.tip = m6
        stored_frontier.as_of = as_of_later
        stored_frontier.revision = 2
        await session.commit()

    stale_claim = replace(
        settled_g1,
        revision=1,
        window_start=samples._M2,
        repair_from=samples._M2,
        next_minute=m6,
        tip=m6,
        as_of=as_of_later,
    )
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="checkpoint CAS lost"):
        await repository.invalidate_portfolio_pnl_correction(
            samples._scope(),
            _request(start=samples._M2, tip=m6, as_of=as_of_later),
            stale_claim,
        )
    preserved = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert preserved is not None and preserved.certificate == write_m3.certificate

    stale_chunk = PnlCorrectionChunk(
        end=m6,
        edits=(),
        prior_peak=None,
        evidence=PnlCorrectionEvidence(stale_claim.key, m6, m6, (), stale_claim.prior_peak),
        withheld_minutes=(m6,),
        reconciles_repair=False,
    )
    stale_after = advance_correction_frontier(stale_claim, stale_chunk)
    staged_write = PnlOpeningCheckpointWrite(
        replace(_certificate_for(stale_after), checkpoint_minute=m6),
        '{"should_not_commit":true}',
    )
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="checkpoint CAS lost"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(),
            stale_claim,
            stale_chunk,
            checkpoint=staged_write,
        )
    preserved = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert preserved is not None and preserved.certificate == write_m3.certificate

    current_frontier = await repository.get_portfolio_pnl_correction_frontier(samples._scope())
    assert current_frontier is not None and current_frontier.revision == 2
    async with repository.session() as session:
        await session.execute(
            text(
                "CREATE TRIGGER reject_frontier_cas_before_delete BEFORE UPDATE ON "
                "portfolio_pnl_correction_frontiers BEGIN SELECT RAISE(IGNORE); END"
            )
        )
        await session.commit()
    with pytest.raises(PortfolioPnlCorrectionConflictError, match="final checkpoint CAS"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(),
            current_frontier,
            stale_chunk,
            checkpoint=staged_write,
        )
    preserved = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert preserved is not None and preserved.certificate == write_m3.certificate
    assert preserved.state_json == write_m3.state_json


async def test_fenced_commit_raises_on_every_write_refusal_without_persisting(
    repository: SQLAlchemyRepository,
) -> None:
    """Enforce ``checkpoint_write_refusal`` inside the transaction as defence in depth.

    Given: A started generation with every distinct refusal condition crafted cleanly.
    When: ``commit_portfolio_pnl_correction_chunk`` is called with each invalid write.
    Then: Every call raises ``ValueError`` naming the refusal code and leaves the
        frontier and checkpoint table unchanged.
    """
    settled_g1, _ = await _seed_settled_checkpoint(repository, tip=samples._M2)
    g2 = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        _request(start=samples._M3, tip=samples._M5, as_of=samples._M5),
        settled_g1,
    )
    cases: list[
        tuple[str, PnlCorrectionFrontier, PnlCorrectionChunk, PnlOpeningCheckpointCertificate]
    ] = []

    mid_chunk = _empty_chunk(g2, samples._M4)
    mid_after = advance_correction_frontier(g2, mid_chunk)
    cases.append(("not_settled", g2, mid_chunk, _certificate_for(mid_after)))

    tip_chunk = _empty_chunk(g2, samples._M5)
    tip_after = advance_correction_frontier(g2, tip_chunk)
    cases.append(
        (
            "not_at_tip",
            g2,
            tip_chunk,
            _certificate_for(tip_after, checkpoint_minute=samples._M4),
        )
    )
    cases.append(
        (
            "generation_mismatch",
            g2,
            tip_chunk,
            _certificate_for(tip_after, source_generation=99),
        )
    )
    cases.append(
        (
            "cut_mismatch",
            g2,
            tip_chunk,
            _certificate_for(tip_after, execution_manifest_digest=_DIGEST_B),
        )
    )
    cases.append(
        (
            "key_mismatch",
            g2,
            tip_chunk,
            _certificate_for(tip_after, key=replace(_key(), valuation_ccy="EUR")),
        )
    )

    for reason, expected_f, chunk, cert in cases:
        with pytest.raises(ValueError, match=reason):
            await repository.commit_portfolio_pnl_correction_chunk(
                samples._scope(),
                expected_f,
                chunk,
                checkpoint=PnlOpeningCheckpointWrite(cert, '{"bad":1}'),
            )
        assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == g2
        kept = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
        assert kept is not None and kept.certificate.checkpoint_minute == settled_g1.tip

    alt = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        replace(_request(start=samples._M3, tip=samples._M5), has_fill_gap=True),
        g2,
    )
    alt_chunk = _empty_chunk(alt, samples._M5)
    alt_after = advance_correction_frontier(alt, alt_chunk)
    with pytest.raises(ValueError, match="fill_gap"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(),
            alt,
            alt_chunk,
            checkpoint=PnlOpeningCheckpointWrite(_certificate_for(alt_after), '{"bad":1}'),
        )
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == alt
    kept = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert kept is not None and kept.certificate.checkpoint_minute == settled_g1.tip

    excess = list(range(6, 6 + MAX_DEFERRED_EXECUTIONS + 1))
    wide_watermarks = {"walutomat": 4, "kraken": 5 + MAX_DEFERRED_EXECUTIONS + 1}
    g_many = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        replace(
            _request(start=samples._M3, tip=samples._M5),
            baseline_watermarks=wide_watermarks,
            pending_watermarks=wide_watermarks,
            raw_execution_watermarks=wide_watermarks,
        ),
        alt,
    )
    many_chunk = _empty_chunk(g_many, samples._M5)
    many_after = advance_correction_frontier(g_many, many_chunk)
    too_many = _certificate_for(
        many_after,
        cutoff_execution_watermarks=wide_watermarks,
        cutoff_execution_counts=wide_watermarks,
        deferred_execution_sequences={"kraken": excess},
    )
    with pytest.raises(ValueError, match="deferred_tail_too_long"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(),
            g_many,
            many_chunk,
            checkpoint=PnlOpeningCheckpointWrite(too_many, '{"bad":1}'),
        )
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == g_many
    kept = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert kept is not None and kept.certificate.checkpoint_minute == settled_g1.tip

    async with repository.session() as session:
        await session.execute(delete(PortfolioPnlCorrectionFrontier))
        await session.commit()
    incomplete = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        replace(
            _request(start=samples._M3, tip=samples._M5),
            raw_execution_watermarks=None,
            execution_manifest_digest=None,
            has_fill_gap=None,
        ),
        None,
    )
    inc_chunk = _empty_chunk(incomplete, samples._M5)
    inc_after = advance_correction_frontier(incomplete, inc_chunk)
    with pytest.raises(ValueError, match="frontier_evidence_incomplete"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(),
            incomplete,
            inc_chunk,
            checkpoint=PnlOpeningCheckpointWrite(
                _certificate_for(inc_after, source_generation=incomplete.generation),
                '{"bad":1}',
            ),
        )
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == incomplete
    kept = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert kept is not None and kept.certificate.checkpoint_minute == settled_g1.tip


@pytest.mark.parametrize(
    "source_override",
    [
        {"source_expected_venues": frozenset({"kraken"})},
        {"source_venue_event_watermarks": {"kraken:spot": 9, "walutomat:spot": 3}},
        {"source_fill_witness_digest": _DIGEST_A},
    ],
)
async def test_fenced_commit_raises_on_source_cut_mismatch_and_rolls_back(
    repository: SQLAlchemyRepository,
    source_override: dict[str, object],
) -> None:
    """Reject source-cut mismatches inside the transaction and roll back frontier and checkpoint.

    Given: A settled prior checkpoint at ``M2`` and a started generation at ``M3..M5``
        whose candidate write mutates one source cut field against the frontier.
    When: ``commit_portfolio_pnl_correction_chunk`` attempts to commit the mismatched write.
    Then: It raises ``ValueError`` naming ``cut_mismatch``, leaves the frontier revision
        unchanged, and leaves the prior checkpoint intact.
    """
    settled_g1, write_g1 = await _seed_settled_checkpoint(repository, tip=samples._M2)
    g2 = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        _request(start=samples._M3, tip=samples._M5, as_of=samples._M5),
        settled_g1,
    )
    tip_chunk = _empty_chunk(g2, samples._M5)
    tip_after = advance_correction_frontier(g2, tip_chunk)
    bad_cert = _certificate_for(tip_after, **source_override)
    with pytest.raises(ValueError, match="cut_mismatch"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(),
            g2,
            tip_chunk,
            checkpoint=PnlOpeningCheckpointWrite(bad_cert, '{"bad":1}'),
        )
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == g2
    kept = await repository.get_portfolio_pnl_opening_checkpoint(samples._scope())
    assert kept is not None and kept.certificate == write_g1.certificate
    assert kept.state_json == write_g1.state_json


async def test_257_deferred_tail_refusal_drops_checkpoint_and_still_commits_frontier(
    repository: SQLAlchemyRepository,
) -> None:
    """Follow caller usage pattern: pre-check refuses and ``checkpoint=None`` commits cleanly.

    Given: A chunk planning to settle with 257 deferred execution sequences.
    When: ``checkpoint_write_refusal`` returns ``"deferred_tail_too_long"`` and the
        caller commits with ``checkpoint=None``.
    Then: The frontier settles normally and no checkpoint row is stored.
    """
    watermarks = {"kraken": 5 + MAX_DEFERRED_EXECUTIONS + 1}
    started = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        PnlCorrectionRequest(
            start=samples._M1,
            tip=samples._M3,
            as_of=samples._M5,
            baseline_watermarks=watermarks,
            pending_watermarks=watermarks,
            prior_peak=None,
            raw_execution_watermarks=watermarks,
            execution_manifest_digest=_DIGEST_A,
            has_fill_gap=False,
            expected_venues=frozenset({"kraken"}),
            venue_event_watermarks={"kraken:spot": 9},
            fill_witness_digest=_DIGEST_B,
        ),
        None,
    )
    chunk = _empty_chunk(started, samples._M3)
    planned_after = advance_correction_frontier(started, chunk)
    deferred = list(range(6, 6 + MAX_DEFERRED_EXECUTIONS + 1))
    cert = _certificate_for(
        planned_after,
        cutoff_execution_watermarks=watermarks,
        execution_chain_digests={"kraken": _DIGEST_A},
        cutoff_execution_counts=watermarks,
        applied_watermarks_by_exchange={"kraken": 5},
        deferred_execution_sequences={"kraken": deferred},
        prefix_venue_event_watermarks={"kraken:spot": 9},
        source_expected_venues=frozenset({"kraken"}),
        source_venue_event_watermarks={"kraken:spot": 9},
    )
    assert checkpoint_write_refusal(planned_after, chunk, cert) == "deferred_tail_too_long"
    settled = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(),
        started,
        chunk,
        checkpoint=None,
    )
    assert settled.next_minute is None
    assert await repository.get_portfolio_pnl_opening_checkpoint(samples._scope()) is None


async def test_checkpoint_upsert_failure_rolls_back_savepoint_only_and_commits_samples(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Roll back only the SAVEPOINT when checkpoint upsert fails so samples and frontier commit.

    Given: A settling generation with one edited sample row and a staged checkpoint write
        whose ``_upsert_pnl_opening_checkpoint`` raises ``IntegrityError`` inside ``begin_nested()``.
    When: ``commit_portfolio_pnl_correction_chunk`` commits the chunk.
    Then: The sample edit and advanced frontier commit, no checkpoint row is stored,
        and ``consume_last_pnl_checkpoint_write_refusal()`` returns ``"capture_upsert_failed"``.

    Args:
        repository: Isolated file-backed SQLite repository fixture.
        monkeypatch: Pytest monkeypatch fixture.
    """
    started = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        _request(start=samples._M1, tip=samples._M2, as_of=samples._M5),
        None,
    )
    row = samples._complete_sample(samples._M1, timestamp=samples._M5)
    chunk = replace(
        _empty_chunk(started, samples._M2),
        edits=(PnlCorrectionEdit(samples._M1, None, row),),
        prior_peak=1250.0,
    )
    planned_after = advance_correction_frontier(started, chunk)
    write = PnlOpeningCheckpointWrite(_certificate_for(planned_after), '{"state":1}')

    async def boom(s: AsyncSession, checkpoint: PnlOpeningCheckpointWrite) -> None:
        """Fail with a DB IntegrityError inside the SAVEPOINT after sample edits flush."""
        del s, checkpoint
        raise IntegrityError("INSERT", {}, Exception("synthetic constraint failure"))

    monkeypatch.setattr(repository, "_upsert_pnl_opening_checkpoint", boom)
    committed = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(),
        started,
        chunk,
        checkpoint=write,
    )
    assert committed == planned_after
    assert repository.consume_last_pnl_checkpoint_write_refusal() == "capture_upsert_failed"
    assert await repository.get_portfolio_pnl_opening_checkpoint(samples._scope()) is None
    observed = await repository.get_portfolio_pnl_correction_evidence(
        samples._scope(),
        samples._M1,
        samples._M2,
    )
    assert len(observed.versions) == 1
    assert observed.versions[0].row["realized_pnl"] == pytest.approx(12.5)


async def test_checkpoint_upsert_programming_error_propagates_and_rolls_back_chunk(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate non-SQLAlchemy programming errors instead of committing without checkpoint.

    Given: A settling generation with one edited sample row and a staged checkpoint write
        whose ``_upsert_pnl_opening_checkpoint`` raises ``RuntimeError`` inside ``begin_nested()``.
    When: ``commit_portfolio_pnl_correction_chunk`` commits the chunk.
    Then: The ``RuntimeError`` propagates and rolls back the outer sample and frontier writes.

    Args:
        repository: Isolated file-backed SQLite repository fixture.
        monkeypatch: Pytest monkeypatch fixture.
    """
    started = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        _request(start=samples._M1, tip=samples._M2, as_of=samples._M5),
        None,
    )
    row = samples._complete_sample(samples._M1, timestamp=samples._M5)
    chunk = replace(
        _empty_chunk(started, samples._M2),
        edits=(PnlCorrectionEdit(samples._M1, None, row),),
        prior_peak=1250.0,
    )
    planned_after = advance_correction_frontier(started, chunk)
    write = PnlOpeningCheckpointWrite(_certificate_for(planned_after), '{"state":1}')

    async def bug(s: AsyncSession, checkpoint: PnlOpeningCheckpointWrite) -> None:
        """Simulate a programming error inside the SAVEPOINT."""
        del s, checkpoint
        raise RuntimeError("synthetic programming bug")

    monkeypatch.setattr(repository, "_upsert_pnl_opening_checkpoint", bug)
    with pytest.raises(RuntimeError, match="synthetic programming bug"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(),
            started,
            chunk,
            checkpoint=write,
        )
    observed = await repository.get_portfolio_pnl_correction_evidence(
        samples._scope(),
        samples._M1,
        samples._M2,
    )
    assert observed.versions == ()
    assert await repository.get_portfolio_pnl_correction_frontier(samples._scope()) == started


async def test_checkpoint_upsert_outer_abort_propagates_and_rolls_back_chunk(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate an outer-transaction abort during checkpoint upsert so callers retry cleanly.

    Given: A settling generation with one edited sample row and a staged checkpoint write
        whose ``_upsert_pnl_opening_checkpoint`` aborts the outer session before raising
        ``OperationalError``.
    When: ``commit_portfolio_pnl_correction_chunk`` attempts to commit with the checkpoint.
    Then: ``OperationalError`` propagates instead of committing an empty session, the outer
        transaction rolls back cleanly, and a subsequent retry without checkpoint succeeds.

    Args:
        repository: Isolated file-backed SQLite repository fixture.
        monkeypatch: Pytest monkeypatch fixture.
    """
    started = await repository.invalidate_portfolio_pnl_correction(
        samples._scope(),
        _request(start=samples._M1, tip=samples._M2, as_of=samples._M5),
        None,
    )
    row = samples._complete_sample(samples._M1, timestamp=samples._M5)
    chunk = replace(
        _empty_chunk(started, samples._M2),
        edits=(PnlCorrectionEdit(samples._M1, None, row),),
        prior_peak=1250.0,
    )
    planned_after = advance_correction_frontier(started, chunk)
    write = PnlOpeningCheckpointWrite(_certificate_for(planned_after), '{"state":1}')

    async def abort_outer(s: AsyncSession, checkpoint: PnlOpeningCheckpointWrite) -> None:
        """Abort the outer transaction before raising a SQLAlchemy error."""
        del checkpoint
        await s.rollback()
        raise OperationalError("INSERT", {}, Exception("outer transaction aborted"))

    monkeypatch.setattr(repository, "_upsert_pnl_opening_checkpoint", abort_outer)
    with pytest.raises(OperationalError, match="outer transaction aborted"):
        await repository.commit_portfolio_pnl_correction_chunk(
            samples._scope(),
            started,
            chunk,
            checkpoint=write,
        )
    observed = await repository.get_portfolio_pnl_correction_evidence(
        samples._scope(),
        samples._M1,
        samples._M2,
    )
    assert observed.versions == ()
    committed = await repository.commit_portfolio_pnl_correction_chunk(
        samples._scope(),
        started,
        chunk,
        checkpoint=None,
    )
    assert committed == planned_after


async def test_accrual_prefix_fingerprint_matches_python_across_lifecycle_and_overflow(
    repository: SQLAlchemyRepository,
) -> None:
    """Verify accrual aggregate against Python across retirement, boundaries, and overflow.

    Given: Accruals inside ``(M1 .. M3]``, retired/replaced rows, future/expired
        ``as_of`` rows, boundary ``accrued_at == after_exclusive``, and other scopes.
    When: ``get_pnl_accrual_prefix_fingerprint`` runs on SQLite.
    Then: Empty reads return zero, active reads match Python recomputation exactly,
        and a SQLite sum beyond ``2**63`` fails closed with ``DBAPIError``.
    """
    empty = await repository.get_pnl_accrual_prefix_fingerprint(
        samples._WALLET,
        "live",
        samples._M1,
        samples._M3,
        samples._M5,
    )
    assert empty == IdAggregate(0, 0, 0, None)

    t_retire = samples._M1 + timedelta(seconds=30)
    t_m2_a = samples._M2 + timedelta(seconds=10)
    t_m2_b = samples._M2 + timedelta(seconds=20)
    rows = [
        _accrual_orm(samples._M2, 10, timestamp=samples._M1, known_to=t_retire),
        _accrual_orm(samples._M2, 25, timestamp=t_retire, known_to=KNOWN_TO_MAX),
        _accrual_orm(samples._M3, 30, timestamp=samples._M3, known_to=KNOWN_TO_MAX),
        _accrual_orm(samples._M1, 40, timestamp=samples._M1, known_to=KNOWN_TO_MAX),
        _accrual_orm(samples._M4, 50, timestamp=samples._M1, known_to=KNOWN_TO_MAX),
        _accrual_orm(
            samples._M2,
            60,
            scope=(samples._OTHER_WALLET, "live"),
            timestamp=samples._M1,
        ),
        _accrual_orm(
            samples._M2,
            70,
            scope=(samples._WALLET, "paper"),
            timestamp=samples._M1,
        ),
        _accrual_orm(
            t_m2_a,
            80,
            timestamp=samples._M5 + timedelta(seconds=1),
            known_to=KNOWN_TO_MAX,
        ),
        _accrual_orm(t_m2_b, 90, timestamp=samples._M1, known_to=samples._M5),
    ]
    async with repository.session() as session:
        session.add_all(rows)
        await session.commit()

    active_before = await repository.get_pnl_accrual_prefix_fingerprint(
        samples._WALLET,
        "live",
        samples._M1,
        samples._M3,
        samples._M1 + timedelta(seconds=10),
    )
    assert active_before == _python_id_aggregate([(10, samples._M1), (90, samples._M1)])

    active_after = await repository.get_pnl_accrual_prefix_fingerprint(
        samples._WALLET,
        "live",
        samples._M1,
        samples._M3,
        samples._M5,
    )
    assert active_after == _python_id_aggregate([(25, t_retire), (30, samples._M3)])

    async with repository.session() as session:
        session.add_all(
            [
                _accrual_orm(
                    samples._M2 + timedelta(seconds=30),
                    (1 << 62) + 1,
                    timestamp=samples._M1,
                ),
                _accrual_orm(
                    samples._M2 + timedelta(seconds=40),
                    (1 << 62) + 2,
                    timestamp=samples._M1,
                ),
            ]
        )
        await session.commit()
    with pytest.raises(DBAPIError):
        await repository.get_pnl_accrual_prefix_fingerprint(
            samples._WALLET,
            "live",
            samples._M1,
            samples._M3,
            samples._M5,
        )


def test_combine_id_aggregates_covers_empty_and_non_empty_inputs() -> None:
    """Combine disjoint ``IdAggregate`` values identically across empty and populated sides.

    Given: Empty, left-populated, right-populated, and both-populated aggregates
        with large integer sums.
    When: ``_combine_id_aggregates`` reduces pairs in both orders.
    Then: Identities and commutative ``(count, id_sum, max_id, max_timestamp)`` merges
        preserve exact Python int arithmetic.
    """
    empty = IdAggregate(0, 0, 0, None)
    left = IdAggregate(2, (1 << 63) + 5, (1 << 62) + 3, samples._M2)
    right = IdAggregate(3, (1 << 63) + 27, (1 << 62) + 9, samples._M3)
    later = IdAggregate(1, 200, 200, samples._M5)
    assert _combine_id_aggregates(empty, empty) == empty
    assert _combine_id_aggregates(empty, right) == right
    assert _combine_id_aggregates(left, empty) == left
    assert _combine_id_aggregates(left, right) == IdAggregate(
        5, (1 << 64) + 32, (1 << 62) + 9, samples._M3
    )
    assert _combine_id_aggregates(right, later) == IdAggregate(
        4, (1 << 63) + 227, (1 << 62) + 9, samples._M5
    )


async def test_event_fx_candle_fingerprint_empty_inputs_skip_query(
    repository: SQLAlchemyRepository,
) -> None:
    """Short-circuit empty planes or empty opens without opening a database session.

    Given: A SQLite repository whose ``session`` factory is spied upon.
    When: ``get_pnl_event_fx_candle_fingerprint`` receives ``planes=[]`` or ``open_ats=[]``.
    Then: Both return the zero ``IdAggregate`` without calling ``repository.session``.
    """
    with patch.object(repository, "session") as session_mock:
        assert await repository.get_pnl_event_fx_candle_fingerprint(
            [], [samples._M1], samples._M5
        ) == IdAggregate(0, 0, 0, None)
        assert await repository.get_pnl_event_fx_candle_fingerprint(
            [("EUR", "USD", "kraken")], [], samples._M5
        ) == IdAggregate(0, 0, 0, None)
        session_mock.assert_not_called()


async def test_event_fx_candle_fingerprint_matches_replay_across_proof_retire_and_variants(
    repository: SQLAlchemyRepository,
) -> None:
    """Match ``get_pnl_fx_rate_candles_at`` across proofs, retirement, multiplicity, and variants.

    Given: Proven forex listings (including multiplicity on ``("EUR", "USD", "kraken")``),
        failed-proof crypto and denomination-conflicted instruments, retired and replaced
        candles, non-1m and incomplete candles, and future-horizon candles.
    When: Differential checks compare fingerprint and replay rows at ``M2`` and ``M5``.
    Then: Failed proofs, retired rows, incomplete rows, non-1m rows, and future rows are
        excluded identically while active rows match with exact multiplicity and sum changes.
    """
    t_base = samples._M1
    t_retire = samples._M2 + timedelta(seconds=15)
    t_retire_only = samples._M4
    seed_rows: list[object] = [
        _fx_symbol("sym-eu-1", "EUR-USD", "EUR", "USD", timestamp=t_base),
        _fx_instrument("ins-eu-1", "sym-eu-1", "kraken", timestamp=t_base),
        _fx_symbol("sym-eu-2", "EURUSD-ALT", "EUR", "USD", timestamp=t_base),
        _fx_instrument("ins-eu-2", "sym-eu-2", "kraken", timestamp=t_base),
        _fx_symbol("sym-ue", "USD-EUR", "USD", "EUR", timestamp=t_base),
        _fx_instrument("ins-ue", "sym-ue", "walutomat", timestamp=t_base),
        _fx_symbol("sym-eu-crypto", "EUR-USD-PERP", "EUR", "USD", "crypto", timestamp=t_base),
        _fx_instrument("ins-eu-crypto", "sym-eu-crypto", "kraken", timestamp=t_base),
        _fx_symbol(
            "sym-gbp-bad", "GBP-BAD-A", "GBP", "PLN", known_to=t_base + timedelta(seconds=5)
        ),
        _fx_symbol(
            "sym-gbp-bad", "GBP-BAD-B", "GBP", "USD", timestamp=t_base + timedelta(seconds=5)
        ),
        _fx_instrument("ins-gbp-bad", "sym-gbp-bad", "kraken", timestamp=t_base),
        _fx_candle_seeded("ins-eu-1", samples._M1, 101, window=(t_base, t_retire)),
        _fx_candle_seeded("ins-eu-1", samples._M1, 151, window=(t_retire, KNOWN_TO_MAX)),
        _fx_candle_seeded("ins-eu-2", samples._M1, 175, window=(t_base, KNOWN_TO_MAX)),
        _fx_candle_seeded("ins-eu-1", samples._M2, 200, window=(t_base, t_retire_only)),
        _fx_candle_seeded("ins-ue", samples._M2, 250, window=(samples._M2, KNOWN_TO_MAX)),
        _fx_candle_seeded(
            "ins-eu-1", samples._M1, 300, window=(t_base, KNOWN_TO_MAX), variant=("5m", True)
        ),
        _fx_candle_seeded(
            "ins-eu-1", samples._M3, 310, window=(t_base, KNOWN_TO_MAX), variant=("1m", False)
        ),
        _fx_candle_seeded("ins-eu-crypto", samples._M1, 400, window=(t_base, KNOWN_TO_MAX)),
        _fx_candle_seeded("ins-gbp-bad", samples._M1, 410, window=(t_base, KNOWN_TO_MAX)),
        _fx_candle_seeded(
            "ins-eu-1",
            samples._M4,
            500,
            window=(samples._M5 + timedelta(seconds=1), KNOWN_TO_MAX),
        ),
    ]
    async with repository.session() as session:
        session.add_all(seed_rows)
        await session.commit()

    planes: list[PnlFxRatePlane] = [
        ("EUR", "USD", "kraken"),
        ("GBP", "USD", "kraken"),
        ("USD", "EUR", "walutomat"),
    ]
    opens = [samples._M1, samples._M2, samples._M3, samples._M4]

    before_fp, before_rows = await _assert_fx_fingerprint_matches_replay(
        repository, planes, opens, samples._M2
    )
    assert [row["candle_id"] for row in before_rows] == [101, 175, 200, 250]
    assert before_fp.id_sum == 101 + 175 + 200 + 250

    after_fp, after_rows = await _assert_fx_fingerprint_matches_replay(
        repository, planes, opens, samples._M5
    )
    assert [row["candle_id"] for row in after_rows] == [151, 175, 250]
    assert after_fp.id_sum == 151 + 175 + 250


async def test_event_fx_candle_fingerprint_tracks_symbol_and_instrument_version_visibility(
    repository: SQLAlchemyRepository,
) -> None:
    """Reflect ``Symbol`` and ``Instrument`` metadata version changes identically to replay.

    Given: A symbol transitioning ``crypto`` to ``forex``, a symbol introducing a later
        denomination conflict, and an instrument moving from ``polygon`` to ``kraken``.
    When: Differential checks query before and after each metadata version timestamp.
    Then: Every horizon sees the exact same row ids in fingerprint and replay reads.
    """
    t1 = samples._M1
    t2 = samples._M2
    t3 = samples._M3
    t4 = samples._M4
    t_conflict = samples._M5
    inst_old = _fx_instrument("ins-move", "sym-move", "polygon", timestamp=t1)
    inst_old.known_to = t3
    inst_new = _fx_instrument("ins-move", "sym-move", "kraken", timestamp=t3)
    async with repository.session() as session:
        session.add_all(
            [
                _fx_symbol(
                    "sym-cad", "CAD-USD-CRYPTO", "CAD", "USD", "crypto", timestamp=t1, known_to=t3
                ),
                _fx_symbol("sym-cad", "CAD-USD", "CAD", "USD", "forex", timestamp=t3),
                _fx_instrument("ins-cad", "sym-cad", "kraken", timestamp=t1),
                _fx_candle_seeded("ins-cad", t1, 600, window=(t1, KNOWN_TO_MAX)),
                _fx_symbol(
                    "sym-nzd", "NZD-USD", "NZD", "USD", "forex", timestamp=t1, known_to=t_conflict
                ),
                _fx_symbol("sym-nzd", "NZD-PLN", "NZD", "PLN", "forex", timestamp=t_conflict),
                _fx_instrument("ins-nzd", "sym-nzd", "kraken", timestamp=t1),
                _fx_candle_seeded("ins-nzd", t1, 610, window=(t1, KNOWN_TO_MAX)),
                _fx_symbol("sym-move", "AUD-USD", "AUD", "USD", "forex", timestamp=t1),
                inst_old,
                inst_new,
                _fx_candle_seeded("ins-move", t1, 700, window=(t2, KNOWN_TO_MAX)),
                _fx_candle_seeded("ins-move", t2, 710, window=(t4, KNOWN_TO_MAX)),
            ]
        )
        await session.commit()

    opens = [t1, t2]
    cad_plane: list[PnlFxRatePlane] = [("CAD", "USD", "kraken")]
    _, cad_before = await _assert_fx_fingerprint_matches_replay(repository, cad_plane, opens, t2)
    assert cad_before == []
    _, cad_after = await _assert_fx_fingerprint_matches_replay(repository, cad_plane, opens, t3)
    assert [row["candle_id"] for row in cad_after] == [600]

    nzd_plane: list[PnlFxRatePlane] = [("NZD", "USD", "kraken")]
    _, nzd_before = await _assert_fx_fingerprint_matches_replay(repository, nzd_plane, opens, t4)
    assert [row["candle_id"] for row in nzd_before] == [610]
    _, nzd_after = await _assert_fx_fingerprint_matches_replay(
        repository, nzd_plane, opens, t_conflict
    )
    assert nzd_after == []

    aud_planes: list[PnlFxRatePlane] = [("AUD", "USD", "polygon"), ("AUD", "USD", "kraken")]
    _, aud_before = await _assert_fx_fingerprint_matches_replay(repository, aud_planes, opens, t3)
    assert [row["candle_id"] for row in aud_before] == [700]
    _, aud_after = await _assert_fx_fingerprint_matches_replay(repository, aud_planes, opens, t4)
    assert [row["candle_id"] for row in aud_after] == [700, 710]


async def test_event_fx_candle_fingerprint_matches_replay_across_batches_and_overflow(
    repository: SQLAlchemyRepository,
) -> None:
    """Combine >400 open-minute batches matching replay and fail closed on SQLite overflow.

    Given: 450 proven FX candles queried alongside absent opens spanning three batches,
        plus two large-id candles whose sum exceeds signed 64-bit integers.
    When: ``get_pnl_event_fx_candle_fingerprint`` runs on SQLite.
    Then: The batched fingerprint matches ``get_pnl_fx_rate_candles_at`` across all 450
        candles, and a batch sum beyond ``2**63 - 1`` fails closed with ``DBAPIError``.
    """
    batch_base = samples._M5 + timedelta(hours=1)
    many_opens = [batch_base + timedelta(minutes=offset) for offset in range(450)]
    batch_rows: list[object] = [
        _fx_symbol("sym-batch", "EUR-USD", "EUR", "USD", timestamp=samples._M1),
        _fx_instrument("ins-batch", "sym-batch", "kraken", timestamp=samples._M1),
        *[
            _fx_candle_seeded(
                "ins-batch",
                open_at,
                10_000 + offset,
                window=(samples._M1 + timedelta(milliseconds=offset), KNOWN_TO_MAX),
            )
            for offset, open_at in enumerate(many_opens)
        ],
    ]
    async with repository.session() as session:
        session.add_all(batch_rows)
        await session.commit()

    many_opens_with_empty = [
        *many_opens,
        *[batch_base + timedelta(minutes=offset) for offset in range(500, 900)],
    ]
    combined, replay_rows = await _assert_fx_fingerprint_matches_replay(
        repository, [("EUR", "USD", "kraken")], many_opens_with_empty, samples._M5
    )
    assert combined.count == len(replay_rows) == 450

    async with repository.session() as session:
        session.add_all(
            [
                _fx_symbol("sym-ovf", "CHF-USD", "CHF", "USD", timestamp=samples._M1),
                _fx_instrument("ins-ovf", "sym-ovf", "kraken", timestamp=samples._M1),
                _fx_candle_seeded("ins-ovf", samples._M1, (1 << 62) + 1),
                _fx_candle_seeded("ins-ovf", samples._M2, (1 << 62) + 2),
            ]
        )
        await session.commit()
    with pytest.raises(DBAPIError):
        await repository.get_pnl_event_fx_candle_fingerprint(
            [("CHF", "USD", "kraken")],
            [samples._M1, samples._M2],
            samples._M5,
        )


async def test_execution_prefix_counts_match_the_replay_prefix_rows(
    repository: SQLAlchemyRepository,
) -> None:
    """Count exactly the rows the watermarked replay prefix folds, per exchange.

    Given: Empty maps, zero watermarks, executions across exchanges, rows beyond
        ``W_C[ex]``, rows in a foreign wallet or mode, a row with a future bus
        timestamp, and a closed in-range row.
    When: ``get_pnl_execution_prefix_counts`` is queried.
    Then: Each exchange counts every in-range row of the wallet and mode, including
        the future-stamped and the closed one, because the replay read applies no
        knowledge horizon and no ``known_to`` filter to executions.
    """
    assert await repository.get_pnl_execution_prefix_counts(samples._WALLET, "live", {}) == {}
    assert await repository.get_pnl_execution_prefix_counts(
        samples._WALLET, "live", {"walutomat": 0}
    ) == {"walutomat": (0, 0)}
    assert await repository.get_pnl_execution_prefix_counts(
        samples._WALLET, "live", {"kraken": 3, "walutomat": 0}
    ) == {"kraken": (0, 0), "walutomat": (0, 0)}

    async with repository.session() as session:
        session.add_all(
            [
                _execution_orm("kraken", 1, timestamp=samples._M1),
                _execution_orm("kraken", 2, timestamp=samples._M2),
                _execution_orm("kraken", 3, timestamp=samples._M5 + timedelta(seconds=1)),
                _execution_orm("kraken", 4, timestamp=samples._M2, known_to=samples._M5),
                _execution_orm("kraken", 5, timestamp=samples._M1),
                _execution_orm("walutomat", 1, timestamp=samples._M1),
                _execution_orm(
                    "kraken",
                    1,
                    scope=(samples._OTHER_WALLET, "live"),
                    timestamp=samples._M1,
                ),
                _execution_orm(
                    "kraken",
                    1,
                    scope=(samples._WALLET, "paper"),
                    timestamp=samples._M1,
                ),
            ]
        )
        await session.commit()

    counts = await repository.get_pnl_execution_prefix_counts(
        samples._WALLET,
        "live",
        {"kraken": 4, "walutomat": 1, "coinbase": 0},
    )
    assert counts == {
        "coinbase": (0, 0),
        "kraken": (4, 4),
        "walutomat": (1, 1),
    }


def _rows_id_aggregates_by_plane(rows: Sequence[PnlFxRateRow]) -> dict[PnlFxRatePlane, IdAggregate]:
    """Group FX replay rows by ``(base, quote, exchange)`` and aggregate each plane.

    Args:
        rows: Rows returned by ``get_pnl_fx_rate_candles_at``.

    Returns:
        Expected per-plane ``IdAggregate`` map, omitting empty planes.
    """
    grouped: dict[PnlFxRatePlane, list[tuple[int, datetime]]] = {}
    for row in rows:
        plane: PnlFxRatePlane = (row["base"], row["quote"], row["exchange"])
        grouped.setdefault(plane, []).append((row["candle_id"], row["candle_timestamp"]))
    return {plane: _python_id_aggregate(items) for plane, items in grouped.items()}


def _combine_all_aggregates(aggregates: Sequence[IdAggregate]) -> IdAggregate:
    """Reduce a sequence of disjoint ``IdAggregate`` instances via ``_combine_id_aggregates``.

    Args:
        aggregates: Per-plane or per-batch aggregates to fold.

    Returns:
        Combined ``IdAggregate``, or the zero aggregate when ``aggregates`` is empty.
    """
    total = IdAggregate(0, 0, 0, None)
    for item in aggregates:
        total = _combine_id_aggregates(total, item)
    return total


async def _assert_fx_fingerprints_by_plane_match_replay(
    repository: SQLAlchemyRepository,
    planes: Sequence[PnlFxRatePlane],
    opens: Sequence[datetime],
    as_of: datetime,
) -> tuple[dict[PnlFxRatePlane, IdAggregate], list[PnlFxRateRow]]:
    """Assert per-plane FX aggregates match grouped replay and combine to the pooled total.

    Args:
        repository: Repository under test.
        planes: Pinned FX planes.
        opens: Target 1-minute candle open timestamps.
        as_of: Knowledge horizon.

    Returns:
        Tuple of the per-plane map and underlying replay rows.
    """
    rows = await repository.get_pnl_fx_rate_candles_at(planes, opens, as_of)
    expected = _rows_id_aggregates_by_plane(rows)
    actual = await repository.get_pnl_event_fx_candle_fingerprints_by_plane(planes, opens, as_of)
    assert actual == expected
    pooled = await repository.get_pnl_event_fx_candle_fingerprint(planes, opens, as_of)
    assert _combine_all_aggregates(list(actual.values())) == pooled
    return actual, rows


async def test_event_fx_candle_fingerprints_by_plane_empty_inputs_skip_query(
    repository: SQLAlchemyRepository,
) -> None:
    """Short-circuit empty planes or opens to ``{}`` without opening a session.

    Given: A SQLite repository whose ``session`` factory is spied upon.
    When: ``get_pnl_event_fx_candle_fingerprints_by_plane`` receives empty ``planes``
        or empty ``open_ats``.
    Then: Both return ``{}`` without invoking ``repository.session``.
    """
    with patch.object(repository, "session") as session_mock:
        assert (
            await repository.get_pnl_event_fx_candle_fingerprints_by_plane(
                [], [samples._M1], samples._M5
            )
            == {}
        )
        assert (
            await repository.get_pnl_event_fx_candle_fingerprints_by_plane(
                [("EUR", "USD", "kraken")], [], samples._M5
            )
            == {}
        )
        session_mock.assert_not_called()


async def test_event_fx_candle_fingerprints_by_plane_matches_grouped_replay_and_total(
    repository: SQLAlchemyRepository,
) -> None:
    """Match grouped ``get_pnl_fx_rate_candles_at`` and omit empty planes across proofs.

    Given: Multiplicity on ``("EUR", "USD", "kraken")``, a second populated plane
        ``("USD", "EUR", "walutomat")``, an empty plane ``("GBP", "USD", "kraken")``,
        retired/replaced candles, non-1m/incomplete candles, and future-horizon rows.
    When: ``get_pnl_event_fx_candle_fingerprints_by_plane`` queries at ``M2`` and ``M5``.
    Then: Each returned plane matches grouped replay rows, empty planes are absent from
        the map, and ``_combine_id_aggregates`` over map values equals the pooled total.
    """
    t_base = samples._M1
    t_retire = samples._M2 + timedelta(seconds=15)
    t_retire_only = samples._M4
    seed_rows: list[object] = [
        _fx_symbol("sym-eu-1", "EUR-USD", "EUR", "USD", timestamp=t_base),
        _fx_instrument("ins-eu-1", "sym-eu-1", "kraken", timestamp=t_base),
        _fx_symbol("sym-eu-2", "EURUSD-ALT", "EUR", "USD", timestamp=t_base),
        _fx_instrument("ins-eu-2", "sym-eu-2", "kraken", timestamp=t_base),
        _fx_symbol("sym-ue", "USD-EUR", "USD", "EUR", timestamp=t_base),
        _fx_instrument("ins-ue", "sym-ue", "walutomat", timestamp=t_base),
        _fx_symbol("sym-gbp-empty", "GBP-USD", "GBP", "USD", timestamp=t_base),
        _fx_instrument("ins-gbp-empty", "sym-gbp-empty", "kraken", timestamp=t_base),
        _fx_candle_seeded("ins-eu-1", samples._M1, 101, window=(t_base, t_retire)),
        _fx_candle_seeded("ins-eu-1", samples._M1, 151, window=(t_retire, KNOWN_TO_MAX)),
        _fx_candle_seeded("ins-eu-2", samples._M1, 175, window=(t_base, KNOWN_TO_MAX)),
        _fx_candle_seeded("ins-eu-1", samples._M2, 200, window=(t_base, t_retire_only)),
        _fx_candle_seeded("ins-ue", samples._M2, 250, window=(samples._M2, KNOWN_TO_MAX)),
        _fx_candle_seeded(
            "ins-eu-1", samples._M1, 300, window=(t_base, KNOWN_TO_MAX), variant=("5m", True)
        ),
        _fx_candle_seeded(
            "ins-eu-1", samples._M3, 310, window=(t_base, KNOWN_TO_MAX), variant=("1m", False)
        ),
        _fx_candle_seeded(
            "ins-eu-1",
            samples._M4,
            500,
            window=(samples._M5 + timedelta(seconds=1), KNOWN_TO_MAX),
        ),
    ]
    async with repository.session() as session:
        session.add_all(seed_rows)
        await session.commit()

    planes: list[PnlFxRatePlane] = [
        ("EUR", "USD", "kraken"),
        ("GBP", "USD", "kraken"),
        ("USD", "EUR", "walutomat"),
    ]
    opens = [samples._M1, samples._M2, samples._M3, samples._M4]

    before_map, _ = await _assert_fx_fingerprints_by_plane_match_replay(
        repository, planes, opens, samples._M2
    )
    assert ("GBP", "USD", "kraken") not in before_map
    assert set(before_map) == {("EUR", "USD", "kraken"), ("USD", "EUR", "walutomat")}

    after_map, _ = await _assert_fx_fingerprints_by_plane_match_replay(
        repository, planes, opens, samples._M5
    )
    assert ("GBP", "USD", "kraken") not in after_map
    assert after_map[("EUR", "USD", "kraken")].id_sum == 151 + 175
    assert after_map[("USD", "EUR", "walutomat")].id_sum == 250


async def test_event_fx_candle_fingerprints_by_plane_detects_symbol_remap_with_equal_pool(
    repository: SQLAlchemyRepository,
) -> None:
    """Detect a candle remapped between two consumed planes while the pooled total is unchanged.

    Given: Two consumed planes ``("EUR", "USD", "kraken")`` and
        ``("GBP", "USD", "kraken")`` and three active candles, one of which moves
        from the EUR symbol to the GBP symbol via an SCD2 symbol-mapping revision.
    When: Per-plane and pooled fingerprints are queried before and after the remap.
    Then: The pooled ``get_pnl_event_fx_candle_fingerprint`` stays equal while
        ``get_pnl_event_fx_candle_fingerprints_by_plane`` shifts the candle between planes.
    """
    t1 = samples._M1
    t2 = samples._M2
    t3 = samples._M3
    remap_inst_v1 = _fx_instrument("ins-remap", "sym-eu-remap", "kraken", timestamp=t1)
    async with repository.session() as session:
        session.add_all(
            [
                _fx_symbol("sym-eu-keep", "EUR-USD", "EUR", "USD", timestamp=t1),
                _fx_symbol("sym-gbp-keep", "GBP-USD", "GBP", "USD", timestamp=t1),
                _fx_symbol("sym-eu-remap", "EUR-USD-ALT", "EUR", "USD", timestamp=t1),
                _fx_symbol("sym-gbp-remap", "GBP-USD-ALT", "GBP", "USD", timestamp=t1),
                _fx_instrument("ins-eur-keep", "sym-eu-keep", "kraken", timestamp=t1),
                _fx_instrument("ins-gbp-keep", "sym-gbp-keep", "kraken", timestamp=t1),
                remap_inst_v1,
                _fx_candle_seeded("ins-eur-keep", samples._M1, 799, window=(t2, KNOWN_TO_MAX)),
                _fx_candle_seeded("ins-remap", samples._M2, 800, window=(t2, KNOWN_TO_MAX)),
                _fx_candle_seeded("ins-gbp-keep", samples._M2, 801, window=(t2, KNOWN_TO_MAX)),
            ]
        )
        await session.commit()

    planes: list[PnlFxRatePlane] = [("EUR", "USD", "kraken"), ("GBP", "USD", "kraken")]
    opens = [samples._M1, samples._M2]
    before_pooled = await repository.get_pnl_event_fx_candle_fingerprint(planes, opens, t3)
    before_by_plane, _ = await _assert_fx_fingerprints_by_plane_match_replay(
        repository, planes, opens, t3
    )

    async with repository.session() as session:
        stored_inst = (
            await session.execute(
                select(Instrument).where(
                    Instrument.public_id == "ins-remap",
                    Instrument.known_to == KNOWN_TO_MAX,
                )
            )
        ).scalar_one()
        stored_inst.known_to = t2
        remap_inst_v2 = _fx_instrument("ins-remap", "sym-gbp-remap", "kraken", timestamp=t2)
        remap_inst_v2.sequence_id = 2
        session.add(remap_inst_v2)
        await session.commit()

    after_pooled = await repository.get_pnl_event_fx_candle_fingerprint(planes, opens, t3)
    after_by_plane, _ = await _assert_fx_fingerprints_by_plane_match_replay(
        repository, planes, opens, t3
    )
    assert after_pooled == before_pooled == IdAggregate(3, 799 + 800 + 801, 801, t2)
    assert after_by_plane != before_by_plane
    assert before_by_plane[("EUR", "USD", "kraken")].id_sum == 799 + 800
    assert before_by_plane[("GBP", "USD", "kraken")].id_sum == 801
    assert after_by_plane[("EUR", "USD", "kraken")].id_sum == 799
    assert after_by_plane[("GBP", "USD", "kraken")].id_sum == 800 + 801


async def test_event_fx_candle_fingerprints_by_plane_combines_batches_and_postgresql_jit(
    repository: SQLAlchemyRepository,
) -> None:
    """Combine >400 open-minute batches per plane and issue PostgreSQL ``SET LOCAL jit = off``.

    Given: 450 opens across two FX planes plus 400 empty opens spanning three
        ``_PNL_FX_MINUTE_BATCH`` batches, and a mock PostgreSQL dialect session.
    When: ``get_pnl_event_fx_candle_fingerprints_by_plane`` executes on SQLite and
        under a mocked PostgreSQL session.
    Then: Multi-batch per-plane reduction matches grouped replay and pooled total,
        and the PostgreSQL path emits ``SET LOCAL jit = off``.
    """
    batch_base = samples._M5 + timedelta(hours=1)
    many_opens = [batch_base + timedelta(minutes=offset) for offset in range(450)]
    batch_rows: list[object] = [
        _fx_symbol("sym-b-eu", "EUR-USD", "EUR", "USD", timestamp=samples._M1),
        _fx_instrument("ins-b-eu", "sym-b-eu", "kraken", timestamp=samples._M1),
        _fx_symbol("sym-b-gb", "GBP-USD", "GBP", "USD", timestamp=samples._M1),
        _fx_instrument("ins-b-gb", "sym-b-gb", "walutomat", timestamp=samples._M1),
        *[
            _fx_candle_seeded(
                "ins-b-eu" if offset % 2 == 0 else "ins-b-gb",
                open_at,
                20_000 + offset,
                window=(samples._M1 + timedelta(milliseconds=offset), KNOWN_TO_MAX),
            )
            for offset, open_at in enumerate(many_opens)
        ],
    ]
    async with repository.session() as session:
        session.add_all(batch_rows)
        await session.commit()

    planes: list[PnlFxRatePlane] = [("EUR", "USD", "kraken"), ("GBP", "USD", "walutomat")]
    opens_with_empty = [
        *many_opens,
        *[batch_base + timedelta(minutes=offset) for offset in range(500, 900)],
    ]
    by_plane, replay_rows = await _assert_fx_fingerprints_by_plane_match_replay(
        repository, planes, opens_with_empty, samples._M5
    )
    assert len(replay_rows) == 450
    assert by_plane[("EUR", "USD", "kraken")].count == 225
    assert by_plane[("GBP", "USD", "walutomat")].count == 225

    executed: list[object] = []

    class _PgSession:
        async def __aenter__(self) -> _PgSession:
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            del exc_info

        async def execute(self, statement: object) -> Mock:
            executed.append(statement)
            return Mock(
                all=Mock(
                    return_value=[("EUR", "USD", "kraken", 1, 5, 5, samples._M1)],
                )
            )

    with (
        patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ),
        patch.object(repository, "session", lambda: _PgSession()),
    ):
        pg_result = await repository.get_pnl_event_fx_candle_fingerprints_by_plane(
            [("EUR", "USD", "kraken")],
            [samples._M1],
            samples._M5,
        )
    assert pg_result == {("EUR", "USD", "kraken"): IdAggregate(1, 5, 5, samples._M1)}
    assert str(executed[0]) == "SET LOCAL jit = off"
