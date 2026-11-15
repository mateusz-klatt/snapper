"""Opt-in live-PostgreSQL witness for the windowed trade-integrity probes.

The mocked dialect tests pin statement text and binds only. This module runs
the real windowed statements against a daily range-partitioned ``trades``
table on the production dialect and proves three things:

- in-window conflicts are findings and out-of-window ones are not, for both
  monitors, and an obligation without an execution time is a finding;
- for candidates without a finding, PostgreSQL never executes the scans of
  populated partitions outside the probe window while it scans the
  in-window control partition, under a custom and under a forced generic
  plan, with the parameter types the driver binds;
- one complete repository call of a 15,000-row windowed page and a full
  2,000-row worklog batch finishes inside the 45-second monitor budget.

Every test SKIPS unless the session database URL (``DB_URL``, the Makefile's
``TEST_DB_URL`` pass-through) is a PostgreSQL URL, so the default SQLite
runs are undisturbed. Each test builds its own private schema holding
``trades`` with 25 daily partitions, ``trade_integrity_worklog`` and
``trade_integrity_monitor_cursors``, runs with that schema as its only
search path, and drops the schema on teardown. Nothing in ``public`` is read
or written. Opt in against an isolated scratch database, never production::

    make test TEST_DB_URL=postgresql+asyncpg://USER:PASS@HOST/SCRATCH_DB

Run with ``--log-cli-level=INFO`` to see the measured call times.
"""

import json
import logging
import os
import time
from collections.abc import AsyncIterator
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from typing import cast
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from snapper.core.json_types import JsonObject
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeIntegrityFinding
from snapper.data.trade_integrity import _M1_WINDOWED_POSTGRES_QUERY
from snapper.data.trade_integrity import _M2_WINDOWED_POSTGRES_QUERY
from snapper.data.trade_integrity import _find_m1_postgresql
from snapper.data.trade_integrity import _find_m1_windowed_postgresql
from snapper.data.trade_integrity import _find_m2_windowed_postgresql
from snapper.data.trade_integrity import _FindingCollector
from snapper.data.trade_integrity import _M1Candidate
from snapper.data.trade_integrity import _M1WindowCandidate
from snapper.data.trade_integrity import _M2WindowCandidate

logger = logging.getLogger(__name__)

_ANCHOR = datetime(2026, 7, 20, tzinfo=UTC)
_FIRST_DAY_OFFSET = -12
_LAST_DAY_OFFSET = 12
_CANDIDATE_TIME = _ANCHOR + timedelta(hours=12)
_OUT_OF_WINDOW_DAYS = (-12, -9, 9, 12)
_SHIFTED_TIME = _CANDIDATE_TIME + timedelta(days=3)
_EXPECTED_LOOPS = {0: 2, -12: 0, -9: 0, 9: 1, 12: 0}
_ACTIVE_LITERAL = "TIMESTAMPTZ '9999-12-31 23:59:59+00'"
_SESSION_ID = "40000000-0000-0000-0000-000000000001"
_BUDGET_NOW = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)
_BUDGET_SWEEP_START = datetime(2026, 7, 21, 6, 0, tzinfo=UTC)
_BUDGET_SWEEP_ROWS = 15_000
_BUDGET_WORKLOG_ROWS = 2_000
_BUDGET_SECONDS = 45.0
_M1_PARAMETERS = ("instrument_public_ids", "trade_ids", "executed_ats", "probe_window")
_M2_PARAMETERS = ("public_ids", "executed_ats", "probe_window")

type _Monitor = Literal["m1", "m2"]


def _configured_backend_name() -> str:
    """Return the backend name of the session-configured database URL.

    Reads the ``DB_URL`` environment variable the Makefile's ``TEST_DB_URL``
    pass-through sets for every test run; an unset or unparseable value maps
    to the empty string so the module skips instead of erroring.
    """
    url = os.environ.get("DB_URL", "")
    if not url:
        return ""
    try:
        return make_url(url).get_backend_name()
    except ArgumentError:
        return ""


pytestmark = pytest.mark.skipif(
    _configured_backend_name() != "postgresql",
    reason=(
        "live-PostgreSQL witness module: opt in via "
        "make test TEST_DB_URL=postgresql+asyncpg://... (see module docstring)"
    ),
)


def _day(offset: int) -> datetime:
    """Return the UTC midnight ``offset`` days after the anchor day.

    Args:
        offset: Day offset from the anchor.

    Returns:
        The partition's inclusive lower bound.
    """
    return _ANCHOR + timedelta(days=offset)


def _partition_name(offset: int) -> str:
    """Return the name of the daily partition ``offset`` days after the anchor.

    Args:
        offset: Day offset from the anchor.

    Returns:
        The unqualified partition table name.
    """
    return f"trades_d{_day(offset):%Y%m%d}"


def _schema_statements(schema: str) -> list[str]:
    """Build the private schema with partitioned trades and the monitor tables.

    The trades table mirrors the production column types, the daily range
    partitioning on ``executed_at`` and the indexes the probes and the sweep
    use. The active public-id index is not unique here, so that the tests can
    stage the duplicates M2 exists to detect.

    Args:
        schema: Private schema name.

    Returns:
        DDL statements in execution order.
    """
    statements = [
        f"CREATE SCHEMA {schema}",
        f"CREATE SEQUENCE {schema}.trades_id_seq",
        f"""
        CREATE TABLE {schema}.trades (
            id bigint NOT NULL DEFAULT nextval('{schema}.trades_id_seq'),
            public_id uuid NOT NULL,
            session_id uuid NOT NULL,
            sequence_id integer NOT NULL,
            timestamp timestamptz NOT NULL,
            known_to timestamptz NOT NULL,
            instrument_public_id uuid NOT NULL,
            price double precision NOT NULL,
            size double precision NOT NULL,
            side varchar(4) NOT NULL,
            trade_id varchar(64),
            executed_at timestamptz NOT NULL
        ) PARTITION BY RANGE (executed_at)
        """,
        (
            f"CREATE UNIQUE INDEX trades_p_uq_instr_tid_exec ON {schema}.trades "
            "(instrument_public_id, trade_id, executed_at)"
        ),
        f"CREATE INDEX trades_p_ix_ts ON {schema}.trades (timestamp)",
        (
            f"CREATE INDEX trades_p_ix_public_id_active ON {schema}.trades (public_id) "
            f"WHERE known_to = {_ACTIVE_LITERAL}"
        ),
        f"""
        CREATE TABLE {schema}.trade_integrity_worklog (
            id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            public_id uuid NOT NULL,
            instrument_public_id uuid NOT NULL,
            trade_id varchar(64),
            executed_at timestamptz,
            enqueued_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            m1_pending boolean NOT NULL DEFAULT true,
            m2_pending boolean NOT NULL DEFAULT true
        )
        """,
        (
            f"CREATE INDEX ix_m1_pending ON {schema}.trade_integrity_worklog (id) "
            "WHERE m1_pending IS TRUE"
        ),
        (
            f"CREATE INDEX ix_m2_pending ON {schema}.trade_integrity_worklog (id) "
            "WHERE m2_pending IS TRUE"
        ),
        f"""
        CREATE TABLE {schema}.trade_integrity_monitor_cursors (
            monitor varchar(2) PRIMARY KEY,
            covered_through timestamptz NOT NULL,
            scan_cursor_timestamp timestamptz NOT NULL,
            scan_cursor_id bigint NOT NULL,
            scan_end timestamptz NOT NULL,
            updated_at timestamptz NOT NULL
        )
        """,
    ]
    for offset in range(_FIRST_DAY_OFFSET, _LAST_DAY_OFFSET + 1):
        statements.append(
            f"CREATE TABLE {schema}.{_partition_name(offset)} PARTITION OF {schema}.trades "
            f"FOR VALUES FROM ('{_day(offset).isoformat()}') "
            f"TO ('{_day(offset + 1).isoformat()}')"
        )
    return statements


@pytest_asyncio.fixture
async def schema_engine() -> AsyncIterator[AsyncEngine]:
    """Yield an engine whose only search path is a fresh private schema.

    Yields:
        Engine with UTC sessions, as the repository configures them, bound
        to the private schema; the schema is dropped on teardown.
    """
    db_url = os.environ["DB_URL"]
    schema = f"ti_window_{uuid4().hex[:12]}"
    admin = create_async_engine(db_url, connect_args={"server_settings": {"timezone": "UTC"}})
    async with admin.begin() as connection:
        for statement in _schema_statements(schema):
            await connection.execute(text(statement))
    engine = create_async_engine(
        db_url,
        connect_args={"server_settings": {"timezone": "UTC", "search_path": schema}},
    )
    try:
        yield engine
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        await admin.dispose()


async def _insert_trade(
    session: AsyncSession,
    *,
    public_id: str,
    instrument_public_id: str,
    trade_id: str,
    executed_at: datetime,
) -> None:
    """Insert one active trade row whose bus time lies outside every sweep.

    Args:
        session: Session bound to the private schema.
        public_id: Logical row identity.
        instrument_public_id: Instrument identity.
        trade_id: Venue trade identity.
        executed_at: Venue execution time, the partition key.
    """
    await session.execute(
        text(
            "INSERT INTO trades (public_id, session_id, sequence_id, timestamp, known_to, "
            "instrument_public_id, price, size, side, trade_id, executed_at) VALUES ("
            "CAST(:public_id AS uuid), CAST(:session_id AS uuid), 1, "
            "TIMESTAMPTZ '2026-06-01 00:00:00+00', "
            f"{_ACTIVE_LITERAL}, CAST(:instrument AS uuid), 1.0, 1.0, 'buy', :trade_id, "
            "CAST(:executed_at AS timestamptz))"
        ),
        {
            "public_id": public_id,
            "session_id": _SESSION_ID,
            "instrument": instrument_public_id,
            "trade_id": trade_id,
            "executed_at": executed_at,
        },
    )


@dataclass(frozen=True, slots=True)
class _Witness:
    """Identities of the two clean witness candidates.

    Attributes:
        instrument: Instrument shared by both candidates.
        anchored_public_id: Public id of the candidate at the anchor time.
        shifted_public_id: Public id of the candidate three days later.
    """

    instrument: str
    anchored_public_id: str
    shifted_public_id: str


async def _stage_witness_rows(engine: AsyncEngine) -> _Witness:
    """Stage two clean candidates whose windows cover different partitions.

    The anchored candidate ``witness-a`` has its control row at its own time
    in the anchor day's partition, and rows of the same venue identity and
    public id, active, in the partitions of days -12, -9, +9 and +12, so each
    would be a finding if its partition were read for it. The shifted
    candidate ``witness-b``, three days later, has only its control row; its
    window covers day +9 but not days -12, -9 or +12.

    Args:
        engine: Engine bound to the private schema.

    Returns:
        The identities of both candidates.
    """
    witness = _Witness(
        instrument=str(uuid4()),
        anchored_public_id=str(uuid4()),
        shifted_public_id=str(uuid4()),
    )
    async with async_sessionmaker(engine)() as session:
        for offset in (0, *_OUT_OF_WINDOW_DAYS):
            await _insert_trade(
                session,
                public_id=witness.anchored_public_id,
                instrument_public_id=witness.instrument,
                trade_id="witness-a",
                executed_at=_CANDIDATE_TIME + timedelta(days=offset),
            )
        await _insert_trade(
            session,
            public_id=witness.shifted_public_id,
            instrument_public_id=witness.instrument,
            trade_id="witness-b",
            executed_at=_SHIFTED_TIME,
        )
        await session.commit()
    async with engine.begin() as connection:
        await connection.execute(text("ANALYZE trades"))
    return witness


def _numbered(statement: str, names: Sequence[str]) -> str:
    """Replace the module's named binds with positional parameters for PREPARE.

    Args:
        statement: The module's statement text.
        names: Bind names in positional order; ``finding_limit`` comes last.

    Returns:
        The same statement with ``$1``..``$n`` parameters.
    """
    for index, name in enumerate((*names, "finding_limit"), start=1):
        statement = statement.replace(f":{name}", f"${index}")
    return statement


def _loops_by_relation(plan: JsonObject) -> dict[str, int]:
    """Sum the executed loops of every scan node per relation name.

    Args:
        plan: Root plan node of ``EXPLAIN (ANALYZE, FORMAT JSON)``.

    Returns:
        Actual loops per scanned relation; a relation that was never
        executed has zero, and one removed at plan start is absent.
    """
    loops: dict[str, int] = {}
    stack = [plan]
    while stack:
        node = stack.pop()
        relation = node.get("Relation Name")
        if isinstance(relation, str):
            loops[relation] = loops.get(relation, 0) + cast(int, node["Actual Loops"])
        stack.extend(cast(list[JsonObject], node.get("Plans", [])))
    return loops


async def _explain_executed(
    engine: AsyncEngine,
    *,
    plan_cache_mode: str,
    prepare: str,
    execute: str,
) -> dict[str, int]:
    """Explain one prepared execution under a forced plan cache mode.

    Args:
        engine: Engine bound to the private schema.
        plan_cache_mode: ``force_custom_plan`` or ``force_generic_plan``.
        prepare: ``PREPARE`` statement with the driver's parameter types.
        execute: ``EXECUTE`` statement with literal arguments.

    Returns:
        Actual loops per scanned relation.
    """
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f"SET LOCAL plan_cache_mode = {plan_cache_mode}")
        await connection.exec_driver_sql(prepare)
        try:
            raw = (
                await connection.exec_driver_sql(
                    f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {execute}"
                )
            ).scalar_one()
        finally:
            await connection.exec_driver_sql("DEALLOCATE witness")
    document = cast(list[JsonObject], json.loads(raw) if isinstance(raw, str) else raw)
    return _loops_by_relation(cast(JsonObject, document[0]["Plan"]))


def _assert_pruned(loops: dict[str, int]) -> None:
    """Require partition scans that follow each candidate's own window.

    The anchor partition lies in both windows and is scanned once per
    candidate. Day +9 lies only in the shifted candidate's window, and days
    -12, -9 and +12 lie in neither, so their populated scans never execute.

    Args:
        loops: Actual loops per scanned relation.
    """
    observed = {offset: loops.get(_partition_name(offset), 0) for offset in _EXPECTED_LOOPS}
    assert observed == _EXPECTED_LOOPS, loops


@pytest.mark.parametrize("plan_cache_mode", ["force_custom_plan", "force_generic_plan"])
async def test_windowed_m1_never_executes_out_of_window_partitions(
    schema_engine: AsyncEngine,
    plan_cache_mode: str,
) -> None:
    """Prune populated M1 partitions per candidate, outside each one's window.

    Given: Two clean venue identities three days apart, the first with
        conflicting rows in four populated partitions outside its window, one
        of which lies inside the second candidate's window.
    When: The windowed M1 statement runs prepared with the driver's types
        under a custom and under a forced generic plan, and through the probe.
    Then: The probe finds nothing, and each partition is scanned exactly for
        the candidates whose window covers it.
    """
    witness = await _stage_witness_rows(schema_engine)
    async with async_sessionmaker(schema_engine)() as session:
        findings = await _find_m1_windowed_postgresql(
            session,
            [
                _M1WindowCandidate(witness.instrument, "witness-a", _CANDIDATE_TIME),
                _M1WindowCandidate(witness.instrument, "witness-b", _SHIFTED_TIME),
            ],
            _FindingCollector(),
        )

    loops = await _explain_executed(
        schema_engine,
        plan_cache_mode=plan_cache_mode,
        prepare=(
            "PREPARE witness(text[], text[], timestamptz[], interval, bigint) AS "
            + _numbered(_M1_WINDOWED_POSTGRES_QUERY.text, _M1_PARAMETERS)
        ),
        execute=(
            f"EXECUTE witness(ARRAY['{witness.instrument}', '{witness.instrument}']::text[], "
            "ARRAY['witness-a', 'witness-b']::text[], "
            f"ARRAY['{_CANDIDATE_TIME.isoformat()}', '{_SHIFTED_TIME.isoformat()}']"
            "::timestamptz[], interval '7 days', 20)"
        ),
    )

    assert findings == ()
    _assert_pruned(loops)


@pytest.mark.parametrize("plan_cache_mode", ["force_custom_plan", "force_generic_plan"])
async def test_windowed_m2_never_executes_out_of_window_partitions(
    schema_engine: AsyncEngine,
    plan_cache_mode: str,
) -> None:
    """Prune populated M2 partitions per candidate, outside each one's window.

    Given: Two clean public ids three days apart, the first with four more
        active rows in populated partitions outside its window, one of which
        lies inside the second candidate's window.
    When: The windowed M2 statement runs prepared with the driver's types
        under a custom and under a forced generic plan, and through the probe.
    Then: The probe finds nothing, and each partition is scanned exactly for
        the candidates whose window covers it.
    """
    witness = await _stage_witness_rows(schema_engine)
    async with async_sessionmaker(schema_engine)() as session:
        findings = await _find_m2_windowed_postgresql(
            session,
            [
                _M2WindowCandidate(witness.anchored_public_id, _CANDIDATE_TIME),
                _M2WindowCandidate(witness.shifted_public_id, _SHIFTED_TIME),
            ],
            _FindingCollector(),
        )

    loops = await _explain_executed(
        schema_engine,
        plan_cache_mode=plan_cache_mode,
        prepare=(
            "PREPARE witness(text[], timestamptz[], interval, bigint) AS "
            + _numbered(_M2_WINDOWED_POSTGRES_QUERY.text, _M2_PARAMETERS)
        ),
        execute=(
            f"EXECUTE witness(ARRAY['{witness.anchored_public_id}', "
            f"'{witness.shifted_public_id}']::text[], "
            f"ARRAY['{_CANDIDATE_TIME.isoformat()}', '{_SHIFTED_TIME.isoformat()}']"
            "::timestamptz[], interval '7 days', 20)"
        ),
    )

    assert findings == ()
    _assert_pruned(loops)


async def test_windowed_and_exact_probes_report_findings_on_postgresql(
    schema_engine: AsyncEngine,
) -> None:
    """Report in-window conflicts, skip out-of-window ones, and honour null times.

    Given: One venue identity with a conflict six days away, one with a
        conflict eight days away, one public id with active rows six days
        before and after its candidate, and one with the later row nine days
        after.
    When: The windowed probes and the exact M1 probe run on PostgreSQL.
    Then: Only the in-window conflicts are windowed findings, and an exact
        candidate without a time reports the stored row of its identity.
    """
    instrument = str(uuid4())
    near_public_id = str(uuid4())
    far_public_id = str(uuid4())
    async with async_sessionmaker(schema_engine)() as session:
        for trade_id, conflict_offset in (("near", 6), ("far", 8)):
            await _insert_trade(
                session,
                public_id=str(uuid4()),
                instrument_public_id=instrument,
                trade_id=trade_id,
                executed_at=_CANDIDATE_TIME + timedelta(days=conflict_offset),
            )
        for public_id, later_offset in ((near_public_id, 6), (far_public_id, 9)):
            for index, offset in enumerate((-6, later_offset)):
                await _insert_trade(
                    session,
                    public_id=public_id,
                    instrument_public_id=instrument,
                    trade_id=f"active-{public_id}-{index}",
                    executed_at=_CANDIDATE_TIME + timedelta(days=offset),
                )
        await session.commit()

        m1 = await _find_m1_windowed_postgresql(
            session,
            [
                _M1WindowCandidate(instrument, "near", _CANDIDATE_TIME),
                _M1WindowCandidate(instrument, "far", _CANDIDATE_TIME),
            ],
            _FindingCollector(),
        )
        m2 = await _find_m2_windowed_postgresql(
            session,
            [
                _M2WindowCandidate(near_public_id, _CANDIDATE_TIME),
                _M2WindowCandidate(far_public_id, _CANDIDATE_TIME),
            ],
            _FindingCollector(),
        )
        exact = await _find_m1_postgresql(session, [_M1Candidate(instrument, "far", None)])

    assert [(item.trade_id, item.conflicting_executed_at) for item in m1] == [
        ("near", _CANDIDATE_TIME + timedelta(days=6))
    ]
    assert [(item.public_id, item.active_count) for item in m2] == [(near_public_id, 2)]
    assert exact == (
        TradeIntegrityFinding(
            monitor="m1",
            public_id=None,
            instrument_public_id=instrument,
            trade_id="far",
            expected_executed_at=None,
            conflicting_executed_at=_CANDIDATE_TIME + timedelta(days=8),
            active_count=None,
        ),
    )


async def _stage_budget_rows(engine: AsyncEngine) -> None:
    """Stage one full windowed page and one full batch of clean obligations.

    The 15,000 swept rows have bus times inside the first sweep of
    ``_BUDGET_NOW`` and execution times over three days. The 2,000 owed rows
    have bus times before the sweep and execution times over all 25
    partitions, and each has a worklog obligation with its own values, so
    every exact probe reads every partition and finds nothing.

    Args:
        engine: Engine bound to the private schema.
    """
    instrument = str(uuid4())
    insert = (
        "INSERT INTO trades (public_id, session_id, sequence_id, timestamp, known_to, "
        "instrument_public_id, price, size, side, trade_id, executed_at) "
        "SELECT gen_random_uuid(), CAST(:session_id AS uuid), 1, {bus_time}, "
        f"{_ACTIVE_LITERAL}, CAST(:instrument AS uuid), 1.0, 1.0, 'buy', {{trade_id}}, "
        "{executed_at} FROM generate_series(1, CAST(:rows AS integer)) AS n"
    )
    async with engine.begin() as connection:
        await connection.execute(
            text(
                insert.format(
                    bus_time="CAST(:first_bus_time AS timestamptz) + n * interval '1 second'",
                    trade_id="'swept-' || n",
                    executed_at=(
                        "CAST(:anchor AS timestamptz) + (n % 72) * interval '1 hour' "
                        "+ n * interval '1 microsecond'"
                    ),
                )
            ),
            {
                "session_id": _SESSION_ID,
                "instrument": instrument,
                "first_bus_time": _BUDGET_SWEEP_START,
                "anchor": _ANCHOR,
                "rows": _BUDGET_SWEEP_ROWS,
            },
        )
        await connection.execute(
            text(
                insert.format(
                    bus_time="TIMESTAMPTZ '2026-06-01 00:00:00+00' + n * interval '1 second'",
                    trade_id="'owed-' || n",
                    executed_at=(
                        "CAST(:first_day AS timestamptz) + (n % 25) * interval '1 day' "
                        "+ interval '6 hours' + n * interval '1 microsecond'"
                    ),
                )
            ),
            {
                "session_id": _SESSION_ID,
                "instrument": instrument,
                "first_day": _day(_FIRST_DAY_OFFSET),
                "rows": _BUDGET_WORKLOG_ROWS,
            },
        )
        await connection.execute(
            text(
                "INSERT INTO trade_integrity_worklog "
                "(public_id, instrument_public_id, trade_id, executed_at) "
                "SELECT public_id, instrument_public_id, trade_id, executed_at FROM trades "
                "WHERE trade_id LIKE 'owed-%' ORDER BY id"
            )
        )
        await connection.execute(text("ANALYZE trades"))
        await connection.execute(text("ANALYZE trade_integrity_worklog"))


@pytest.mark.parametrize("monitor", ["m1", "m2"])
async def test_complete_call_with_full_page_and_worklog_fits_the_budget(
    schema_engine: AsyncEngine,
    monitor: _Monitor,
) -> None:
    """Finish one complete repository call inside the 45-second monitor budget.

    Given: A first sweep of 15,000 clean rows and 2,000 clean worklog
        obligations spread over 25 daily partitions.
    When: The repository runs one complete pass of the monitor with the
        default page, a full worklog batch and the derived statement timeout.
    Then: The pass reads the whole page and batch, finds nothing and finishes
        inside 45 seconds.
    """
    await _stage_budget_rows(schema_engine)
    repository = SQLAlchemyRepository.__new__(SQLAlchemyRepository)
    repository.engine = schema_engine
    repository.session_factory = async_sessionmaker(
        schema_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    started = time.perf_counter()
    result = await repository.run_trade_integrity_monitor(
        monitor=monitor,
        now=_BUDGET_NOW,
        sweep_limit=_BUDGET_SWEEP_ROWS,
        worklog_limit=_BUDGET_WORKLOG_ROWS,
        statement_timeout_seconds=_BUDGET_SECONDS - 2.0,
    )
    elapsed = time.perf_counter() - started
    logger.info("trade integrity %s complete call took %.3f s", monitor, elapsed)

    assert result.findings == ()
    assert (result.sweep_rows, result.worklog_rows) == (_BUDGET_SWEEP_ROWS, _BUDGET_WORKLOG_ROWS)
    assert elapsed < _BUDGET_SECONDS
