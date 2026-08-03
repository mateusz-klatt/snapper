"""Repository tests for durable P&L activation anchors."""

import asyncio
import os
from collections.abc import AsyncIterator
from copy import deepcopy
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Literal
from typing import Protocol
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch
from uuid import uuid7

import pytest
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.execution_chain import execution_row_digest
from snapper.application.portfolio.pnl_anchor_identity import normalize_portfolio_pnl_valuation_ccy
from snapper.application.portfolio.pnl_anchor_identity import portfolio_pnl_anchor_public_id
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import ExecutionAnnulment
from snapper.data.models import ExecutionAnnulmentVisibility
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import Symbol
from snapper.data.models import VenueEvent
from snapper.data.repository import PnlTimelineAnchorEvidenceMismatchError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _ExecutionKnowledgeHorizon
from snapper.data.repository import _PortfolioPnlAnchorFencedCuts
from snapper.data.repository import _resolved_execution_prefix_cut_horizons
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineExecutionPrefixBundle
from snapper.data.repository_types import PortfolioPnlAnchorRow
from snapper.data.repository_types import PortfolioPnlAnchorWriteEvidence

_T0 = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_FUTURE = datetime(2099, 1, 1, tzinfo=UTC)
_WALLET = "0000face-0000-7000-8000-0000000000a1"
_ANCHOR_PUBLIC_ID = portfolio_pnl_anchor_public_id(_WALLET, "live", "USD")
_EPOCH_PUBLIC_ID = "00000000-0000-7000-8000-000000000102"
_SESSION_PUBLIC_ID = "00000000-0000-7000-8000-000000000103"
_SYMBOL_PUBLIC_ID = "00000000-0000-7000-8000-000000000104"
_INSTRUMENT_PUBLIC_ID = "00000000-0000-7000-8000-000000000105"
_ORDER_PUBLIC_ID = "00000000-0000-7000-8000-000000000106"
_CLIENT_ORDER_ID = "anchor-fence-client"
_LOCAL_POSTGRES_URL_ENV = "PNL_ANCHOR_FENCE_POSTGRES_URL"


class _SyncCursor(Protocol):
    """Synchronous DBAPI cursor surface used by the connect event."""

    def execute(self, statement: str) -> object:
        """Execute one connection configuration statement."""
        ...

    def close(self) -> None:
        """Close the configuration cursor."""
        ...


class _SyncDbapiConnection(Protocol):
    """Synchronous DBAPI connection surface exposed by SQLAlchemy events."""

    def cursor(self) -> _SyncCursor:
        """Return a cursor for one session-level setting."""
        ...

    def commit(self) -> None:
        """Commit the session-level setting outside test transactions."""
        ...


def _configured_local_postgresql_url() -> str | None:
    """Return only the explicitly opted-in local disposable-schema target."""
    database_url = os.environ.get(_LOCAL_POSTGRES_URL_ENV)
    if database_url is None:
        return None
    try:
        parsed = make_url(database_url)
    except ArgumentError:
        return None
    if (
        parsed.get_backend_name() != "postgresql"
        or parsed.host != "127.0.0.1"
        or parsed.port != 5432
        or parsed.username != "snapper"
        or parsed.password != "example"
        or parsed.database != "snapper"
    ):
        return None
    return database_url


def _anchor(
    public_id: str = _ANCHOR_PUBLIC_ID,
    point_time: datetime = _T0,
    timestamp: datetime = _T0,
) -> PortfolioPnlAnchorRow:
    """Build one complete anchor payload containing every persisted field."""
    return {
        "public_id": public_id,
        "session_id": _SESSION_PUBLIC_ID,
        "sequence_id": 41,
        "timestamp": timestamp,
        "wallet_public_id": _WALLET,
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": point_time,
        "point_kind": "anchor",
        "epoch_public_id": _EPOCH_PUBLIC_ID,
        "calc_version": "5A.2",
        "valuation_status": "complete",
        "realized_pnl": 0.0,
        "fee_pnl": 0.0,
        "accrual_pnl": 0.0,
        "unrealized_pnl": 812.25,
        "external_flow_adjustment": 0.0,
        "cash_usd": 1200.5,
        "position_value_usd": 4812.75,
        "drawdown": None,
        "mark_source": "finalized_1m",
        "mark_time": point_time,
        "watermarks_json": '{"kraken":7,"zonda":3}',
        "opening_basket_json": '{"native":{"BTC":"1.25"},"pools":{"pool-a":"2"}}',
        "contributions_json": '{"unattributed":1.0}',
    }


def _malformed_anchor(**overrides: object) -> PortfolioPnlAnchorRow:
    """Build a deliberately invalid runtime payload for writer guard tests."""
    return cast(PortfolioPnlAnchorRow, {**_anchor(), **overrides})


def _atomic_anchor(
    point_time: datetime = _T0,
    timestamp: datetime = _T0,
) -> PortfolioPnlAnchorRow:
    """Build one empty-prefix candidate for atomic writer tests."""
    anchor = _anchor(point_time=point_time, timestamp=timestamp)
    anchor["watermarks_json"] = "{}"
    return anchor


def _empty_anchor_write_evidence(
    request_as_of: datetime = _T0,
    activation_as_of: datetime = _T0,
) -> PortfolioPnlAnchorWriteEvidence:
    """Build one valid empty derivation bundle for an atomic anchor write.

    The RESOLVED cut instants live on the bundle the repository would have
    produced; the evidence itself carries only the caller's nullable intents.
    """
    empty = PnlTimelineExecutionPrefix(watermarks={}, executions=[], annulments=[])
    return PortfolioPnlAnchorWriteEvidence(
        wallet_public_id=_WALLET,
        mode="live",
        requested_as_of=request_as_of,
        requested_activation_as_of=activation_as_of,
        execution_prefix_bundle=PnlTimelineExecutionPrefixBundle(
            request=empty,
            activation=empty,
            request_as_of=request_as_of,
            activation_as_of=activation_as_of,
        ),
    )


def _fenced_cuts(
    bundle: PnlTimelineExecutionPrefixBundle,
    request_as_of: datetime | None = _T0,
    activation_as_of: datetime | None = _T0,
) -> _PortfolioPnlAnchorFencedCuts:
    """Build one fenced re-read result at the horizons those intents resolve to.

    The horizons are produced by the repository's own resolver rather than
    copied off the bundle, which is the property the fenced loader now has and
    the reason a stub for it must not be free to pair any instant with any
    provenance.
    """
    request_horizon, activation_horizon = _resolved_execution_prefix_cut_horizons(
        request_as_of,
        activation_as_of,
    )
    return _PortfolioPnlAnchorFencedCuts(
        request_horizon=request_horizon,
        activation_horizon=activation_horizon,
        bundle=bundle,
    )


def _source_symbol() -> Symbol:
    """Build one active symbol row used by real writer-fence tests."""
    return Symbol(
        public_id=_SYMBOL_PUBLIC_ID,
        native_symbol="BTC-USD",
        base="BTC",
        quote="USD",
        asset_type="crypto",
        created_at=_T0 - timedelta(minutes=1),
        timestamp=_T0 - timedelta(minutes=1),
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION_PUBLIC_ID,
        sequence_id=1,
    )


def _source_instrument() -> Instrument:
    """Build one active execution instrument lineage row."""
    return Instrument(
        public_id=_INSTRUMENT_PUBLIC_ID,
        symbol_public_id=_SYMBOL_PUBLIC_ID,
        exchange="kraken",
        timestamp=_T0 - timedelta(minutes=1),
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION_PUBLIC_ID,
        sequence_id=1,
    )


def _source_order() -> Order:
    """Build one active execution order lineage row."""
    return Order(
        public_id=_ORDER_PUBLIC_ID,
        instrument_public_id=_INSTRUMENT_PUBLIC_ID,
        wallet_public_id=_WALLET,
        mode="live",
        client_order_id=_CLIENT_ORDER_ID,
        exchange_order_id="anchor-fence-venue-order",
        created_at=_T0 - timedelta(minutes=1),
        timestamp=_T0 - timedelta(minutes=1),
        side="buy",
        order_type="limit",
        price=100.0,
        size=1.0,
        status="filled",
        session_id=_SESSION_PUBLIC_ID,
        sequence_id=1,
        known_to=KNOWN_TO_MAX,
    )


def _source_execution() -> Execution:
    """Build one execution that advances the tested scope prefix."""
    return Execution(
        order_public_id=_ORDER_PUBLIC_ID,
        wallet_public_id=_WALLET,
        operator_public_id=None,
        exchange="kraken",
        mode="live",
        scope_sequence=1,
        exec_id="anchor-fence-exec",
        trade_id="anchor-fence-trade",
        side="buy",
        status="filled",
        price=100.0,
        size=1.0,
        fee=0.25,
        fee_asset="USD",
        executed_at=None,
        liquidity_role="maker",
        timestamp=_T0 - timedelta(minutes=1),
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION_PUBLIC_ID,
        sequence_id=1,
    )


def _source_fill() -> VenueEvent:
    """Build exact durable fill evidence for the source execution."""
    return VenueEvent(
        event_type="fill_observed",
        shard_key="kraken.BTC-USD.live",
        wallet_public_id=_WALLET,
        command_public_id=None,
        exchange="kraken",
        instrument="BTC-USD",
        mode="live",
        exchange_order_id="anchor-fence-venue-order",
        client_order_id=_CLIENT_ORDER_ID,
        venue_client_id=_CLIENT_ORDER_ID,
        side="buy",
        status="filled",
        fill_price=100.0,
        fill_size=1.0,
        cum_fill_size=1.0,
        fee=0.25,
        fee_asset="USD",
        exec_id="anchor-fence-exec",
        trade_id="anchor-fence-trade",
        error=None,
        venue_timestamp=_T0 - timedelta(minutes=1),
        received_at=_T0 - timedelta(minutes=1),
        payload_json=None,
        liquidity_role="maker",
        paired_group_id=None,
        timestamp=_T0 - timedelta(minutes=1),
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION_PUBLIC_ID,
        sequence_id=1,
    )


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated repository with anchor and prefix-source tables."""
    db_path = tmp_path / "pnl-anchor.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Symbol.__table__.create(schema_engine)
    Instrument.__table__.create(schema_engine)
    Order.__table__.create(schema_engine)
    Execution.__table__.create(schema_engine)
    ExecutionAnnulment.__table__.create(schema_engine)
    ExecutionAnnulmentVisibility.__table__.create(schema_engine)
    VenueEvent.__table__.create(schema_engine)
    PortfolioPnlPoint.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def _create_postgresql_anchor_fence_tables(connection: Connection) -> None:
    """Create only the six isolated relations exercised by the live proof."""
    for table in (
        Symbol.__table__,
        Instrument.__table__,
        Order.__table__,
        Execution.__table__,
        VenueEvent.__table__,
        PortfolioPnlPoint.__table__,
    ):
        table.create(connection)


@pytest.fixture
async def postgresql_repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Create and later drop one randomized schema on the validated local PG16."""
    database_url = _configured_local_postgresql_url()
    if database_url is None:
        pytest.skip(f"local PG16 proof requires a validated {_LOCAL_POSTGRES_URL_ENV}")
    schema_name = f"pnl_anchor_fence_{uuid7().hex}"
    quoted_schema = f'"{schema_name}"'
    repo = SQLAlchemyRepository(database_url)

    def set_search_path(
        dbapi_connection: _SyncDbapiConnection,
        connection_record: object,
    ) -> None:
        """Bind every pooled connection to the disposable schema."""
        del connection_record
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"SET SESSION search_path TO {quoted_schema}, public")
            dbapi_connection.commit()
        finally:
            cursor.close()

    event.listen(repo.engine.sync_engine, "connect", set_search_path)
    try:
        async with repo.engine.begin() as connection:
            await connection.execute(text(f"CREATE SCHEMA {quoted_schema}"))
            await connection.run_sync(_create_postgresql_anchor_fence_tables)
        yield repo
    finally:
        async with repo.engine.begin() as connection:
            await connection.execute(text(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"))
        event.remove(repo.engine.sync_engine, "connect", set_search_path)
        await repo.engine.dispose()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param([], False, id="not_a_mapping"),
        pytest.param({1: 1}, False, id="non_string_exchange"),
        pytest.param({"": 1}, False, id="empty_exchange"),
        pytest.param({"Kraken": 1}, False, id="noncanonical_exchange"),
        pytest.param({"kraken": "1"}, False, id="non_integer_sequence"),
        pytest.param({"kraken": True}, False, id="boolean_sequence"),
        pytest.param({"kraken": 0}, False, id="nonpositive_sequence"),
        pytest.param({"kraken": 1}, True, id="valid"),
    ],
)
def test_atomic_watermark_runtime_validator_covers_every_guard(
    value: object,
    expected: bool,
) -> None:
    """Malformed runtime watermark values fail each structural guard."""
    assert SQLAlchemyRepository._pnl_timeline_watermarks_are_valid(value) is expected


@pytest.mark.parametrize(
    ("value", "expected_size"),
    [
        pytest.param({}, None, id="not_a_list"),
        pytest.param([1], None, id="not_a_row"),
        pytest.param(
            [{"exchange": 1, "scope_sequence": 1}],
            None,
            id="non_string_exchange",
        ),
        pytest.param(
            [{"exchange": "kraken", "scope_sequence": "1"}],
            None,
            id="non_integer_sequence",
        ),
        pytest.param(
            [{"exchange": "kraken", "scope_sequence": True}],
            None,
            id="boolean_sequence",
        ),
        pytest.param(
            [
                {"exchange": "kraken", "scope_sequence": 1},
                {"exchange": "kraken", "scope_sequence": 1},
            ],
            None,
            id="duplicate_sequence",
        ),
        pytest.param(
            [{"exchange": "kraken", "scope_sequence": 1}],
            1,
            id="valid",
        ),
    ],
)
def test_atomic_prefix_row_index_refuses_malformed_or_duplicate_keys(
    value: object,
    expected_size: int | None,
) -> None:
    """The activation-subset index never accepts ambiguous runtime rows."""
    result = SQLAlchemyRepository._pnl_timeline_prefix_rows_by_sequence(value)
    assert (None if result is None else len(result)) == expected_size


@pytest.mark.parametrize(
    "value",
    [
        pytest.param([], id="not_a_bundle"),
        pytest.param({}, id="missing_cuts"),
        pytest.param({"request": {}, "activation": []}, id="non_mapping_cut"),
        pytest.param(
            {
                "request": {"watermarks": [], "executions": []},
                "activation": {"watermarks": {}, "executions": []},
            },
            id="invalid_watermarks",
        ),
        pytest.param(
            {
                "request": {"watermarks": {}, "executions": []},
                "activation": {
                    "watermarks": {"kraken": 1},
                    "executions": [],
                },
            },
            id="missing_request_exchange",
        ),
        pytest.param(
            {
                "request": {
                    "watermarks": {"kraken": 1},
                    "executions": [],
                },
                "activation": {
                    "watermarks": {"kraken": 2},
                    "executions": [],
                },
            },
            id="regressed_request_sequence",
        ),
        pytest.param(
            {
                "request": {"watermarks": {}, "executions": {}},
                "activation": {"watermarks": {}, "executions": []},
            },
            id="invalid_request_rows",
        ),
        pytest.param(
            {
                "request": {"watermarks": {}, "executions": []},
                "activation": {"watermarks": {}, "executions": {}},
            },
            id="invalid_activation_rows",
        ),
        pytest.param(
            {
                "request": {
                    "watermarks": {"kraken": 1},
                    "executions": [{"exchange": "kraken", "scope_sequence": 1, "price": 1.0}],
                },
                "activation": {
                    "watermarks": {"kraken": 1},
                    "executions": [{"exchange": "kraken", "scope_sequence": 1, "price": 2.0}],
                },
            },
            id="activation_row_not_exact_subset",
        ),
    ],
)
def test_atomic_bundle_monotonicity_refuses_every_invalid_relation(
    value: object,
) -> None:
    """Malformed cuts, regressed maps, and changed subset rows all fail."""
    assert not SQLAlchemyRepository._pnl_timeline_prefix_bundle_is_monotonic(value)


def test_atomic_bundle_monotonicity_accepts_request_only_exchange() -> None:
    """A later request-only exchange remains a valid monotonic relation."""
    value = {
        "request": {
            "watermarks": {"zonda": 1},
            "executions": [{"exchange": "zonda", "scope_sequence": 1}],
        },
        "activation": {"watermarks": {}, "executions": []},
    }
    assert SQLAlchemyRepository._pnl_timeline_prefix_bundle_is_monotonic(value)


def _atomic_bundle(runtime_evidence: dict[str, object]) -> dict[str, object]:
    """Return the mutable bundle carrying the evidence's RESOLVED cut instants."""
    return cast(dict[str, object], runtime_evidence["execution_prefix_bundle"])


def _set_atomic_cut(runtime_evidence: dict[str, object], cut: str, value: object) -> None:
    """Move one cut's resolved instant AND its matching intent together.

    The intent check fires before the anchor-row comparison, so a fixture that
    moved only the resolved instant would trip that instead of the mismatch it
    means to exercise. Moving both keeps each case testing exactly one relation.
    """
    _atomic_bundle(runtime_evidence)[f"{cut}_as_of"] = value
    runtime_evidence["requested_as_of" if cut == "request" else "requested_activation_as_of"] = (
        value
    )


def _apply_atomic_scope_mismatch(
    mismatch: str,
    runtime_evidence: dict[str, object],
) -> None:
    """Apply one identity, mode, type, or request-cut mismatch."""
    if mismatch == "noncanonical_wallet":
        runtime_evidence["wallet_public_id"] = _WALLET.upper()
    elif mismatch == "different_wallet":
        runtime_evidence["wallet_public_id"] = "0000face-0000-7000-8000-0000000000a2"
    elif mismatch == "mode":
        runtime_evidence["mode"] = "paper"
    elif mismatch == "request_type":
        _set_atomic_cut(runtime_evidence, "request", "not-a-time")
    elif mismatch == "activation_type":
        _set_atomic_cut(runtime_evidence, "activation", "not-a-time")
    else:
        _set_atomic_cut(runtime_evidence, "request", _T0 + timedelta(minutes=1))


def _apply_atomic_cut_mismatch(
    mismatch: str,
    anchor: PortfolioPnlAnchorRow,
    runtime_evidence: dict[str, object],
) -> None:
    """Apply one activation, timezone, ordering, or bundle mismatch."""
    if mismatch == "activation_cut":
        _set_atomic_cut(runtime_evidence, "activation", _T0 - timedelta(minutes=1))
    elif mismatch == "request_timezone":
        _set_atomic_cut(runtime_evidence, "request", _T0.astimezone(timezone(timedelta(hours=1))))
    elif mismatch == "activation_timezone":
        _set_atomic_cut(
            runtime_evidence, "activation", _T0.astimezone(timezone(timedelta(hours=1)))
        )
    elif mismatch == "inverted_cuts":
        activation = _T0 + timedelta(minutes=1)
        anchor["point_time"] = activation
        _set_atomic_cut(runtime_evidence, "activation", activation)
    elif mismatch == "bundle":
        runtime_evidence["execution_prefix_bundle"] = {
            "request": {},
            "activation": [],
            "request_as_of": _T0,
            "activation_as_of": _T0,
        }
    else:
        anchor["watermarks_json"] = '{"kraken":1}'


@pytest.mark.parametrize(
    "mismatch",
    [
        "noncanonical_wallet",
        "different_wallet",
        "mode",
        "request_type",
        "activation_type",
        "request_cut",
        "activation_cut",
        "request_timezone",
        "activation_timezone",
        "inverted_cuts",
        "bundle",
        "watermarks_json",
    ],
)
def test_atomic_write_evidence_refuses_every_scope_and_cut_mismatch(
    mismatch: str,
) -> None:
    """Every caller-controlled relation is checked before a transaction."""
    anchor = _atomic_anchor()
    evidence = _empty_anchor_write_evidence()
    runtime_evidence = cast(dict[str, object], evidence)
    if mismatch in {
        "noncanonical_wallet",
        "different_wallet",
        "mode",
        "request_type",
        "activation_type",
        "request_cut",
    }:
        _apply_atomic_scope_mismatch(mismatch, runtime_evidence)
    else:
        _apply_atomic_cut_mismatch(mismatch, anchor, runtime_evidence)

    s5778_value_1 = cast(PortfolioPnlAnchorWriteEvidence, runtime_evidence)
    with pytest.raises(PnlTimelineAnchorEvidenceMismatchError, match="does not match"):
        SQLAlchemyRepository._validate_portfolio_pnl_anchor_write_evidence(
            anchor,
            s5778_value_1,
        )


def test_atomic_write_evidence_wraps_malformed_wallet_identity() -> None:
    """A runtime identity parser failure becomes the typed repository error."""
    evidence = _empty_anchor_write_evidence()
    evidence["wallet_public_id"] = "not-a-wallet"

    s5778_value_1 = _atomic_anchor()
    with pytest.raises(PnlTimelineAnchorEvidenceMismatchError, match="malformed"):
        SQLAlchemyRepository._validate_portfolio_pnl_anchor_write_evidence(
            s5778_value_1,
            evidence,
        )


async def test_atomic_anchor_writer_sets_read_committed_then_locks_in_order(
    repository: SQLAlchemyRepository,
) -> None:
    """PostgreSQL begins before advisory and deterministic source table locks."""
    events: list[str] = []
    session = AsyncMock()
    session.add = MagicMock()

    async def execute(statement: object, parameters: object = None) -> MagicMock:
        """Record every SQL statement issued by the atomic writer."""
        del parameters
        events.append(str(statement))
        return MagicMock()

    async def load(
        loaded_session: AsyncSession,
        evidence: PortfolioPnlAnchorWriteEvidence,
    ) -> _PortfolioPnlAnchorFencedCuts:
        """Record that source reload begins only after every PostgreSQL lock."""
        assert loaded_session is session
        events.append("load")
        return _fenced_cuts(evidence["execution_prefix_bundle"])

    session.execute = AsyncMock(side_effect=execute)
    current_anchor = AsyncMock(return_value=None)
    with (
        patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ),
        patch.object(repository, "session") as session_context,
        patch.object(
            repository,
            "_load_portfolio_pnl_anchor_fenced_cuts",
            side_effect=load,
        ),
        patch.object(
            repository,
            "_pnl_timeline_scope_has_fill_gap_in_session",
            new=AsyncMock(return_value=False),
        ) as gap_check,
        patch.object(
            repository,
            "_read_current_portfolio_pnl_anchor",
            new=current_anchor,
        ),
    ):
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        result = await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            _atomic_anchor(),
            _empty_anchor_write_evidence(),
        )

    assert events == [
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SELECT pg_advisory_xact_lock(hashtext('portfolio_pnl_anchor'), hashtext(:scope))",
        (
            "LOCK TABLE execution_annulments, executions, instruments, orders, "
            "symbols, venue_events IN SHARE MODE"
        ),
        "load",
    ]
    assert result == _atomic_anchor()
    session.add.assert_called_once()
    session.commit.assert_awaited_once_with()
    session.rollback.assert_not_awaited()
    gap_check.assert_awaited_once()
    assert gap_check.await_args_list[0].args[0] is session


async def test_atomic_anchor_writer_rolls_back_changed_sqlite_bundle(
    repository: SQLAlchemyRepository,
) -> None:
    """SQLite takes its reservation first and writes no anchor after a change."""
    session = AsyncMock()
    session.add = MagicMock()
    current = PnlTimelineExecutionPrefixBundle(
        request=PnlTimelineExecutionPrefix(watermarks={"kraken": 1}, executions=[], annulments=[]),
        activation=PnlTimelineExecutionPrefix(
            watermarks={"kraken": 1}, executions=[], annulments=[]
        ),
        request_as_of=_T0,
        activation_as_of=_T0,
    )
    with (
        patch.object(repository, "session") as session_context,
        patch.object(
            repository,
            "_load_portfolio_pnl_anchor_fenced_cuts",
            new=AsyncMock(return_value=_fenced_cuts(current)),
        ),
        patch.object(
            repository,
            "_read_current_portfolio_pnl_anchor",
            new=AsyncMock(return_value=None),
        ) as current_anchor,
    ):
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        s5778_value_1 = _atomic_anchor()
        s5778_value_2 = _empty_anchor_write_evidence()
        with pytest.raises(
            PnlTimelineAnchorEvidenceMismatchError,
            match="changed before anchor persistence",
        ):
            await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
                s5778_value_1,
                s5778_value_2,
            )

    first_statement = session.execute.await_args_list[0].args[0]
    assert str(first_statement) == "BEGIN IMMEDIATE"
    assert session.execute.await_count == 1
    session.add.assert_not_called()
    session.commit.assert_not_awaited()
    session.rollback.assert_awaited_once_with()
    current_anchor.assert_not_awaited()


async def test_atomic_transaction_and_fence_refuse_unknown_dialect(
    repository: SQLAlchemyRepository,
) -> None:
    """Neither half of the durability protocol silently weakens on a new DB."""
    session = AsyncMock()
    with patch.object(
        SQLAlchemyRepository,
        "dialect_name",
        new_callable=PropertyMock,
        return_value="unknown",
    ):
        with pytest.raises(NotImplementedError, match="write transaction"):
            await repository._begin_portfolio_pnl_anchor_write_transaction(session)
        s5778_value_1 = _atomic_anchor()
        with pytest.raises(NotImplementedError, match="evidence fence"):
            await repository._acquire_portfolio_pnl_anchor_evidence_fence(
                session,
                s5778_value_1,
            )
    session.execute.assert_not_awaited()


async def test_fenced_bundle_reload_reads_distinct_cuts_independently(
    repository: SQLAlchemyRepository,
) -> None:
    """Two named horizons are each re-read historically at the instant named.

    Given: Evidence whose two cuts name different past instants outright.
    When: The fence re-derives them.
    Then: Each cut is loaded separately, at exactly the instant its own intent
        named, and both carry ``requested=True`` — so both still owe every
        correction the visibility observation a historical read demands.
    """
    request = PnlTimelineExecutionPrefix(watermarks={"kraken": 2}, executions=[], annulments=[])
    activation = PnlTimelineExecutionPrefix(watermarks={"kraken": 1}, executions=[], annulments=[])
    loader = AsyncMock(side_effect=[request, activation])
    evidence = _empty_anchor_write_evidence(
        request_as_of=_T0 + timedelta(minutes=1),
        activation_as_of=_T0,
    )
    with patch.object(
        repository,
        "_load_pnl_timeline_execution_prefix_snapshot",
        new=loader,
    ):
        cuts = await repository._load_portfolio_pnl_anchor_fenced_cuts(
            AsyncMock(),
            evidence,
        )

    assert cuts.request_horizon == _ExecutionKnowledgeHorizon(
        as_of=_T0 + timedelta(minutes=1),
        requested=True,
    )
    assert cuts.activation_horizon == _ExecutionKnowledgeHorizon(as_of=_T0, requested=True)
    assert cuts.bundle == {
        "request": request,
        "activation": activation,
        "request_as_of": _T0 + timedelta(minutes=1),
        "activation_as_of": _T0,
    }
    assert [call.args[3] for call in loader.await_args_list] == [
        cuts.request_horizon,
        cuts.activation_horizon,
    ]


async def test_fenced_bundle_reload_captures_its_own_present_for_absent_intents(
    repository: SQLAlchemyRepository,
) -> None:
    """An absent intent is re-read at the FENCE's present, never at the echo.

    Given: Evidence whose two intents are absent — the shape of a current-truth
        derivation — while the bundle travelling with it echoes instants from
        deep in the past, which a caller of this public writer is free to write
        into a plain mutable mapping.
    When: The fence re-derives both cuts.
    Then: Both horizons are the fence's OWN freshly captured present (the
        activation cut being that present truncated to its minute), and both are
        unrequested. The echoed instants are not read at, so the exemption from
        the durability proof is earned by this read having captured the instant
        itself — the only thing that ever earns it — and a backdated horizon can
        no longer be smuggled in wearing the current-truth exemption.
    """
    empty = PnlTimelineExecutionPrefix(watermarks={}, executions=[], annulments=[])
    loader = AsyncMock(return_value=empty)
    evidence = _empty_anchor_write_evidence()
    evidence["requested_as_of"] = None
    evidence["requested_activation_as_of"] = None

    before = datetime.now(UTC)
    with patch.object(
        repository,
        "_load_pnl_timeline_execution_prefix_snapshot",
        new=loader,
    ):
        cuts = await repository._load_portfolio_pnl_anchor_fenced_cuts(
            AsyncMock(),
            evidence,
        )
    after = datetime.now(UTC)

    assert cuts.request_horizon.requested is False
    assert cuts.activation_horizon.requested is False
    assert before <= cuts.request_horizon.as_of <= after
    assert cuts.activation_horizon.as_of == cuts.request_horizon.as_of.replace(
        second=0,
        microsecond=0,
    )
    assert cuts.bundle["request_as_of"] == cuts.request_horizon.as_of
    assert cuts.bundle["activation_as_of"] == cuts.activation_horizon.as_of
    assert _T0 not in (cuts.request_horizon.as_of, cuts.activation_horizon.as_of)
    assert [call.args[3] for call in loader.await_args_list] == [
        cuts.request_horizon,
        cuts.activation_horizon,
    ]


@pytest.mark.parametrize("failure", ["unproven", "inconsistent"])
async def test_atomic_writer_rolls_back_unprovable_or_inconsistent_current_bundle(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    failure: Literal["unproven", "inconsistent"],
) -> None:
    """Fence-time validation failures are typed and leave no durable anchor."""

    async def fail_reload(
        session: AsyncSession,
        evidence: PortfolioPnlAnchorWriteEvidence,
    ) -> _PortfolioPnlAnchorFencedCuts:
        """Raise or return the requested invalid current evidence."""
        del session, evidence
        if failure == "unproven":
            raise ExecutionChainError("backfilled lineage")
        return _fenced_cuts(
            PnlTimelineExecutionPrefixBundle(
                request=PnlTimelineExecutionPrefix(
                    watermarks={},
                    executions=[],
                    annulments=[],
                ),
                activation=PnlTimelineExecutionPrefix(
                    watermarks={"kraken": 1},
                    executions=[],
                    annulments=[],
                ),
                request_as_of=_T0,
                activation_as_of=_T0,
            )
        )

    monkeypatch.setattr(
        repository,
        "_load_portfolio_pnl_anchor_fenced_cuts",
        fail_reload,
    )
    message = "cannot be proven" if failure == "unproven" else "inconsistent"
    s5778_value_1 = _atomic_anchor()
    s5778_value_2 = _empty_anchor_write_evidence()
    with pytest.raises(PnlTimelineAnchorEvidenceMismatchError, match=message):
        await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            s5778_value_1,
            s5778_value_2,
        )

    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 0


async def test_atomic_writer_integrity_race_returns_validated_winner(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A legacy concurrent insert collision still returns its canonical winner."""
    winner = _atomic_anchor()
    async with repository.session() as s:
        s.add(PortfolioPnlPoint(**winner, known_to=KNOWN_TO_MAX))
        await s.commit()
    original_read = repository._read_current_portfolio_pnl_anchor
    read_count = 0

    async def hide_first_winner(
        session: AsyncSession,
        wallet_public_id: str,
        mode: str,
        valuation_ccy: str,
    ) -> PortfolioPnlPoint | None:
        """Force one duplicate insert before exposing the stored winner."""
        nonlocal read_count
        read_count += 1
        if read_count == 1:
            return None
        return await original_read(
            session,
            wallet_public_id,
            mode,
            valuation_ccy,
        )

    monkeypatch.setattr(
        repository,
        "_read_current_portfolio_pnl_anchor",
        hide_first_winner,
    )
    result = await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
        _atomic_anchor(),
        _empty_anchor_write_evidence(),
    )

    assert result == winner
    assert read_count == 2


async def test_atomic_writer_reraises_integrity_failure_without_winner(
    repository: SQLAlchemyRepository,
) -> None:
    """An unrelated insert constraint failure is never classified as a race."""
    invalid = cast(
        PortfolioPnlAnchorRow,
        {
            **_atomic_anchor(),
            "session_id": None,
        },
    )

    s5778_value_1 = _empty_anchor_write_evidence()
    with pytest.raises(IntegrityError):
        await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            invalid,
            s5778_value_1,
        )

    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 0


async def test_sqlite_atomic_writer_refuses_append_committed_after_initial_bundle(
    repository: SQLAlchemyRepository,
) -> None:
    """A source append between derivation and the fence leaves no anchor."""
    initial = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0,
        _T0,
    )
    assert initial["request"]["watermarks"] == {}
    async with repository.session() as s:
        s.add_all(
            [
                _source_symbol(),
                _source_instrument(),
                _source_order(),
                _source_execution(),
                _source_fill(),
            ]
        )
        await s.commit()

    evidence = _empty_anchor_write_evidence()
    evidence["execution_prefix_bundle"] = initial
    s5778_value_1 = _atomic_anchor()
    with pytest.raises(
        PnlTimelineAnchorEvidenceMismatchError,
        match="changed before anchor persistence",
    ):
        await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            s5778_value_1,
            evidence,
        )

    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 0


async def test_sqlite_atomic_writer_refuses_orphan_fill_after_clean_bundle(
    repository: SQLAlchemyRepository,
) -> None:
    """A committed orphan CID is found by the fenced scope-wide gap read."""
    initial = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0,
        _T0,
    )
    assert initial["request"]["executions"] == []
    async with repository.session() as s:
        s.add(_source_fill())
        await s.commit()
    evidence = _empty_anchor_write_evidence()
    evidence["execution_prefix_bundle"] = initial

    s5778_value_1 = _atomic_anchor()
    with pytest.raises(
        PnlTimelineAnchorEvidenceMismatchError,
        match="unconsumed fill evidence",
    ):
        await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            s5778_value_1,
            evidence,
        )

    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 0


async def test_sqlite_atomic_writer_persists_exact_no_gap_prefix(
    repository: SQLAlchemyRepository,
) -> None:
    """Matching scope-wide fill and execution quantities still permit a write."""
    async with repository.session() as s:
        s.add_all(
            [
                _source_symbol(),
                _source_instrument(),
                _source_order(),
                _source_execution(),
                _source_fill(),
            ]
        )
        await s.commit()
    bundle = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0,
        _T0,
    )
    evidence = _empty_anchor_write_evidence()
    evidence["execution_prefix_bundle"] = bundle
    candidate = _atomic_anchor()
    candidate["watermarks_json"] = '{"kraken":1}'

    recorded = await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
        candidate,
        evidence,
    )

    assert recorded == candidate
    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 1


async def test_sqlite_source_append_after_atomic_fence_waits_for_anchor_commit(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BEGIN IMMEDIATE blocks a relevant source append until anchor commit."""
    initial = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0,
        _T0,
    )
    evidence = _empty_anchor_write_evidence()
    evidence["execution_prefix_bundle"] = initial
    original_load = repository._load_pnl_timeline_execution_prefix_snapshot
    fence_acquired = asyncio.Event()
    release_reload = asyncio.Event()
    writer_attempted = asyncio.Event()

    async def pause_after_fence(
        session: AsyncSession,
        wallet_public_id: str,
        mode: str,
        horizon: object,
    ) -> PnlTimelineExecutionPrefix:
        """Hold the fenced writer immediately before its source reload."""
        fence_acquired.set()
        await asyncio.wait_for(release_reload.wait(), timeout=5.0)
        return await original_load(session, wallet_public_id, mode, horizon)

    async def append_relevant_source() -> None:
        """Attempt an orphan fill insert on a second connection after the fence."""
        async with repository.session() as s:
            s.add(_source_fill())
            writer_attempted.set()
            await s.commit()

    monkeypatch.setattr(
        repository,
        "_load_pnl_timeline_execution_prefix_snapshot",
        pause_after_fence,
    )
    anchor_task = asyncio.create_task(
        repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            _atomic_anchor(),
            evidence,
        )
    )
    await asyncio.wait_for(fence_acquired.wait(), timeout=5.0)
    writer_task = asyncio.create_task(append_relevant_source())
    try:
        await asyncio.wait_for(writer_attempted.wait(), timeout=5.0)
        done, pending = await asyncio.wait({writer_task}, timeout=0.1)
        assert done == set()
        assert pending == {writer_task}
        release_reload.set()
        recorded = await asyncio.wait_for(anchor_task, timeout=5.0)
        await asyncio.wait_for(writer_task, timeout=5.0)
    finally:
        release_reload.set()
        await asyncio.gather(anchor_task, writer_task, return_exceptions=True)

    assert recorded == _atomic_anchor()
    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
        fill_count = await s.scalar(select(func.count()).select_from(VenueEvent))
    assert anchor_count == 1
    assert fill_count == 1


async def test_postgresql_atomic_writer_refuses_append_after_initial_bundle(
    postgresql_repository: SQLAlchemyRepository,
) -> None:
    """A real PG16 commit after the RR bundle is visible behind the fence."""
    initial = await postgresql_repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0,
        _T0,
    )
    assert initial["request"]["watermarks"] == {}
    async with postgresql_repository.session() as s:
        s.add_all(
            [
                _source_symbol(),
                _source_instrument(),
                _source_order(),
                _source_execution(),
                _source_fill(),
            ]
        )
        await s.commit()
    evidence = _empty_anchor_write_evidence()
    evidence["execution_prefix_bundle"] = initial

    s5778_value_1 = _atomic_anchor()
    with pytest.raises(
        PnlTimelineAnchorEvidenceMismatchError,
        match="changed before anchor persistence",
    ):
        await postgresql_repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            s5778_value_1,
            evidence,
        )

    async with postgresql_repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
        server_version = await s.scalar(text("SHOW server_version_num"))
    assert anchor_count == 0
    assert server_version is not None
    assert int(server_version) >= 160_000


async def test_postgresql_atomic_writer_refuses_orphan_fill_after_clean_bundle(
    postgresql_repository: SQLAlchemyRepository,
) -> None:
    """The fenced local PG16 scope read detects an orphan committed beforehand."""
    initial = await postgresql_repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0,
        _T0,
    )
    assert initial["request"]["executions"] == []
    async with postgresql_repository.session() as s:
        s.add(_source_fill())
        await s.commit()
    evidence = _empty_anchor_write_evidence()
    evidence["execution_prefix_bundle"] = initial

    s5778_value_1 = _atomic_anchor()
    with pytest.raises(
        PnlTimelineAnchorEvidenceMismatchError,
        match="unconsumed fill evidence",
    ):
        await postgresql_repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            s5778_value_1,
            evidence,
        )

    async with postgresql_repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 0


async def test_postgresql_atomic_writer_persists_exact_no_gap_prefix(
    postgresql_repository: SQLAlchemyRepository,
) -> None:
    """Matching local PG16 source planes remain eligible for atomic insert."""
    async with postgresql_repository.session() as s:
        s.add_all(
            [
                _source_symbol(),
                _source_instrument(),
                _source_order(),
                _source_execution(),
                _source_fill(),
            ]
        )
        await s.commit()
    bundle = await postgresql_repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0,
        _T0,
    )
    evidence = _empty_anchor_write_evidence()
    evidence["execution_prefix_bundle"] = bundle
    candidate = _atomic_anchor()
    candidate["watermarks_json"] = '{"kraken":1}'

    recorded = await postgresql_repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
        candidate,
        evidence,
    )

    assert recorded == candidate
    async with postgresql_repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 1


async def test_postgresql_source_append_after_fence_waits_for_anchor_commit(
    postgresql_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The PG SHARE table fence holds a source writer through anchor commit."""
    initial = await postgresql_repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0,
        _T0,
    )
    evidence = _empty_anchor_write_evidence()
    evidence["execution_prefix_bundle"] = initial
    original_load = postgresql_repository._load_pnl_timeline_execution_prefix_snapshot
    fence_acquired = asyncio.Event()
    release_reload = asyncio.Event()
    writer_attempted = asyncio.Event()

    async def pause_after_fence(
        session: AsyncSession,
        wallet_public_id: str,
        mode: str,
        horizon: object,
    ) -> PnlTimelineExecutionPrefix:
        """Hold the atomic transaction after all source table locks."""
        fence_acquired.set()
        await asyncio.wait_for(release_reload.wait(), timeout=5.0)
        return await original_load(session, wallet_public_id, mode, horizon)

    async def append_relevant_source() -> None:
        """Try a real orphan fill insert after the source table fence is held."""
        async with postgresql_repository.session() as s:
            s.add(_source_fill())
            writer_attempted.set()
            await s.commit()

    monkeypatch.setattr(
        postgresql_repository,
        "_load_pnl_timeline_execution_prefix_snapshot",
        pause_after_fence,
    )
    anchor_task = asyncio.create_task(
        postgresql_repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            _atomic_anchor(),
            evidence,
        )
    )
    await asyncio.wait_for(fence_acquired.wait(), timeout=5.0)
    writer_task = asyncio.create_task(append_relevant_source())
    try:
        await asyncio.wait_for(writer_attempted.wait(), timeout=5.0)
        done, pending = await asyncio.wait({writer_task}, timeout=0.1)
        assert done == set()
        assert pending == {writer_task}
        release_reload.set()
        recorded = await asyncio.wait_for(anchor_task, timeout=5.0)
        await asyncio.wait_for(writer_task, timeout=5.0)
    finally:
        release_reload.set()
        await asyncio.gather(anchor_task, writer_task, return_exceptions=True)

    assert recorded == _atomic_anchor()
    async with postgresql_repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
        fill_count = await s.scalar(select(func.count()).select_from(VenueEvent))
    assert anchor_count == 1
    assert fill_count == 1


async def test_sqlite_atomic_anchor_candidates_return_one_concurrent_winner(
    repository: SQLAlchemyRepository,
) -> None:
    """Serialized candidates re-read and return the first canonical winner."""
    first = _atomic_anchor()
    second = _atomic_anchor()
    second["unrealized_pnl"] = 999.0
    second["opening_basket_json"] = '{"native":{"ETH":"1"},"pools":{}}'
    evidence = _empty_anchor_write_evidence()

    results = await asyncio.gather(
        repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            first,
            evidence,
        ),
        repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            second,
            evidence,
        ),
    )

    assert results[0] == results[1]
    assert results[0] in (first, second)
    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 1


def _mutated_prefix_evidence(
    bundle: PnlTimelineExecutionPrefixBundle,
    evidence_kind: Literal["execution", "fill", "lineage"],
) -> PnlTimelineExecutionPrefixBundle:
    """Change one exact execution, fill, or lineage field in both cuts."""
    mutated = deepcopy(bundle)
    rows = (
        mutated["request"]["executions"][0],
        mutated["activation"]["executions"][0],
    )
    for row in rows:
        if evidence_kind == "execution":
            row["price"] = 101.0
        elif evidence_kind == "fill":
            row["shard_key"] = "kraken.BTC-USD.live.changed"
        else:
            row["instrument_public_id"] = "00000000-0000-7000-8000-000000000199"
    return mutated


_PHANTOM_ORDER_PUBLIC_ID = "00000000-0000-7000-8000-000000000186"
_PHANTOM_CLIENT_ORDER_ID = "anchor-fence-phantom-client"
_ANNULLING_USER = "0000face-0000-7000-8000-0000000000d1"
_ANNULMENT_PUBLIC_ID = "0000face-0000-7000-8000-0000000000d9"


def _phantom_source_order() -> Order:
    """Build the order lineage of an unwitnessed booking in the same scope."""
    order = _source_order()
    order.public_id = _PHANTOM_ORDER_PUBLIC_ID
    order.client_order_id = _PHANTOM_CLIENT_ORDER_ID
    order.exchange_order_id = "anchor-fence-phantom-venue-order"
    return order


def _phantom_source_execution() -> Execution:
    """Build one unwitnessed booking at the scope's second sequence."""
    execution = _source_execution()
    execution.order_public_id = _PHANTOM_ORDER_PUBLIC_ID
    execution.scope_sequence = 2
    execution.sequence_id = 2
    execution.exec_id = "anchor-fence-phantom-exec"
    execution.trade_id = "anchor-fence-phantom-trade"
    return execution


async def _seed_witnessed_scope(repository: SQLAlchemyRepository) -> None:
    """Seed one fully witnessed booking and its complete lineage."""
    async with repository.session() as s:
        s.add_all(
            [
                _source_symbol(),
                _source_instrument(),
                _source_order(),
                _source_execution(),
                _source_fill(),
            ]
        )
        await s.commit()


async def _seed_annulled_scope(
    repository: SQLAlchemyRepository,
    observed_at: datetime = _T0 - timedelta(minutes=5),
) -> None:
    """Seed one witnessed booking, one unwitnessed booking, and its correction.

    The manifest row is appended directly rather than through the guarded
    writer so its SERVER knowledge stamp can be a fixture instant instead of the
    wall clock: these fences are asserted at fixed horizons, and a row the
    writer stamped with the real ``now`` would fall outside every one of them.
    Its binding is still the real one — the stored target's own public id and
    freshly recomputed canonical digest — so read-time validation is exercised.

    ``observed_at`` is the instant the correction was proven durable, which is
    the only thing that makes it foldable by a read at a named past.
    """
    async with repository.session() as s:
        s.add_all(
            [
                _source_symbol(),
                _source_instrument(),
                _source_order(),
                _phantom_source_order(),
                _source_execution(),
                _phantom_source_execution(),
                _source_fill(),
            ]
        )
        await s.commit()
    async with repository.session() as s:
        phantom = (
            (
                await s.execute(
                    select(Execution).where(Execution.order_public_id == _PHANTOM_ORDER_PUBLIC_ID)
                )
            )
            .scalars()
            .one()
        )
        s.add(
            ExecutionAnnulment(
                public_id=_ANNULMENT_PUBLIC_ID,
                target_execution_public_id=phantom.public_id,
                target_execution_digest=execution_row_digest(
                    SQLAlchemyRepository._execution_chain_record(phantom)
                ),
                wallet_public_id=_WALLET,
                exchange="kraken",
                mode="live",
                scope_sequence=2,
                annulled_by_user_public_id=_ANNULLING_USER,
                correction_time=_T0 - timedelta(minutes=1),
                reason="unwitnessed_phantom",
                evidence_json='{"diagnosis":"unwitnessed booking in the anchor fence fixture"}',
                session_id=_SESSION_PUBLIC_ID,
                sequence_id=9,
                timestamp=_T0 - timedelta(minutes=5),
                known_to=KNOWN_TO_MAX,
            )
        )
        await s.flush()
        s.add(
            ExecutionAnnulmentVisibility(
                annulment_public_id=_ANNULMENT_PUBLIC_ID,
                annulment_id=1,
                observed_at=observed_at,
                wallet_public_id=_WALLET,
                exchange="kraken",
                mode="live",
                session_id=_SESSION_PUBLIC_ID,
                sequence_id=9,
                timestamp=observed_at,
                known_to=KNOWN_TO_MAX,
            )
        )
        await s.commit()


def _manifest_drifted_evidence(
    bundle: PnlTimelineExecutionPrefixBundle,
    drift: Literal["forgotten", "invented"],
) -> PnlTimelineExecutionPrefixBundle:
    """Restate one derivation bundle as if its manifest evidence had moved."""
    mutated = deepcopy(bundle)
    for cut in ("request", "activation"):
        if drift == "forgotten":
            mutated[cut]["annulments"] = []
        else:
            invented = deepcopy(mutated[cut]["annulments"][0])
            invented["public_id"] = "00000000-0000-7000-8000-0000000000df"
            invented["scope_sequence"] = 1
            mutated[cut]["annulments"].append(invented)
    return mutated


@pytest.mark.parametrize("drift", ["forgotten", "invented"])
async def test_atomic_writer_treats_a_manifest_change_as_evidence_drift(
    repository: SQLAlchemyRepository,
    drift: Literal["forgotten", "invented"],
) -> None:
    """An anchor may only persist against the exact corrections it folded.

    Given: A scope whose opening prefix proves only because one unwitnessed
        booking is repudiated by the append-only manifest.
    When: The candidate is offered with evidence whose manifest no longer
        matches the ledger — either derived before the correction landed
        ("forgotten") or claiming a correction the ledger never recorded
        ("invented").
    Then: The atomic writer refuses. The manifest travels INSIDE the prefix
        value, so the same equality check that catches a late execution catches
        a late correction, and an anchor — which is permanent — can never state
        an opening it did not actually derive.
    """
    await _seed_annulled_scope(repository)
    current = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0,
        _T0,
    )
    assert len(current["activation"]["annulments"]) == 1
    evidence = PortfolioPnlAnchorWriteEvidence(
        wallet_public_id=_WALLET,
        mode="live",
        requested_as_of=_T0,
        requested_activation_as_of=_T0,
        execution_prefix_bundle=_manifest_drifted_evidence(current, drift),
    )
    candidate = _atomic_anchor()
    candidate["watermarks_json"] = '{"kraken":2}'

    with pytest.raises(
        PnlTimelineAnchorEvidenceMismatchError,
        match="changed before anchor persistence",
    ):
        await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            candidate,
            evidence,
        )

    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 0


@pytest.mark.parametrize("evidence_kind", ["execution", "fill", "lineage"])
async def test_atomic_writer_exactly_compares_every_prefix_evidence_plane(
    repository: SQLAlchemyRepository,
    evidence_kind: Literal["execution", "fill", "lineage"],
) -> None:
    """Scalar execution, fill-shard, and active lineage changes all refuse."""
    async with repository.session() as s:
        s.add_all(
            [
                _source_symbol(),
                _source_instrument(),
                _source_order(),
                _source_execution(),
                _source_fill(),
            ]
        )
        await s.commit()
    current = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0,
        _T0,
    )
    evidence = PortfolioPnlAnchorWriteEvidence(
        wallet_public_id=_WALLET,
        mode="live",
        requested_as_of=_T0,
        requested_activation_as_of=_T0,
        execution_prefix_bundle=_mutated_prefix_evidence(current, evidence_kind),
    )
    candidate = _atomic_anchor()
    candidate["watermarks_json"] = '{"kraken":1}'

    with pytest.raises(
        PnlTimelineAnchorEvidenceMismatchError,
        match="changed before anchor persistence",
    ):
        await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            candidate,
            evidence,
        )

    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 0


async def test_atomic_writer_refuses_a_backdated_bundle_offered_as_current_truth(
    repository: SQLAlchemyRepository,
) -> None:
    """A caller cannot buy a past cut by declaring it the repository's present.

    Given: A scope holding one real, fully witnessed booking, and evidence whose
        bundle was cut before that booking existed — an empty opening — but
        whose two horizon intents are absent, the shape that says "no horizon
        was requested, this instant is the repository's own".
    When: The candidate anchor is offered against that evidence.
    Then: The write refuses and nothing is persisted. The bundle is
        caller-supplied data, so the fence resolves its horizons from the
        intents alone and re-reads at its OWN present, where the booking is
        plainly there. An anchor is permanent, and this is precisely the anchor
        that would have declared an opening of nothing while real money sat in
        the ledger.
    """
    await _seed_witnessed_scope(repository)
    backdated = _T0 - timedelta(minutes=5)
    forged = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        backdated,
        backdated,
    )
    assert forged["request"] == {"watermarks": {}, "executions": [], "annulments": []}
    evidence = PortfolioPnlAnchorWriteEvidence(
        wallet_public_id=_WALLET,
        mode="live",
        requested_as_of=None,
        requested_activation_as_of=None,
        execution_prefix_bundle=forged,
    )

    s5778_value_1 = _atomic_anchor(point_time=backdated, timestamp=backdated)
    with pytest.raises(
        PnlTimelineAnchorEvidenceMismatchError,
        match="changed before anchor persistence",
    ):
        await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            s5778_value_1,
            evidence,
        )

    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 0
    current = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        None,
        None,
    )
    assert [row["scope_sequence"] for row in current["request"]["executions"]] == [1]


async def test_atomic_writer_persists_a_current_truth_anchor_it_recaptured_itself(
    repository: SQLAlchemyRepository,
) -> None:
    """The honest current-truth path still writes, at instants nobody can repeat.

    Given: A scope holding one real, fully witnessed booking, and evidence
        derived with both intents absent, so both cut instants were captured by
        the repository and the anchor is pinned to them.
    When: The candidate anchor is offered against that evidence.
    Then: It persists unchanged. The fence captures its own, strictly later
        present and cannot reproduce the candidate's — a captured present never
        repeats — so what it compares is the two PROVEN cuts, and those are
        still identical. Comparing the echoed instants instead would refuse
        every honest anchor this repository is asked to create.
    """
    await _seed_witnessed_scope(repository)
    bundle = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        None,
        None,
    )
    evidence = PortfolioPnlAnchorWriteEvidence(
        wallet_public_id=_WALLET,
        mode="live",
        requested_as_of=None,
        requested_activation_as_of=None,
        execution_prefix_bundle=bundle,
    )
    candidate = _atomic_anchor(
        point_time=bundle["activation_as_of"],
        timestamp=bundle["request_as_of"],
    )
    candidate["watermarks_json"] = '{"kraken":1}'

    recorded = await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
        candidate,
        evidence,
    )

    assert recorded == candidate
    assert bundle["request_as_of"] < datetime.now(UTC)
    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 1


async def test_atomic_writer_persists_a_named_past_whose_correction_is_observed(
    repository: SQLAlchemyRepository,
) -> None:
    """A named past still folds a correction once its durability is proven.

    Given: A scope whose opening proves only because one unwitnessed booking is
        repudiated, whose correction carries a visibility observation before the
        named instant, and evidence naming that instant on both cuts.
    When: The candidate anchor is offered against that evidence.
    Then: It persists. Resolving from the intents did not make the historical
        path stricter than the doctrine says it is — a past a caller names is
        answerable exactly when every correction it folds was already proven
        durable at it.
    """
    await _seed_annulled_scope(repository)
    bundle = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0,
        _T0,
    )
    assert len(bundle["activation"]["annulments"]) == 1
    evidence = PortfolioPnlAnchorWriteEvidence(
        wallet_public_id=_WALLET,
        mode="live",
        requested_as_of=_T0,
        requested_activation_as_of=_T0,
        execution_prefix_bundle=bundle,
    )
    candidate = _atomic_anchor()
    candidate["watermarks_json"] = '{"kraken":2}'

    recorded = await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
        candidate,
        evidence,
    )

    assert recorded == candidate
    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 1


async def test_atomic_writer_refuses_a_named_past_whose_correction_is_unobserved(
    repository: SQLAlchemyRepository,
) -> None:
    """A named past may not fold a correction nothing had yet proven durable.

    Given: The same scope, but with the correction's visibility observation
        landing AFTER the named instant, and evidence forged to carry the folded
        prefix while its echoed instants and both intents name that earlier past.
    When: The candidate anchor is offered against that evidence.
    Then: The fenced re-read refuses, because at a named past the correction is
        not yet visible and the booking it repudiates is back and unwitnessed —
        the prefix cannot be proven at all. The visibility requirement is
        untouched by resolving horizons from the intents; it is what a named
        past has owed since the observation ledger replaced the settling margin.
    """
    await _seed_annulled_scope(repository, observed_at=_T0 + timedelta(minutes=5))
    visible = await repository.get_pnl_timeline_execution_prefix_bundle(
        _WALLET,
        "live",
        _T0 + timedelta(minutes=10),
        _T0 + timedelta(minutes=10),
    )
    assert len(visible["activation"]["annulments"]) == 1
    forged = deepcopy(visible)
    forged["request_as_of"] = _T0
    forged["activation_as_of"] = _T0
    evidence = PortfolioPnlAnchorWriteEvidence(
        wallet_public_id=_WALLET,
        mode="live",
        requested_as_of=_T0,
        requested_activation_as_of=_T0,
        execution_prefix_bundle=forged,
    )
    candidate = _atomic_anchor()
    candidate["watermarks_json"] = '{"kraken":2}'

    with pytest.raises(
        PnlTimelineAnchorEvidenceMismatchError,
        match="cannot be proven",
    ):
        await repository.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            candidate,
            evidence,
        )

    async with repository.session() as s:
        anchor_count = await s.scalar(select(func.count()).select_from(PortfolioPnlPoint))
    assert anchor_count == 0


async def test_record_stores_and_returns_every_anchor_field(
    repository: SQLAlchemyRepository,
) -> None:
    """A successful first insert round-trips the complete typed payload."""
    anchor = _anchor()

    recorded = await repository.record_portfolio_pnl_anchor(anchor)
    loaded = await repository.get_portfolio_pnl_anchor(_WALLET, "live", "USD", None)

    assert recorded == anchor
    assert loaded == anchor
    async with repository.session() as s:
        row = (await s.execute(select(PortfolioPnlPoint))).scalar_one()
        assert row.known_to == KNOWN_TO_MAX


async def test_wallet_uuid_aliases_share_one_persisted_anchor(
    repository: SQLAlchemyRepository,
) -> None:
    """Uppercase and lowercase UUID spellings remain one durable scope."""
    uppercase = _malformed_anchor(wallet_public_id=_WALLET.upper())

    recorded = await repository.record_portfolio_pnl_anchor(uppercase)
    repeated = await repository.record_portfolio_pnl_anchor(_anchor())
    loaded = await repository.get_portfolio_pnl_anchor(
        _WALLET.upper(),
        "live",
        "USD",
        None,
    )

    assert recorded["wallet_public_id"] == _WALLET
    assert repeated == recorded
    assert loaded == recorded
    async with repository.session() as s:
        assert await s.scalar(select(func.count()).select_from(PortfolioPnlPoint)) == 1


def test_anchor_scope_identity_is_normalized_and_deterministic() -> None:
    """Equivalent scope spellings share one stable UUID5 anchor identity."""
    assert normalize_portfolio_pnl_valuation_ccy(" usd ") == "USD"
    assert portfolio_pnl_anchor_public_id(_WALLET.upper(), "live", " usd ") == _ANCHOR_PUBLIC_ID
    assert portfolio_pnl_anchor_public_id(_WALLET, "paper", "USD") != _ANCHOR_PUBLIC_ID
    assert portfolio_pnl_anchor_public_id(_WALLET, "live", "EUR") != _ANCHOR_PUBLIC_ID


@pytest.mark.parametrize(
    ("wallet_public_id", "mode", "valuation_ccy", "message"),
    [
        pytest.param("not-a-uuid", "live", "USD", "wallet identity", id="wallet"),
        pytest.param(_WALLET, "shadow", "USD", "mode", id="mode"),
        pytest.param(_WALLET, "live", "US", "three-letter", id="currency_length"),
        pytest.param(_WALLET, "live", "U1D", "three-letter", id="currency_characters"),
    ],
)
def test_anchor_scope_identity_rejects_invalid_components(
    wallet_public_id: str,
    mode: str,
    valuation_ccy: str,
    message: str,
) -> None:
    """Invalid scope components cannot mint an alternative durable identity."""
    with pytest.raises(ValueError, match=message):
        portfolio_pnl_anchor_public_id(wallet_public_id, mode, valuation_ccy)


async def test_historical_anchor_read_uses_half_open_temporal_boundaries(
    repository: SQLAlchemyRepository,
) -> None:
    """Explicit horizons select exact versions while current uses the sentinel."""
    historical = _anchor()
    current = _anchor(timestamp=_FUTURE)
    current["unrealized_pnl"] = 900.0
    current["opening_basket_json"] = '{"native":{"BTC":"2"},"pools":{}}'
    async with repository.session() as s:
        s.add_all(
            [
                PortfolioPnlPoint(**historical, known_to=_FUTURE),
                PortfolioPnlPoint(**current, known_to=KNOWN_TO_MAX),
            ]
        )
        await s.commit()

    before = await repository.get_portfolio_pnl_anchor(
        _WALLET, "live", "USD", _T0 - timedelta(microseconds=1)
    )
    at_start = await repository.get_portfolio_pnl_anchor(_WALLET, "live", "USD", _T0)
    before_successor = await repository.get_portfolio_pnl_anchor(
        _WALLET, "live", "USD", _FUTURE - timedelta(microseconds=1)
    )
    at_successor = await repository.get_portfolio_pnl_anchor(_WALLET, "live", "USD", _FUTURE)
    sentinel_current = await repository.get_portfolio_pnl_anchor(_WALLET, "live", "USD", None)

    assert before is None
    assert at_start == historical
    assert before_successor == historical
    assert at_successor == current
    assert sentinel_current == current


async def test_current_anchor_read_fails_on_multiple_active_rows(
    repository: SQLAlchemyRepository,
) -> None:
    """Corrupt duplicate anchors are never reduced to an arbitrary winner."""
    first = _anchor()
    second = _anchor(
        public_id="00000000-0000-7000-8000-000000000105",
        point_time=_T0 + timedelta(minutes=1),
    )
    async with repository.session() as s:
        s.add_all(
            [
                PortfolioPnlPoint(**first, known_to=KNOWN_TO_MAX),
                PortfolioPnlPoint(**second, known_to=KNOWN_TO_MAX),
            ]
        )
        await s.commit()

    with pytest.raises(MultipleResultsFound):
        await repository.get_portfolio_pnl_anchor(_WALLET, "live", "USD", None)


async def test_concurrent_different_minute_anchor_inserts_return_one_physical_winner(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent different-minute candidates share one unique scope identity."""
    first = _anchor()
    second = _anchor(point_time=_T0 + timedelta(minutes=1))
    second["unrealized_pnl"] = 913.5
    second["mark_time"] = second["point_time"]
    second["opening_basket_json"] = '{"native":{"ETH":"4"},"pools":{}}'
    original_read = repository._read_current_portfolio_pnl_anchor
    both_candidates_ready = asyncio.Event()
    pre_read_arrivals = 0

    async def synchronize_empty_pre_reads(
        s: AsyncSession,
        wallet_public_id: str,
        mode: str,
        valuation_ccy: str,
    ) -> PortfolioPnlPoint | None:
        """Force both candidates to reach their insert after an empty pre-read."""
        nonlocal pre_read_arrivals
        pre_read_arrivals += 1
        if pre_read_arrivals <= 2:
            if pre_read_arrivals == 2:
                both_candidates_ready.set()
            await asyncio.wait_for(both_candidates_ready.wait(), timeout=5.0)
            return None
        return await original_read(s, wallet_public_id, mode, valuation_ccy)

    monkeypatch.setattr(
        repository,
        "_read_current_portfolio_pnl_anchor",
        synchronize_empty_pre_reads,
    )

    results = await asyncio.gather(
        repository.record_portfolio_pnl_anchor(first),
        repository.record_portfolio_pnl_anchor(second),
    )

    assert results[0] == results[1]
    assert results[0] in (first, second)
    assert await repository.get_portfolio_pnl_anchor(_WALLET, "live", "USD", None) == results[0]
    assert pre_read_arrivals == 4
    async with repository.session() as s:
        count = (await s.execute(select(func.count()).select_from(PortfolioPnlPoint))).scalar_one()
        assert count == 1


async def test_sequential_different_minute_anchor_insert_returns_first_winner(
    repository: SQLAlchemyRepository,
) -> None:
    """A later valid candidate cannot create a second active scope anchor."""
    first = _anchor()
    second = _anchor(point_time=_T0 + timedelta(minutes=1))
    second["unrealized_pnl"] = 913.5
    second["mark_time"] = second["point_time"]
    second["opening_basket_json"] = '{"native":{"ETH":"4"},"pools":{}}'

    first_result = await repository.record_portfolio_pnl_anchor(first)
    second_result = await repository.record_portfolio_pnl_anchor(second)

    assert first_result == first
    assert second_result == first
    async with repository.session() as s:
        count = (await s.execute(select(func.count()).select_from(PortfolioPnlPoint))).scalar_one()
    assert count == 1


async def test_anchor_integrity_error_without_winner_is_reraised(
    repository: SQLAlchemyRepository,
) -> None:
    """A database failure beyond structural guards is not mistaken for a race."""
    invalid = _malformed_anchor(session_id=None)

    with pytest.raises(IntegrityError):
        await repository.record_portfolio_pnl_anchor(invalid)

    assert await repository.get_portfolio_pnl_anchor(_WALLET, "live", "USD", None) is None


async def test_anchor_pre_read_does_not_accept_a_noncanonical_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """A pre-existing noncanonical row is corruption, never a scope winner."""
    existing = _anchor(public_id="00000000-0000-7000-8000-000000000199")
    async with repository.session() as s:
        s.add(PortfolioPnlPoint(**existing, known_to=KNOWN_TO_MAX))
        await s.commit()

    s5778_value_1 = _anchor()
    with pytest.raises(ValueError, match="public_id"):
        await repository.record_portfolio_pnl_anchor(s5778_value_1)


async def test_anchor_read_rejects_a_structurally_incomplete_stored_row(
    repository: SQLAlchemyRepository,
) -> None:
    """A schema-valid incomplete anchor never escapes as the complete TypedDict."""
    incomplete = _malformed_anchor(
        valuation_status="incomplete",
        unrealized_pnl=None,
        mark_source=None,
        mark_time=None,
    )
    async with repository.session() as s:
        s.add(PortfolioPnlPoint(**incomplete, known_to=KNOWN_TO_MAX))
        await s.commit()

    with pytest.raises(ValueError, match="valuation_status"):
        await repository.get_portfolio_pnl_anchor(_WALLET, "live", "USD", None)


async def test_anchor_race_winner_is_validated_before_return(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed pre-existing winner cannot bypass the candidate writer guard."""
    incomplete = _malformed_anchor(
        valuation_status="incomplete",
        unrealized_pnl=None,
        mark_source=None,
        mark_time=None,
    )
    async with repository.session() as s:
        s.add(PortfolioPnlPoint(**incomplete, known_to=KNOWN_TO_MAX))
        await s.commit()
    original_read = repository._read_current_portfolio_pnl_anchor
    read_count = 0

    async def hide_winner_from_pre_read(
        s: AsyncSession,
        wallet_public_id: str,
        mode: str,
        valuation_ccy: str,
    ) -> PortfolioPnlPoint | None:
        """Expose the stored row only after the candidate insert loses its race."""
        nonlocal read_count
        read_count += 1
        if read_count == 1:
            return None
        return await original_read(s, wallet_public_id, mode, valuation_ccy)

    monkeypatch.setattr(
        repository,
        "_read_current_portfolio_pnl_anchor",
        hide_winner_from_pre_read,
    )

    s5778_value_1 = _anchor()
    with pytest.raises(ValueError, match="valuation_status"):
        await repository.record_portfolio_pnl_anchor(s5778_value_1)


async def test_anchor_read_rejects_nonfinite_optional_component(
    repository: SQLAlchemyRepository,
) -> None:
    """A non-finite optional value stored out of band never escapes the DAL."""
    malformed = _anchor()
    malformed["cash_usd"] = float("inf")
    async with repository.session() as s:
        s.add(PortfolioPnlPoint(**malformed, known_to=KNOWN_TO_MAX))
        await s.commit()

    with pytest.raises(ValueError, match="cash_usd"):
        await repository.get_portfolio_pnl_anchor(_WALLET, "live", "USD", None)


async def test_anchor_read_rejects_non_strict_stored_json(
    repository: SQLAlchemyRepository,
) -> None:
    """A non-standard JSON constant stored out of band never escapes the DAL."""
    malformed = _anchor()
    malformed["opening_basket_json"] = '{"nested":{"value":NaN}}'
    async with repository.session() as s:
        s.add(PortfolioPnlPoint(**malformed, known_to=KNOWN_TO_MAX))
        await s.commit()

    with pytest.raises(ValueError, match="constant"):
        await repository.get_portfolio_pnl_anchor(_WALLET, "live", "USD", None)


@pytest.mark.parametrize(
    ("field_name", "raw", "message"),
    [
        pytest.param(
            "watermarks_json",
            '{"kraken":NaN}',
            "constant",
            id="watermark_nan",
        ),
        pytest.param(
            "opening_basket_json",
            '{"pools":{"pool-a":Infinity}}',
            "constant",
            id="basket_infinity",
        ),
        pytest.param(
            "contributions_json",
            '{"unattributed":1e400}',
            "finite",
            id="contribution_overflow",
        ),
        pytest.param(
            "opening_basket_json",
            '{"pools":{"pool-a":1,"pool-a":2}}',
            "duplicate",
            id="nested_duplicate_key",
        ),
    ],
)
async def test_anchor_writer_rejects_non_strict_json_before_insert(
    repository: SQLAlchemyRepository,
    field_name: str,
    raw: str,
    message: str,
) -> None:
    """Every anchor JSON field rejects non-standard or lossy numeric input."""
    s5778_value_1 = _malformed_anchor(**{field_name: raw})
    with pytest.raises(ValueError, match=message):
        await repository.record_portfolio_pnl_anchor(s5778_value_1)

    async with repository.session() as s:
        count = (await s.execute(select(func.count()).select_from(PortfolioPnlPoint))).scalar_one()
    assert count == 0


async def test_anchor_writer_accepts_finite_nested_json_arrays(
    repository: SQLAlchemyRepository,
) -> None:
    """Strict JSON validation preserves ordinary nested finite payloads."""
    anchor = _anchor()
    anchor["contributions_json"] = '{"weights":[1.25,{"residual":2}]}'

    assert await repository.record_portfolio_pnl_anchor(anchor) == anchor


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        pytest.param({"point_kind": "sample"}, "point_kind", id="sample_kind"),
        pytest.param({"mode": "shadow"}, "mode", id="bad_mode"),
        pytest.param(
            {"valuation_status": "incomplete"},
            "valuation_status",
            id="incomplete_valuation",
        ),
        pytest.param({"realized_pnl": 1.0}, "finite zero", id="nonzero_realized"),
        pytest.param({"fee_pnl": float("nan")}, "finite zero", id="nonfinite_fee"),
        pytest.param({"unrealized_pnl": None}, "unrealized_pnl", id="missing_unrealized"),
        pytest.param(
            {"unrealized_pnl": float("inf")},
            "unrealized_pnl",
            id="nonfinite_unrealized",
        ),
        pytest.param({"cash_usd": float("inf")}, "cash_usd", id="nonfinite_cash"),
        pytest.param(
            {"position_value_usd": float("nan")},
            "position_value_usd",
            id="nonfinite_position_value",
        ),
        pytest.param({"drawdown": False}, "drawdown", id="boolean_drawdown"),
        pytest.param({"mark_source": ""}, "mark_source", id="empty_mark_source"),
        pytest.param({"mark_time": None}, "mark_time", id="missing_mark_time"),
        pytest.param(
            {"mark_time": _T0 + timedelta(minutes=1)},
            "mark_time",
            id="misaligned_mark_time",
        ),
        pytest.param({"watermarks_json": None}, "watermarks_json", id="missing_watermarks"),
        pytest.param({"opening_basket_json": ""}, "opening_basket_json", id="empty_basket"),
        pytest.param(
            {"contributions_json": "[]"},
            "contributions_json",
            id="nonobject_contributions",
        ),
        pytest.param({"watermarks_json": "{"}, "valid JSON", id="invalid_watermark_json"),
        pytest.param(
            {"public_id": "00000000-0000-7000-8000-000000000199"},
            "public_id",
            id="noncanonical_public_id",
        ),
        pytest.param({"valuation_ccy": None}, "must be a string", id="nonstring_currency"),
        pytest.param({"valuation_ccy": "usd"}, "normalized", id="noncanonical_currency"),
    ],
)
async def test_anchor_writer_rejects_malformed_structure_before_insert(
    repository: SQLAlchemyRepository,
    overrides: dict[str, object],
    message: str,
) -> None:
    """Malformed anchor contracts fail before they can poison the active scope."""
    s5778_value_1 = _malformed_anchor(**overrides)
    with pytest.raises(ValueError, match=message):
        await repository.record_portfolio_pnl_anchor(s5778_value_1)

    async with repository.session() as s:
        count = (await s.execute(select(func.count()).select_from(PortfolioPnlPoint))).scalar_one()
    assert count == 0


@pytest.mark.parametrize(
    "field_name",
    ["requested_as_of", "requested_activation_as_of"],
)
def test_atomic_write_evidence_refuses_a_claimed_horizon_its_prefix_lacks(
    field_name: str,
) -> None:
    """Evidence cannot claim a horizon the prefix it carries was not read at.

    Given: Evidence whose nullable intent for one cut names an instant that is
        neither ``None`` nor the instant the bundle reports for that cut.
    When: The write evidence is validated.
    Then: It is refused by name. Without this the intent fields would be
        decorative: a caller could name a past horizon on one cut, leave the
        intent ``None``, and have the fenced re-read reproduce a current-truth
        exemption that nothing established.
    """
    evidence = _empty_anchor_write_evidence()
    runtime_evidence = cast(dict[str, object], evidence)
    runtime_evidence[field_name] = _T0 + timedelta(minutes=5)

    s5778_value_1 = _atomic_anchor()
    with pytest.raises(
        PnlTimelineAnchorEvidenceMismatchError,
        match="claims a horizon its prefix was not read at",
    ):
        SQLAlchemyRepository._validate_portfolio_pnl_anchor_write_evidence(
            s5778_value_1,
            evidence,
        )


@pytest.mark.parametrize(
    "field_name",
    ["requested_as_of", "requested_activation_as_of"],
)
def test_atomic_write_evidence_accepts_an_absent_horizon_intent(
    field_name: str,
) -> None:
    """``None`` passes validation and buys the instant beside it nothing.

    Given: Evidence whose intent for one cut is absent — the shape produced when
        no horizon was requested and the repository derived the instant itself —
        while the bundle still echoes a fixed past instant on that cut.
    When: The write evidence is validated, and then the fence re-derives the
        cuts from that same evidence.
    Then: Validation passes, because ``None`` is not a claim about the past and
        there is nothing to contradict. It does NOT certify the echoed instant:
        no horizon the fence goes on to resolve is both exempt and dated at that
        echo. An absent request intent captures a fresh present instead, and an
        absent ACTIVATION intent is not an exemption at all — it is the request
        cut's own minute, inheriting that cut's provenance, historical here
        because the request cut was named. The echoed instant survives only as
        the anchor timestamp the rest of this validator pins it to.
    """
    evidence = _empty_anchor_write_evidence()
    runtime_evidence = cast(dict[str, object], evidence)
    runtime_evidence[field_name] = None

    SQLAlchemyRepository._validate_portfolio_pnl_anchor_write_evidence(
        _atomic_anchor(),
        evidence,
    )

    horizons = _resolved_execution_prefix_cut_horizons(
        evidence["requested_as_of"],
        evidence["requested_activation_as_of"],
    )
    assert all(horizon.requested or horizon.as_of > _T0 for horizon in horizons)
