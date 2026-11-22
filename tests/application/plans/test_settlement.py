"""Unit and SQLite integration tests for deterministic execution-plan settlement."""

import sys
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import select

from snapper.application.plans import settlement as settlement_module
from snapper.application.plans.settlement import PlanSettlementCandidate
from snapper.application.plans.settlement import PlanSettlementConfigError
from snapper.application.plans.settlement import PlanSettlementDriftError
from snapper.application.plans.settlement import PlanSettlementSkipped
from snapper.application.plans.settlement import apply_plan_settlements
from snapper.application.plans.settlement import extract_plan_child_ids
from snapper.application.plans.settlement import inspect_plan_settlement
from snapper.application.plans.settlement import preview_plan_settlements
from snapper.application.plans.settlement import serialize_candidate
from snapper.application.plans.settlement import settle_single_recovered_plan
from snapper.application.plans.settlement import settlement_code_revision
from snapper.application.plans.settlement import settlement_database_endpoint
from snapper.core.json_types import JsonObject
from snapper.data.models import ExecutionPlan
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventInsertRow
from snapper.data.repository_types import VenueEventRow

_T0 = datetime(2026, 11, 15, 11, 0, 0, tzinfo=UTC)
_WALLET_ID = "01960000-0000-7000-8000-000000000001"
_INSTRUMENT_ID = "01960000-0000-7000-8000-000000000002"
_USER_ID = "01960000-0000-7000-8000-000000000003"


def _plan_row(
    *,
    public_id: str = "01960000-0000-7000-8000-000000000010",
    plan_type: str = "manual_once",
    status: str = "active",
    params: JsonObject | None = None,
    filled_quantity: float = 0.0,
) -> ExecutionPlanRow:
    """Build a synthetic `ExecutionPlanRow` for pure inspection tests.

    Args:
        public_id: Public id of the execution plan.
        plan_type: Evaluator plan type.
        status: Current plan status.
        params: Optional plan parameter dictionary.
        filled_quantity: Current filled quantity on the plan row.

    Returns:
        Typed `ExecutionPlanRow` dictionary.
    """
    resolved_params: JsonObject = (
        {"child_client_order_id": "cid-1", "native_instrument": "XRP-EUR"}
        if params is None
        else params
    )
    return {
        "public_id": public_id,
        "timestamp": _T0,
        "session_id": "sess-1",
        "sequence_id": 1,
        "plan_type": plan_type,
        "created_by_user_id": _USER_ID,
        "created_by_strategy": None,
        "created_via": "api",
        "instrument_public_id": _INSTRUMENT_ID,
        "exchange": "kraken",
        "mode": "live",
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "wallet_public_id": _WALLET_ID,
        "operator_public_id": None,
        "total_quantity": 10.0,
        "filled_quantity": filled_quantity,
        "side": "buy",
        "parent_plan_public_id": None,
        "position_cycle_public_id": None,
        "params": resolved_params,
        "status": status,
        "created_at": _T0,
        "started_at": _T0,
        "completed_at": None,
        "expires_at": None,
        "cancel_requested_at": _T0 if status == "cancel_requested" else None,
        "last_evaluated_at": None,
        "last_error": None,
        "idempotency_key": None,
        "cancel_idempotency_key": None,
    }


def _command_row(
    *,
    client_order_id: str = "cid-1",
    status: str = "filled",
    plan_public_id: str | None = "01960000-0000-7000-8000-000000000010",
    last_error: str | None = None,
    exchange_order_id: str | None = "EX-1",
) -> TradeCommandRow:
    """Build a synthetic `TradeCommandRow` for pure inspection tests.

    Args:
        client_order_id: Client order id on the command.
        status: Command status string.
        plan_public_id: Linked execution plan public id.
        last_error: Optional error message on the command.
        exchange_order_id: Venue order id stored on the command.

    Returns:
        Typed `TradeCommandRow` dictionary.
    """
    return {
        "public_id": "01960000-0000-7000-8000-000000000020",
        "timestamp": _T0,
        "session_id": "sess-1",
        "sequence_id": 1,
        "command_type": "create",
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "exchange": "kraken",
        "instrument": "XRP-EUR",
        "mode": "live",
        "strategy_id": "manual_once",
        "client_order_id": client_order_id,
        "venue_client_id": client_order_id,
        "exchange_order_id": exchange_order_id,
        "side": "buy",
        "order_type": "market",
        "quantity": 10.0,
        "price": None,
        "stop_price": None,
        "leverage": None,
        "reduce_only": False,
        "post_only": False,
        "status": status,
        "created_at": _T0,
        "dispatched_at": _T0,
        "acked_at": _T0,
        "terminal_at": _T0,
        "attempt_count": 1,
        "last_error": last_error,
        "correlation_id": plan_public_id or "corr-1",
        "supersedes_command_id": None,
        "wallet_public_id": _WALLET_ID,
        "operator_public_id": None,
        "user_public_id": _USER_ID,
        "source_surface": "api",
        "idempotency_key": None,
        "plan_public_id": plan_public_id,
        "submitted_notional_usd": None,
    }


def _venue_event_row(
    *,
    event_id: int,
    event_type: str,
    status: str | None = None,
    cum_fill_size: float | None = None,
) -> VenueEventRow:
    """Build a synthetic `VenueEventRow` for pure inspection tests.

    Args:
        event_id: Monotonic database row id.
        event_type: Venue event type string.
        status: Optional order status string.
        cum_fill_size: Optional cumulative fill quantity.

    Returns:
        Typed `VenueEventRow` dictionary.
    """
    return {
        "id": event_id,
        "public_id": f"01960000-0000-7000-8000-{event_id:012d}",
        "timestamp": _T0,
        "session_id": "sess-1",
        "sequence_id": event_id,
        "event_type": event_type,
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "exchange": "kraken",
        "instrument": "XRP-EUR",
        "mode": "live",
        "command_public_id": None,
        "client_order_id": "cid-1",
        "venue_client_id": "cid-1",
        "exchange_order_id": "EX-1",
        "exec_id": None,
        "trade_id": None,
        "side": "buy",
        "status": status,
        "fill_size": cum_fill_size,
        "fill_price": 2.5 if cum_fill_size is not None else None,
        "cum_fill_size": cum_fill_size,
        "fee": None,
        "fee_asset": None,
        "liquidity_role": "taker",
        "error": None,
        "venue_timestamp": _T0,
        "received_at": _T0 + timedelta(seconds=event_id),
        "wallet_public_id": _WALLET_ID,
    }


def _mock_reader(
    *,
    command: TradeCommandRow | None = None,
    events: list[VenueEventRow] | None = None,
    fill_event: VenueEventRow | None = None,
    latest_event_at: datetime | None = None,
    order_exchange_order_id: str | None = None,
) -> MagicMock:
    """Construct a typed mock `PlanSettlementReader`.

    Args:
        command: Active child command returned by lookup.
        events: Lifecycle venue events returned for the child.
        fill_event: Cumulative fill venue event returned for the child.
        latest_event_at: Timestamp returned by `get_latest_venue_event_received_at`.
        order_exchange_order_id: Venue id returned by the active Order lookup.

    Returns:
        Configured `MagicMock` reader.
    """
    reader = MagicMock()
    reader.get_active_execution_plans = AsyncMock(return_value=[])
    reader.get_active_create_command_by_client_order_id = AsyncMock(return_value=command)
    reader.get_exchange_order_id_for_client_order_id = AsyncMock(
        return_value=order_exchange_order_id
    )
    reader.get_order_lifecycle_events = AsyncMock(return_value=events or [])
    reader.get_max_cumulative_fill_venue_event = AsyncMock(return_value=fill_event)
    reader.get_latest_venue_event_received_at = AsyncMock(return_value=latest_event_at)
    return reader


@pytest_asyncio.fixture
async def sqlite_repo(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create and dispose an isolated SQLite repository for settlement tests.

    Args:
        tmp_path: Pytest temporary directory.

    Yields:
        Initialized `SQLAlchemyRepository`.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'settlement.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _seed_plan_with_child(
    repo: SQLAlchemyRepository,
    *,
    created_at: datetime,
    plan_status: str,
    child_order_id: str,
    child_status: str,
) -> str:
    """Insert an execution plan and linked child create command in SQLite.

    Args:
        repo: Target SQLite repository.
        created_at: Creation timestamp for deterministic ordering.
        plan_status: Initial status of the execution plan.
        child_order_id: Client order id of the child create command.
        child_status: Initial status of the child create command.

    Returns:
        Assigned `plan_public_id`.
    """
    plan_insert: ExecutionPlanInsertRow = {
        "plan_type": "manual_once",
        "created_by_user_id": _USER_ID,
        "created_via": "api",
        "instrument_public_id": _INSTRUMENT_ID,
        "exchange": "kraken",
        "mode": "live",
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "wallet_public_id": _WALLET_ID,
        "operator_public_id": None,
        "total_quantity": 10.0,
        "side": "buy",
        "params": {
            "order_type": "market",
            "side": "buy",
            "child_client_order_id": child_order_id,
            "native_instrument": "XRP-EUR",
        },
        "status": plan_status,
        "created_at": created_at,
        "session_id": "seed-sess",
        "sequence_id": 1,
        "timestamp": created_at,
    }
    _row_id, plan_public_id = await repo.insert_execution_plan(plan_insert)
    cmd_insert: TradeCommandInsertRow = {
        "command_type": "create",
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "exchange": "kraken",
        "instrument": "XRP-EUR",
        "mode": "live",
        "strategy_id": "manual_once",
        "client_order_id": child_order_id,
        "venue_client_id": child_order_id,
        "side": "buy",
        "order_type": "market",
        "quantity": 10.0,
        "status": "dispatched",
        "created_at": created_at,
        "correlation_id": plan_public_id,
        "session_id": "seed-sess",
        "sequence_id": 2,
        "timestamp": created_at,
        "wallet_public_id": _WALLET_ID,
        "plan_public_id": plan_public_id,
    }
    _cmd_id, cmd_public_id = await repo.insert_trade_command(cmd_insert)
    last_error: str | None = None
    if child_status == "expired":
        last_error = "expired by outbox dispatch TTL (age 90.0s > 60.0s)"
    elif child_status == "failed":
        last_error = "order_interlock_blocked"
    elif child_status == "rejected":
        last_error = "EOrder:Insufficient funds"
    await repo.advance_trade_command_lifecycle(
        public_id=cmd_public_id,
        expected_status="dispatched",
        new_status=child_status,
        bus_time=created_at + timedelta(milliseconds=100),
        session_id="seed-sess",
        sequence_id=3,
        terminal_at=(
            created_at + timedelta(milliseconds=100) if child_status != "accepted" else None
        ),
        last_error=last_error,
    )
    return plan_public_id


def test_settlement_endpoint_and_code_revision_validate_and_bind_cleanly(
    tmp_path: Path,
) -> None:
    """Canonicalize credential-free DB endpoints and reject unsafe URLs.

    Given: Valid file-SQLite and SSL PostgreSQL URLs, malformed/in-memory/optioned
        URLs, an unsupported dialect URL, and `settlement_code_revision`.
    When: `settlement_database_endpoint` and `settlement_code_revision` execute.
    Then: Valid URLs strip credentials, unsafe URLs raise
        `PlanSettlementConfigError`, and module SHA-256 digests are returned.
    """
    sqlite_file = tmp_path / "target.db"
    sqlite_ep = settlement_database_endpoint(f"sqlite+aiosqlite:///{sqlite_file}")
    assert sqlite_ep == {"dialect": "sqlite", "path": str(sqlite_file.resolve())}

    pg_ep = settlement_database_endpoint(
        "postgresql+asyncpg://operator:secret@db.internal:5432/snapper?ssl=require"
    )
    assert pg_ep == {
        "dialect": "postgresql",
        "host": "db.internal",
        "port": 5432,
        "database": "snapper",
    }

    for invalid_url in (
        "sqlite+aiosqlite:///:memory:",
        f"sqlite+aiosqlite:///{sqlite_file}?mode=ro",
        "postgresql+asyncpg://operator:secret@db.internal/snapper",
        "postgresql+asyncpg://operator:secret@a,b:5432/snapper",
        "postgresql+asyncpg://operator:secret@db.internal:5432/snapper?options=-cfoo=1",
        "mysql+aiomysql://operator:secret@db.internal:3306/snapper",
    ):
        with pytest.raises(PlanSettlementConfigError):
            settlement_database_endpoint(invalid_url)

    revisions = settlement_code_revision((sys.modules[__name__],))
    assert "snapper/application/plans/settlement.py" in revisions
    assert "snapper/data/plan_settlement_reads.py" in revisions
    assert "snapper/application/trade/cancel_target.py" in revisions


def test_extract_plan_child_ids_preserves_order_and_deduplicates() -> None:
    """Extract child order ids deterministically from list and scalar fields.

    Given: Plan params containing list entries, duplicates, non-strings, empty
        strings, and a trailing scalar `child_client_order_id`.
    When: `extract_plan_child_ids` parses the params dictionary.
    Then: Only unique non-empty strings are returned in first-seen order.
    """
    params: dict[str, object] = {
        "child_client_order_ids": ["cid-a", "", 42, "cid-b", "cid-a"],
        "child_client_order_id": "cid-c",
    }
    assert extract_plan_child_ids(params) == ["cid-a", "cid-b", "cid-c"]
    assert extract_plan_child_ids(
        {"child_client_order_ids": ["cid-a"], "child_client_order_id": "cid-a"}
    ) == ["cid-a"]
    assert (
        extract_plan_child_ids({"child_client_order_ids": "invalid", "child_client_order_id": ""})
        == []
    )


@pytest.mark.asyncio
async def test_inspect_skips_unsupported_plan_type_and_empty_children() -> None:
    """Skip non-single-order plan types and plans without child order ids.

    Given: One `passive_mm` plan and one `trailing_stop` plan with no child ids.
    When: `inspect_plan_settlement` inspects each plan.
    Then: Both return `PlanSettlementSkipped` with specific skip codes.
    """
    reader = _mock_reader()
    unsupported = await inspect_plan_settlement(reader, _plan_row(plan_type="passive_mm"))
    no_children = await inspect_plan_settlement(
        reader, _plan_row(plan_type="trailing_stop", params={"native_instrument": "XRP-EUR"})
    )

    assert isinstance(unsupported, PlanSettlementSkipped)
    assert unsupported.skip_code == "unsupported_plan_type"
    assert isinstance(no_children, PlanSettlementSkipped)
    assert no_children.skip_code == "no_child_orders"


@pytest.mark.asyncio
async def test_inspect_skips_missing_mismatched_and_non_terminal_children() -> None:
    """Skip plans whose child create command is absent, foreign, or still open.

    Given: Three active `manual_once` plans with missing, foreign, and
        `accepted` child commands respectively.
    When: `inspect_plan_settlement` inspects each plan.
    Then: Each returns `PlanSettlementSkipped` with the matching skip code.
    """
    plan = _plan_row()
    missing = await inspect_plan_settlement(_mock_reader(command=None), plan)
    mismatch = await inspect_plan_settlement(
        _mock_reader(command=_command_row(plan_public_id="01960000-0000-7000-8000-000000000099")),
        plan,
    )
    open_child = await inspect_plan_settlement(
        _mock_reader(command=_command_row(status="accepted")),
        plan,
    )

    assert isinstance(missing, PlanSettlementSkipped)
    assert missing.skip_code == "missing_child_command"
    assert isinstance(mismatch, PlanSettlementSkipped)
    assert mismatch.skip_code == "plan_link_mismatch"
    assert isinstance(open_child, PlanSettlementSkipped)
    assert open_child.skip_code == "non_terminal_child"


@pytest.mark.asyncio
async def test_inspect_maps_active_plan_terminal_children_and_evidence_ids() -> None:
    """Map terminal children and record qualifying venue event evidence ids.

    Given: Active `manual_once` plans whose child commands ended `filled` (with
        `order_terminal(closed)` and fallback `fill_observed`), `rejected`,
        pre-venue `failed`, `cancelled`, and `expired`.
    When: `inspect_plan_settlement` inspects each plan.
    Then: Each candidate carries the expected terminal status, fill size,
        bounded error text, `terminal_venue_event_id`, and `fill_venue_event_id`.
    """
    plan = _plan_row()
    filled_with_terminal = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="filled"),
            events=[
                _venue_event_row(event_id=2, event_type="fill_observed", cum_fill_size=10.0),
                _venue_event_row(event_id=5, event_type="order_terminal", status="closed"),
            ],
            fill_event=_venue_event_row(event_id=2, event_type="fill_observed", cum_fill_size=10.0),
        ),
        plan,
    )
    filled_fallback_fill_id = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="filled"),
            events=[_venue_event_row(event_id=3, event_type="fill_observed", cum_fill_size=10.0)],
            fill_event=_venue_event_row(event_id=3, event_type="fill_observed", cum_fill_size=10.0),
        ),
        plan,
    )
    filled_without_event = await inspect_plan_settlement(
        _mock_reader(command=_command_row(status="filled"), fill_event=None),
        plan,
    )
    rejected_default_err = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="rejected", last_error=None),
            events=[
                _venue_event_row(event_id=1, event_type="order_accepted"),
                _venue_event_row(event_id=2, event_type="order_rejected"),
            ],
        ),
        plan,
    )
    failed_long_err = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="failed", last_error="x" * 1500),
            events=[_venue_event_row(event_id=7, event_type="order_breaker_open")],
        ),
        plan,
    )
    failed_default_plan = _plan_row()
    failed_default_plan["last_error"] = "order_interlock_blocked"
    failed_default_err = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="failed", last_error=None),
            events=[_venue_event_row(event_id=8, event_type="order_interlock_blocked")],
        ),
        failed_default_plan,
    )
    cancelled_cand = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="cancelled"),
            events=[
                _venue_event_row(event_id=10, event_type="order_terminal", status="canceled"),
                _venue_event_row(event_id=11, event_type="order_terminal", status="open"),
            ],
        ),
        plan,
    )
    expired_cand = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="expired"),
            events=[_venue_event_row(event_id=11, event_type="order_terminal", status="expired")],
        ),
        plan,
    )

    assert isinstance(filled_with_terminal, PlanSettlementCandidate)
    assert filled_with_terminal.target_status == "completed"
    assert filled_with_terminal.filled_quantity == 10.0
    assert filled_with_terminal.children[0].terminal_venue_event_id == 5
    assert filled_with_terminal.children[0].fill_venue_event_id == 2
    assert isinstance(filled_fallback_fill_id, PlanSettlementCandidate)
    assert filled_fallback_fill_id.children[0].terminal_venue_event_id == 3
    assert isinstance(filled_without_event, PlanSettlementCandidate)
    assert filled_without_event.filled_quantity is None
    assert filled_without_event.children[0].terminal_venue_event_id is None
    assert isinstance(rejected_default_err, PlanSettlementCandidate)
    assert rejected_default_err.target_status == "failed"
    assert rejected_default_err.last_error == "venue rejected"
    assert rejected_default_err.children[0].terminal_venue_event_id == 2
    assert isinstance(failed_long_err, PlanSettlementCandidate)
    assert failed_long_err.last_error == "x" * 1024
    assert failed_long_err.children[0].terminal_venue_event_id == 7
    assert isinstance(failed_default_err, PlanSettlementCandidate)
    assert failed_default_err.last_error == "order_interlock_blocked"
    assert isinstance(cancelled_cand, PlanSettlementCandidate)
    assert cancelled_cand.target_status == "cancelled"
    assert cancelled_cand.children[0].terminal_venue_event_id == 10
    assert isinstance(expired_cand, PlanSettlementCandidate)
    assert expired_cand.target_status == "expired"
    assert expired_cand.children[0].terminal_venue_event_id == 11


@pytest.mark.asyncio
async def test_inspect_skips_false_rejection_and_unconfirmed_failed_in_active_plan() -> None:
    """Guard active plans against false rejections and unconfirmed `failed` children.

    Given: Active plans whose child command is `rejected` with later live
        evidence, `failed` with zero refusal events, or `failed` with live
        `order_accepted` evidence alongside refusal.
    When: `inspect_plan_settlement` inspects each plan.
    Then: False rejections return `false_rejection_pending_resurrection` and
        unconfirmed failures return `unconfirmed_failed_child`.
    """
    plan = _plan_row()
    after_reject = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="rejected"),
            events=[
                _venue_event_row(event_id=4, event_type="order_rejected"),
                _venue_event_row(event_id=5, event_type="order_accepted"),
            ],
        ),
        plan,
    )
    without_reject_row = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="rejected"),
            events=[_venue_event_row(event_id=1, event_type="fill_observed", cum_fill_size=1.0)],
        ),
        plan,
    )
    no_events_rejected = await inspect_plan_settlement(
        _mock_reader(command=_command_row(status="rejected"), events=[]),
        plan,
    )
    failed_no_refusal = await inspect_plan_settlement(
        _mock_reader(command=_command_row(status="failed"), events=[]),
        plan,
    )
    failed_with_live = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="failed"),
            events=[
                _venue_event_row(event_id=1, event_type="order_interlock_blocked"),
                _venue_event_row(event_id=2, event_type="order_accepted"),
            ],
        ),
        plan,
    )

    assert isinstance(after_reject, PlanSettlementSkipped)
    assert after_reject.skip_code == "false_rejection_pending_resurrection"
    assert isinstance(without_reject_row, PlanSettlementSkipped)
    assert without_reject_row.skip_code == "false_rejection_pending_resurrection"
    assert isinstance(no_events_rejected, PlanSettlementCandidate)
    assert no_events_rejected.target_status == "failed"
    assert isinstance(failed_no_refusal, PlanSettlementSkipped)
    assert failed_no_refusal.skip_code == "unconfirmed_failed_child"
    assert isinstance(failed_with_live, PlanSettlementSkipped)
    assert failed_with_live.skip_code == "unconfirmed_failed_child"


@pytest.mark.asyncio
async def test_inspect_cancel_requested_preserves_g2b_target_confirmation() -> None:
    """Settle `cancel_requested` plans only when venue/pre-venue end is proven.

    Given: `cancel_requested` plans across confirmed (`order_terminal`,
        pre-dispatch outbox TTL expiry, `order_interlock_blocked`) and
        unconfirmed (local cancel, rejected, unconfirmed failed/expired) states.
    When: `inspect_plan_settlement` evaluates each case.
    Then: Confirmed ends settle to terminal while unconfirmed targets are
        skipped so Item G2b keeps re-emitting cancels.
    """
    plan = _plan_row(status="cancel_requested")
    confirmed_cancel = await inspect_plan_settlement(
        _mock_reader(command=_command_row(status="cancelled"), latest_event_at=_T0),
        plan,
    )
    unconfirmed_cancel = await inspect_plan_settlement(
        _mock_reader(command=_command_row(status="cancelled"), latest_event_at=None),
        plan,
    )
    venue_expired = await inspect_plan_settlement(
        _mock_reader(command=_command_row(status="expired"), latest_event_at=_T0),
        plan,
    )
    outbox_expired = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(
                status="expired",
                last_error="expired by outbox dispatch TTL (age 75.0s > 60.0s)",
            ),
            events=[],
            latest_event_at=None,
        ),
        plan,
    )
    unconfirmed_expired = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="expired", last_error="local expiry"),
            events=[],
            latest_event_at=None,
        ),
        plan,
    )
    interlock_failed = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(
                status="failed",
                last_error="order_interlock_blocked",
                exchange_order_id=None,
            ),
            events=[_venue_event_row(event_id=1, event_type="order_interlock_blocked")],
        ),
        plan,
    )
    unconfirmed_failed = await inspect_plan_settlement(
        _mock_reader(command=_command_row(status="failed"), events=[]),
        plan,
    )
    rejected_cancel_req = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="rejected"),
            events=[_venue_event_row(event_id=1, event_type="order_rejected")],
        ),
        plan,
    )

    assert isinstance(confirmed_cancel, PlanSettlementCandidate)
    assert confirmed_cancel.target_status == "cancelled"
    assert isinstance(unconfirmed_cancel, PlanSettlementSkipped)
    assert unconfirmed_cancel.skip_code == "cancel_requested_unconfirmed_target"
    assert isinstance(venue_expired, PlanSettlementCandidate)
    assert venue_expired.target_status == "expired"
    assert isinstance(outbox_expired, PlanSettlementCandidate)
    assert outbox_expired.target_status == "expired"
    assert isinstance(unconfirmed_expired, PlanSettlementSkipped)
    assert isinstance(interlock_failed, PlanSettlementCandidate)
    assert interlock_failed.target_status == "failed"
    assert isinstance(unconfirmed_failed, PlanSettlementSkipped)
    assert unconfirmed_failed.skip_code == "unconfirmed_failed_child"
    assert isinstance(rejected_cancel_req, PlanSettlementSkipped)
    assert rejected_cancel_req.skip_code == "cancel_requested_unconfirmed_target"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_type", ["order_accepted", "fill_observed", "order_submit_unknown", "order_rejected"]
)
async def test_inspect_failed_requires_exclusive_pre_venue_refusals(event_type: str) -> None:
    """Reject failed-child settlement when any non-refusal lifecycle event exists.

    Given: A failed child with an interlock refusal and one parametrized other event.
    When: `inspect_plan_settlement` classifies the child.
    Then: It returns `unconfirmed_failed_child` instead of a settlement candidate.
    """
    decision = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(status="failed"),
            events=[
                _venue_event_row(event_id=1, event_type="order_interlock_blocked"),
                _venue_event_row(event_id=2, event_type=event_type),
            ],
        ),
        _plan_row(),
    )

    assert isinstance(decision, PlanSettlementSkipped)
    assert decision.skip_code == "unconfirmed_failed_child"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command_exchange_order_id", "order_exchange_order_id"),
    [("EX-COMMAND", None), (None, "EX-ORDER")],
)
async def test_inspect_cancel_requested_failed_rejects_either_venue_id(
    command_exchange_order_id: str | None,
    order_exchange_order_id: str | None,
) -> None:
    """Reject cancel recovery settlement when either durable venue id exists.

    Given: An interlock-only failed child with an id on its command or Order.
    When: `inspect_plan_settlement` classifies the cancel-requested plan.
    Then: It returns `unconfirmed_failed_child`.
    """
    decision = await inspect_plan_settlement(
        _mock_reader(
            command=_command_row(
                status="failed",
                exchange_order_id=command_exchange_order_id,
            ),
            events=[_venue_event_row(event_id=1, event_type="order_interlock_blocked")],
            order_exchange_order_id=order_exchange_order_id,
        ),
        _plan_row(status="cancel_requested"),
    )

    assert isinstance(decision, PlanSettlementSkipped)
    assert decision.skip_code == "unconfirmed_failed_child"


@pytest.mark.asyncio
async def test_preview_and_apply_settle_sqlite_plans_end_to_end(
    sqlite_repo: SQLAlchemyRepository,
) -> None:
    """Preview and SCD2-apply all terminal plan archetypes with bound metadata.

    Given: Five live execution plans in SQLite covering `filled`, `rejected`,
        outbox-`expired`, `cancel_requested` with `order_interlock_blocked`,
        and `cancel_requested` with an open `accepted` child.
    When: `preview_plan_settlements` and `apply_plan_settlements` run with bound
        `target_database` and `code_revision`.
    Then: Changing endpoint or code revision changes `preview.digest`; the first
        batch settles the four ready plans atomically and records child evidence
        ids in `settled_candidates`; the fifth plan settles once cancelled; and
        an empty preview commits cleanly with `count == 0`.
    """
    p_filled = await _seed_plan_with_child(
        sqlite_repo,
        created_at=_T0,
        plan_status="active",
        child_order_id="cid-filled",
        child_status="filled",
    )
    fill_event: VenueEventInsertRow = {
        "event_type": "fill_observed",
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "exchange": "kraken",
        "instrument": "XRP-EUR",
        "mode": "live",
        "client_order_id": "cid-filled",
        "venue_client_id": "cid-filled",
        "exchange_order_id": "EX-FILL",
        "exec_id": "EXEC-1",
        "side": "buy",
        "status": "filled",
        "fill_size": 10.0,
        "fill_price": 2.4,
        "cum_fill_size": 10.0,
        "liquidity_role": "taker",
        "received_at": _T0 + timedelta(seconds=2),
        "session_id": "seed-sess",
        "sequence_id": 10,
        "timestamp": _T0 + timedelta(seconds=2),
        "wallet_public_id": _WALLET_ID,
    }
    fill_event_id = await sqlite_repo.insert_venue_event(fill_event)
    p_rejected = await _seed_plan_with_child(
        sqlite_repo,
        created_at=_T0 + timedelta(seconds=10),
        plan_status="active",
        child_order_id="cid-rejected",
        child_status="rejected",
    )
    p_expired = await _seed_plan_with_child(
        sqlite_repo,
        created_at=_T0 + timedelta(seconds=20),
        plan_status="active",
        child_order_id="cid-expired",
        child_status="expired",
    )
    p_interlock = await _seed_plan_with_child(
        sqlite_repo,
        created_at=_T0 + timedelta(seconds=30),
        plan_status="cancel_requested",
        child_order_id="cid-interlock",
        child_status="failed",
    )
    interlock_event: VenueEventInsertRow = {
        "event_type": "order_interlock_blocked",
        "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
        "exchange": "kraken",
        "instrument": "XRP-EUR",
        "mode": "live",
        "client_order_id": "cid-interlock",
        "venue_client_id": "cid-interlock",
        "side": "buy",
        "status": "failed",
        "error": "order_interlock_blocked",
        "liquidity_role": "unknown",
        "received_at": _T0 + timedelta(seconds=31),
        "session_id": "seed-sess",
        "sequence_id": 11,
        "timestamp": _T0 + timedelta(seconds=31),
        "wallet_public_id": _WALLET_ID,
    }
    interlock_event_id = await sqlite_repo.insert_venue_event(interlock_event)
    p_open_cancel = await _seed_plan_with_child(
        sqlite_repo,
        created_at=_T0 + timedelta(seconds=40),
        plan_status="cancel_requested",
        child_order_id="cid-open-cancel",
        child_status="accepted",
    )

    target_db: JsonObject = {"dialect": "sqlite", "path": "/tmp/settlement.db"}
    code_rev: JsonObject = {"snapper/application/plans/settlement.py": "1" * 64}
    preview_one = await preview_plan_settlements(
        sqlite_repo,
        mode="live",
        target_database=target_db,
        code_revision=code_rev,
    )
    other_db_preview = await preview_plan_settlements(
        sqlite_repo,
        mode="live",
        target_database={"dialect": "sqlite", "path": "/tmp/other.db"},
        code_revision=code_rev,
    )
    other_rev_preview = await preview_plan_settlements(
        sqlite_repo,
        mode="live",
        target_database=target_db,
        code_revision={"snapper/application/plans/settlement.py": "2" * 64},
    )
    assert preview_one.digest != other_db_preview.digest
    assert preview_one.digest != other_rev_preview.digest
    assert [c.plan_public_id for c in preview_one.candidates] == [
        p_filled,
        p_rejected,
        p_expired,
        p_interlock,
    ]
    assert [s.plan_public_id for s in preview_one.skipped] == [p_open_cancel]

    settled_at = _T0 + timedelta(minutes=5)
    receipt_one = await apply_plan_settlements(
        sqlite_repo,
        preview_one,
        settled_at=settled_at,
        session_id="cli-test-1",
    )
    assert receipt_one.count == 4
    assert receipt_one.target_database == target_db
    assert receipt_one.code_revision == code_rev
    serialized_first = serialize_candidate(receipt_one.settled_candidates[0])
    assert serialized_first["plan_public_id"] == p_filled
    assert receipt_one.settled_candidates[0].children[0].fill_venue_event_id == fill_event_id
    assert receipt_one.settled_candidates[3].children[0].terminal_venue_event_id == (
        interlock_event_id
    )

    filled_after = await sqlite_repo.get_execution_plan(p_filled, as_of=settled_at)
    rejected_after = await sqlite_repo.get_execution_plan(p_rejected, as_of=settled_at)
    expired_after = await sqlite_repo.get_execution_plan(p_expired, as_of=settled_at)
    interlock_after = await sqlite_repo.get_execution_plan(p_interlock, as_of=settled_at)
    assert filled_after is not None and filled_after["status"] == "completed"
    assert filled_after["filled_quantity"] == 10.0
    assert rejected_after is not None and rejected_after["status"] == "failed"
    assert expired_after is not None and expired_after["status"] == "expired"
    assert interlock_after is not None and interlock_after["status"] == "failed"

    open_cmd = await sqlite_repo.get_active_create_command_by_client_order_id(
        "cid-open-cancel", "kraken"
    )
    assert open_cmd is not None
    await sqlite_repo.insert_order_terminal_once(
        {
            "event_type": "order_terminal",
            "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
            "exchange": "kraken",
            "instrument": "XRP-EUR",
            "mode": "live",
            "client_order_id": "cid-open-cancel",
            "venue_client_id": "cid-open-cancel",
            "exchange_order_id": "EX-OPEN",
            "side": "buy",
            "status": "canceled",
            "liquidity_role": "unknown",
            "received_at": settled_at + timedelta(seconds=1),
            "session_id": "seed-sess",
            "sequence_id": 12,
            "timestamp": settled_at + timedelta(seconds=1),
            "wallet_public_id": _WALLET_ID,
        }
    )
    await sqlite_repo.advance_trade_command_lifecycle(
        public_id=open_cmd["public_id"],
        expected_status="accepted",
        new_status="cancelled",
        bus_time=settled_at + timedelta(seconds=2),
        session_id="seed-sess",
        sequence_id=13,
        terminal_at=settled_at + timedelta(seconds=2),
    )

    preview_two = await preview_plan_settlements(sqlite_repo, mode="live")
    assert [c.plan_public_id for c in preview_two.candidates] == [p_open_cancel]
    receipt_two = await apply_plan_settlements(
        sqlite_repo,
        preview_two,
        settled_at=settled_at + timedelta(seconds=5),
        session_id="cli-test-2",
    )
    assert receipt_two.count == 1

    preview_empty = await preview_plan_settlements(sqlite_repo, mode="live")
    assert preview_empty.candidates == ()
    receipt_empty = await apply_plan_settlements(
        sqlite_repo,
        preview_empty,
        settled_at=settled_at + timedelta(seconds=10),
        session_id="cli-test-3",
    )
    assert receipt_empty.count == 0


@pytest.mark.asyncio
async def test_two_engine_sqlite_plan_or_child_evidence_drift_refuses_cleanly(
    tmp_path: Path,
) -> None:
    """Refuse settlement when another engine mutates plan or child venue evidence.

    Given: Two independent SQLite `SQLAlchemyRepository` engines sharing one DB
        file with one `filled` plan and one `rejected` plan.
    When: Engine B appends late `order_accepted` evidence to the rejected child
        before `settle_single_recovered_plan` and `apply_plan_settlements`, and
        later mutates the remaining plan's status before apply.
    Then: `settle_single_recovered_plan` returns False and
        `apply_plan_settlements` raises `PlanSettlementDriftError` with zero
        terminal plan versions written.
    """
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'two_engine.db'}"
    repo_a = SQLAlchemyRepository(db_url)
    repo_b = SQLAlchemyRepository(db_url)
    await repo_a.create_all()
    try:
        plan_rej = await _seed_plan_with_child(
            repo_a,
            created_at=_T0,
            plan_status="active",
            child_order_id="cid-rej-race",
            child_status="rejected",
        )
        await repo_a.insert_venue_event(
            {
                "event_type": "order_rejected",
                "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
                "exchange": "kraken",
                "instrument": "XRP-EUR",
                "mode": "live",
                "client_order_id": "cid-rej-race",
                "venue_client_id": "cid-rej-race",
                "side": "buy",
                "status": "rejected",
                "liquidity_role": "unknown",
                "received_at": _T0 + timedelta(seconds=1),
                "session_id": "seed-sess",
                "sequence_id": 4,
                "timestamp": _T0 + timedelta(seconds=1),
                "wallet_public_id": _WALLET_ID,
            }
        )
        preview_rej = await preview_plan_settlements(repo_a, mode="live")
        rej_candidate = preview_rej.candidates[0]

        await repo_b.insert_venue_event(
            {
                "event_type": "order_accepted",
                "shard_key": f"kraken.XRP-EUR.live.{_WALLET_ID}",
                "exchange": "kraken",
                "instrument": "XRP-EUR",
                "mode": "live",
                "client_order_id": "cid-rej-race",
                "venue_client_id": "cid-rej-race",
                "exchange_order_id": "EX-LATE-ACCEPT",
                "side": "buy",
                "status": "open",
                "liquidity_role": "unknown",
                "received_at": _T0 + timedelta(seconds=2),
                "session_id": "race-sess",
                "sequence_id": 5,
                "timestamp": _T0 + timedelta(seconds=2),
                "wallet_public_id": _WALLET_ID,
            }
        )

        assert (
            await settle_single_recovered_plan(
                repo_a,
                rej_candidate,
                settled_at=_T0 + timedelta(seconds=5),
                session_id="rec-sess",
                sequence_id=1,
            )
            is False
        )
        with pytest.raises(PlanSettlementDriftError, match="drifted before apply"):
            await apply_plan_settlements(
                repo_a,
                preview_rej,
                settled_at=_T0 + timedelta(seconds=6),
                session_id="apply-sess-1",
            )
        rej_after = await repo_a.get_execution_plan(plan_rej, as_of=_T0 + timedelta(seconds=7))
        assert rej_after is not None and rej_after["status"] == "active"

        plan_id = await _seed_plan_with_child(
            repo_a,
            created_at=_T0 + timedelta(seconds=10),
            plan_status="active",
            child_order_id="cid-race",
            child_status="filled",
        )
        preview = await preview_plan_settlements(repo_a, mode="live")
        await repo_b.update_execution_plan_status(
            public_id=plan_id,
            new_status="cancel_requested",
            bus_time=_T0 + timedelta(seconds=15),
            session_id="race-sess",
            sequence_id=9,
            cancel_requested_at=_T0 + timedelta(seconds=15),
        )
        with pytest.raises(PlanSettlementDriftError, match="drifted before apply"):
            await apply_plan_settlements(
                repo_a,
                preview,
                settled_at=_T0 + timedelta(seconds=20),
                session_id="apply-sess-2",
            )
        current = await repo_a.get_execution_plan(plan_id, as_of=_T0 + timedelta(seconds=25))
        assert current is not None and current["status"] == "cancel_requested"
    finally:
        await repo_a.engine.dispose()
        await repo_b.engine.dispose()


@pytest.mark.asyncio
async def test_single_and_batch_locked_version_and_rowcount_guards(
    sqlite_repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Handle concurrent version changes, missing rows, and rowcount mismatches.

    Given: Previewed settlement candidates against real and mocked repository
        sessions covering stale version identity, missing locked rows, and zero
        `UPDATE` rowcounts.
    When: `settle_single_recovered_plan` and `apply_plan_settlements` execute.
    Then: Single recovery settlement returns False without writing, and batch
        settlement raises `PlanSettlementDriftError`.
    """
    plan_id = await _seed_plan_with_child(
        sqlite_repo,
        created_at=_T0,
        plan_status="active",
        child_order_id="cid-guard",
        child_status="expired",
    )
    preview = await preview_plan_settlements(sqlite_repo, mode=None)
    candidate = preview.candidates[0]

    stale_candidate = PlanSettlementCandidate(
        plan_public_id=candidate.plan_public_id,
        plan_type=candidate.plan_type,
        exchange=candidate.exchange,
        mode=candidate.mode,
        shard_key=candidate.shard_key,
        current_status="cancel_requested",
        current_session_id=candidate.current_session_id,
        current_sequence_id=candidate.current_sequence_id,
        current_filled_quantity=candidate.current_filled_quantity,
        target_status=candidate.target_status,
        filled_quantity=candidate.filled_quantity,
        last_error=candidate.last_error,
        children=candidate.children,
    )
    assert (
        await settle_single_recovered_plan(
            sqlite_repo,
            stale_candidate,
            settled_at=_T0 + timedelta(seconds=5),
            session_id="s-stale",
            sequence_id=1,
        )
        is False
    )

    settled = await settle_single_recovered_plan(
        sqlite_repo,
        candidate,
        settled_at=_T0 + timedelta(seconds=6),
        session_id="s-ok",
        sequence_id=2,
    )
    assert settled is True

    async with sqlite_repo.session() as check_session:
        versions = (
            (
                await check_session.execute(
                    select(ExecutionPlan).where(ExecutionPlan.public_id == plan_id)
                )
            )
            .scalars()
            .all()
        )
    assert len(versions) == 2

    assert (
        await settle_single_recovered_plan(
            sqlite_repo,
            candidate,
            settled_at=_T0 + timedelta(seconds=7),
            session_id="s-repeat",
            sequence_id=3,
        )
        is False
    )

    existing_orm = versions[0]
    existing_orm.status = candidate.current_status
    existing_orm.session_id = candidate.current_session_id
    existing_orm.sequence_id = candidate.current_sequence_id
    existing_orm.filled_quantity = candidate.current_filled_quantity

    zero_update = MagicMock(rowcount=0)
    fake_session = AsyncMock(add=MagicMock(), execute=AsyncMock(return_value=zero_update))
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = fake_session
    session_cm.__aexit__.return_value = False

    fake_pg_repo = MagicMock(spec=SQLAlchemyRepository)
    fake_pg_repo.dialect_name = "postgresql"
    fake_pg_repo.session.return_value = session_cm
    fake_pg_repo._plan_row_to_dict.return_value = _plan_row(public_id=plan_id)

    monkeypatch.setattr(
        settlement_module,
        "begin_plan_settlement_transaction",
        AsyncMock(),
    )
    monkeypatch.setattr(
        settlement_module,
        "lock_child_create_commands",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        settlement_module,
        "lock_active_execution_plans",
        AsyncMock(return_value={plan_id: existing_orm}),
    )
    monkeypatch.setattr(
        settlement_module,
        "inspect_plan_settlement",
        AsyncMock(return_value=candidate),
    )
    assert (
        await settle_single_recovered_plan(
            fake_pg_repo,
            candidate,
            settled_at=_T0 + timedelta(seconds=8),
            session_id="s-zero",
            sequence_id=4,
        )
        is False
    )

    monkeypatch.setattr(
        settlement_module,
        "preview_plan_settlements",
        AsyncMock(return_value=preview),
    )
    with pytest.raises(PlanSettlementDriftError, match="close count mismatch"):
        await apply_plan_settlements(
            fake_pg_repo,
            preview,
            settled_at=_T0 + timedelta(seconds=9),
            session_id="s-batch-zero",
        )

    monkeypatch.setattr(
        settlement_module,
        "lock_active_execution_plans",
        AsyncMock(return_value={}),
    )
    with pytest.raises(PlanSettlementDriftError, match="changed concurrently"):
        await apply_plan_settlements(
            fake_pg_repo,
            preview,
            settled_at=_T0 + timedelta(seconds=10),
            session_id="s-batch-drift",
        )
