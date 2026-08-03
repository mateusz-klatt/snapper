"""Repository tests for immutable spot reconciliation anchors."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import PortfolioReconciliationMethodConfig
from snapper.data.models import PortfolioReconciliationState
from snapper.data.models import PortfolioSpotReconciliationAnchor
from snapper.data.models import Wallet
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import SpotReconciliationAnchorRow

_T0 = datetime(2026, 7, 14, 8, 0, tzinfo=UTC)
_WALLET = "00000000-0000-7000-8000-000000000101"
_OTHER_WALLET = "00000000-0000-7000-8000-000000000102"
_ALPHA_WALLET = "abcdefab-cdef-7abc-8def-abcdefabcdef"
_ANCHOR = "00000000-0000-7000-8000-000000000301"
_SESSION = "00000000-0000-7000-8000-000000000501"
_SEED_INSTRUMENT = "00000000-0000-7000-8000-000000000731"
_SEED_ORDER = "00000000-0000-7000-8000-000000000631"


async def _repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Create a fresh SQLite repository."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'anchor.db'}")
    await repo.create_all()
    async with repo.session() as session:
        session.add_all(
            [
                Wallet(
                    public_id=_WALLET,
                    label="spot-reconciliation",
                    description=None,
                    is_paper=False,
                    session_id=_SESSION,
                    sequence_id=1,
                    timestamp=_T0 - timedelta(days=1),
                    known_to=KNOWN_TO_MAX,
                ),
                PortfolioReconciliationMethodConfig(
                    wallet_public_id=_WALLET,
                    exchange="kraken",
                    mode="live",
                    method="spot_execution_replay",
                    session_id=_SESSION,
                    sequence_id=2,
                    timestamp=_T0 - timedelta(days=1),
                    known_to=KNOWN_TO_MAX,
                ),
            ]
        )
        await session.commit()
    return repo


def _anchor(
    *,
    wallet_public_id: str = _WALLET,
    public_id: str = _ANCHOR,
    source_watermark: int = 2,
) -> SpotReconciliationAnchorRow:
    """Build exact canonical anchor evidence.

    Exact assertions deliberately avoid float approximation. SQLite text
    coverage cannot prove PostgreSQL UUID, BIGINT, or driver behavior, so this
    case must also run against the disposable PostgreSQL test database.
    """
    return {
        "public_id": public_id,
        "wallet_public_id": wallet_public_id,
        "exchange": "kraken",
        "mode": "live",
        "venue_account_state_public_id": "00000000-0000-7000-8000-000000000201",
        "balance_observation_id": 41,
        "source_watermark_kind": "scope_sequence",
        "source_watermark": source_watermark,
        "balances_json": (
            '{"BTC":"0.100000000000000005","USD":"123456789012345678.123456789012345678"}'
        ),
        "first_request_started_at": _T0,
        "first_request_completed_at": _T0 + timedelta(seconds=1),
        "second_request_started_at": _T0 + timedelta(seconds=1),
        "second_request_completed_at": _T0 + timedelta(seconds=2),
        "boundary_status": "cursor_certified",
        "inventory_status": "venue_reported_full",
        "margin_status": "cash",
        "provenance": "kraken:ccxt.fetch_balance",
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": _T0 + timedelta(seconds=2),
        "source_chain_tip": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
        "venue_cursor_kind": "account_history_item_id",
        "venue_cursor_scheme": "kraken:ccxt:account/history:v1",
        "venue_cursor_value": "987654321",
        "venue_cursor_requested_at": _T0 - timedelta(seconds=4),
        "venue_cursor_observed_at": _T0 - timedelta(seconds=3),
        "venue_cursor_confirmed_at": _T0 + timedelta(seconds=2),
        "source_watermark_requested_at": _T0 - timedelta(seconds=2),
        "source_watermark_captured_at": _T0 - timedelta(seconds=1),
    }


def _evaluation(
    *,
    anchor_public_id: str | None,
    wallet_public_id: str = _WALLET,
    source_watermark: int = 3,
    status: str = "mismatched",
    source_watermark_kind: str = "scope_sequence",
    sequence_id: int = 2,
) -> PortfolioReconciliationEvaluationRow:
    """Build one full or incomplete S1 evaluation."""
    full = status in ("matched", "mismatched")
    return {
        "wallet_public_id": wallet_public_id,
        "exchange": "kraken",
        "mode": "live",
        "method": "spot_execution_replay",
        "evaluation_status": status,
        "venue_account_state_public_id": "00000000-0000-7000-8000-000000000202" if full else None,
        "venue_account_observation_id": 42 if full else None,
        "account_authoritative_until": _T0 + timedelta(minutes=5) if full else None,
        "source_watermark_kind": source_watermark_kind if full else None,
        "source_watermark": source_watermark if full else None,
        "anchor_public_id": anchor_public_id if full else None,
        "expected_json": '{"BTC":"0.2"}' if full else None,
        "actual_json": '{"BTC":"0.1"}' if full else None,
        "difference_json": '{"BTC":"0.1"}' if full else None,
        "tolerance_json": '{"BTC":"0.0001"}' if full else None,
        "error": None,
        "session_id": _SESSION,
        "sequence_id": sequence_id,
        "bus_time": _T0 + timedelta(seconds=3),
    }


async def _seed_scope(
    repo: SQLAlchemyRepository,
    count: int,
    *,
    wallet: str = _WALLET,
    exchange: str = "kraken",
    mode: str = "live",
) -> None:
    """Seed ``count`` committed executions so the anchor CAS finds a real tip.

    Executions carry no foreign keys, so one instrument and order are seeded to
    resolve the ingest scope, then ``insert_execution`` allocates scope_sequence
    ``1..count`` under the per-wallet fence — the exact tip the writer re-reads.
    """
    instrument_public_id = _SEED_INSTRUMENT
    order_public_id = _SEED_ORDER
    async with repo.session() as session:
        session.add_all(
            [
                Instrument(
                    public_id=instrument_public_id,
                    symbol_public_id=instrument_public_id,
                    exchange=exchange,
                    timestamp=_T0 - timedelta(days=1),
                    session_id=_SESSION,
                    sequence_id=1,
                ),
                Order(
                    public_id=order_public_id,
                    instrument_public_id=instrument_public_id,
                    mode=mode,
                    wallet_public_id=wallet,
                    created_at=_T0 - timedelta(hours=1),
                    timestamp=_T0 - timedelta(hours=1),
                    side="buy",
                    order_type="limit",
                    price=0.1,
                    size=1.0,
                    status="filled",
                    session_id=_SESSION,
                    sequence_id=1,
                ),
            ]
        )
        await session.commit()
    for offset in range(count):
        await repo.insert_execution(
            order_public_id=order_public_id,
            wallet_public_id=wallet,
            timestamp=_T0,
            side="buy",
            status="filled",
            price=0.1,
            size=1.0,
            fee=0.0,
            fee_asset="BTC",
            session_id=_SESSION,
            sequence_id=10 + offset,
        )


async def test_anchor_exact_round_trip_idempotence_and_conflict(tmp_path: Path) -> None:
    """Exact decimal strings round-trip; identical replay is idempotent."""
    repo = await _repo(tmp_path)
    await _seed_scope(repo, 2)
    evidence = _anchor()
    first_id = await repo.record_spot_reconciliation_anchor(evidence)
    second_id = await repo.record_spot_reconciliation_anchor(evidence.copy())
    assert first_id == second_id
    stored = await repo.get_spot_reconciliation_anchor(_WALLET, "kraken", "live")
    assert stored == evidence
    assert stored is not None
    decoded = {
        asset: Decimal(value)
        for asset, value in {
            "BTC": "0.100000000000000005",
            "USD": "123456789012345678.123456789012345678",
        }.items()
    }
    assert Decimal("0.100000000000000005") == decoded["BTC"]
    assert Decimal("123456789012345678.123456789012345678") == decoded["USD"]
    conflicting = evidence.copy()
    conflicting["provenance"] = "different"
    with pytest.raises(RuntimeError, match="conflicting spot reconciliation"):
        await repo.record_spot_reconciliation_anchor(conflicting)
    assert await repo.get_spot_reconciliation_anchor(_OTHER_WALLET, "kraken", "live") is None
    await repo.engine.dispose()


@pytest.mark.parametrize(
    "wallet_alias",
    [_ALPHA_WALLET.upper(), _ALPHA_WALLET.replace("-", "")],
)
async def test_anchor_read_canonicalizes_wallet_aliases(
    tmp_path: Path,
    wallet_alias: str,
) -> None:
    """Alias-spelled reads find a canonically stored spot anchor.

    Given: A spot anchor written under a canonical alphabetic wallet UUID,
    When: The anchor read uses its uppercase or hyphenless spelling,
    Then: SQLite returns the anchor under the canonical wallet identity.
    """
    repo = await _repo(tmp_path)
    await _seed_scope(repo, 2, wallet=_ALPHA_WALLET)
    await repo.record_spot_reconciliation_anchor(_anchor(wallet_public_id=_ALPHA_WALLET))

    stored = await repo.get_spot_reconciliation_anchor(wallet_alias, "kraken", "live")

    assert stored is not None
    assert stored["wallet_public_id"] == _ALPHA_WALLET
    await repo.engine.dispose()


async def test_anchor_read_rejects_malformed_wallet_identity(tmp_path: Path) -> None:
    """A malformed anchor wallet identity raises the exact shared ValueError.

    Given: A spot reconciliation anchor repository,
    When: The anchor read receives a non-UUID wallet identity,
    Then: The DAL rejects it with the writer-compatible canonicalization error.
    """
    repo = await _repo(tmp_path)

    with pytest.raises(ValueError) as exc_info:
        await repo.get_spot_reconciliation_anchor("not-a-wallet-uuid", "kraken", "live")

    assert str(exc_info.value) == "reconciliation wallet identity is invalid"
    await repo.engine.dispose()


@pytest.mark.parametrize(
    ("balances_json", "message"),
    [
        ('{"BTC":"1","BTC":"2"}', "duplicate anchor balance asset"),
        ("{}", "anchor balances must be a non-empty object"),
        ('{" BTC":"1"}', "anchor balance asset is invalid"),
        ('{"BTC":1}', "anchor balance must be an exact decimal string"),
        ('{"BTC":"abc"}', "anchor balance decimal is malformed"),
        ('{"BTC":"NaN"}', "anchor balance decimal is not finite or bounded"),
        ('{"BTC": "1"}', "anchor balances must use canonical sorted decimal strings"),
    ],
)
async def test_anchor_rejects_invalid_balance_evidence_before_database_access(
    tmp_path: Path,
    balances_json: str,
    message: str,
) -> None:
    """Every malformed balance-evidence shape fails before opening a session."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'invalid-anchor.db'}")
    evidence = _anchor()
    evidence["balances_json"] = balances_json
    with pytest.raises(ValueError, match=message):
        await repo.record_spot_reconciliation_anchor(evidence)
    await repo.engine.dispose()


@pytest.mark.parametrize("winner_matches", [True, False])
async def test_anchor_integrity_race_recovers_winner_or_rejects_conflict(
    tmp_path: Path,
    winner_matches: bool,
) -> None:
    """A real unique collision re-reads and compares the committed winner."""
    repo = await _repo(tmp_path)
    await _seed_scope(repo, 2)
    requested = _anchor()
    winner = requested.copy()
    if not winner_matches:
        winner["provenance"] = "competing-process"
    async with repo.session() as session:
        winner_row = PortfolioSpotReconciliationAnchor(**winner, known_to=KNOWN_TO_MAX)
        session.add(winner_row)
        await session.commit()
        winner_id = int(winner_row.id)

    original_execute = AsyncSession.execute
    execute_calls = 0

    async def hide_initial_winner(
        session: AsyncSession,
        statement: Select[tuple[PortfolioSpotReconciliationAnchor]],
    ) -> object:
        """Hide only the locking read that precedes the competing insert."""
        nonlocal execute_calls
        execute_calls += 1
        if execute_calls == 1:
            missing = MagicMock()
            missing.scalars.return_value.first.return_value = None
            return missing
        return await original_execute(session, statement)

    with patch.object(AsyncSession, "execute", new=hide_initial_winner):
        if winner_matches:
            assert await repo.record_spot_reconciliation_anchor(requested) == winner_id
        else:
            with pytest.raises(
                RuntimeError,
                match="conflicting spot reconciliation bootstrap anchor",
            ):
                await repo.record_spot_reconciliation_anchor(requested)
    assert execute_calls == 3
    await repo.engine.dispose()


async def test_execution_repository_preserves_float_and_raw_decimal_evidence(
    tmp_path: Path,
) -> None:
    """Execution raw text survives exactly beside compatibility floats.

    This exact assertion must also run against PostgreSQL because SQLite text
    success cannot prove PostgreSQL driver or BIGINT insertion behavior. The
    ingest path resolves the fill's scope from the active
    Order -> Instrument lineage, so the fill's order and instrument are
    seeded first.
    """
    repo = await _repo(tmp_path)
    async with repo.session() as session:
        session.add_all(
            [
                Instrument(
                    public_id="00000000-0000-7000-8000-000000000701",
                    symbol_public_id="00000000-0000-7000-8000-000000000701",
                    exchange="kraken",
                    timestamp=_T0 - timedelta(days=1),
                    session_id=_SESSION,
                    sequence_id=1,
                ),
                Order(
                    public_id="00000000-0000-7000-8000-000000000601",
                    instrument_public_id="00000000-0000-7000-8000-000000000701",
                    mode="live",
                    wallet_public_id=_WALLET,
                    created_at=_T0 - timedelta(hours=1),
                    timestamp=_T0 - timedelta(hours=1),
                    side="buy",
                    order_type="limit",
                    price=0.1,
                    size=1.0,
                    status="filled",
                    session_id=_SESSION,
                    sequence_id=1,
                ),
            ]
        )
        await session.commit()
    execution_id = await repo.insert_execution(
        order_public_id="00000000-0000-7000-8000-000000000601",
        wallet_public_id=_WALLET,
        timestamp=_T0,
        side="buy",
        status="filled",
        price=0.1,
        size=float("123456789012345678.123456789012345678"),
        fee=float("0.000000000000000001"),
        fee_asset="BTC",
        price_decimal="0.100000000000000005",
        size_decimal="123456789012345678.123456789012345678",
        fee_decimal="0.000000000000000001",
        numeric_provenance="venue_raw",
        session_id=_SESSION,
        sequence_id=1,
    )
    async with repo.session() as session:
        stored = (
            await session.execute(select(Execution).where(Execution.id == execution_id))
        ).scalar_one()
    assert stored.price == 0.1
    assert stored.price_decimal == "0.100000000000000005"
    assert stored.size_decimal == "123456789012345678.123456789012345678"
    assert stored.fee_decimal == "0.000000000000000001"
    assert stored.numeric_provenance == "venue_raw"
    await repo.engine.dispose()


@pytest.mark.parametrize(
    ("seed", "evaluation", "message"),
    [
        (None, _evaluation(anchor_public_id=None), "requires anchor lineage"),
        (None, _evaluation(anchor_public_id=_ANCHOR), "anchor lineage"),
        (
            _anchor(wallet_public_id=_OTHER_WALLET),
            _evaluation(anchor_public_id=_ANCHOR),
            "anchor lineage",
        ),
        (
            _anchor(source_watermark=4),
            _evaluation(anchor_public_id=_ANCHOR),
            "anchor lineage",
        ),
        (
            _anchor(),
            _evaluation(
                anchor_public_id=_ANCHOR,
                source_watermark_kind="venue_event_id",
            ),
            "scope-sequence lineage",
        ),
    ],
)
async def test_full_spot_reconciliation_rejects_missing_foreign_or_future_anchor(
    tmp_path: Path,
    seed: SpotReconciliationAnchorRow | None,
    evaluation: PortfolioReconciliationEvaluationRow,
    message: str,
) -> None:
    """Full spot truth cannot cite absent, foreign, or later bootstrap evidence."""
    repo = await _repo(tmp_path)
    if seed is not None:
        await _seed_scope(repo, seed["source_watermark"], wallet=seed["wallet_public_id"])
        await repo.record_spot_reconciliation_anchor(seed)
    with pytest.raises(RuntimeError, match=message):
        await repo.record_portfolio_reconciliation(evaluation)
    await repo.engine.dispose()


async def test_full_spot_reconciliation_retains_real_anchor_and_nonfull_needs_none(
    tmp_path: Path,
) -> None:
    """Valid full spot state retains bootstrap identity; incomplete needs none."""
    repo = await _repo(tmp_path)
    await _seed_scope(repo, 2)
    await repo.record_spot_reconciliation_anchor(_anchor())
    await repo.record_portfolio_reconciliation(
        _evaluation(anchor_public_id=None, status="incomplete")
    )
    await repo.record_portfolio_reconciliation(_evaluation(anchor_public_id=_ANCHOR, sequence_id=3))
    async with repo.session() as session:
        state = (
            await session.execute(
                select(PortfolioReconciliationState).where(
                    PortfolioReconciliationState.known_to == KNOWN_TO_MAX
                )
            )
        ).scalar_one()
    assert state.anchor_public_id == _ANCHOR
    assert state.current_evaluation_status == "mismatched"
    assert state.source_watermark == 3
    await repo.engine.dispose()


async def test_anchor_write_refuses_when_watermark_is_below_the_committed_tip(
    tmp_path: Path,
) -> None:
    """The commit-time CAS refuses a caller watermark below the committed tip.

    Given: A scope with three committed executions,
    When: An anchor claiming watermark 2 is recorded,
    Then: The re-read tip (3) refuses the stale/fabricated watermark — the shipped
        gap-3 defect (the writer trusting source_watermark verbatim) is closed.
    """
    repo = await _repo(tmp_path)
    await _seed_scope(repo, 3)
    s5778_value_1 = _anchor(source_watermark=2)
    with pytest.raises(RuntimeError, match="not the committed execution tip"):
        await repo.record_spot_reconciliation_anchor(s5778_value_1)
    await repo.engine.dispose()


async def test_anchor_write_refuses_when_an_execution_commits_after_capture(
    tmp_path: Path,
) -> None:
    """The CAS refuses when a fill commits between watermark capture and insert.

    Given: An anchor captured at watermark 2, then a third execution commits,
    When: The anchor (still claiming watermark 2) is recorded,
    Then: The re-read tip (3) refuses — the ledger must be frozen from capture
        through commit (O3), stronger than 'the watermark did not change'.
    """
    repo = await _repo(tmp_path)
    await _seed_scope(repo, 2)
    evidence = _anchor(source_watermark=2)
    await repo.insert_execution(
        order_public_id=_SEED_ORDER,
        wallet_public_id=_WALLET,
        timestamp=_T0,
        side="buy",
        status="filled",
        price=0.1,
        size=1.0,
        fee=0.0,
        fee_asset="BTC",
        session_id=_SESSION,
        sequence_id=99,
    )
    with pytest.raises(RuntimeError, match="not the committed execution tip"):
        await repo.record_spot_reconciliation_anchor(evidence)
    await repo.engine.dispose()


async def test_exact_idempotent_replay_succeeds_after_the_tip_advances(
    tmp_path: Path,
) -> None:
    """An exact replay still succeeds after the committed tip has moved on.

    Given: An anchor recorded at watermark 2, then a third execution advances the
        committed tip,
    When: The identical anchor is recorded again,
    Then: The idempotent replay returns the original id — the CAS guards only new
        inserts, never the accepted replay of a committed anchor.
    """
    repo = await _repo(tmp_path)
    await _seed_scope(repo, 2)
    evidence = _anchor(source_watermark=2)
    first_id = await repo.record_spot_reconciliation_anchor(evidence)
    await repo.insert_execution(
        order_public_id=_SEED_ORDER,
        wallet_public_id=_WALLET,
        timestamp=_T0,
        side="buy",
        status="filled",
        price=0.1,
        size=1.0,
        fee=0.0,
        fee_asset="BTC",
        session_id=_SESSION,
        sequence_id=98,
    )
    second_id = await repo.record_spot_reconciliation_anchor(evidence.copy())
    assert first_id == second_id
    await repo.engine.dispose()
