"""Tests for the transactionally complete spot reconciliation bundle (S4c-4a).

The bundle is exercised through the production writers wherever a scenario is
reachable through them: executions enter via ``insert_execution`` (allocating
``scope_sequence`` under the ingest fence), anchors via the CAS-guarded
``record_spot_reconciliation_anchor`` with the REAL derived chain tip, and
checkpoints via ``record_portfolio_reconciliation``. Rows the writers refuse to
produce (a sub-one anchor watermark, a same-instrument identity conflict) are
injected through a mocked session, mirroring the futures bundle suite.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import TypedDict
from typing import Unpack
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
from sqlalchemy import event
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.execution_chain import execution_chain_genesis
from snapper.application.portfolio.execution_chain import execution_row_digest
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import ExecutionAnnulment
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Order
from snapper.data.models import PortfolioReconciliationMethodConfig
from snapper.data.models import PortfolioReconciliationState
from snapper.data.models import PortfolioSpotReconciliationAnchor
from snapper.data.models import SpotAssetPrecisionEvidence
from snapper.data.models import Symbol
from snapper.data.models import VenueEvent
from snapper.data.models import Wallet
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import SpotReconciliationAnchorRow

_T0 = datetime(2026, 7, 14, 8, 0, tzinfo=UTC)
_AS_OF = _T0 + timedelta(hours=1)
_WALLET = "00000000-0000-7000-8000-000000000101"
_ALPHA_WALLET = "abcdefab-cdef-7abc-8def-abcdefabcdef"
_ANCHOR = "00000000-0000-7000-8000-000000000301"
_FOREIGN_ANCHOR = "00000000-0000-7000-8000-000000000302"
_SESSION = "00000000-0000-7000-8000-000000000501"
_SYMBOL_ID = "00000000-0000-7000-8000-000000000701"
_SECOND_SYMBOL_ID = "00000000-0000-7000-8000-000000000702"
_INSTRUMENT = "00000000-0000-7000-8000-000000000731"
_ORDER = "00000000-0000-7000-8000-000000000631"
_WRONG_TIP = "ab" * 32
_LATE_WITNESS_CLIENT_ORDER_ID = "spot-bundle-late-witness"
_CONFIRMED_ACTUAL = (
    '{"assets":{"DOGE":{"absent_as_zero":false,"total":"1"},'
    '"ETH":{"absent_as_zero":true,"total":"0"}}}'
)


async def _repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Create a fresh SQLite repository seeded with wallet and method config."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'bundle.db'}")
    await repo.create_all()
    async with repo.session() as session:
        session.add_all(
            [
                Wallet(
                    public_id=_WALLET,
                    label="spot-bundle",
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


async def _seed_market(
    repo: SQLAlchemyRepository,
    *,
    wallet: str = _WALLET,
    with_symbol: bool = True,
    with_spec: bool = False,
) -> None:
    """Seed the active Symbol, Instrument, Order, and optional spec lineage."""
    rows: list[object] = [
        Instrument(
            public_id=_INSTRUMENT,
            symbol_public_id=_SYMBOL_ID,
            exchange="kraken",
            timestamp=_T0 - timedelta(days=1),
            session_id=_SESSION,
            sequence_id=1,
            known_to=KNOWN_TO_MAX,
        ),
        Order(
            public_id=_ORDER,
            instrument_public_id=_INSTRUMENT,
            mode="live",
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
            known_to=KNOWN_TO_MAX,
        ),
    ]
    if with_symbol:
        rows.append(_symbol(_SYMBOL_ID, "BTC/USD", "BTC", 3))
    if with_spec:
        rows.append(_spec("00000000-0000-7000-8000-000000000801", 4))
    async with repo.session() as session:
        session.add_all(rows)
        await session.commit()


def _symbol(public_id: str, native_symbol: str, base: str, sequence_id: int) -> Symbol:
    """Build one active temporal symbol quoted in USD."""
    return Symbol(
        native_symbol=native_symbol,
        base=base,
        quote="USD",
        asset_type="crypto",
        created_at=_T0 - timedelta(days=1),
        public_id=public_id,
        session_id=_SESSION,
        sequence_id=sequence_id,
        timestamp=_T0 - timedelta(days=1),
        known_to=KNOWN_TO_MAX,
    )


def _spec(public_id: str, sequence_id: int) -> InstrumentSpec:
    """Build one active instrument specification for the seeded instrument."""
    return InstrumentSpec(
        instrument_public_id=_INSTRUMENT,
        unit_certified=False,
        public_id=public_id,
        session_id=_SESSION,
        sequence_id=sequence_id,
        timestamp=_T0 - timedelta(minutes=30),
        known_to=KNOWN_TO_MAX,
    )


def _precision_evidence(
    asset: str,
    sequence_id: int,
    *,
    balance_decimals: int | None,
    fee_decimals: int | None,
) -> SpotAssetPrecisionEvidence:
    """Build one active precision-evidence row with the requested planes."""
    balance_observed = balance_decimals is not None
    return SpotAssetPrecisionEvidence(
        exchange="kraken",
        asset=asset,
        balance_decimals=balance_decimals,
        balance_decimals_max=balance_decimals,
        balance_max_source="kraken:test" if balance_observed else None,
        balance_max_version="v1" if balance_observed else None,
        balance_max_observed_at=_T0 if balance_observed else None,
        balance_source="kraken:test" if balance_observed else None,
        balance_version="v1" if balance_observed else None,
        balance_observed_at=_T0 if balance_observed else None,
        fee_decimals=fee_decimals,
        fee_source="kraken:test" if fee_decimals is not None else None,
        fee_version="v1" if fee_decimals is not None else None,
        fee_observed_at=_T0 if fee_decimals is not None else None,
        session_id=_SESSION,
        sequence_id=sequence_id,
        timestamp=_T0 - timedelta(minutes=20),
        known_to=KNOWN_TO_MAX,
    )


async def _seed_executions(
    repo: SQLAlchemyRepository,
    count: int,
    *,
    wallet: str = _WALLET,
    start_sequence_id: int = 10,
    counter_amount_decimal: str | None = None,
) -> None:
    """Ingest ``count`` executions through the production scope-counter path."""
    for offset in range(count):
        await repo.insert_execution(
            order_public_id=_ORDER,
            wallet_public_id=wallet,
            timestamp=_T0,
            side="buy",
            status="filled",
            price=0.1,
            size=1.0,
            fee=0.0,
            fee_asset="BTC",
            counter_amount_decimal=counter_amount_decimal,
            session_id=_SESSION,
            sequence_id=start_sequence_id + offset,
        )


async def _derived_tip(repo: SQLAlchemyRepository, upto: int, *, wallet: str = _WALLET) -> str:
    """Fold the whole committed prefix from genesis to ``upto`` watermarks."""
    genesis = execution_chain_genesis(wallet, "kraken", "live")
    tip: str = await repo.get_spot_execution_chain_tip(wallet, "kraken", "live", 0, genesis, upto)
    return tip


def _anchor_row(
    *,
    wallet_public_id: str = _WALLET,
    source_watermark: int = 2,
    source_chain_tip: str = _WRONG_TIP,
) -> SpotReconciliationAnchorRow:
    """Build canonical anchor evidence with an overridable chain tip."""
    return {
        "public_id": _ANCHOR,
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
        "source_chain_tip": source_chain_tip,
        "venue_cursor_kind": "account_history_item_id",
        "venue_cursor_scheme": "kraken:ccxt:account/history:v1",
        "venue_cursor_value": "987654321",
        "venue_cursor_requested_at": _T0 - timedelta(seconds=4),
        "venue_cursor_observed_at": _T0 - timedelta(seconds=3),
        "venue_cursor_confirmed_at": _T0 + timedelta(seconds=2),
        "source_watermark_requested_at": _T0 - timedelta(seconds=2),
        "source_watermark_captured_at": _T0 - timedelta(seconds=1),
    }


async def _record_real_anchor(
    repo: SQLAlchemyRepository,
    *,
    wallet: str = _WALLET,
    watermark: int = 2,
) -> SpotReconciliationAnchorRow:
    """Record the anchor with its REAL derived chain tip and return the row."""
    tip = await _derived_tip(repo, watermark, wallet=wallet)
    row = _anchor_row(
        wallet_public_id=wallet,
        source_watermark=watermark,
        source_chain_tip=tip,
    )
    await repo.record_spot_reconciliation_anchor(row)
    return row


def _evaluation(
    *,
    status: str,
    source_watermark: int,
    source_chain_tip: str | None,
    actual_json: str = _CONFIRMED_ACTUAL,
) -> PortfolioReconciliationEvaluationRow:
    """Build one full S1 spot evaluation citing the seeded anchor."""
    return {
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "method": "spot_execution_replay",
        "evaluation_status": status,
        "venue_account_state_public_id": "00000000-0000-7000-8000-000000000202",
        "venue_account_observation_id": 42,
        "account_authoritative_until": _T0 + timedelta(minutes=5),
        "source_watermark_kind": "scope_sequence",
        "source_watermark": source_watermark,
        "source_chain_tip": source_chain_tip,
        "anchor_public_id": _ANCHOR,
        "expected_json": '{"BTC":"0.2"}',
        "actual_json": actual_json,
        "difference_json": '{"BTC":"0.1"}',
        "tolerance_json": '{"BTC":"0.0001"}',
        "error": None,
        "session_id": _SESSION,
        "sequence_id": 90,
        "bus_time": _T0 + timedelta(seconds=3),
    }


async def test_bundle_reads_anchor_replay_specs_precisions_and_tip_in_eight_sets(
    tmp_path: Path,
) -> None:
    """One snapshot serves every evaluator input from exactly eight set reads.

    Given: An anchored scope at watermark 2 with two later ingested fills,
        an active spec, and precision evidence for anchor, replay, and
        venue-only assets,
    When: The bundle is read at boundary watermark 4,
    Then: Replay, identity, spec, precision, and confirmed collections are
        complete, the counted proof passes, the boundary tip equals the
        independently derived genesis fold, and eight SELECTs were issued
        (anchor, replay, annulment manifest, state, chain fold, specs,
        precisions). The manifest read joins the snapshot because the replay
        the evaluator folds is the EFFECTIVE range, and evidence about which
        bookings still count has to come from the same pinned instant as the
        bookings themselves.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo, with_spec=True)
    async with repo.session() as session:
        session.add_all(
            [
                _precision_evidence("BTC", 1, balance_decimals=8, fee_decimals=None),
                _precision_evidence("USD", 2, balance_decimals=None, fee_decimals=4),
                _precision_evidence("EUR", 3, balance_decimals=2, fee_decimals=2),
            ]
        )
        await session.commit()
    await _seed_executions(repo, 2)
    anchor = await _record_real_anchor(repo)
    await _seed_executions(repo, 2, start_sequence_id=20)
    select_statements: list[str] = []

    def count_selects(*args: object) -> None:
        """Capture only SQL set reads emitted by the bundle operation."""
        if len(args) > 2 and isinstance(args[2], str):
            statement = args[2].lstrip()
            if statement.upper().startswith("SELECT"):
                select_statements.append(statement)

    event.listen(repo.engine.sync_engine, "before_cursor_execute", count_selects)
    try:
        bundle = await repo.get_spot_reconciliation_bundle(
            _WALLET, "kraken", "live", _AS_OF, 4, frozenset({"EUR"})
        )
    finally:
        event.remove(repo.engine.sync_engine, "before_cursor_execute", count_selects)

    assert bundle.error is None
    assert bundle.anchor == anchor
    assert [row["scope_sequence"] for row in bundle.replay] == [3, 4]
    assert bundle.replay[0] == {
        "scope_sequence": 3,
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "status": "filled",
        "instrument_public_id": _INSTRUMENT,
        "symbol": "BTC/USD",
        "base_asset": "BTC",
        "quote_asset": "USD",
        "side": "buy",
        "price": 0.1,
        "size": 1.0,
        "fee": 0.0,
        "fee_asset": "BTC",
        "price_decimal": None,
        "size_decimal": None,
        "fee_decimal": None,
        "counter_amount_decimal": None,
        "numeric_provenance": "legacy_float",
    }
    assert bundle.instruments_by_public_id == {
        _INSTRUMENT: {
            "instrument_public_id": _INSTRUMENT,
            "symbol": "BTC/USD",
            "base_asset": "BTC",
            "quote_asset": "USD",
        }
    }
    spec = bundle.specs_by_instrument_public_id[_INSTRUMENT]
    assert spec is not None
    assert spec["instrument_public_id"] == _INSTRUMENT
    assert set(bundle.asset_precisions) == {"BTC", "USD", "EUR"}
    assert bundle.asset_precisions["BTC"]["balance_decimals"] == 8
    assert bundle.asset_precisions["USD"]["balance_decimals"] is None
    assert bundle.asset_precisions["USD"]["fee_decimals"] == 4
    assert bundle.previously_confirmed_assets == frozenset({"BTC", "USD"})
    assert bundle.range_complete is True
    assert bundle.boundary_chain_tip == await _derived_tip(repo, 4)
    assert len(select_statements) == 8
    await repo.engine.dispose()


class _AnnulmentOptions(TypedDict, total=False):
    """Optional fields accepted by the replay-range manifest-row builder."""

    target_execution_public_id: str
    target_execution_digest: str
    scope_sequence: int
    timestamp: datetime


async def _annul_replay_execution(
    repo: SQLAlchemyRepository,
    scope_sequence: int,
    **options: Unpack[_AnnulmentOptions],
) -> None:
    """Append one manifest row bound to a real execution in the replay range.

    Built directly rather than through the guarded writer so its SERVER
    knowledge stamp can be a fixture instant: this suite asserts at a fixed
    historical ``as_of``, and a row the writer stamped with the wall clock
    would fall outside it. The default stamp sits well behind that horizon so
    the correction has SETTLED for it; the settling rule itself is pinned in
    the certification suite. The binding defaults to the stored target's own public id and
    freshly recomputed canonical digest, so the fold's real proof runs; the
    overrides exist to poison exactly one component at a time.
    """
    async with repo.session() as session:
        target = (
            (
                await session.execute(
                    select(Execution).where(Execution.scope_sequence == scope_sequence)
                )
            )
            .scalars()
            .one()
        )
        digest = execution_row_digest(SQLAlchemyRepository._execution_chain_record(target))
        session.add(
            ExecutionAnnulment(
                target_execution_public_id=options.get(
                    "target_execution_public_id", target.public_id
                ),
                target_execution_digest=options.get("target_execution_digest", digest),
                wallet_public_id=_WALLET,
                exchange="kraken",
                mode="live",
                scope_sequence=options.get("scope_sequence", scope_sequence),
                annulled_by_user_public_id="00000000-0000-7000-8000-0000000009d1",
                correction_time=_AS_OF,
                reason="unwitnessed_phantom",
                evidence_json='{"diagnosis":"hand-built"}',
                session_id=_SESSION,
                sequence_id=1,
                timestamp=options.get("timestamp", _AS_OF - timedelta(minutes=5)),
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()


async def _anchored_replay_scope(tmp_path: Path) -> SQLAlchemyRepository:
    """Seed one anchored scope at watermark 2 with two later ingested fills."""
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 2, start_sequence_id=20)
    return repo


async def test_bundle_drops_an_annulled_booking_after_counting_the_range(
    tmp_path: Path,
) -> None:
    """The counted proof stays physical while the fold consumes only what still counts.

    Given: An anchored scope at watermark 2 with two later ingested fills, the
        second of which an operator has repudiated through a fully bound
        manifest row.
    When: The bundle is read at boundary watermark 4.
    Then: ``range_complete`` still holds — contiguity is counted over the RAW
        range first, so a correction can never make a purged or tampered ledger
        look complete — while the replay the evaluator folds carries only the
        surviving booking, and the boundary chain tip is unmoved.
    """
    repo = await _anchored_replay_scope(tmp_path)
    await _annul_replay_execution(repo, 4)

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 4, frozenset({"EUR"})
    )

    assert bundle.error is None
    assert bundle.range_complete is True
    assert [row["scope_sequence"] for row in bundle.replay] == [3]
    assert bundle.boundary_chain_tip == await _derived_tip(repo, 4)
    await repo.engine.dispose()


async def test_bundle_ignores_a_correction_its_horizon_does_not_know(
    tmp_path: Path,
) -> None:
    """A repudiation recorded after the capture instant cannot rewrite it.

    Given: The same scope with the correction's SERVER knowledge stamp set one
        second AFTER the bundle's pinned capture instant.
    When: The bundle is read at that capture instant.
    Then: Both bookings still replay. The capture answers for its own moment,
        so a correction that was not yet known then must not retroactively
        change the range it certified.
    """
    repo = await _anchored_replay_scope(tmp_path)
    await _annul_replay_execution(repo, 4, timestamp=_AS_OF + timedelta(seconds=1))

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 4, frozenset({"EUR"})
    )

    assert bundle.error is None
    assert [row["scope_sequence"] for row in bundle.replay] == [3, 4]
    await repo.engine.dispose()


@pytest.mark.parametrize(
    ("options", "reason"),
    [
        pytest.param(
            {"target_execution_public_id": "00000000-0000-7000-8000-0000000009e1"},
            "dangling_annulment_binding",
            id="dangling-binding",
        ),
        pytest.param(
            {"target_execution_digest": "0" * 64},
            "annulment_digest_mismatch",
            id="digest-mismatch",
        ),
    ],
)
async def test_bundle_refuses_an_unprovable_manifest_binding(
    tmp_path: Path,
    options: _AnnulmentOptions,
    reason: str,
) -> None:
    """The replay may only suppress a booking on the SAME proof certification demands.

    Given: A manifest row covering the replay range whose binding is not
        provable — it names a row this range does not hold, or authorizes
        content the stored row does not have.
    When: The bundle is read.
    Then: It raises by name instead of dropping the coordinate. A scope-sequence
        match alone would let reconciliation certify balances against a
        repudiation the P&L certification plane would have refused, which is the
        exact divergence one shared validated fold exists to prevent.
    """
    repo = await _anchored_replay_scope(tmp_path)
    await _annul_replay_execution(repo, 4, **options)

    with pytest.raises(ExecutionChainError, match=reason):
        await repo.get_spot_reconciliation_bundle(
            _WALLET, "kraken", "live", _AS_OF, 4, frozenset({"EUR"})
        )
    await repo.engine.dispose()


async def test_bundle_refuses_a_contradicted_annulment(
    tmp_path: Path,
) -> None:
    """A fill that arrives after the correction refuses the whole replay.

    Given: A correctly bound repudiation of a replay-range booking, and a
        durable ``fill_observed`` witness for that booking's order delivered
        afterwards — the case the append-only writer could not have refused.
    When: The bundle is read.
    Then: It raises by name. The reconciliation plane re-asks the writer's own
        question at its own instant, so money the venue confirmed can never stay
        suppressed just because the suppression was already committed.
    """
    repo = await _anchored_replay_scope(tmp_path)
    await _annul_replay_execution(repo, 4)
    async with repo.session() as session:
        await session.execute(
            update(Order)
            .where(Order.public_id == _ORDER)
            .values(client_order_id=_LATE_WITNESS_CLIENT_ORDER_ID)
        )
        session.add(
            VenueEvent(
                event_type="fill_observed",
                shard_key="kraken.BTC-USD.live",
                wallet_public_id=_WALLET,
                command_public_id=None,
                exchange="kraken",
                instrument="BTC/USD",
                mode="live",
                exchange_order_id=None,
                client_order_id=_LATE_WITNESS_CLIENT_ORDER_ID,
                venue_client_id=_LATE_WITNESS_CLIENT_ORDER_ID,
                side="buy",
                status="filled",
                fill_price=0.1,
                fill_size=1.0,
                cum_fill_size=1.0,
                fee=0.0,
                fee_asset="BTC",
                exec_id="late-witness",
                trade_id=None,
                error=None,
                venue_timestamp=_AS_OF,
                received_at=_AS_OF,
                payload_json=None,
                liquidity_role="unknown",
                paired_group_id=None,
                timestamp=_AS_OF,
                known_to=KNOWN_TO_MAX,
                session_id=_SESSION,
                sequence_id=1,
            )
        )
        await session.commit()

    with pytest.raises(ExecutionChainError, match="annulled_execution_witnessed"):
        await repo.get_spot_reconciliation_bundle(
            _WALLET, "kraken", "live", _AS_OF, 4, frozenset({"EUR"})
        )
    await repo.engine.dispose()


async def test_bundle_empty_range_is_complete_and_returns_the_anchor_tip(
    tmp_path: Path,
) -> None:
    """A boundary equal to the anchor watermark is a proven-complete empty range.

    Given: An anchored scope at watermark 2 with no later fills,
    When: The bundle is read at boundary watermark 2,
    Then: The replay is empty yet complete and the boundary tip is the anchor
        tip folded over zero records.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    anchor = await _record_real_anchor(repo)

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 2, frozenset()
    )

    assert bundle.error is None
    assert bundle.replay == []
    assert bundle.instruments_by_public_id == {}
    assert bundle.specs_by_instrument_public_id == {}
    assert bundle.range_complete is True
    assert bundle.boundary_chain_tip == anchor["source_chain_tip"]
    assert bundle.previously_confirmed_assets == frozenset({"BTC", "USD"})
    await repo.engine.dispose()


async def test_bundle_replay_source_row_carries_the_exact_counter_amount(
    tmp_path: Path,
) -> None:
    """The exact per-fill counter amount round-trips into the replay source row.

    Given: An anchored scope whose later fill was ingested with an exact
        counter amount decimal,
    When: The bundle is read across the replay range,
    Then: The replay source row projects the persisted counter amount verbatim.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo, with_spec=True)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20, counter_amount_decimal="0.099")

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error is None
    assert [row["scope_sequence"] for row in bundle.replay] == [3]
    assert bundle.replay[0]["counter_amount_decimal"] == "0.099"
    await repo.engine.dispose()


async def test_bundle_without_anchor_returns_the_empty_ok_bundle(tmp_path: Path) -> None:
    """An unanchored account is honest emptiness, never a named refusal.

    Given: A scope with no recorded bootstrap anchor,
    When: The bundle is read,
    Then: Every collection is empty with ``anchor=None`` and ``error=None`` so
        the evaluator classifies ``missing_anchor`` itself.
    """
    repo = await _repo(tmp_path)

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 5, frozenset()
    )

    assert bundle.error is None
    assert bundle.anchor is None
    assert bundle.replay == []
    assert bundle.range_complete is False
    assert bundle.boundary_chain_tip is None
    assert bundle.previously_confirmed_assets == frozenset()
    await repo.engine.dispose()


@pytest.mark.parametrize(
    ("mode", "exchange"),
    [("paper", "kraken"), ("live", ""), ("live", "Kraken")],
)
async def test_bundle_rejects_invalid_snapshot_identity(
    tmp_path: Path,
    mode: str,
    exchange: str,
) -> None:
    """Non-live mode and non-canonical exchange fail before any set read.

    Given: A fresh repository,
    When: The bundle is requested with a paper mode, an empty exchange, or a
        mixed-case exchange,
    Then: The named identity refusal is returned with empty collections.

    Args:
        tmp_path: Pytest temporary directory.
        mode: Trading mode under test.
        exchange: Exchange spelling under test.
    """
    repo = await _repo(tmp_path)

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, exchange, mode, _AS_OF, 5, frozenset()
    )

    assert bundle.error == "invalid_spot_bundle_identity"
    assert bundle.anchor is None
    assert bundle.range_complete is False
    await repo.engine.dispose()


async def test_bundle_rejects_unsupported_repository_dialect(tmp_path: Path) -> None:
    """A repository outside the certified dialect pair fails before reading.

    Given: A repository whose dialect reports as MySQL,
    When: The bundle is requested,
    Then: The named dialect refusal is returned.
    """
    repo = await _repo(tmp_path)
    with patch.object(
        SQLAlchemyRepository,
        "dialect_name",
        new_callable=PropertyMock,
        return_value="mysql",
    ):
        bundle = await repo.get_spot_reconciliation_bundle(
            _WALLET, "kraken", "live", _AS_OF, 5, frozenset()
        )
    assert bundle.error == "unsupported_spot_bundle_dialect"
    await repo.engine.dispose()


@pytest.mark.parametrize("malformed_wallet", ["", "not-a-wallet-uuid"])
async def test_bundle_rejects_malformed_wallet_identity(
    tmp_path: Path,
    malformed_wallet: str,
) -> None:
    """Malformed wallet text raises the shared stable ValueError.

    Given: A fresh repository,
    When: The bundle is requested with empty or non-UUID wallet text,
    Then: The writer-compatible canonicalization error is raised.

    Args:
        tmp_path: Pytest temporary directory.
        malformed_wallet: Empty or non-UUID wallet text.
    """
    repo = await _repo(tmp_path)

    with pytest.raises(ValueError, match="reconciliation wallet identity is invalid"):
        await repo.get_spot_reconciliation_bundle(
            malformed_wallet, "kraken", "live", _AS_OF, 5, frozenset()
        )
    await repo.engine.dispose()


@pytest.mark.parametrize(
    "wallet_alias",
    [_ALPHA_WALLET.upper(), _ALPHA_WALLET.replace("-", "")],
)
async def test_bundle_wallet_filter_canonicalizes_uuid_aliases(
    tmp_path: Path,
    wallet_alias: str,
) -> None:
    """Uppercase and hyphenless wallet aliases find the canonical anchor scope.

    Given: An anchored scope stored under a canonical alphabetic wallet UUID,
    When: The bundle read uses its uppercase or hyphenless spelling,
    Then: The canonical anchor identity is served without error.

    Args:
        tmp_path: Pytest temporary directory.
        wallet_alias: Alternate spelling of the canonical wallet UUID.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo, wallet=_ALPHA_WALLET)
    await _seed_executions(repo, 2, wallet=_ALPHA_WALLET)
    await _record_real_anchor(repo, wallet=_ALPHA_WALLET)

    bundle = await repo.get_spot_reconciliation_bundle(
        wallet_alias, "kraken", "live", _AS_OF, 2, frozenset()
    )

    assert bundle.error is None
    assert bundle.anchor is not None
    assert bundle.anchor["wallet_public_id"] == _ALPHA_WALLET
    await repo.engine.dispose()


async def test_bundle_rejects_a_boundary_below_the_anchor_watermark(
    tmp_path: Path,
) -> None:
    """A boundary behind the sealed anchor can never define a replay range.

    Given: An anchored scope at watermark 2,
    When: The bundle is read at boundary watermark 1,
    Then: The named watermark refusal is returned.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 1, frozenset()
    )

    assert bundle.error == "invalid_spot_bundle_watermarks"
    await repo.engine.dispose()


async def test_bundle_rejects_a_sub_one_anchor_watermark(tmp_path: Path) -> None:
    """Identity corruption below the schema floor refuses by name.

    Given: A session serving an anchor row claiming watermark 0, which the
        0031 CHECK and the writer CAS both forbid ever persisting,
    When: The bundle is read,
    Then: The named watermark refusal is returned, never a genesis replay.
    """
    repo = await _repo(tmp_path)
    corrupt_anchor = PortfolioSpotReconciliationAnchor(
        **_anchor_row(source_watermark=0), known_to=KNOWN_TO_MAX
    )
    anchor_result = MagicMock()
    anchor_result.scalars.return_value.first.return_value = corrupt_anchor
    transaction_context = AsyncMock()
    session = AsyncMock(begin=MagicMock(return_value=transaction_context))
    session.execute = AsyncMock(return_value=anchor_result)
    with patch.object(repo, "session") as session_context:
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        bundle = await repo.get_spot_reconciliation_bundle(
            _WALLET, "kraken", "live", _AS_OF, 5, frozenset()
        )
    assert bundle.error == "invalid_spot_bundle_watermarks"
    await repo.engine.dispose()


async def test_bundle_rejects_corrupt_anchor_balance_evidence(tmp_path: Path) -> None:
    """A non-canonical balances payload refuses instead of shrinking the union.

    Given: A session serving an anchor whose balances carry a non-string
        amount, which the writer validation would have refused,
    When: The bundle is read,
    Then: The named corrupt-balances refusal is returned.
    """
    repo = await _repo(tmp_path)
    corrupt = _anchor_row(source_watermark=2)
    corrupt["balances_json"] = '{"BTC":1}'
    corrupt_anchor = PortfolioSpotReconciliationAnchor(**corrupt, known_to=KNOWN_TO_MAX)
    anchor_result = MagicMock()
    anchor_result.scalars.return_value.first.return_value = corrupt_anchor
    transaction_context = AsyncMock()
    session = AsyncMock(begin=MagicMock(return_value=transaction_context))
    session.execute = AsyncMock(return_value=anchor_result)
    with patch.object(repo, "session") as session_context:
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        bundle = await repo.get_spot_reconciliation_bundle(
            _WALLET, "kraken", "live", _AS_OF, 5, frozenset()
        )
    assert bundle.error == "corrupt_spot_anchor_balances"
    await repo.engine.dispose()


async def test_bundle_flags_a_retired_row_inside_the_replay_range(
    tmp_path: Path,
) -> None:
    """The read includes retired rows and trips on them, never filters them.

    Given: An anchored scope whose in-range execution row carries a closed
        ``known_to`` (inserted raw; the 0030 triggers forbid closing one),
    When: The bundle is read across that row,
    Then: The named unstable-range refusal proves the row was read, not
        silently excluded by an active-only filter.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    async with repo.session() as session:
        session.add(
            Execution(
                order_public_id=_ORDER,
                wallet_public_id=_WALLET,
                operator_public_id=None,
                exchange="kraken",
                mode="live",
                scope_sequence=3,
                side="buy",
                status="filled",
                price=0.1,
                size=1.0,
                fee=0.0,
                fee_asset="BTC",
                timestamp=_T0,
                session_id=_SESSION,
                sequence_id=30,
                known_to=_T0,
            )
        )
        await session.commit()

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error == "unstable_spot_replay_range"
    await repo.engine.dispose()


async def test_bundle_flags_a_dangling_execution_order_identity(tmp_path: Path) -> None:
    """An in-range fill whose order is inactive at the pin instant refuses.

    Given: An anchored scope with one replay fill whose order version was
        closed after ingest,
    When: The bundle is read at an instant where no order version is active,
    Then: The named missing-order refusal is returned.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20)
    async with repo.session() as session:
        await session.execute(
            update(Order)
            .where(Order.public_id == _ORDER, Order.known_to == KNOWN_TO_MAX)
            .values(known_to=_T0 - timedelta(hours=2))
        )
        await session.commit()

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error == "missing_spot_execution_order_identity"
    await repo.engine.dispose()


async def test_bundle_flags_a_missing_instrument_symbol_identity(tmp_path: Path) -> None:
    """A replay fill without an active symbol identity refuses by name.

    Given: An anchored scope whose instrument has no active Symbol row,
    When: The bundle is read across one replay fill,
    Then: The named missing-instrument refusal is returned.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo, with_symbol=False)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20)

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error == "missing_spot_instrument_identity"
    await repo.engine.dispose()


async def test_bundle_flags_a_joined_versus_stored_scope_conflict(tmp_path: Path) -> None:
    """The joined lineage must agree with the execution's sealed scope.

    Given: An anchored scope whose active order version was rewritten to
        paper mode after the live fill was ingested,
    When: The bundle is read across that fill,
    Then: The named conflicting-identity refusal is returned.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20)
    async with repo.session() as session:
        await session.execute(
            update(Order)
            .where(Order.public_id == _ORDER, Order.known_to == KNOWN_TO_MAX)
            .values(mode="paper")
        )
        await session.commit()

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error == "conflicting_spot_instrument_identity"
    await repo.engine.dispose()


async def test_bundle_flags_conflicting_duplicate_instrument_identity(
    tmp_path: Path,
) -> None:
    """One instrument id resolving to two different identities fails closed.

    Given: A session serving two replay rows that share one instrument id but
        disagree on its native symbol,
    When: The bundle is read,
    Then: The named conflicting-identity refusal is returned.
    """
    repo = await _repo(tmp_path)
    anchor = PortfolioSpotReconciliationAnchor(
        **_anchor_row(source_watermark=1), known_to=KNOWN_TO_MAX
    )
    anchor_result = MagicMock()
    anchor_result.scalars.return_value.first.return_value = anchor

    def _execution(scope_sequence: int) -> Execution:
        """Build one detached active execution row for the mocked join."""
        return Execution(
            order_public_id=_ORDER,
            wallet_public_id=_WALLET,
            operator_public_id=None,
            exchange="kraken",
            mode="live",
            scope_sequence=scope_sequence,
            side="buy",
            status="filled",
            price=0.1,
            size=1.0,
            fee=0.0,
            fee_asset="BTC",
            timestamp=_T0,
            session_id=_SESSION,
            sequence_id=scope_sequence,
            known_to=KNOWN_TO_MAX,
        )

    replay_result = MagicMock()
    replay_result.all.return_value = [
        (_execution(2), _ORDER, "live", _INSTRUMENT, "kraken", "BTC/USD", "BTC", "USD"),
        (_execution(3), _ORDER, "live", _INSTRUMENT, "kraken", "ETH/USD", "BTC", "USD"),
    ]
    transaction_context = AsyncMock()
    session = AsyncMock(begin=MagicMock(return_value=transaction_context))
    session.execute = AsyncMock(side_effect=[anchor_result, replay_result])
    with patch.object(repo, "session") as session_context:
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        bundle = await repo.get_spot_reconciliation_bundle(
            _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
        )
    assert bundle.error == "conflicting_spot_instrument_identity"
    await repo.engine.dispose()


async def test_bundle_flags_an_ambiguous_replay_join(tmp_path: Path) -> None:
    """Two active instrument rows for one logical id fail the replay closed.

    Given: An anchored scope whose instrument public id resolves to two active
        rows after the uniqueness index is removed,
    When: The bundle is read across one replay fill,
    Then: The duplicated join row is refused by name.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20)
    async with repo.session() as session:
        await session.execute(text("DROP INDEX ix_instruments_public_id"))
        session.add_all(
            [
                _symbol(_SECOND_SYMBOL_ID, "ETH/USD", "ETH", 40),
                Instrument(
                    public_id=_INSTRUMENT,
                    symbol_public_id=_SECOND_SYMBOL_ID,
                    exchange="kraken",
                    timestamp=_T0 - timedelta(days=1),
                    session_id=_SESSION,
                    sequence_id=41,
                    known_to=KNOWN_TO_MAX,
                ),
            ]
        )
        await session.commit()

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error == "ambiguous_spot_instrument_identity"
    await repo.engine.dispose()


async def test_bundle_flags_duplicate_active_instrument_specs(tmp_path: Path) -> None:
    """Two active specs for one replay instrument are never collapsed.

    Given: An anchored scope whose instrument carries two active spec versions
        after the uniqueness index is removed,
    When: The bundle is read across one replay fill,
    Then: The named duplicate-spec refusal is returned.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo, with_spec=True)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20)
    async with repo.session() as session:
        await session.execute(text("DROP INDEX uq_instrument_spec_instrument"))
        session.add(_spec("00000000-0000-7000-8000-000000000802", 42))
        await session.commit()

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error == "duplicate_spot_instrument_spec"
    await repo.engine.dispose()


async def test_bundle_flags_a_boundary_behind_the_matched_checkpoint(
    tmp_path: Path,
) -> None:
    """A boundary behind the newest matched checkpoint is a rollback signal.

    Given: An anchored scope whose matched verdict checkpointed watermark 4,
    When: The bundle is read at boundary watermark 3,
    Then: The named chain-regression refusal is returned.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 2, start_sequence_id=20)
    checkpoint_tip = await _derived_tip(repo, 4)
    await repo.record_portfolio_reconciliation(
        _evaluation(status="matched", source_watermark=4, source_chain_tip=checkpoint_tip)
    )

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error == "execution_chain_regressed"
    await repo.engine.dispose()


async def test_bundle_flags_a_checkpoint_tip_that_no_longer_derives(
    tmp_path: Path,
) -> None:
    """A stored matched tip that the anchor fold cannot reproduce refuses.

    Given: An anchored scope whose matched verdict checkpointed a well-formed
        but foreign chain tip at watermark 3,
    When: The bundle is read at boundary watermark 3,
    Then: The named chain-divergence refusal is returned.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20)
    await repo.record_portfolio_reconciliation(
        _evaluation(status="matched", source_watermark=3, source_chain_tip=_WRONG_TIP)
    )

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error == "execution_chain_diverged"
    await repo.engine.dispose()


async def test_bundle_extends_the_boundary_tip_from_a_matched_checkpoint(
    tmp_path: Path,
) -> None:
    """A verified checkpoint becomes the fold base for the boundary extension.

    Given: An anchored scope whose matched verdict checkpointed the REAL tip
        at watermark 3 and one later fill,
    When: The bundle is read at boundary watermark 4,
    Then: The checkpoint re-derives, the boundary tip extends beyond it, and
        the eligible verdict's confirmed assets join the anchor keys.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20)
    checkpoint_tip = await _derived_tip(repo, 3)
    await repo.record_portfolio_reconciliation(
        _evaluation(status="matched", source_watermark=3, source_chain_tip=checkpoint_tip)
    )
    await _seed_executions(repo, 1, start_sequence_id=30)

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 4, frozenset()
    )

    assert bundle.error is None
    assert bundle.range_complete is True
    assert bundle.boundary_chain_tip == await _derived_tip(repo, 4)
    assert bundle.previously_confirmed_assets == frozenset({"BTC", "USD", "DOGE"})
    await repo.engine.dispose()


async def test_bundle_confirms_assets_from_a_mismatched_full_state(
    tmp_path: Path,
) -> None:
    """A mismatched full verdict confirms observed assets without a checkpoint.

    Given: An anchored scope whose newest full verdict is mismatched, carries
        no chain tip, and observed DOGE present but ETH only absent-as-zero,
    When: The bundle is read at the verdict watermark,
    Then: DOGE joins the anchor keys, ETH does not, and the fold roots at the
        anchor because a mismatched verdict is never a chain checkpoint.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20)
    await repo.record_portfolio_reconciliation(
        _evaluation(status="mismatched", source_watermark=3, source_chain_tip=None)
    )

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error is None
    assert bundle.previously_confirmed_assets == frozenset({"BTC", "USD", "DOGE"})
    assert bundle.boundary_chain_tip == await _derived_tip(repo, 3)
    await repo.engine.dispose()


async def test_bundle_checkpoint_survives_a_later_mismatched_full_verdict(
    tmp_path: Path,
) -> None:
    """The chain checkpoint outlives the state row a mismatched verdict evicts.

    Given: An anchored scope whose matched verdict checkpointed the REAL tip
        at watermark 3 and whose later mismatched verdict carries a foreign
        tip, overwriting the active full-state row that once held the match,
    When: The bundle is read at boundary watermark 3,
    Then: The matched checkpoint is still found in the append-only epoch
        observations, never in the evicted state row nor the mismatched tip,
        so the read succeeds with the independently derived boundary tip.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20)
    checkpoint_tip = await _derived_tip(repo, 3)
    await repo.record_portfolio_reconciliation(
        _evaluation(status="matched", source_watermark=3, source_chain_tip=checkpoint_tip)
    )
    mismatched = _evaluation(status="mismatched", source_watermark=3, source_chain_tip=_WRONG_TIP)
    mismatched["sequence_id"] = 91
    mismatched["bus_time"] = _T0 + timedelta(seconds=4)
    await repo.record_portfolio_reconciliation(mismatched)

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error is None
    assert bundle.range_complete is True
    assert bundle.boundary_chain_tip == checkpoint_tip
    assert bundle.boundary_chain_tip == await _derived_tip(repo, 3)
    await repo.engine.dispose()


async def test_bundle_epoch_foreign_state_yields_anchor_keys_only(
    tmp_path: Path,
) -> None:
    """A verdict from another anchor epoch never contributes confirmed assets.

    Given: An anchored scope whose full-state row was rewritten to cite a
        foreign anchor epoch,
    When: The bundle is read,
    Then: Only the anchor's own balance keys are previously confirmed.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20)
    await repo.record_portfolio_reconciliation(
        _evaluation(status="mismatched", source_watermark=3, source_chain_tip=None)
    )
    async with repo.session() as session:
        await session.execute(
            update(PortfolioReconciliationState)
            .where(PortfolioReconciliationState.known_to == KNOWN_TO_MAX)
            .values(anchor_public_id=_FOREIGN_ANCHOR)
        )
        await session.commit()

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error is None
    assert bundle.previously_confirmed_assets == frozenset({"BTC", "USD"})
    await repo.engine.dispose()


@pytest.mark.parametrize(
    "corrupt_actual_json",
    [
        '{"assets":[]}',
        (
            '{"assets":{"BTC":{"absent_as_zero":false,"total":"1"},'
            '"BTC":{"absent_as_zero":false,"total":"1"}}}'
        ),
        '{"assets":{"BTC":"bad"}}',
    ],
)
async def test_bundle_flags_a_corrupt_confirmed_asset_source(
    tmp_path: Path,
    corrupt_actual_json: str,
) -> None:
    """A malformed retained payload refuses instead of shrinking the tripwire.

    Given: An eligible full-state row whose retained actual payload was
        rewritten to a non-object, duplicate-key, or malformed-detail shape,
    When: The bundle is read,
    Then: The named corrupt-confirmed-source refusal is returned.

    Args:
        tmp_path: Pytest temporary directory.
        corrupt_actual_json: The tampered retained payload under test.
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)
    await _seed_executions(repo, 1, start_sequence_id=20)
    await repo.record_portfolio_reconciliation(
        _evaluation(status="mismatched", source_watermark=3, source_chain_tip=None)
    )
    async with repo.session() as session:
        await session.execute(
            update(PortfolioReconciliationState)
            .where(PortfolioReconciliationState.known_to == KNOWN_TO_MAX)
            .values(actual_json=corrupt_actual_json)
        )
        await session.commit()

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 3, frozenset()
    )

    assert bundle.error == "corrupt_confirmed_asset_source"
    await repo.engine.dispose()


async def test_bundle_flags_a_broken_chain_when_the_range_has_a_gap(
    tmp_path: Path,
) -> None:
    """A counted gap fails both the proof and the fold, and the fold wins.

    Given: An anchored scope at watermark 2 with no fills beyond it,
    When: The bundle is read at boundary watermark 4,
    Then: The fold over the missing range raises and the named broken-chain
        refusal is returned (the fold runs regardless of the counted proof).
    """
    repo = await _repo(tmp_path)
    await _seed_market(repo)
    await _seed_executions(repo, 2)
    await _record_real_anchor(repo)

    bundle = await repo.get_spot_reconciliation_bundle(
        _WALLET, "kraken", "live", _AS_OF, 4, frozenset()
    )

    assert bundle.error == "execution_chain_broken"
    assert bundle.range_complete is False
    assert bundle.boundary_chain_tip is None
    await repo.engine.dispose()


async def test_bundle_sets_repeatable_read_only_transaction_on_postgresql(
    tmp_path: Path,
) -> None:
    """PostgreSQL bundle reads begin with the certified snapshot isolation.

    Given: A repository whose dialect reports as PostgreSQL,
    When: The bundle is read from a scope without an anchor,
    Then: The transaction's first statement is the REPEATABLE READ, READ ONLY
        SET and the empty OK bundle is returned.
    """
    repo = await _repo(tmp_path)
    anchor_result = MagicMock()
    anchor_result.scalars.return_value.first.return_value = None
    transaction_context = AsyncMock()
    session = AsyncMock(begin=MagicMock(return_value=transaction_context))
    session.execute = AsyncMock(side_effect=[MagicMock(), anchor_result])
    with (
        patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ),
        patch.object(repo, "session") as session_context,
    ):
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        bundle = await repo.get_spot_reconciliation_bundle(
            _WALLET, "kraken", "live", _AS_OF, 5, frozenset()
        )
    statement = session.execute.await_args_list[0].args[0]
    assert str(statement) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert session.execute.await_count == 2
    assert bundle.error is None
    assert bundle.anchor is None
    await repo.engine.dispose()


async def test_chain_tip_read_rejects_an_invalid_fold_range(tmp_path: Path) -> None:
    """The own-session fold delegate refuses malformed range bounds.

    Given: A fresh repository,
    When: The chain tip is requested with a negative lower watermark,
    Then: The fold raises the named chain error before reading any rows.
    """
    repo = await _repo(tmp_path)
    genesis = execution_chain_genesis(_WALLET, "kraken", "live")

    with pytest.raises(ExecutionChainError, match="invalid chain range"):
        await repo.get_spot_execution_chain_tip(_WALLET, "kraken", "live", -1, genesis, 0)
    await repo.engine.dispose()


async def test_precision_evidence_read_keeps_its_own_session_contract(
    tmp_path: Path,
) -> None:
    """The extracted session-scoped loader still serves the public reader.

    Given: One active precision-evidence row and a duplicate active pair made
        possible by removing the uniqueness index,
    When: The public reader is called with no assets, one asset, and the
        duplicated asset,
    Then: It returns the empty map, the single identity, and the duplicate
        refusal respectively, all through the shared session-scoped loader.
    """
    repo = await _repo(tmp_path)
    async with repo.session() as session:
        session.add(_precision_evidence("BTC", 1, balance_decimals=8, fee_decimals=None))
        await session.commit()

    assert await repo.get_spot_asset_precision_evidence("kraken", [], _AS_OF) == {}
    evidence = await repo.get_spot_asset_precision_evidence("kraken", ["BTC"], _AS_OF)
    assert set(evidence) == {"BTC"}
    assert evidence["BTC"]["balance_decimals"] == 8

    async with repo.session() as session:
        await session.execute(text("DROP INDEX uq_spot_asset_precision_evidence_exchange_asset"))
        session.add(_precision_evidence("BTC", 2, balance_decimals=9, fee_decimals=None))
        await session.commit()
    with pytest.raises(RuntimeError, match="duplicate spot asset precision evidence"):
        await repo.get_spot_asset_precision_evidence("kraken", ["BTC"], _AS_OF)
    await repo.engine.dispose()
