"""SQLite serialization, SAVEPOINT recovery, registry, and error-path tests for PR-1."""

import asyncio
import json
import sqlite3
from collections.abc import AsyncIterator
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import UUID
from uuid import uuid4

import pytest
from sqlalchemy import and_
from sqlalchemy import event
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session as SyncSession
from sqlalchemy.sql import Executable

import snapper.cli.app as cli_app
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry_syncer import ProcessRegistrySyncer
from snapper.application.process_manager.registry_syncer import upsert_scd2 as real_syncer_upsert
from snapper.application.updaters.symbols.walutomat import WalutomatSymbolUpdaterService
from snapper.cli.app import upsert_scd2 as real_cli_upsert
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.financial_account_owner import FinancialAccountOwnerAssignment
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.financial_account_owner_policy_writer import FinancialOwnerPolicyChange
from snapper.data.financial_account_owner_policy_writer import _lock_reserved_policy
from snapper.data.financial_account_owner_policy_writer import write_financial_owner_policy
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import InstrumentOrderCapability
from snapper.data.models import InstrumentSpec
from snapper.data.models import InstrumentUnderlyingMapping
from snapper.data.models import MarketSnapshot
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import Setting
from snapper.data.models import SpotAssetPrecisionEvidence
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import SymbolMarketDataChannelCapability
from snapper.data.models import TradeProjectionCheckpoint
from snapper.data.models import UnderlyingAsset
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.models import UserTradingCaps
from snapper.data.repository import SYMBOL_UPDATER_LOCK_IDENTITY
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import Scd2UpsertSpec
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import TemporalVersionConflictError
from snapper.data.repository import _apply_scd2_savepoint_async as real_savepoint_async
from snapper.data.repository import _apply_scd2_savepoint_sync as real_savepoint_sync
from snapper.data.repository import _is_unique_violation
from snapper.data.repository import _matches_target_unique_index
from snapper.data.repository import _resolve_target_unique_index
from snapper.data.repository import active_partial_unique_indexes
from snapper.data.repository import assert_scd2_write_reserved
from snapper.data.repository import begin_scd2_write
from snapper.data.repository import begin_scd2_write_sync
from snapper.data.repository import close_and_insert
from snapper.data.repository import close_and_insert_existing
from snapper.data.repository import close_and_insert_existing_sync
from snapper.data.repository import close_and_insert_sync
from snapper.data.repository import upsert_scd2
from snapper.data.repository import upsert_scd2_sync
from snapper.data.repository_types import MarketSnapshotUpsertRow
from snapper.infrastructure.security.encryption import SettingsEncryptionService
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.data.test_scd2_absence_pg18 import _session_uuid
from tests.data.test_scd2_absence_pg18 import _wait_for_blocked_lock
from tests.helpers.native_account_authority import build_native_authority_payload
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.native_account_authority import insert_native_authority
from tests.helpers.native_account_authority import native_authority_scenario

_T0 = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)
_T1 = _T0 + timedelta(minutes=1)
_T2 = _T0 + timedelta(minutes=2)


def _attach_begin_immediate_counter(engine: Engine) -> list[str]:
    """Attach a SQL listener recording every ``BEGIN IMMEDIATE`` statement."""
    statements: list[str] = []

    def _on_execute(
        _conn: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if "BEGIN IMMEDIATE" in statement.upper():
            statements.append(statement)

    event.listen(engine, "before_cursor_execute", _on_execute)
    return statements


def _setting_payload(key: str, val: str, seq: int) -> dict[str, object]:
    """Return a clean Setting upsert values dict."""
    return {
        "key": key,
        "value": val,
        "category": "app",
        "description": None,
        "is_encrypted": False,
        "session_id": "test",
        "sequence_id": seq,
    }


def _snapshot_row(instrument_pid: str, seq: int, bid: float) -> MarketSnapshotUpsertRow:
    """Return a MarketSnapshotUpsertRow for concurrency and fallback tests."""
    return MarketSnapshotUpsertRow(
        instrument_public_id=instrument_pid,
        timestamp=_T1,
        session_id=f"snap-{seq}",
        sequence_id=seq,
        bid=bid,
        bid_volume=1.0,
        ask=bid + 1.0,
        ask_volume=1.0,
        last_price=bid + 0.5,
        volume_24h=10.0,
        vwap_24h=bid + 0.5,
        low_24h=bid,
        high_24h=bid + 1.0,
        change_24h=0.0,
        spread=1.0,
        spread_pct=0.01,
        public_id=str(uuid4()),
        known_to=KNOWN_TO_MAX,
    )


@pytest.fixture
async def file_async_repo(tmp_path: Path) -> AsyncIterator[tuple[SQLAlchemyRepository, Path]]:
    """Provide a file-backed async SQLite repository with all tables created."""
    db_path = tmp_path / "scd2-async.db"
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await repo.create_all()
    try:
        yield repo, db_path
    finally:
        await repo.engine.dispose()


@pytest.fixture
def sync_repo(tmp_path: Path) -> Iterator[DatabaseRepository]:
    """Provide an isolated file-backed sync SQLite repository."""
    db_path = tmp_path / "scd2-sync.db"
    repo = DatabaseRepository(f"sqlite:///{db_path}")
    repo.create_all()
    try:
        yield repo
    finally:
        repo.engine.dispose()


async def _seed_symbol_and_instrument(
    repo: SQLAlchemyRepository,
    *,
    native_symbol: str = "BTC-USD",
    exchange: str = "kraken",
) -> tuple[str, str]:
    """Insert active Symbol and Instrument rows and return ``(symbol_pid, instrument_pid)``."""
    symbol_pid = str(uuid4())
    instrument_pid = str(uuid4())
    base, quote = native_symbol.split("-", maxsplit=1)
    async with repo.session() as session:
        await begin_scd2_write(session)
        session.add(
            Symbol(
                public_id=symbol_pid,
                native_symbol=native_symbol,
                base=base,
                quote=quote,
                asset_type="crypto",
                created_at=_T0,
                timestamp=_T0,
                session_id="seed",
                sequence_id=1,
            )
        )
        session.add(
            Instrument(
                public_id=instrument_pid,
                symbol_public_id=symbol_pid,
                exchange=exchange,
                timestamp=_T0,
                session_id="seed",
                sequence_id=2,
            )
        )
        await session.commit()
    return symbol_pid, instrument_pid


@pytest.mark.asyncio
async def test_sqlite_concurrent_revise_instrument_and_snapshots_serialize(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
) -> None:
    """Two concurrent SQLite writers serialize via BEGIN IMMEDIATE without SQLITE_BUSY."""
    repo1, db_path = file_async_repo
    repo2 = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        symbol_pid, instrument_pid = await _seed_symbol_and_instrument(repo1)
        barrier = asyncio.Barrier(2)

        async def _revise(target: SQLAlchemyRepository, seq: int) -> int:
            await barrier.wait()
            return await target.revise_instrument(
                instrument_public_id=instrument_pid,
                symbol_public_id=symbol_pid,
                exchange="kraken",
                session_id=f"worker-{seq}",
                sequence_id=seq,
                timestamp=_T1,
            )

        res = await asyncio.gather(_revise(repo1, 10), _revise(repo2, 11))
        assert len(res) == 2

        snap_barrier = asyncio.Barrier(2)

        async def _snap(target: SQLAlchemyRepository, seq: int, bid: float) -> int:
            await snap_barrier.wait()
            return await target.upsert_market_snapshots([_snapshot_row(instrument_pid, seq, bid)])

        snap_counts = await asyncio.gather(_snap(repo1, 1, 100.0), _snap(repo2, 2, 101.0))
        assert snap_counts == [1, 1]
    finally:
        await repo2.engine.dispose()


@pytest.mark.asyncio
async def test_sqlite_savepoint_unique_violation_preserves_outer_tx_work(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SAVEPOINT recovery inside BEGIN IMMEDIATE keeps prior outer-transaction writes."""
    repo, _ = file_async_repo
    async with repo.session() as seed_session:
        await begin_scd2_write(seed_session)
        seeded = await upsert_scd2(
            seed_session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "race_key"],
                bus_time=_T0,
                decide=lambda _row: _setting_payload("race_key", "v1", 1),
            ),
        )
        assert seeded.row is not None
        race_public_id = seeded.row.public_id
        await seed_session.commit()

    original_flush = AsyncSession.flush
    injected = False

    async def _fault_on_race_key(session: AsyncSession) -> None:
        nonlocal injected
        has_race_key = any(
            isinstance(obj, Setting) and obj.key == "race_key" and obj.value == "winner-v3"
            for obj in session.new
        )
        if has_race_key and not injected:
            injected = True
            session.expunge_all()
            original_exec = session.execute
            re_read_done = False

            async def _swap_winner(stmt: Executable) -> object:
                nonlocal re_read_done
                if not re_read_done and getattr(stmt, "is_select", False):
                    re_read_done = True
                    await original_exec(
                        update(Setting)
                        .where(Setting.key == "race_key", Setting.known_to == KNOWN_TO_MAX)
                        .values(known_to=_T1)
                    )
                    await original_exec(
                        insert(Setting).values(
                            id=9999,
                            public_id=race_public_id,
                            **_setting_payload("race_key", "v2-winner", 2),
                            timestamp=_T1,
                            known_to=KNOWN_TO_MAX,
                        )
                    )
                return await original_exec(stmt)

            monkeypatch.setattr(session, "execute", _swap_winner)
            raise IntegrityError(
                "insert",
                {},
                sqlite3.IntegrityError("UNIQUE constraint failed: settings.key"),
            )
        await original_flush(session)

    monkeypatch.setattr(AsyncSession, "flush", _fault_on_race_key)
    async with repo.session() as session:
        await begin_scd2_write(session)
        await upsert_scd2(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "prior_key"],
                bus_time=_T0,
                decide=lambda _row: _setting_payload("prior_key", "keep-me", 1),
            ),
        )
        result = await upsert_scd2(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "race_key"],
                bus_time=_T2,
                decide=lambda existing: _setting_payload(
                    "race_key",
                    "winner-v3" if existing is None or existing.id != 9999 else "final-v4",
                    3,
                ),
            ),
        )
        await session.commit()
    assert injected is True
    assert result.outcome == "updated"
    assert result.row is not None
    assert result.row.value == "final-v4"
    assert result.row.public_id == race_public_id
    async with repo.session() as session:
        keys = set(
            (
                await session.scalars(select(Setting.key).where(Setting.known_to == KNOWN_TO_MAX))
            ).all()
        )
        assert {"prior_key", "race_key"}.issubset(keys)


@pytest.mark.asyncio
async def test_sqlite_bulk_market_snapshots_fallback_on_unique_conflict(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bulk snapshot flush failure falls back per-row cleanly inside one BEGIN IMMEDIATE."""
    repo, _ = file_async_repo
    _, inst_pid = await _seed_symbol_and_instrument(repo)
    original_flush = AsyncSession.flush
    tripped = False

    async def _fail_bulk_once(session: AsyncSession) -> None:
        nonlocal tripped
        pending_snaps = [obj for obj in session.new if isinstance(obj, MarketSnapshot)]
        if pending_snaps and not tripped:
            tripped = True
            raise IntegrityError(
                "insert",
                {},
                sqlite3.IntegrityError(
                    "UNIQUE constraint failed: market_snapshots.instrument_public_id"
                ),
            )
        await original_flush(session)

    monkeypatch.setattr(AsyncSession, "flush", _fail_bulk_once)
    written = await repo.upsert_market_snapshots([_snapshot_row(inst_pid, 1, 99.5)])
    assert tripped is True
    assert written == 1


@pytest.mark.asyncio
async def test_composed_tx_regression_encryption_rotation(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_run_encryption_rotation rotates multiple settings under one outer BEGIN IMMEDIATE."""
    repo, db_path = file_async_repo
    monkeypatch.setenv("DB_URL", f"sqlite+aiosqlite:///{db_path}")
    old_enc = SettingsEncryptionService("old-password")
    async with repo.session() as session:
        await begin_scd2_write(session)
        await session.execute(Setting.__table__.delete())
        for seq, (key, secret) in enumerate((("enc_a", "val-1"), ("enc_b", "val-2")), start=1):
            session.add(
                Setting(
                    key=key,
                    value=old_enc.encrypt(secret),
                    category="api_keys",
                    description=None,
                    is_encrypted=True,
                    session_id="seed",
                    sequence_id=seq,
                    timestamp=_T0,
                    known_to=KNOWN_TO_MAX,
                )
            )
        await session.commit()

    begin_calls: list[str] = []

    def _on_execute(
        _conn: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if "BEGIN IMMEDIATE" in statement.upper():
            begin_calls.append(statement)

    event.listen(Engine, "before_cursor_execute", _on_execute)
    try:
        await cli_app._run_encryption_rotation("new-password", "old-password", dry_run=True)
        assert begin_calls == []

        await cli_app._run_encryption_rotation("new-password", "old-password", dry_run=False)
        assert len(begin_calls) == 1
    finally:
        event.remove(Engine, "before_cursor_execute", _on_execute)

    new_enc = SettingsEncryptionService("new-password")
    async with repo.session() as session:
        active = {
            row.key: new_enc.decrypt(row.value)
            for row in (
                await session.scalars(select(Setting).where(Setting.known_to == KNOWN_TO_MAX))
            ).all()
        }
        assert active == {"enc_a": "val-1", "enc_b": "val-2"}


@pytest.mark.asyncio
async def test_composed_tx_regression_financial_owner_policy_and_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """write_financial_owner_policy shares one outer BEGIN IMMEDIATE and guards unreserved use."""
    async with native_authority_scenario(tmp_path, monkeypatch) as scenario:
        outcome = await capture_native_account_read(scenario)
        payload = await build_native_authority_payload(scenario, outcome)
        await insert_native_authority(scenario, payload, 500)
        policy = FinancialAccountOwnerPolicy(
            assignments=(
                FinancialAccountOwnerAssignment(
                    owner_key=financial_account_owner_key_from_authority(payload),
                    owner_wallet_public_id=payload.scope.wallet_public_id,
                ),
            )
        )
        begin_calls = _attach_begin_immediate_counter(scenario.repository.engine.sync_engine)
        result = await write_financial_owner_policy(
            scenario.repository,
            FinancialOwnerPolicyChange(policy=policy, updated_by="ops-admin"),
            SequenceTracker(),
        )
        assert result.changed is True
        assert len(begin_calls) == 1

        async with scenario.repository.session() as unreserved:
            with pytest.raises(RuntimeError, match="without outer begin_scd2_write"):
                await _lock_reserved_policy(unreserved)


@pytest.mark.asyncio
async def test_composed_tx_regression_walutomat_updater_single_begin_immediate(
    sync_repo: DatabaseRepository,
) -> None:
    """Walutomat _update_database keeps precision evidence inside one BEGIN IMMEDIATE."""
    begin_calls = _attach_begin_immediate_counter(sync_repo.engine)
    updater = WalutomatSymbolUpdaterService(update_threshold_hours=1, force=True)
    updater.repository = sync_repo
    records: list[dict[str, object]] = [
        {
            "symbol": "EUR_PLN",
            "walutomat_rest_symbol": "EURPLN",
            "native_symbol": "EUR-PLN",
            "base": "EUR",
            "quote": "PLN",
        }
    ]
    await updater._update_database(records)
    assert len(begin_calls) == 1
    with sync_repo.get_session() as session:
        evidence = session.scalars(
            select(SpotAssetPrecisionEvidence).where(
                SpotAssetPrecisionEvidence.known_to == KNOWN_TO_MAX
            )
        ).all()
        assert len(evidence) >= 1


def test_reservation_lifecycle_boundaries_sync(sync_repo: DatabaseRepository) -> None:
    """Verify SAVEPOINT, commit, rollback, close, and DEFERRED detection on sync SQLite."""
    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)
        with session.begin_nested():
            assert_scd2_write_reserved(session)
        try:
            with session.begin_nested():
                session.add(
                    Setting(
                        **_setting_payload("sp_rolled", "v0", 1),
                        timestamp=_T0,
                        known_to=KNOWN_TO_MAX,
                    )
                )
                raise ValueError("force savepoint rollback")
        except ValueError:
            pass
        assert_scd2_write_reserved(session)
        outcome = upsert_scd2_sync(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "lifecycle_k"],
                bus_time=_T0,
                decide=lambda _r: _setting_payload("lifecycle_k", "v1", 2),
            ),
        )
        assert outcome.outcome == "created"
        session.commit()

        with pytest.raises(RuntimeError, match="without outer begin_scd2_write"):
            assert_scd2_write_reserved(session)

        begin_scd2_write_sync(session)
        assert_scd2_write_reserved(session)
        session.rollback()
        with pytest.raises(RuntimeError, match="without outer begin_scd2_write"):
            assert_scd2_write_reserved(session)

        session.execute(select(Setting))
        with pytest.raises(RuntimeError, match="already in progress"):
            begin_scd2_write_sync(session)
        session.rollback()

    with sync_repo.get_session() as fresh_session:
        with pytest.raises(RuntimeError, match="without outer begin_scd2_write"):
            assert_scd2_write_reserved(fresh_session)
        begin_scd2_write_sync(fresh_session)
        assert_scd2_write_reserved(fresh_session)


@pytest.mark.asyncio
async def test_reservation_lifecycle_boundaries_async(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
) -> None:
    """Verify SAVEPOINT, commit, rollback, close, and DEFERRED detection on async SQLite."""
    repo, _ = file_async_repo
    async with repo.session() as session:
        await begin_scd2_write(session)
        async with session.begin_nested():
            assert_scd2_write_reserved(session)
        try:
            async with session.begin_nested():
                session.add(
                    Setting(
                        **_setting_payload("sp_rolled_async", "v0", 1),
                        timestamp=_T0,
                        known_to=KNOWN_TO_MAX,
                    )
                )
                raise ValueError("force async savepoint rollback")
        except ValueError:
            pass
        assert_scd2_write_reserved(session)
        outcome = await upsert_scd2(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "lifecycle_async_k"],
                bus_time=_T0,
                decide=lambda _r: _setting_payload("lifecycle_async_k", "v1", 2),
            ),
        )
        assert outcome.outcome == "created"
        await session.commit()

        with pytest.raises(RuntimeError, match="without outer begin_scd2_write"):
            assert_scd2_write_reserved(session)

        await begin_scd2_write(session)
        assert_scd2_write_reserved(session)
        await session.rollback()
        with pytest.raises(RuntimeError, match="without outer begin_scd2_write"):
            assert_scd2_write_reserved(session)

        await session.execute(select(Setting))
        with pytest.raises(RuntimeError, match="already in progress"):
            await begin_scd2_write(session)
        await session.rollback()

    async with repo.session() as fresh_session:
        with pytest.raises(RuntimeError, match="without outer begin_scd2_write"):
            assert_scd2_write_reserved(fresh_session)
        await begin_scd2_write(fresh_session)
        assert_scd2_write_reserved(fresh_session)


def test_active_partial_unique_indexes_covers_all_17_models_and_exclusions() -> None:
    """Mapper inspection discovers all active partial unique indexes across 17 in-scope models."""
    cases = (
        (
            InstrumentSpec,
            "instrument_specs",
            "uq_instrument_spec_instrument",
            [InstrumentSpec.instrument_public_id == "inst-1"],
        ),
        (Symbol, "symbols", "uq_symbols_active_native", [Symbol.native_symbol == "BTC-USD"]),
        (
            SymbolAlias,
            "symbol_aliases",
            "uq_alias_spid_exchange_channel",
            [
                SymbolAlias.symbol_public_id == "s1",
                SymbolAlias.exchange == "kraken",
                SymbolAlias.channel == "ws",
            ],
        ),
        (
            SymbolExchangeCapability,
            "symbol_exchange_capabilities",
            "uq_sec_symbol_exchange",
            [
                SymbolExchangeCapability.symbol_public_id == "s1",
                SymbolExchangeCapability.exchange == "kraken",
            ],
        ),
        (
            SymbolMarketDataChannelCapability,
            "symbol_market_data_channel_capabilities",
            "uq_smdcc_symbol_exchange_channel",
            [
                SymbolMarketDataChannelCapability.symbol_public_id == "s1",
                SymbolMarketDataChannelCapability.exchange == "kraken_futures",
                SymbolMarketDataChannelCapability.channel == "trade",
            ],
        ),
        (
            InstrumentOrderCapability,
            "instrument_order_capabilities",
            "ix_ioc_instrument_exchange_active",
            [
                InstrumentOrderCapability.instrument_public_id == "i1",
                InstrumentOrderCapability.exchange == "kraken",
            ],
        ),
        (Setting, "settings", "uq_settings_key", [Setting.key == "k1"]),
        (
            MarketSnapshot,
            "market_snapshots",
            "uq_market_snapshot_instrument",
            [MarketSnapshot.instrument_public_id == "i1"],
        ),
        (
            Instrument,
            "instruments",
            "uq_instrument_spid_exchange",
            [Instrument.symbol_public_id == "s1", Instrument.exchange == "kraken"],
        ),
        (User, "users", "uq_users_username", [User.username == "alice"]),
        (
            UserTradingCaps,
            "user_trading_caps",
            "ix_user_trading_caps_active",
            [UserTradingCaps.user_public_id == "u1"],
        ),
        (
            UserOperatorMembership,
            "user_operator_memberships",
            "ix_user_operator_memberships_unique_active",
            [
                UserOperatorMembership.user_public_id == "u1",
                UserOperatorMembership.operator_public_id == "o1",
            ],
        ),
        (
            SpotAssetPrecisionEvidence,
            "spot_asset_precision_evidence",
            "uq_spot_asset_precision_evidence_exchange_asset",
            [
                SpotAssetPrecisionEvidence.exchange == "walutomat",
                SpotAssetPrecisionEvidence.asset == "EUR",
            ],
        ),
        (
            PortfolioPnlPoint,
            "portfolio_pnl_points",
            "uq_portfolio_pnl_points_identity",
            [
                PortfolioPnlPoint.wallet_public_id == "w1",
                PortfolioPnlPoint.mode == "live",
                PortfolioPnlPoint.point_time == _T0,
                PortfolioPnlPoint.valuation_ccy == "USD",
            ],
        ),
        (
            TradeProjectionCheckpoint,
            "trade_projection_checkpoints",
            "uq_trade_checkpoints_shard_key",
            [TradeProjectionCheckpoint.shard_key == "shard-1"],
        ),
        (
            UnderlyingAsset,
            "underlying_assets",
            "uq_underlying_assets_active_ticker",
            [UnderlyingAsset.ticker == "SPX"],
        ),
        (
            InstrumentUnderlyingMapping,
            "instrument_underlying_mappings",
            "uq_ium_active_instrument",
            [InstrumentUnderlyingMapping.instrument_public_id == "i1"],
        ),
    )
    assert len(cases) == 17
    for model, table_name, expected_index, filters in cases:
        discovered = {idx.name for idx in active_partial_unique_indexes(model)}
        assert expected_index in discovered
        resolved = _resolve_target_unique_index(model, table_name, filters, allow_insert=True)
        assert resolved is not None and resolved.name == expected_index

    and_resolved = _resolve_target_unique_index(
        Setting, "settings", [and_(Setting.key == "k1")], allow_insert=True
    )
    assert and_resolved is not None and and_resolved.name == "uq_settings_key"

    with pytest.raises(ValueError, match="cannot uniquely resolve active partial unique index"):
        _resolve_target_unique_index(Setting, "settings", [Setting.id == 1], allow_insert=True)
    assert (
        _resolve_target_unique_index(Setting, "settings", [Setting.id == 1], allow_insert=False)
        is None
    )

    membership_names = {idx.name for idx in active_partial_unique_indexes(UserOperatorMembership)}
    assert "ix_user_operator_memberships_primary_unique_active" not in membership_names
    candle_names = {idx.name for idx in active_partial_unique_indexes(Candle)}
    assert "ix_candle_instrument_open" not in candle_names
    assert "uq_candle_itf_open" in candle_names


@pytest.mark.asyncio
async def test_contract_a_vs_contract_b_clock_skew_and_absence(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
    sync_repo: DatabaseRepository,
) -> None:
    """Verify Contract A refusal vs Contract B clamping for absence and clock skew."""
    repo, _ = file_async_repo
    async with repo.session() as session:
        await begin_scd2_write(session)
        with pytest.raises(TemporalVersionConflictError):
            await close_and_insert_existing(
                session, Setting, [Setting.key == "absent"], _setting_payload("absent", "v", 1), _T1
            )
        first = await close_and_insert(
            session, Setting, [Setting.key == "skew_k"], _setting_payload("skew_k", "v1", 1), _T2
        )
        with pytest.raises(TemporalVersionConflictError):
            await close_and_insert_existing(
                session,
                Setting,
                [Setting.key == "skew_k"],
                _setting_payload("skew_k", "v2", 2),
                _T1,
            )
        clamped = await close_and_insert(
            session, Setting, [Setting.key == "skew_k"], _setting_payload("skew_k", "v2", 2), _T1
        )
        assert clamped.timestamp == _T2
        assert first.known_to == _T2
        await session.commit()

    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)
        with pytest.raises(TemporalVersionConflictError):
            close_and_insert_existing_sync(
                session, Setting, [Setting.key == "absent"], _setting_payload("absent", "v", 1), _T1
            )
        first_sync = close_and_insert_sync(
            session, Setting, [Setting.key == "skew_s"], _setting_payload("skew_s", "v1", 1), _T2
        )
        with pytest.raises(TemporalVersionConflictError):
            close_and_insert_existing_sync(
                session,
                Setting,
                [Setting.key == "skew_s"],
                _setting_payload("skew_s", "v2", 2),
                _T1,
            )
        clamped_sync = close_and_insert_sync(
            session, Setting, [Setting.key == "skew_s"], _setting_payload("skew_s", "v2", 2), _T1
        )
        assert clamped_sync.timestamp == _T2
        assert first_sync.known_to == _T2
        session.commit()


def test_closed_version_guard_contract_b_sync(sync_repo: DatabaseRepository) -> None:
    """Contract B refuses when a fetched version is already closed."""
    closed_row = Setting(
        id=42,
        **_setting_payload("closed_k", "old", 3),
        timestamp=_T0,
        known_to=_T1,
    )
    mock_session = MagicMock(spec=SyncSession)
    mock_session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    mock_session.execute.return_value.scalars.return_value.first.return_value = closed_row
    with pytest.raises(TemporalVersionConflictError):
        close_and_insert_sync(
            mock_session,
            Setting,
            [Setting.key == "closed_k"],
            _setting_payload("closed_k", "new", 4),
            _T2,
        )


@pytest.mark.asyncio
async def test_closed_version_guard_real_db_contract_a_and_b(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
    sync_repo: DatabaseRepository,
) -> None:
    """Contract A and Contract B refuse closing an already-closed version in a real DB."""
    repo, _ = file_async_repo
    t_mid = _T0 + timedelta(seconds=30)
    async with repo.session() as session:
        await begin_scd2_write(session)
        await close_and_insert(
            session,
            Setting,
            [Setting.key == "closed_real_a"],
            _setting_payload("closed_real_a", "v1", 1),
            _T0,
        )
        await close_and_insert(
            session,
            Setting,
            [Setting.key == "closed_real_a"],
            _setting_payload("closed_real_a", "v2", 2),
            _T2,
        )
        await session.commit()

    async with repo.session() as session:
        await begin_scd2_write(session)
        with pytest.raises(TemporalVersionConflictError):
            await close_and_insert_existing(
                session,
                Setting,
                [Setting.key == "closed_real_a"],
                _setting_payload("closed_real_a", "v3", 3),
                t_mid,
            )
        with pytest.raises(TemporalVersionConflictError):
            await close_and_insert(
                session,
                Setting,
                [Setting.key == "closed_real_a"],
                _setting_payload("closed_real_a", "v3", 3),
                t_mid,
            )

    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)
        close_and_insert_sync(
            session,
            Setting,
            [Setting.key == "closed_real_s"],
            _setting_payload("closed_real_s", "v1", 1),
            _T0,
        )
        close_and_insert_sync(
            session,
            Setting,
            [Setting.key == "closed_real_s"],
            _setting_payload("closed_real_s", "v2", 2),
            _T2,
        )
        session.commit()

    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)
        with pytest.raises(TemporalVersionConflictError):
            close_and_insert_existing_sync(
                session,
                Setting,
                [Setting.key == "closed_real_s"],
                _setting_payload("closed_real_s", "v3", 3),
                t_mid,
            )
        with pytest.raises(TemporalVersionConflictError):
            close_and_insert_sync(
                session,
                Setting,
                [Setting.key == "closed_real_s"],
                _setting_payload("closed_real_s", "v3", 3),
                t_mid,
            )
    del sync_repo


def test_secondary_unique_violation_existing_row_sync(sync_repo: DatabaseRepository) -> None:
    """Existing-row secondary unique index collision re-raises immediately with 0 retries."""
    sym_a = str(uuid4())
    sym_b = str(uuid4())
    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)
        for pid, native in ((sym_a, "AAA-USD"), (sym_b, "BBB-USD")):
            session.add(
                Symbol(
                    public_id=pid,
                    native_symbol=native,
                    base=native[:3],
                    quote="USD",
                    asset_type="crypto",
                    created_at=_T0,
                    timestamp=_T0,
                    session_id="s",
                    sequence_id=1,
                )
            )
        session.flush()
        for pid, wire, seq in ((sym_a, "WIRE_A", 1), (sym_b, "WIRE_B", 2)):
            upsert_scd2_sync(
                session,
                Scd2UpsertSpec(
                    model=SymbolAlias,
                    match_filters=[
                        SymbolAlias.symbol_public_id == pid,
                        SymbolAlias.exchange == "kraken",
                        SymbolAlias.channel == "ws",
                    ],
                    bus_time=_T0,
                    decide=lambda _r, p=pid, w=wire, s=seq: {
                        "symbol_public_id": p,
                        "exchange": "kraken",
                        "channel": "ws",
                        "exchange_symbol": w,
                        "created_at": _T0,
                        "session_id": "s",
                        "sequence_id": s,
                    },
                ),
            )
        session.commit()

    attempts = 0
    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)

        def _decide_existing(_existing: SymbolAlias | None) -> dict[str, object]:
            nonlocal attempts
            attempts += 1
            return {
                "symbol_public_id": sym_a,
                "exchange": "kraken",
                "channel": "ws",
                "exchange_symbol": "WIRE_B",
                "created_at": _T0,
                "session_id": "s",
                "sequence_id": 3,
            }

        with pytest.raises(IntegrityError):
            upsert_scd2_sync(
                session,
                Scd2UpsertSpec(
                    model=SymbolAlias,
                    match_filters=[
                        SymbolAlias.symbol_public_id == sym_a,
                        SymbolAlias.exchange == "kraken",
                        SymbolAlias.channel == "ws",
                    ],
                    bus_time=_T1,
                    decide=_decide_existing,
                ),
            )
    assert attempts == 1


def test_secondary_unique_violation_absence_and_non_unique_sync(
    sync_repo: DatabaseRepository,
) -> None:
    """Absence secondary collision and non-unique IntegrityError re-raise on attempt 1."""
    sym_b = str(uuid4())
    sym_c = str(uuid4())
    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)
        for pid, native in ((sym_b, "BBB2-USD"), (sym_c, "CCC2-USD")):
            session.add(
                Symbol(
                    public_id=pid,
                    native_symbol=native,
                    base=native[:4],
                    quote="USD",
                    asset_type="crypto",
                    created_at=_T0,
                    timestamp=_T0,
                    session_id="s",
                    sequence_id=1,
                )
            )
        session.flush()
        upsert_scd2_sync(
            session,
            Scd2UpsertSpec(
                model=SymbolAlias,
                match_filters=[
                    SymbolAlias.symbol_public_id == sym_b,
                    SymbolAlias.exchange == "kraken",
                    SymbolAlias.channel == "ws",
                ],
                bus_time=_T0,
                decide=lambda _r: {
                    "symbol_public_id": sym_b,
                    "exchange": "kraken",
                    "channel": "ws",
                    "exchange_symbol": "WIRE_SEC",
                    "created_at": _T0,
                    "session_id": "s",
                    "sequence_id": 1,
                },
            ),
        )

        absent_attempts = 0

        def _decide_absent(_existing: SymbolAlias | None) -> dict[str, object]:
            nonlocal absent_attempts
            absent_attempts += 1
            return {
                "symbol_public_id": sym_c,
                "exchange": "kraken",
                "channel": "ws",
                "exchange_symbol": "WIRE_SEC",
                "created_at": _T0,
                "session_id": "s",
                "sequence_id": 2,
            }

        with pytest.raises(IntegrityError):
            upsert_scd2_sync(
                session,
                Scd2UpsertSpec(
                    model=SymbolAlias,
                    match_filters=[
                        SymbolAlias.symbol_public_id == sym_c,
                        SymbolAlias.exchange == "kraken",
                        SymbolAlias.channel == "ws",
                    ],
                    bus_time=_T1,
                    decide=_decide_absent,
                ),
            )
        assert absent_attempts == 1

        with pytest.raises(IntegrityError):
            upsert_scd2_sync(
                session,
                Scd2UpsertSpec(
                    model=Symbol,
                    match_filters=[Symbol.native_symbol == "NULL-CHK"],
                    bus_time=_T1,
                    decide=lambda _r: {
                        "native_symbol": None,
                        "base": "NUL",
                        "quote": "USD",
                        "asset_type": "crypto",
                        "created_at": _T1,
                        "session_id": "s",
                        "sequence_id": 3,
                    },
                ),
            )


def test_retry_exhaustion_after_three_attempts_sync(
    sync_repo: DatabaseRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Advancing winners causing 3 unique conflicts raise TemporalVersionConflictError."""
    winners = [
        Setting(
            id=seq,
            **_setting_payload("exhaust_k", f"v{seq}", seq),
            timestamp=_T0,
            known_to=KNOWN_TO_MAX,
        )
        for seq in (10, 11, 12, 13)
    ]
    calls = 0

    def _fake_flush(session: SyncSession) -> None:
        if any(isinstance(obj, Setting) and obj.key == "exhaust_k" for obj in session.new):
            raise IntegrityError(
                "insert",
                {},
                sqlite3.IntegrityError("UNIQUE constraint failed: settings.key"),
            )

    def _advancing_fetch(stmt: object) -> SimpleNamespace:
        nonlocal calls
        if not getattr(stmt, "is_select", False):
            return SimpleNamespace(rowcount=1)
        row = winners[min(calls, len(winners) - 1)]
        calls += 1
        return SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: row))

    monkeypatch.setattr(SyncSession, "flush", _fake_flush)
    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)
        monkeypatch.setattr(session, "execute", _advancing_fetch)
        with pytest.raises(TemporalVersionConflictError):
            upsert_scd2_sync(
                session,
                Scd2UpsertSpec(
                    model=Setting,
                    match_filters=[Setting.key == "exhaust_k"],
                    bus_time=_T1,
                    decide=lambda row: _setting_payload(
                        "exhaust_k", f"new-{row.sequence_id if row else 0}", 99
                    ),
                ),
            )
    assert calls == 4


class _FakeDbApiError(Exception):
    """Minimal DBAPI-shaped exception for dual-dialect classifier tests."""

    def __init__(self, **attrs: object) -> None:
        super().__init__("db error")
        for key, value in attrs.items():
            setattr(self, key, value)


def test_dialect_unique_classifiers_and_identity_update(sync_repo: DatabaseRepository) -> None:
    """Verify asyncpg/psycopg/SQLite unique violation matching and public_id preservation."""
    missing_orig = IntegrityError("x", {}, Exception("x"))
    missing_orig.orig = None
    assert _is_unique_violation(missing_orig) is False

    asyncpg_exc = IntegrityError(
        "x", {}, _FakeDbApiError(sqlstate="23505", constraint_name="uq_settings_key")
    )
    assert _is_unique_violation(asyncpg_exc) is True
    target = _resolve_target_unique_index(
        Setting, "settings", [Setting.key == "k"], allow_insert=True
    )
    assert target is not None
    assert _matches_target_unique_index(asyncpg_exc, "settings", target) is True
    assert _matches_target_unique_index(asyncpg_exc, "settings", None) is False

    other_pg = IntegrityError(
        "x",
        {},
        _FakeDbApiError(pgcode="23505", diag=SimpleNamespace(constraint_name="other_idx")),
    )
    assert _is_unique_violation(other_pg) is True
    assert _matches_target_unique_index(other_pg, "settings", target) is False

    check_pg = IntegrityError("x", {}, _FakeDbApiError(pgcode="23514"))
    assert _is_unique_violation(check_pg) is False

    sqlite_errno_exc = IntegrityError(
        "x",
        {},
        _FakeDbApiError(sqlite_errorcode=2067, args=("UNIQUE constraint failed: settings.key",)),
    )
    assert _is_unique_violation(sqlite_errno_exc) is True
    assert _matches_target_unique_index(sqlite_errno_exc, "settings", target) is True

    pg_session = MagicMock(spec=SyncSession)
    pg_session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    begin_scd2_write_sync(pg_session)
    assert_scd2_write_reserved(pg_session)

    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)
        first_pid = str(uuid4())
        second_pid = str(uuid4())
        created = upsert_scd2_sync(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "keep_pid_k"],
                bus_time=_T0,
                decide=lambda _r: {
                    **_setting_payload("keep_pid_k", "v1", 1),
                    "public_id": first_pid,
                },
            ),
        )
        assert created.outcome == "created"
        assert created.row is not None and created.row.public_id == first_pid
        updated = upsert_scd2_sync(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "keep_pid_k"],
                bus_time=_T1,
                decide=lambda _r: {
                    **_setting_payload("keep_pid_k", "v2", 2),
                    "public_id": second_pid,
                },
            ),
        )
        assert updated.outcome == "updated"
        assert updated.row is not None and updated.row.public_id == first_pid
        unchanged = upsert_scd2_sync(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "keep_pid_k"],
                bus_time=_T2,
                decide=lambda _r: None,
            ),
        )
        assert unchanged.outcome == "unchanged"
        assert unchanged.row is not None and unchanged.row.public_id == first_pid
        session.commit()


def _make_mock_pg_async_session(*, in_tx: bool) -> MagicMock:
    """Create a mock async session reporting PostgreSQL dialect."""
    pg_session = MagicMock(spec=AsyncSession)
    pg_session.info = {}
    pg_session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    pg_session.in_transaction.return_value = in_tx
    pg_session.execute = AsyncMock()
    return pg_session


@pytest.mark.asyncio
async def test_pg_read_committed_isolation_emitted_before_queries() -> None:
    """Verify explicit PG READ COMMITTED emission and idempotency across reserved sessions."""
    pg_async = _make_mock_pg_async_session(in_tx=False)
    await begin_scd2_write(pg_async)
    await begin_scd2_write(pg_async)
    sql_texts = [str(call.args[0]) for call in pg_async.execute.await_args_list]
    assert sql_texts == ["SET TRANSACTION ISOLATION LEVEL READ COMMITTED"]

    pg_sync = MagicMock(spec=SyncSession)
    pg_sync.info = {}
    pg_sync.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    pg_sync.in_transaction.return_value = False
    begin_scd2_write_sync(pg_sync)
    begin_scd2_write_sync(pg_sync)
    DatabaseRepository.acquire_symbol_updater_lock_sync(pg_sync)
    DatabaseRepository._acquire_spot_asset_precision_evidence_advisory_lock_sync(
        pg_sync, "walutomat", "EUR"
    )
    sync_sql = [str(call.args[0]) for call in pg_sync.execute.call_args_list]
    assert sync_sql[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert sync_sql.count("SET TRANSACTION ISOLATION LEVEL READ COMMITTED") == 1
    assert pg_sync.execute.call_args_list[1].args[1] == {"identity": SYMBOL_UPDATER_LOCK_IDENTITY}

    pg_lock_only = MagicMock(spec=SyncSession)
    pg_lock_only.info = {}
    pg_lock_only.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    pg_lock_only.in_transaction.return_value = False
    DatabaseRepository.acquire_symbol_updater_lock_sync(pg_lock_only)
    DatabaseRepository.acquire_symbol_updater_lock_sync(pg_lock_only)
    lock_sql = [str(call.args[0]) for call in pg_lock_only.execute.call_args_list]
    assert lock_sql == [
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SELECT pg_advisory_xact_lock(hashtext(:identity))",
        "SELECT pg_advisory_xact_lock(hashtext(:identity))",
    ]

    pg_direct_async = _make_mock_pg_async_session(in_tx=False)
    pg_direct_async.execute.return_value = MagicMock(
        scalars=MagicMock(return_value=MagicMock(first=MagicMock(return_value=None)))
    )
    direct_spec = Scd2UpsertSpec(
        model=Setting,
        match_filters=[Setting.key == "pg_auto_iso"],
        bus_time=_T0,
        decide=lambda _row: None,
    )
    await upsert_scd2(pg_direct_async, direct_spec)
    await upsert_scd2(pg_direct_async, direct_spec)
    async_direct_sql = [str(call.args[0]) for call in pg_direct_async.execute.await_args_list]
    assert async_direct_sql[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert "FROM settings" in async_direct_sql[1]
    assert async_direct_sql.count("SET TRANSACTION ISOLATION LEVEL READ COMMITTED") == 1

    pg_direct_sync = MagicMock(spec=SyncSession)
    pg_direct_sync.info = {}
    pg_direct_sync.get_bind.return_value = SimpleNamespace(
        dialect=SimpleNamespace(name="postgresql")
    )
    pg_direct_sync.in_transaction.return_value = False
    pg_direct_sync.execute.return_value = MagicMock(
        scalars=MagicMock(return_value=MagicMock(first=MagicMock(return_value=None)))
    )
    sync_spec = Scd2UpsertSpec(
        model=Setting,
        match_filters=[Setting.key == "pg_auto_iso_sync"],
        bus_time=_T0,
        decide=lambda _row: None,
    )
    upsert_scd2_sync(pg_direct_sync, sync_spec)
    upsert_scd2_sync(pg_direct_sync, sync_spec)
    sync_direct_sql = [str(call.args[0]) for call in pg_direct_sync.execute.call_args_list]
    assert sync_direct_sql[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert "FROM settings" in sync_direct_sql[1]
    assert sync_direct_sql.count("SET TRANSACTION ISOLATION LEVEL READ COMMITTED") == 1


@pytest.mark.asyncio
async def test_async_post_conflict_reread_recovers_and_exhausts(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
) -> None:
    """Verify bounded async post-conflict fresh re-read recovery and exhaustion."""
    repo, _ = file_async_repo
    async with repo.session() as session:
        await begin_scd2_write(session)
        await upsert_scd2(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "wake_async"],
                bus_time=_T0,
                decide=lambda _row: _setting_payload("wake_async", "v1", 1),
            ),
        )
        await session.commit()

    async with repo.session() as session:
        await begin_scd2_write(session)
        winner = (
            await session.execute(
                select(Setting).where(Setting.key == "wake_async", Setting.known_to == KNOWN_TO_MAX)
            )
        ).scalar_one()
        fetch_calls = 0
        savepoint_calls = 0

        async def _stub_fetch_async(*args: object, **kwargs: object) -> Setting | None:
            nonlocal fetch_calls
            fetch_calls += 1
            if fetch_calls in (1, 2):
                return None
            return winner

        async def _stub_savepoint_async(
            s: AsyncSession,
            model: type[Setting],
            existing: Setting | None,
            candidate: dict[str, object],
        ) -> Setting:
            nonlocal savepoint_calls
            savepoint_calls += 1
            if savepoint_calls == 1:
                raise IntegrityError(
                    "stmt",
                    {},
                    _FakeDbApiError(
                        sqlite_errorcode=2067, args=("UNIQUE constraint failed: settings.key",)
                    ),
                )
            return await real_savepoint_async(s, model, existing, candidate)

        with (
            patch("snapper.data.repository._fetch_scd2_existing_async", new=_stub_fetch_async),
            patch("snapper.data.repository._apply_scd2_savepoint_async", new=_stub_savepoint_async),
        ):
            outcome = await upsert_scd2(
                session,
                Scd2UpsertSpec(
                    model=Setting,
                    match_filters=[Setting.key == "wake_async"],
                    bus_time=_T1,
                    decide=lambda row: _setting_payload(
                        "wake_async", f"after-{row.value if row else 'none'}", 2
                    ),
                ),
            )
        assert outcome.outcome == "updated"
        assert outcome.row is not None and outcome.row.value == "after-v1"
        await session.commit()

    async with repo.session() as session:
        await begin_scd2_write(session)
        with (
            patch(
                "snapper.data.repository._fetch_scd2_existing_async",
                new=AsyncMock(return_value=None),
            ),
            patch(
                "snapper.data.repository._apply_scd2_savepoint_async",
                new=AsyncMock(
                    side_effect=IntegrityError(
                        "stmt",
                        {},
                        _FakeDbApiError(
                            sqlite_errorcode=2067, args=("UNIQUE constraint failed: settings.key",)
                        ),
                    )
                ),
            ),
            pytest.raises(IntegrityError),
        ):
            await upsert_scd2(
                session,
                Scd2UpsertSpec(
                    model=Setting,
                    match_filters=[Setting.key == "wake_async"],
                    bus_time=_T2,
                    decide=lambda _row: _setting_payload("wake_async", "fail", 3),
                ),
            )


def test_sync_post_conflict_reread_recovers_and_exhausts(
    sync_repo: DatabaseRepository,
) -> None:
    """Verify bounded sync post-conflict fresh re-read recovery and exhaustion."""
    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)
        upsert_scd2_sync(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "wake_sync"],
                bus_time=_T0,
                decide=lambda _row: _setting_payload("wake_sync", "v1", 1),
            ),
        )
        session.commit()

    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)
        sync_winner = session.execute(
            select(Setting).where(Setting.key == "wake_sync", Setting.known_to == KNOWN_TO_MAX)
        ).scalar_one()
        sync_fetches = 0
        sync_saves = 0

        def _stub_fetch_sync(*args: object, **kwargs: object) -> Setting | None:
            nonlocal sync_fetches
            sync_fetches += 1
            if sync_fetches in (1, 2):
                return None
            return sync_winner

        def _stub_savepoint_sync(
            s: SyncSession,
            model: type[Setting],
            existing: Setting | None,
            candidate: dict[str, object],
        ) -> Setting:
            nonlocal sync_saves
            sync_saves += 1
            if sync_saves == 1:
                raise IntegrityError(
                    "stmt",
                    {},
                    _FakeDbApiError(
                        sqlite_errorcode=2067, args=("UNIQUE constraint failed: settings.key",)
                    ),
                )
            return real_savepoint_sync(s, model, existing, candidate)

        with (
            patch("snapper.data.repository._fetch_scd2_existing_sync", new=_stub_fetch_sync),
            patch("snapper.data.repository._apply_scd2_savepoint_sync", new=_stub_savepoint_sync),
        ):
            sync_outcome = upsert_scd2_sync(
                session,
                Scd2UpsertSpec(
                    model=Setting,
                    match_filters=[Setting.key == "wake_sync"],
                    bus_time=_T1,
                    decide=lambda row: _setting_payload(
                        "wake_sync", f"after-{row.value if row else 'none'}", 2
                    ),
                ),
            )
        assert sync_outcome.outcome == "updated"
        assert sync_outcome.row is not None and sync_outcome.row.value == "after-v1"
        session.commit()

    with sync_repo.get_session() as session:
        begin_scd2_write_sync(session)
        with (
            patch("snapper.data.repository._fetch_scd2_existing_sync", return_value=None),
            patch(
                "snapper.data.repository._apply_scd2_savepoint_sync",
                side_effect=IntegrityError(
                    "stmt",
                    {},
                    _FakeDbApiError(
                        sqlite_errorcode=2067, args=("UNIQUE constraint failed: settings.key",)
                    ),
                ),
            ),
            pytest.raises(IntegrityError),
        ):
            upsert_scd2_sync(
                session,
                Scd2UpsertSpec(
                    model=Setting,
                    match_filters=[Setting.key == "wake_sync"],
                    bus_time=_T2,
                    decide=lambda _row: _setting_payload("wake_sync", "fail", 3),
                ),
            )


@pytest.mark.asyncio
async def test_registry_syncer_rereads_active_config_on_lock_wake_miss(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
) -> None:
    """Process config mutations re-read on lock-wake KeyError and raise only when truly absent."""
    repo, db_path = file_async_repo
    db_url = f"sqlite+aiosqlite:///{db_path}"
    future_ts = datetime.now(UTC) + timedelta(minutes=5)
    async with repo.session() as session:
        await begin_scd2_write(session)
        initial_value = json.dumps(
            {"class": "snapper.Mod", "enabled": True, "role": "service", "parameters": {"a": 1}}
        )
        await upsert_scd2(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "process_worker_a"],
                bus_time=future_ts,
                decide=lambda _row: {
                    "key": "process_worker_a",
                    "value": initial_value,
                    "category": "process_config",
                    "description": "Process config for worker_a",
                    "is_encrypted": False,
                    "session_id": str(uuid4()),
                    "sequence_id": 1,
                    "updated_by": "seed",
                },
            ),
        )
        await session.commit()

    syncer = ProcessRegistrySyncer(
        AppSettings(BootstrapSettingsLoader(DB_URL=db_url), settings_service=None)
    )

    upsert_calls = 0

    async def _flaky_wake_upsert(session: AsyncSession, spec: Scd2UpsertSpec[Setting]) -> object:
        nonlocal upsert_calls
        upsert_calls += 1
        if upsert_calls == 1:
            raise KeyError("worker_a")
        return await real_syncer_upsert(session, spec)

    with (
        patch(
            "snapper.application.process_manager.registry_syncer.get_repository",
            return_value=repo,
        ),
        patch(
            "snapper.application.process_manager.registry_syncer.upsert_scd2",
            new=_flaky_wake_upsert,
        ),
    ):
        await syncer.update_process_config(
            name="worker_a",
            enabled=False,
            updated_by="ops",
        )
    assert upsert_calls == 2
    async with repo.session() as session:
        updated_row = (
            await session.execute(
                select(Setting).where(
                    Setting.key == "process_worker_a", Setting.known_to == KNOWN_TO_MAX
                )
            )
        ).scalar_one()
        assert json.loads(updated_row.value)["enabled"] is False
        assert updated_row.timestamp == future_ts

    upsert_calls = 0
    with (
        patch(
            "snapper.application.process_manager.registry_syncer.get_repository",
            return_value=repo,
        ),
        patch(
            "snapper.application.process_manager.registry_syncer.upsert_scd2",
            new=_flaky_wake_upsert,
        ),
    ):
        params = await syncer.update_process_config_parameters(
            name="worker_a",
            parameters={"a": 2},
            updated_by="ops2",
        )
    assert upsert_calls == 2
    assert params == {"a": 2}

    with (
        patch(
            "snapper.application.process_manager.registry_syncer.get_repository",
            return_value=repo,
        ),
        pytest.raises(KeyError, match="ghost"),
    ):
        await syncer.update_process_config(name="ghost", enabled=True, updated_by="ops")

    with (
        patch(
            "snapper.application.process_manager.registry_syncer.get_repository",
            return_value=repo,
        ),
        patch(
            "snapper.application.process_manager.registry_syncer.upsert_scd2",
            new=AsyncMock(side_effect=KeyError("worker_a")),
        ),
        pytest.raises(KeyError, match="worker_a"),
    ):
        await syncer.update_process_config(name="worker_a", enabled=True, updated_by="ops")


@pytest.mark.asyncio
async def test_upsert_market_snapshots_preserves_finite_known_to_across_batch_shapes(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snapshot expiry stays identical across unique, duplicate-key, and conflict-fallback batches."""
    repo, _ = file_async_repo
    async with repo.session() as session:
        await begin_scd2_write(session)
        sym = Symbol(
            native_symbol="SNAP-USD",
            base="SNAP",
            quote="USD",
            asset_type="crypto",
            created_at=_T0,
            timestamp=_T0,
            session_id="seed",
            sequence_id=1,
        )
        session.add(sym)
        await session.flush()
        inst_rows = [
            Instrument(
                symbol_public_id=sym.public_id,
                exchange=ex,
                timestamp=_T0,
                session_id="seed",
                sequence_id=idx,
            )
            for idx, ex in enumerate(("kraken", "binance", "coinbase"), start=1)
        ]
        session.add_all(inst_rows)
        await session.commit()
        inst_u, inst_d, inst_f = [row.public_id for row in inst_rows]

    unique_batch: list[MarketSnapshotUpsertRow] = [
        {**_snapshot_row(inst_u, 1, 10.0), "timestamp": _T0, "known_to": _T1}
    ]
    assert await repo.upsert_market_snapshots(unique_batch) == 1
    assert unique_batch[0]["known_to"] == _T1

    dup_batch: list[MarketSnapshotUpsertRow] = [
        {**_snapshot_row(inst_d, 1, 20.0), "timestamp": _T0, "known_to": _T1},
        {**_snapshot_row(inst_d, 2, 22.0), "timestamp": _T1, "known_to": _T2},
    ]
    assert await repo.upsert_market_snapshots(dup_batch) == 2
    assert dup_batch[0]["known_to"] == _T1
    assert dup_batch[1]["known_to"] == _T2

    fallback_batch: list[MarketSnapshotUpsertRow] = [
        {**_snapshot_row(inst_f, 1, 30.0), "timestamp": _T0, "known_to": _T1}
    ]
    original_flush = AsyncSession.flush
    tripped = False

    async def _fail_bulk_once(session: AsyncSession) -> None:
        nonlocal tripped
        if not tripped and any(isinstance(obj, MarketSnapshot) for obj in session.new):
            tripped = True
            raise IntegrityError(
                "stmt",
                {},
                _FakeDbApiError(
                    sqlite_errorcode=2067,
                    args=("UNIQUE constraint failed: market_snapshots.instrument_public_id",),
                ),
            )
        await original_flush(session)

    monkeypatch.setattr(AsyncSession, "flush", _fail_bulk_once)
    with patch.object(
        SQLAlchemyRepository,
        "_upsert_snapshot_row",
        wraps=repo._upsert_snapshot_row,
    ) as spy_row:
        assert await repo.upsert_market_snapshots(fallback_batch) == 1
    assert tripped is True
    spy_row.assert_awaited_once()
    assert fallback_batch[0]["known_to"] == _T1

    async with repo.session() as session:
        saved = (
            (
                await session.execute(
                    select(MarketSnapshot).order_by(
                        MarketSnapshot.instrument_public_id, MarketSnapshot.timestamp
                    )
                )
            )
            .scalars()
            .all()
        )
        by_inst: dict[str, list[datetime]] = {}
        for row in saved:
            by_inst.setdefault(row.instrument_public_id, []).append(row.known_to)
        assert by_inst[inst_u] == [_T1]
        assert by_inst[inst_d] == [_T1, _T2]
        assert by_inst[inst_f] == [_T1]


@pytest.mark.asyncio
async def test_cli_rotate_single_setting_reads_locked_winner_and_retries_lock_wake(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
) -> None:
    """Key rotation decrypts and carries metadata from the locked active row after concurrent updates."""
    repo, _ = file_async_repo
    old_service = SettingsEncryptionService("old-master-password")
    new_service = SettingsEncryptionService("new-master-password")
    tracker = SequenceTracker()
    stale_enc = old_service.encrypt("stale-secret")
    fresh_enc = old_service.encrypt("fresh-secret")
    future_ts = datetime.now(UTC) + timedelta(minutes=5)

    async with repo.session() as session:
        await begin_scd2_write(session)
        res = await upsert_scd2(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "api_secret"],
                bus_time=_T0,
                decide=lambda _row: {
                    "key": "api_secret",
                    "value": stale_enc,
                    "category": "credentials",
                    "description": "initial",
                    "is_encrypted": True,
                    "session_id": str(uuid4()),
                    "sequence_id": 1,
                    "updated_by": "alice",
                },
            ),
        )
        await upsert_scd2(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "api_secret"],
                bus_time=future_ts,
                decide=lambda _row: {
                    "key": "api_secret",
                    "value": fresh_enc,
                    "category": "credentials",
                    "description": "concurrent-update",
                    "is_encrypted": True,
                    "session_id": str(uuid4()),
                    "sequence_id": 2,
                    "updated_by": "bob",
                },
            ),
        )
        await session.commit()
        stale_snapshot = Setting(
            id=res.row.id if res.row is not None else 1,
            public_id=res.row.public_id if res.row is not None else str(uuid4()),
            key="api_secret",
            value=stale_enc,
            category="credentials",
            description="initial",
            is_encrypted=True,
            session_id="stale",
            sequence_id=1,
            updated_by="alice",
            timestamp=_T0,
            known_to=future_ts,
        )

    async with repo.session() as session:
        await begin_scd2_write(session)
        rotated = await cli_app._rotate_single_setting(
            stale_snapshot,
            old_service,
            new_service,
            False,
            session,
            tracker,
        )
        assert rotated is True
        await session.commit()

    async with repo.session() as session:
        active_row = (
            await session.execute(
                select(Setting).where(Setting.key == "api_secret", Setting.known_to == KNOWN_TO_MAX)
            )
        ).scalar_one()
        assert new_service.decrypt(active_row.value) == "fresh-secret"
        assert active_row.description == "concurrent-update"
        assert active_row.updated_by == "bob"
        assert active_row.timestamp == future_ts

    absent_snapshot = Setting(
        id=999,
        public_id=str(uuid4()),
        key="deleted_secret",
        value=old_service.encrypt("gone"),
        category="credentials",
        description="removed",
        is_encrypted=True,
        session_id="stale",
        sequence_id=1,
        updated_by="alice",
        timestamp=_T0,
        known_to=KNOWN_TO_MAX,
    )
    async with repo.session() as session:
        await begin_scd2_write(session)
        missing_result = await cli_app._rotate_single_setting(
            absent_snapshot,
            old_service,
            new_service,
            False,
            session,
            tracker,
        )
        assert missing_result is False

    attempt_count = 0

    async def _wake_then_rotate(session: AsyncSession, spec: Scd2UpsertSpec[Setting]) -> object:
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            raise KeyError("api_secret")
        return await real_cli_upsert(session, spec)

    async with repo.session() as session:
        await begin_scd2_write(session)
        with patch("snapper.cli.app.upsert_scd2", new=_wake_then_rotate):
            wake_result = await cli_app._rotate_single_setting(
                stale_snapshot,
                new_service,
                old_service,
                False,
                session,
                tracker,
            )
        assert wake_result is True
        assert attempt_count == 2
        await session.commit()

    async with repo.session() as session:
        await begin_scd2_write(session)
        with (
            patch("snapper.cli.app.upsert_scd2", new=AsyncMock(side_effect=KeyError("api_secret"))),
            pytest.raises(KeyError, match="api_secret"),
        ):
            await cli_app._rotate_single_setting(
                stale_snapshot,
                old_service,
                new_service,
                False,
                session,
                tracker,
            )


@pytest.mark.asyncio
async def test_pg18_fixture_helpers_produce_valid_uuids_and_schemas(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
) -> None:
    """Verify PG18 test helpers produce valid UUIDs, valid lock SQL, and valid ORM payloads."""
    parsed = UUID(_session_uuid("pg18-regression"))
    assert str(parsed) == _session_uuid("pg18-regression")

    executed_sql: list[str] = []
    mock_session = MagicMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    async def _capture_exec(stmt: object, params: object = None) -> MagicMock:
        del params
        executed_sql.append(str(stmt))
        return MagicMock(scalar_one=MagicMock(return_value=1))

    mock_session.execute = AsyncMock(side_effect=_capture_exec)
    mock_repo = MagicMock()
    mock_repo.session.return_value = mock_session
    await _wait_for_blocked_lock(mock_repo, "settings")
    assert executed_sql
    assert "pg_stat_activity" in executed_sql[0]
    assert "LEFT JOIN pg_class" in executed_sql[0]
    assert "l.locktype IN ('transactionid', 'tuple')" in executed_sql[0]

    repo, _ = file_async_repo
    async with repo.session() as session:
        sym = Symbol(
            native_symbol="PG18-USD",
            base="PG18",
            quote="USD",
            asset_type="crypto",
            created_at=_T0,
            timestamp=_T0,
            session_id=_session_uuid("sym"),
            sequence_id=1,
        )
        session.add(sym)
        await session.flush()
        inst = Instrument(
            symbol_public_id=sym.public_id,
            exchange="kraken",
            timestamp=_T0,
            session_id=_session_uuid("inst"),
            sequence_id=1,
        )
        session.add(inst)
        await session.flush()
        cap = InstrumentOrderCapability(
            instrument_public_id=inst.public_id,
            exchange="kraken",
            supported_order_types=["market", "limit"],
            session_id=_session_uuid("cap"),
            sequence_id=1,
            timestamp=_T0,
        )
        pnl = PortfolioPnlPoint(
            wallet_public_id=_session_uuid("wallet"),
            mode="paper",
            point_time=_T0,
            valuation_ccy="USD",
            point_kind="sample",
            epoch_public_id=_session_uuid("epoch"),
            calc_version="v1",
            valuation_status="complete",
            realized_pnl=0.0,
            fee_pnl=0.0,
            accrual_pnl=0.0,
            unrealized_pnl=0.0,
            external_flow_adjustment=0.0,
            cash_usd=100.0,
            position_value_usd=0.0,
            mark_source="test",
            mark_time=_T0,
            timestamp=_T0,
            session_id=_session_uuid("pnl"),
            sequence_id=1,
        )
        caps = UserTradingCaps(
            public_id=str(uuid4()),
            user_public_id=_session_uuid("user"),
            max_daily_notional_usd=float(Decimal("100")),
            timestamp=_T0,
            known_to=KNOWN_TO_MAX,
            session_id=_session_uuid("caps"),
            sequence_id=1,
        )
        session.add_all([cap, pnl, caps])
        await session.flush()
        assert cap.supported_order_types == ["market", "limit"]
        assert pnl.point_kind == "sample"
        assert caps.max_daily_notional_usd is not None
        await session.rollback()


async def _seed_sorted_instruments(repo: SQLAlchemyRepository) -> tuple[str, str]:
    """Seed one symbol and two instruments, returning (inst_lo, inst_hi) sorted lexicographically."""
    async with repo.session() as session:
        await begin_scd2_write(session)
        sym = Symbol(
            native_symbol="ORD-USD",
            base="ORD",
            quote="USD",
            asset_type="crypto",
            created_at=_T0,
            timestamp=_T0,
            session_id="seed",
            sequence_id=1,
        )
        session.add(sym)
        await session.flush()
        inst_rows = [
            Instrument(
                symbol_public_id=sym.public_id,
                exchange=ex,
                timestamp=_T0,
                session_id="seed",
                sequence_id=idx,
            )
            for idx, ex in enumerate(("kraken", "binance"), start=1)
        ]
        session.add_all(inst_rows)
        await session.commit()
        inst_pids = sorted(row.public_id for row in inst_rows)
        return inst_pids[0], inst_pids[1]


@pytest.mark.asyncio
async def test_upsert_market_snapshots_orders_locks_fallback_and_avoids_rollback_mutation(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snapshot batch locks, fallback retries, and rollback avoid shape bias or dict leakage."""
    repo, _ = file_async_repo
    inst_lo, inst_hi = await _seed_sorted_instruments(repo)

    captured_sql: list[str] = []

    def _capture_snapshot_lock_sql(
        _conn: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        upper = statement.upper()
        if "FROM MARKET_SNAPSHOTS" in upper and "ORDER BY" in upper:
            captured_sql.append(statement)

    event.listen(repo.engine.sync_engine, "before_cursor_execute", _capture_snapshot_lock_sql)
    try:
        reverse_batch: list[MarketSnapshotUpsertRow] = [
            {**_snapshot_row(inst_hi, 1, 20.0), "timestamp": _T0, "known_to": _T1},
            {**_snapshot_row(inst_lo, 2, 10.0), "timestamp": _T0, "known_to": _T1},
        ]
        assert await repo.upsert_market_snapshots(reverse_batch) == 2
    finally:
        event.remove(repo.engine.sync_engine, "before_cursor_execute", _capture_snapshot_lock_sql)

    assert captured_sql
    assert "ORDER BY market_snapshots.instrument_public_id ASC, market_snapshots.id ASC" in (
        captured_sql[0]
    )

    fallback_order: list[tuple[str, datetime]] = []
    original_flush = AsyncSession.flush
    original_row_upsert = repo._upsert_snapshot_row
    tripped_unique = False

    async def _fail_unique_once(session: AsyncSession) -> None:
        nonlocal tripped_unique
        if not tripped_unique and any(isinstance(obj, MarketSnapshot) for obj in session.new):
            tripped_unique = True
            raise IntegrityError(
                "stmt",
                {},
                _FakeDbApiError(
                    sqlite_errorcode=2067,
                    args=("UNIQUE constraint failed: market_snapshots.instrument_public_id",),
                ),
            )
        await original_flush(session)

    async def _record_fallback(session: AsyncSession, row: MarketSnapshotUpsertRow) -> None:
        fallback_order.append((row["instrument_public_id"], row["timestamp"]))
        await original_row_upsert(session, row)

    fallback_batch: list[MarketSnapshotUpsertRow] = [
        {**_snapshot_row(inst_hi, 3, 25.0), "timestamp": _T1, "known_to": KNOWN_TO_MAX},
        {**_snapshot_row(inst_lo, 4, 15.0), "timestamp": _T1, "known_to": KNOWN_TO_MAX},
    ]
    monkeypatch.setattr(AsyncSession, "flush", _fail_unique_once)
    with patch.object(SQLAlchemyRepository, "_upsert_snapshot_row", new=_record_fallback):
        assert await repo.upsert_market_snapshots(fallback_batch) == 2

    assert tripped_unique is True
    assert fallback_order == [(inst_lo, _T1), (inst_hi, _T1)]

    orig_pid = str(uuid4())
    aborted_batch: list[MarketSnapshotUpsertRow] = [
        {
            **_snapshot_row(inst_lo, 5, 16.0),
            "public_id": orig_pid,
            "timestamp": _T2,
            "known_to": KNOWN_TO_MAX,
        }
    ]

    async def _fail_non_unique(session: AsyncSession) -> None:
        if any(isinstance(obj, MarketSnapshot) for obj in session.new):
            raise IntegrityError("stmt", {}, _FakeDbApiError(pgcode="23514"))
        await original_flush(session)

    monkeypatch.setattr(AsyncSession, "flush", _fail_non_unique)
    with pytest.raises(IntegrityError):
        await repo.upsert_market_snapshots(aborted_batch)

    assert aborted_batch[0]["public_id"] == orig_pid
    assert aborted_batch[0]["timestamp"] == _T2


class _DummySyncProcess(RegisterableProcess):
    """Minimal RegisterableProcess concrete stub for registry syncer tests."""

    async def start(self) -> None:
        """No-op start coroutine."""


def _make_sync_entry() -> ProcessRegistryEntry:
    """Build a test ProcessRegistryEntry without delegate parameter dependencies."""
    return ProcessRegistryEntry(
        class_ref=_DummySyncProcess,
        class_path="tests.DummySyncProcess",
        method="start",
        description="Synced worker",
        priority=50,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.CORE,
        tags=("sync",),
        parameters_model=None,
        parameters_schema=None,
        enabled=True,
        mode=ProcessModeEnum.PROCESS,
    )


@pytest.mark.asyncio
async def test_registry_syncer_sync_existing_process_ignores_deleted_key_and_recovers_lock_wake(
    file_async_repo: tuple[SQLAlchemyRepository, Path],
) -> None:
    """_sync_existing_process skips deleted/closed keys and retries on lock-wake KeyError."""
    repo, db_path = file_async_repo
    syncer = ProcessRegistrySyncer(
        AppSettings(
            BootstrapSettingsLoader(DB_URL=f"sqlite+aiosqlite:///{db_path}"), settings_service=None
        )
    )
    entry = _make_sync_entry()
    real_upsert = upsert_scd2

    async with repo.session() as session:
        await begin_scd2_write(session)
        await upsert_scd2(
            session,
            Scd2UpsertSpec(
                model=Setting,
                match_filters=[Setting.key == "process_worker_b"],
                bus_time=_T0,
                decide=lambda _row: {
                    "key": "process_worker_b",
                    "value": json.dumps(
                        {
                            "class": "old.Worker",
                            "enabled": True,
                            "mode": "process",
                            "role": "core",
                            "parameters": {},
                        }
                    ),
                    "category": "process_config",
                    "description": "Seed worker",
                    "is_encrypted": False,
                    "session_id": str(uuid4()),
                    "sequence_id": 1,
                    "updated_by": "seed",
                },
            ),
        )
        session.add(
            Setting(
                public_id=str(uuid4()),
                key="process_deleted_worker",
                value=json.dumps({"class": "old.Deleted", "enabled": False}),
                category="process_config",
                description="Closed deleted worker",
                is_encrypted=False,
                session_id=str(uuid4()),
                sequence_id=1,
                updated_by="seed",
                timestamp=_T0,
                known_to=_T1,
            )
        )
        await session.commit()

    calls = 0

    async def _flaky_sync_upsert(session: AsyncSession, spec: Scd2UpsertSpec[Setting]) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise KeyError("process_worker_b")
        return await real_upsert(session, spec)

    with patch(
        "snapper.application.process_manager.registry_syncer.upsert_scd2",
        new=_flaky_sync_upsert,
    ):
        await syncer._sync_existing_process("worker_b", entry, repo)

    assert calls == 2

    await syncer._sync_existing_process("deleted_worker", entry, repo)

    async with repo.session() as session:
        active_worker = (
            await session.execute(
                select(Setting).where(
                    Setting.key == "process_worker_b", Setting.known_to == KNOWN_TO_MAX
                )
            )
        ).scalar_one()
        payload = json.loads(active_worker.value)
        assert payload["class"] == "old.Worker"
        assert payload["lifecycle"] == ProcessLifecycleEnum.LONG_RUNNING.value
        assert payload["tags"] == ["sync"]
        deleted_active = (
            await session.execute(
                select(Setting).where(
                    Setting.key == "process_deleted_worker", Setting.known_to == KNOWN_TO_MAX
                )
            )
        ).scalar_one_or_none()
        assert deleted_active is None
