"""Tests for the transactionally complete futures reconciliation bundle."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
from sqlalchemy import event
from sqlalchemy import text

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Position
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
_SESSION = "00000000-0000-7000-8000-000000000101"
_WALLET = "00000000-0000-7000-8000-000000000201"
_ALPHA_WALLET = "abcdefab-cdef-7abc-8def-abcdefabcdef"
_TARGET_SYMBOL_ID = "00000000-0000-7000-8000-000000000301"
_VENUE_ONLY_SYMBOL_ID = "00000000-0000-7000-8000-000000000302"
_OTHER_SYMBOL_ID = "00000000-0000-7000-8000-000000000303"
_TARGET_INSTRUMENT = "00000000-0000-7000-8000-000000000401"
_VENUE_ONLY_INSTRUMENT = "00000000-0000-7000-8000-000000000402"
_OTHER_INSTRUMENT = "00000000-0000-7000-8000-000000000403"
_MISSING_INSTRUMENT = "00000000-0000-7000-8000-000000000404"
_TARGET_SYMBOL = "PF_XBTUSD"
_VENUE_ONLY_SYMBOL = "PF_ETHUSD"
_OTHER_SYMBOL = "XBTUSD"


async def _make_repo(tmp_path: Path, name: str) -> SQLAlchemyRepository:
    """Create one isolated full-schema repository."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / name}")
    await repository.create_all()
    return repository


def _symbol(
    public_id: str,
    native_symbol: str,
    sequence_id: int,
) -> Symbol:
    """Build one active temporal symbol."""
    return Symbol(
        native_symbol=native_symbol,
        base="XBT",
        quote="USD",
        asset_type="crypto",
        created_at=_NOW - timedelta(hours=1),
        public_id=public_id,
        session_id=_SESSION,
        sequence_id=sequence_id,
        timestamp=_NOW - timedelta(hours=1),
        known_to=KNOWN_TO_MAX,
    )


def _instrument(
    public_id: str,
    symbol_public_id: str,
    exchange: str,
    sequence_id: int,
) -> Instrument:
    """Build one active temporal instrument identity."""
    return Instrument(
        symbol_public_id=symbol_public_id,
        exchange=exchange,
        source_exchange=None,
        public_id=public_id,
        session_id=_SESSION,
        sequence_id=sequence_id,
        timestamp=_NOW - timedelta(hours=1),
        known_to=KNOWN_TO_MAX,
    )


def _position(
    public_id: str,
    instrument_public_id: str,
    sequence_id: int,
    *,
    wallet_public_id: str = _WALLET,
) -> Position:
    """Build one active wallet position projection."""
    return Position(
        instrument_public_id=instrument_public_id,
        mode="live",
        wallet_public_id=wallet_public_id,
        quantity=1.0,
        average_price=100.0,
        unrealized_pnl=1.0,
        realized_pnl=0.0,
        mark_price=101.0,
        marked_at=_NOW - timedelta(minutes=1),
        source_venue_event_id=41,
        public_id=public_id,
        session_id=_SESSION,
        sequence_id=sequence_id,
        timestamp=_NOW - timedelta(minutes=2),
        known_to=KNOWN_TO_MAX,
    )


async def test_bundle_reads_complete_projection_mappings_and_specs_in_three_sets(
    tmp_path: Path,
) -> None:
    """One snapshot uses three set reads regardless of candidate count."""
    repository = await _make_repo(tmp_path, "complete-bundle.db")
    async with repository.session() as session:
        session.add_all(
            [
                _symbol(_TARGET_SYMBOL_ID, _TARGET_SYMBOL, 1),
                _symbol(_VENUE_ONLY_SYMBOL_ID, _VENUE_ONLY_SYMBOL, 2),
                _symbol(_OTHER_SYMBOL_ID, _OTHER_SYMBOL, 3),
                _instrument(
                    _TARGET_INSTRUMENT,
                    _TARGET_SYMBOL_ID,
                    "kraken_futures",
                    4,
                ),
                _instrument(
                    _VENUE_ONLY_INSTRUMENT,
                    _VENUE_ONLY_SYMBOL_ID,
                    "kraken_futures",
                    5,
                ),
                _instrument(_OTHER_INSTRUMENT, _OTHER_SYMBOL_ID, "kraken", 6),
                _position(
                    "00000000-0000-7000-8000-000000000501",
                    _TARGET_INSTRUMENT,
                    7,
                ),
                _position(
                    "00000000-0000-7000-8000-000000000502",
                    _OTHER_INSTRUMENT,
                    8,
                ),
                InstrumentSpec(
                    instrument_public_id=_TARGET_INSTRUMENT,
                    unit_certified=False,
                    public_id="00000000-0000-7000-8000-000000000601",
                    session_id=_SESSION,
                    sequence_id=9,
                    timestamp=_NOW - timedelta(minutes=3),
                    known_to=KNOWN_TO_MAX,
                ),
            ]
        )
        await session.commit()
    select_statements: list[str] = []

    def count_selects(*args: object) -> None:
        """Capture only SQL set reads emitted by the bundle operation."""
        if len(args) > 2 and isinstance(args[2], str):
            statement = args[2].lstrip()
            if statement.upper().startswith("SELECT"):
                select_statements.append(statement)

    event.listen(repository.engine.sync_engine, "before_cursor_execute", count_selects)
    try:
        bundle = await repository.get_futures_reconciliation_bundle(
            _WALLET,
            "kraken_futures",
            "live",
            _NOW,
            {_TARGET_SYMBOL, _VENUE_ONLY_SYMBOL},
        )
    finally:
        event.remove(repository.engine.sync_engine, "before_cursor_execute", count_selects)
    assert bundle.error is None
    assert bundle.projection is not None
    assert len(bundle.projection) == 1
    assert bundle.projection[0]["instrument_public_id"] == _TARGET_INSTRUMENT
    assert bundle.projection[0]["instrument"] == _TARGET_SYMBOL
    assert bundle.instrument_public_ids_by_symbol == {
        _TARGET_SYMBOL: _TARGET_INSTRUMENT,
        _VENUE_ONLY_SYMBOL: _VENUE_ONLY_INSTRUMENT,
    }
    assert bundle.specs_by_instrument_public_id[_TARGET_INSTRUMENT] is not None
    assert bundle.specs_by_instrument_public_id[_VENUE_ONLY_INSTRUMENT] is None
    assert len(select_statements) == 3


async def test_bundle_preserves_proven_complete_zero_position_projection(
    tmp_path: Path,
) -> None:
    """No active wallet positions is a complete empty projection."""
    repository = await _make_repo(tmp_path, "empty-bundle.db")
    bundle = await repository.get_futures_reconciliation_bundle(
        _WALLET,
        "kraken_futures",
        "live",
        _NOW,
        set(),
    )
    assert bundle.error is None
    assert bundle.projection == []
    assert bundle.instrument_public_ids_by_symbol == {}
    assert bundle.specs_by_instrument_public_id == {}


async def test_bundle_sets_repeatable_read_only_transaction_on_postgresql(
    tmp_path: Path,
) -> None:
    """PostgreSQL bundle reads begin with the certified snapshot isolation."""
    repository = await _make_repo(tmp_path, "postgresql-snapshot.db")
    position_result = MagicMock()
    position_result.all.return_value = []
    transaction_context = AsyncMock()
    session = AsyncMock(begin=MagicMock(return_value=transaction_context))
    session.execute = AsyncMock(side_effect=[MagicMock(), position_result])
    with (
        patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ),
        patch.object(repository, "session") as session_context,
    ):
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        bundle = await repository.get_futures_reconciliation_bundle(
            _WALLET,
            "kraken_futures",
            "live",
            _NOW,
            set(),
        )
    statement = session.execute.await_args_list[0].args[0]
    assert str(statement) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert session.execute.await_count == 2
    assert bundle.error is None
    assert bundle.projection == []


async def test_bundle_fails_closed_when_position_identity_is_missing(
    tmp_path: Path,
) -> None:
    """An unresolvable wallet position makes the entire projection unavailable."""
    repository = await _make_repo(tmp_path, "missing-position-identity.db")
    async with repository.session() as session:
        session.add(
            _position(
                "00000000-0000-7000-8000-000000000503",
                _MISSING_INSTRUMENT,
                1,
            )
        )
        await session.commit()
    bundle = await repository.get_futures_reconciliation_bundle(
        _WALLET,
        "kraken_futures",
        "live",
        _NOW,
        set(),
    )
    assert bundle.projection is None
    assert bundle.instrument_public_ids_by_symbol == {}
    assert bundle.specs_by_instrument_public_id == {}
    assert bundle.error == "missing_futures_position_instrument_identity"


async def test_bundle_rejects_invalid_or_unsupported_snapshot_identity(
    tmp_path: Path,
) -> None:
    """Invalid mode and exchange identities fail before any set read."""
    repository = await _make_repo(tmp_path, "invalid-bundle-identity.db")
    paper = await repository.get_futures_reconciliation_bundle(
        _WALLET,
        "kraken_futures",
        "paper",
        _NOW,
        set(),
    )
    mixed_case = await repository.get_futures_reconciliation_bundle(
        _WALLET,
        "Kraken_Futures",
        "live",
        _NOW,
        set(),
    )
    assert paper.projection is None
    assert paper.error == "invalid_futures_bundle_identity"
    assert mixed_case.projection is None
    assert mixed_case.error == "invalid_futures_bundle_identity"


@pytest.mark.parametrize("malformed_wallet", ["", "not-a-wallet-uuid"])
async def test_bundle_rejects_malformed_wallet_uuid(
    tmp_path: Path,
    malformed_wallet: str,
) -> None:
    """Malformed wallet text raises the shared stable ValueError.

    Args:
        tmp_path: Pytest temporary directory.
        malformed_wallet: Empty or non-UUID wallet text.
    """
    repository = await _make_repo(tmp_path, "malformed-bundle-wallet.db")

    with pytest.raises(ValueError, match="reconciliation wallet identity is invalid"):
        await repository.get_futures_reconciliation_bundle(
            malformed_wallet,
            "kraken_futures",
            "live",
            _NOW,
            set(),
        )


@pytest.mark.parametrize(
    "wallet_alias",
    [_ALPHA_WALLET.upper(), _ALPHA_WALLET.replace("-", "")],
)
async def test_bundle_wallet_filter_canonicalizes_uuid_aliases(
    tmp_path: Path,
    wallet_alias: str,
) -> None:
    """Uppercase and hyphenless wallet aliases find canonical projections.

    Args:
        tmp_path: Pytest temporary directory.
        wallet_alias: Alternate spelling of the canonical wallet UUID.
    """
    repository = await _make_repo(tmp_path, "canonical-bundle-wallet.db")
    async with repository.session() as session:
        session.add_all(
            [
                _symbol(_TARGET_SYMBOL_ID, _TARGET_SYMBOL, 1),
                _instrument(
                    _TARGET_INSTRUMENT,
                    _TARGET_SYMBOL_ID,
                    "kraken_futures",
                    2,
                ),
                _position(
                    "00000000-0000-7000-8000-000000000501",
                    _TARGET_INSTRUMENT,
                    3,
                    wallet_public_id=_ALPHA_WALLET,
                ),
            ]
        )
        await session.commit()

    bundle = await repository.get_futures_reconciliation_bundle(
        wallet_alias,
        "kraken_futures",
        "live",
        _NOW,
        {_TARGET_SYMBOL},
    )

    assert bundle.error is None
    assert bundle.projection is not None
    assert len(bundle.projection) == 1
    assert bundle.projection[0]["wallet_public_id"] == _ALPHA_WALLET


async def test_bundle_ignores_proven_other_exchange_without_symbol(
    tmp_path: Path,
) -> None:
    """A conclusively foreign position needs no target-exchange symbol mapping."""
    repository = await _make_repo(tmp_path, "foreign-position.db")
    async with repository.session() as session:
        session.add_all(
            [
                _instrument(
                    _OTHER_INSTRUMENT,
                    _MISSING_INSTRUMENT,
                    "kraken",
                    1,
                ),
                _position(
                    "00000000-0000-7000-8000-000000000504",
                    _OTHER_INSTRUMENT,
                    2,
                ),
            ]
        )
        await session.commit()
    bundle = await repository.get_futures_reconciliation_bundle(
        _WALLET,
        "kraken_futures",
        "live",
        _NOW,
        set(),
    )
    assert bundle.error is None
    assert bundle.projection == []


async def test_bundle_rejects_empty_target_native_symbol(tmp_path: Path) -> None:
    """A target position with an empty native symbol is identity-ambiguous."""
    repository = await _make_repo(tmp_path, "empty-native-symbol.db")
    async with repository.session() as session:
        session.add_all(
            [
                _symbol(_TARGET_SYMBOL_ID, "", 1),
                _instrument(
                    _TARGET_INSTRUMENT,
                    _TARGET_SYMBOL_ID,
                    "kraken_futures",
                    2,
                ),
                _position(
                    "00000000-0000-7000-8000-000000000505",
                    _TARGET_INSTRUMENT,
                    3,
                ),
            ]
        )
        await session.commit()
    bundle = await repository.get_futures_reconciliation_bundle(
        _WALLET,
        "kraken_futures",
        "live",
        _NOW,
        set(),
    )
    assert bundle.projection is None
    assert bundle.error == "missing_futures_position_instrument_identity"


async def test_bundle_rejects_ambiguous_position_join(tmp_path: Path) -> None:
    """Two active instrument rows for one logical id fail the projection closed."""
    repository = await _make_repo(tmp_path, "ambiguous-position-join.db")
    async with repository.session() as session:
        await session.execute(text("DROP INDEX ix_instruments_public_id"))
        session.add_all(
            [
                _symbol(_TARGET_SYMBOL_ID, _TARGET_SYMBOL, 1),
                _symbol(_VENUE_ONLY_SYMBOL_ID, _VENUE_ONLY_SYMBOL, 2),
                _instrument(
                    _TARGET_INSTRUMENT,
                    _TARGET_SYMBOL_ID,
                    "kraken_futures",
                    3,
                ),
                _instrument(
                    _TARGET_INSTRUMENT,
                    _VENUE_ONLY_SYMBOL_ID,
                    "kraken_futures",
                    4,
                ),
                _position(
                    "00000000-0000-7000-8000-000000000506",
                    _TARGET_INSTRUMENT,
                    5,
                ),
            ]
        )
        await session.commit()
    bundle = await repository.get_futures_reconciliation_bundle(
        _WALLET,
        "kraken_futures",
        "live",
        _NOW,
        set(),
    )
    assert bundle.projection is None
    assert bundle.error == "ambiguous_futures_position_instrument_identity"


async def test_bundle_rejects_conflicting_joined_instrument_identity(
    tmp_path: Path,
) -> None:
    """A joined instrument id that conflicts with its position fails closed."""
    repository = await _make_repo(tmp_path, "conflicting-position-identity.db")
    position_result = MagicMock()
    position_result.all.return_value = [
        (
            _position(
                "00000000-0000-7000-8000-000000000509",
                _TARGET_INSTRUMENT,
                1,
            ),
            _VENUE_ONLY_INSTRUMENT,
            "kraken_futures",
            _TARGET_SYMBOL,
        )
    ]
    transaction_context = AsyncMock()
    session = AsyncMock(begin=MagicMock(return_value=transaction_context))
    session.execute = AsyncMock(return_value=position_result)
    with patch.object(repository, "session") as session_context:
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        bundle = await repository.get_futures_reconciliation_bundle(
            _WALLET,
            "kraken_futures",
            "live",
            _NOW,
            set(),
        )
    assert bundle.projection is None
    assert bundle.error == "conflicting_futures_position_instrument_identity"


async def test_bundle_rejects_duplicate_target_position_identity(tmp_path: Path) -> None:
    """Two active wallet projections for one instrument fail closed."""
    repository = await _make_repo(tmp_path, "duplicate-position-identity.db")
    async with repository.session() as session:
        await session.execute(text("DROP INDEX uq_positions_instrument_public_id"))
        session.add_all(
            [
                _symbol(_TARGET_SYMBOL_ID, _TARGET_SYMBOL, 1),
                _instrument(
                    _TARGET_INSTRUMENT,
                    _TARGET_SYMBOL_ID,
                    "kraken_futures",
                    2,
                ),
                _position(
                    "00000000-0000-7000-8000-000000000507",
                    _TARGET_INSTRUMENT,
                    3,
                ),
                _position(
                    "00000000-0000-7000-8000-000000000508",
                    _TARGET_INSTRUMENT,
                    4,
                ),
            ]
        )
        await session.commit()
    bundle = await repository.get_futures_reconciliation_bundle(
        _WALLET,
        "kraken_futures",
        "live",
        _NOW,
        set(),
    )
    assert bundle.projection is None
    assert bundle.error == "duplicate_futures_position_identity"


async def test_bundle_rejects_duplicate_native_symbol_mapping(tmp_path: Path) -> None:
    """Two active mappings for one native symbol are never collapsed."""
    repository = await _make_repo(tmp_path, "duplicate-native-mapping.db")
    async with repository.session() as session:
        await session.execute(text("DROP INDEX uq_symbols_active_native"))
        session.add_all(
            [
                _symbol(_TARGET_SYMBOL_ID, _TARGET_SYMBOL, 1),
                _symbol(_VENUE_ONLY_SYMBOL_ID, _TARGET_SYMBOL, 2),
                _instrument(
                    _TARGET_INSTRUMENT,
                    _TARGET_SYMBOL_ID,
                    "kraken_futures",
                    3,
                ),
                _instrument(
                    _VENUE_ONLY_INSTRUMENT,
                    _VENUE_ONLY_SYMBOL_ID,
                    "kraken_futures",
                    4,
                ),
            ]
        )
        await session.commit()
    bundle = await repository.get_futures_reconciliation_bundle(
        _WALLET,
        "kraken_futures",
        "live",
        _NOW,
        {_TARGET_SYMBOL},
    )
    assert bundle.projection is None
    assert bundle.error == "duplicate_futures_native_symbol_mapping"


async def test_bundle_rejects_projection_without_consistent_symbol_mapping(
    tmp_path: Path,
) -> None:
    """A projection identity absent from the mapping set fails closed."""
    repository = await _make_repo(tmp_path, "inconsistent-projection-mapping.db")
    position_result = MagicMock()
    position_result.all.return_value = [
        (
            _position(
                "00000000-0000-7000-8000-000000000510",
                _TARGET_INSTRUMENT,
                1,
            ),
            _TARGET_INSTRUMENT,
            "kraken_futures",
            _TARGET_SYMBOL,
        )
    ]
    symbol_result = MagicMock()
    symbol_result.tuples.return_value.all.return_value = []
    transaction_context = AsyncMock()
    session = AsyncMock(begin=MagicMock(return_value=transaction_context))
    session.execute = AsyncMock(side_effect=[position_result, symbol_result])
    with patch.object(repository, "session") as session_context:
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        bundle = await repository.get_futures_reconciliation_bundle(
            _WALLET,
            "kraken_futures",
            "live",
            _NOW,
            set(),
        )
    assert bundle.projection is None
    assert bundle.error == "inconsistent_futures_projection_symbol_mapping"


async def test_bundle_rejects_duplicate_active_instrument_specs(tmp_path: Path) -> None:
    """Two active specs for one candidate instrument are never collapsed."""
    repository = await _make_repo(tmp_path, "duplicate-instrument-spec.db")
    async with repository.session() as session:
        await session.execute(text("DROP INDEX uq_instrument_spec_instrument"))
        session.add_all(
            [
                _symbol(_TARGET_SYMBOL_ID, _TARGET_SYMBOL, 1),
                _instrument(
                    _TARGET_INSTRUMENT,
                    _TARGET_SYMBOL_ID,
                    "kraken_futures",
                    2,
                ),
                InstrumentSpec(
                    instrument_public_id=_TARGET_INSTRUMENT,
                    unit_certified=False,
                    public_id="00000000-0000-7000-8000-000000000602",
                    session_id=_SESSION,
                    sequence_id=3,
                    timestamp=_NOW - timedelta(minutes=3),
                    known_to=KNOWN_TO_MAX,
                ),
                InstrumentSpec(
                    instrument_public_id=_TARGET_INSTRUMENT,
                    unit_certified=False,
                    public_id="00000000-0000-7000-8000-000000000603",
                    session_id=_SESSION,
                    sequence_id=4,
                    timestamp=_NOW - timedelta(minutes=2),
                    known_to=KNOWN_TO_MAX,
                ),
            ]
        )
        await session.commit()
    bundle = await repository.get_futures_reconciliation_bundle(
        _WALLET,
        "kraken_futures",
        "live",
        _NOW,
        {_TARGET_SYMBOL},
    )
    assert bundle.projection is None
    assert bundle.error == "duplicate_futures_instrument_spec"


async def test_bundle_rejects_unsupported_repository_dialect(tmp_path: Path) -> None:
    """A repository outside the certified dialect pair fails before reading."""
    repository = await _make_repo(tmp_path, "unsupported-dialect.db")
    with patch.object(
        SQLAlchemyRepository,
        "dialect_name",
        new_callable=PropertyMock,
        return_value="mysql",
    ):
        bundle = await repository.get_futures_reconciliation_bundle(
            _WALLET,
            "kraken_futures",
            "live",
            _NOW,
            set(),
        )
    assert bundle.projection is None
    assert bundle.error == "unsupported_futures_bundle_dialect"
