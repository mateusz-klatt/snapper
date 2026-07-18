"""Repository tests for the derived per-scope execution chain-tip read."""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import NAMESPACE_DNS
from uuid import uuid5

import pytest

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.execution_chain import ExecutionChainRecord
from snapper.application.portfolio.execution_chain import execution_chain_genesis
from snapper.application.portfolio.execution_chain import extend_execution_chain
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.repository import SQLAlchemyRepository

_WALLET = "00000000-0000-7000-8000-000000000101"
_EXCHANGE = "walutomat"
_MODE = "live"
_SESSION = "00000000-0000-7000-8000-000000000501"
_TS = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)


def _uuid_for(seed: str) -> str:
    """Map a seed string to a deterministic distinct UUID so unique indexes never collide."""
    return str(uuid5(NAMESPACE_DNS, seed))


async def _repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Create a fresh SQLite repository with the append-only executions schema."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'chain.db'}")
    await repo.create_all()
    return repo


@dataclass(frozen=True)
class _Fill:
    """One execution's field values, the single source of truth for a test row.

    ``execution`` builds the ORM row (adding the non-sealed float mirrors and
    bus columns); ``record`` builds the canonical chain record the tip is
    computed from. Both read the same fields so the fixture cannot drift from
    what the repository reads back.
    """

    scope_sequence: int
    public_id: str
    order_public_id: str
    wallet_public_id: str
    exchange: str
    exec_id: str
    trade_id: str
    known_to: datetime
    mode: str = _MODE
    operator_public_id: str | None = "00000000-0000-7000-8000-000000000401"
    side: str = "buy"
    status: str = "filled"
    fee_asset: str = "PLN"
    price_decimal: str | None = "1.25"
    size_decimal: str | None = "2.0"
    fee_decimal: str | None = "0.1"
    numeric_provenance: str | None = "venue_raw"
    liquidity_role: str = "maker"
    timestamp: datetime = _TS
    executed_at: datetime | None = _TS + timedelta(seconds=1)

    def execution(self) -> Execution:
        """Build the ORM execution row for a direct insert."""
        return Execution(
            scope_sequence=self.scope_sequence,
            public_id=self.public_id,
            order_public_id=self.order_public_id,
            wallet_public_id=self.wallet_public_id,
            operator_public_id=self.operator_public_id,
            exchange=self.exchange,
            mode=self.mode,
            exec_id=self.exec_id,
            trade_id=self.trade_id,
            side=self.side,
            status=self.status,
            price=1.25,
            size=2.0,
            fee=0.1,
            fee_asset=self.fee_asset,
            price_decimal=self.price_decimal,
            size_decimal=self.size_decimal,
            fee_decimal=self.fee_decimal,
            numeric_provenance=self.numeric_provenance,
            liquidity_role=self.liquidity_role,
            session_id=_SESSION,
            sequence_id=self.scope_sequence,
            timestamp=self.timestamp,
            executed_at=self.executed_at,
            known_to=self.known_to,
        )

    def record(self) -> ExecutionChainRecord:
        """Build the canonical chain record the tip is expected to fold."""
        return ExecutionChainRecord(
            scope_sequence=self.scope_sequence,
            public_id=self.public_id,
            order_public_id=self.order_public_id,
            wallet_public_id=self.wallet_public_id,
            operator_public_id=self.operator_public_id,
            exchange=self.exchange,
            mode=self.mode,
            exec_id=self.exec_id,
            trade_id=self.trade_id,
            side=self.side,
            status=self.status,
            fee_asset=self.fee_asset,
            price_decimal=self.price_decimal,
            size_decimal=self.size_decimal,
            fee_decimal=self.fee_decimal,
            numeric_provenance=self.numeric_provenance,
            liquidity_role=self.liquidity_role,
            timestamp=self.timestamp,
            executed_at=self.executed_at,
        )


def _fill(sequence: int, *, exchange: str = _EXCHANGE, known_to: datetime = KNOWN_TO_MAX) -> _Fill:
    """Build a valid fill with identities unique per ``(exchange, sequence)``."""
    return _Fill(
        scope_sequence=sequence,
        public_id=_uuid_for(f"pub-{exchange}-{sequence}"),
        order_public_id=_uuid_for(f"ord-{exchange}-{sequence}"),
        wallet_public_id=_WALLET,
        exchange=exchange,
        exec_id=f"E-{exchange}-{sequence}",
        trade_id=f"T-{exchange}-{sequence}",
        known_to=known_to,
    )


async def _insert(repo: SQLAlchemyRepository, fills: list[_Fill]) -> None:
    """Insert the fixture rows directly (INSERT is not blocked by the immutability triggers)."""
    async with repo.session() as session:
        session.add_all([fill.execution() for fill in fills])
        await session.commit()


def _genesis() -> str:
    """Return the genesis tip for the shared test scope."""
    return execution_chain_genesis(_WALLET, _EXCHANGE, _MODE)


async def test_chain_tip_folds_the_range_in_scope_sequence_order(tmp_path: Path) -> None:
    """The repository tip equals the pure fold of the same rows in sequence order."""
    repo = await _repo(tmp_path)
    fills = [_fill(sequence) for sequence in (1, 2, 3)]
    await _insert(repo, fills)
    genesis = _genesis()
    expected = extend_execution_chain(genesis, [fill.record() for fill in fills])
    actual = await repo.get_spot_execution_chain_tip(_WALLET, _EXCHANGE, _MODE, 0, genesis, 3)
    assert actual == expected


async def test_chain_tip_extends_from_a_base_hash(tmp_path: Path) -> None:
    """Extending from a checkpoint tip over the tail equals the full-from-genesis tip."""
    repo = await _repo(tmp_path)
    await _insert(repo, [_fill(sequence) for sequence in (1, 2, 3)])
    genesis = _genesis()
    full = await repo.get_spot_execution_chain_tip(_WALLET, _EXCHANGE, _MODE, 0, genesis, 3)
    checkpoint = await repo.get_spot_execution_chain_tip(_WALLET, _EXCHANGE, _MODE, 0, genesis, 1)
    extended = await repo.get_spot_execution_chain_tip(_WALLET, _EXCHANGE, _MODE, 1, checkpoint, 3)
    assert extended == full


async def test_chain_tip_over_an_empty_range_returns_the_base(tmp_path: Path) -> None:
    """An empty range folds nothing and returns the base tip unchanged."""
    repo = await _repo(tmp_path)
    genesis = _genesis()
    actual = await repo.get_spot_execution_chain_tip(_WALLET, _EXCHANGE, _MODE, 0, genesis, 0)
    assert actual == genesis


async def test_chain_tip_fails_closed_on_a_gap_in_the_range(tmp_path: Path) -> None:
    """A missing sequence value in the range is a purge or tamper and fails closed."""
    repo = await _repo(tmp_path)
    await _insert(repo, [_fill(1), _fill(3)])
    genesis = _genesis()
    with pytest.raises(ExecutionChainError):
        await repo.get_spot_execution_chain_tip(_WALLET, _EXCHANGE, _MODE, 0, genesis, 3)


async def test_chain_tip_validates_watermark_bounds(tmp_path: Path) -> None:
    """Inverted or negative watermark bounds fail closed before any read."""
    repo = await _repo(tmp_path)
    genesis = _genesis()
    with pytest.raises(ExecutionChainError):
        await repo.get_spot_execution_chain_tip(_WALLET, _EXCHANGE, _MODE, 5, genesis, 3)
    with pytest.raises(ExecutionChainError):
        await repo.get_spot_execution_chain_tip(_WALLET, _EXCHANGE, _MODE, -1, genesis, 3)


async def test_chain_tip_is_scoped_to_one_wallet_exchange_mode(tmp_path: Path) -> None:
    """The read folds only the requested scope's rows, never another scope's."""
    repo = await _repo(tmp_path)
    walutomat_fill = _fill(1, exchange="walutomat")
    kraken_fill = _fill(1, exchange="kraken")
    await _insert(repo, [walutomat_fill, kraken_fill])
    genesis = _genesis()
    actual = await repo.get_spot_execution_chain_tip(_WALLET, _EXCHANGE, _MODE, 0, genesis, 1)
    assert actual == extend_execution_chain(genesis, [walutomat_fill.record()])
    kraken_genesis = execution_chain_genesis(_WALLET, "kraken", _MODE)
    kraken_tip = await repo.get_spot_execution_chain_tip(
        _WALLET, "kraken", _MODE, 0, kraken_genesis, 1
    )
    assert kraken_tip != actual


async def test_chain_tip_read_does_not_filter_on_known_to(tmp_path: Path) -> None:
    """A row whose ``known_to`` is in the past is still folded (reads ignore known_to)."""
    repo = await _repo(tmp_path)
    retired = _fill(1, known_to=datetime(2026, 7, 16, 8, 0, tzinfo=UTC))
    await _insert(repo, [retired])
    genesis = _genesis()
    actual = await repo.get_spot_execution_chain_tip(_WALLET, _EXCHANGE, _MODE, 0, genesis, 1)
    assert actual == extend_execution_chain(genesis, [retired.record()])
