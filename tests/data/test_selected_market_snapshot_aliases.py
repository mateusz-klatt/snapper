"""Exercise selected Kraken alias resolution against isolated real SQLite tables."""

import asyncio
import math
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import NoReturn
from uuid import NAMESPACE_URL
from uuid import uuid5

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from snapper.core.market_snapshot_freshness import MAX_REFRESH_TARGETS
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import _selected_kraken_ticker_aliases_query
from snapper.data.selected_market_snapshot_aliases import read_selected_kraken_alias_proofs
from snapper.data.selected_market_snapshot_aliases import read_selected_kraken_ticker_aliases

_OBSERVED = datetime(2090, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class _Seed:
    """Independent selected metadata, alias and capability fixture controls."""

    name: str = "selected"
    active_symbol: bool = True
    active_instrument: bool = True
    active_capability: bool = True
    active_alias: bool = True
    can_market_data: bool = True
    instrument_exchange: str = "kraken"
    capability_exchange: str = "kraken"
    alias_exchange: str = "kraken"
    alias_channel: str = "ws"


@pytest.fixture
async def session(tmp_path: Path) -> AsyncIterator[AsyncSession]:
    """Own four SQLite metadata tables without any application database connection.

    Args:
        tmp_path: Isolated fixture directory.

    Yields:
        A real asynchronous session with no market snapshot table.
    """
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'aliases.db'}")
    async with engine.begin() as connection:
        for model in (Symbol, Instrument, SymbolAlias, SymbolExchangeCapability):
            await connection.run_sync(model.__table__.create)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as owned_session:
            yield owned_session
    finally:
        await engine.dispose()


def _identity(name: str) -> str:
    """Produce one deterministic synthetic public identity.

    Args:
        name: Fixture-local logical name.

    Returns:
        A stable UUID string.
    """
    return str(uuid5(NAMESPACE_URL, f"selected-kraken-alias:{name}"))


async def _seed(session: AsyncSession, seed: _Seed) -> str:
    """Persist independently controlled temporal metadata for one selected identity.

    Args:
        session: Fixture-owned metadata session.
        seed: Current-state and source-affinity controls.

    Returns:
        The synthetic instrument public identity.
    """
    symbol_id = _identity(f"symbol:{seed.name}")
    instrument_id = _identity(f"instrument:{seed.name}")
    session.add(
        Symbol(
            public_id=symbol_id,
            timestamp=_OBSERVED,
            known_to=KNOWN_TO_MAX if seed.active_symbol else _OBSERVED,
            session_id="test",
            sequence_id=1,
            native_symbol=f"{seed.name}-EUR",
            base=seed.name,
            quote="EUR",
            asset_type="crypto",
            created_at=_OBSERVED,
        )
    )
    session.add(
        Instrument(
            public_id=instrument_id,
            timestamp=_OBSERVED,
            known_to=KNOWN_TO_MAX if seed.active_instrument else _OBSERVED,
            session_id="test",
            sequence_id=1,
            symbol_public_id=symbol_id,
            exchange=seed.instrument_exchange,
        )
    )
    session.add(
        SymbolExchangeCapability(
            public_id=_identity(f"capability:{seed.name}"),
            timestamp=_OBSERVED,
            known_to=KNOWN_TO_MAX if seed.active_capability else _OBSERVED,
            session_id="test",
            sequence_id=1,
            symbol_public_id=symbol_id,
            exchange=seed.capability_exchange,
            can_market_data=seed.can_market_data,
            can_trade=False,
            created_at=_OBSERVED,
        )
    )
    session.add(
        SymbolAlias(
            public_id=_identity(f"alias:{seed.name}"),
            timestamp=_OBSERVED,
            known_to=KNOWN_TO_MAX if seed.active_alias else _OBSERVED,
            session_id="test",
            sequence_id=1,
            symbol_public_id=symbol_id,
            exchange=seed.alias_exchange,
            channel=seed.alias_channel,
            exchange_symbol=f"{seed.name}/EUR",
            created_at=_OBSERVED,
        )
    )
    await session.commit()
    return instrument_id


@pytest.mark.asyncio
async def test_selected_aliases_do_not_require_snapshots_or_load_unselected_rows(
    session: AsyncSession,
) -> None:
    """Resolve exact current aliases independently of snapshot availability.

    Given: Current selected and unselected metadata exists without a snapshot table.
    When: The reader receives only the selected identity, including a duplicate.
    Then: It returns one exact alias using current-row rather than wall-clock visibility.
    """
    selected = await _seed(session, _Seed())
    await _seed(session, _Seed(name="unselected"))

    rows = await read_selected_kraken_ticker_aliases(
        session, [selected, selected], asyncio.get_running_loop().time() + 1
    )

    assert len(rows) == 1
    assert rows[0].instrument_public_id == selected
    assert rows[0].native_symbol == "selected-EUR"
    assert rows[0].exchange_symbol == "selected/EUR"
    assert rows[0].alias_public_id == _identity("alias:selected")
    assert rows[0].alias_row_id > 0
    assert (rows[0].base, rows[0].quote, rows[0].asset_type) == ("selected", "EUR", "crypto")


@pytest.mark.asyncio
async def test_alias_proofs_preserve_current_exact_versions_in_canonical_order(
    session: AsyncSession,
) -> None:
    """Bind alias evidence separately from the existing instrument metadata digest.

    Given: Two selected Kraken instruments have current WebSocket aliases.
    When: Proofs are requested in reversed order with a duplicate identity.
    Then: Exact alias versions are returned once in canonical instrument order.
    """
    identities = [await _seed(session, _Seed(name=name)) for name in ("first", "second")]

    proofs = await read_selected_kraken_alias_proofs(
        session,
        [identities[1], identities[0], identities[1]],
        asyncio.get_running_loop().time() + 1,
    )

    assert [proof.instrument_public_id for proof in proofs] == sorted(identities)
    assert all(proof.exchange == "kraken" and proof.channel == "ws" for proof in proofs)
    assert {proof.alias_row_id for proof in proofs} == {1, 2}
    assert {proof.alias_public_id for proof in proofs} == {
        _identity("alias:first"),
        _identity("alias:second"),
    }
    assert {proof.symbol_public_id for proof in proofs} == {
        _identity("symbol:first"),
        _identity("symbol:second"),
    }


@pytest.mark.asyncio
async def test_alias_replacement_changes_proof_even_with_identical_wire_and_public_identity(
    session: AsyncSession,
) -> None:
    """Detect alias-version replacement rather than comparing only displayed symbols.

    Given: A current alias is superseded by identical metadata under the same public ID.
    When: Alias proofs are read before and after the version replacement.
    Then: Their physical row identities differ, so stale admitted proof cannot match.
    """
    selected = await _seed(session, _Seed())
    deadline = asyncio.get_running_loop().time() + 1
    before = await read_selected_kraken_alias_proofs(session, [selected], deadline)
    old = (await session.execute(select(SymbolAlias))).scalar_one()
    replaced_at = _OBSERVED + timedelta(seconds=1)
    old.known_to = replaced_at
    await session.flush()
    session.add(
        SymbolAlias(
            public_id=old.public_id,
            timestamp=replaced_at,
            known_to=KNOWN_TO_MAX,
            session_id="test",
            sequence_id=2,
            symbol_public_id=old.symbol_public_id,
            exchange=old.exchange,
            channel=old.channel,
            exchange_symbol=old.exchange_symbol,
            created_at=replaced_at,
        )
    )
    await session.flush()

    after = await read_selected_kraken_alias_proofs(session, [selected], deadline)

    assert before[0].alias_row_id != after[0].alias_row_id
    assert before[0].model_dump(exclude={"alias_row_id"}) == after[0].model_dump(
        exclude={"alias_row_id"}
    )


@pytest.mark.parametrize("identities", [[], ["id"] * (MAX_REFRESH_TARGETS + 1)])
def test_current_alias_query_rejects_an_unbounded_scope(identities: list[str]) -> None:
    """Keep the centralized current-row query independently bounded.

    Given: Direct query construction receives empty or excessive identities.
    When: The sanctioned repository helper validates its scope.
    Then: It refuses instead of constructing an implicit catalog query.
    """
    with pytest.raises(ValueError, match="bounded explicit scope"):
        _selected_kraken_ticker_aliases_query(identities)


@pytest.mark.asyncio
async def test_missing_requested_identity_rejects_a_partial_alias_set(
    session: AsyncSession,
) -> None:
    """Keep missing requested evidence visible instead of returning partial success.

    Given: One selected identity exists and another requested identity is absent.
    When: The reader resolves the complete explicit set.
    Then: It raises rather than silently omitting the absent instrument.
    """
    selected = await _seed(session, _Seed())

    with pytest.raises(ValueError, match="unambiguous current ticker aliases"):
        await read_selected_kraken_ticker_aliases(
            session, [selected, _identity("missing")], asyncio.get_running_loop().time() + 1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "seed",
    [
        _Seed(active_symbol=False),
        _Seed(active_instrument=False),
        _Seed(active_capability=False),
        _Seed(active_alias=False),
        _Seed(can_market_data=False),
        _Seed(instrument_exchange="walutomat"),
        _Seed(capability_exchange="walutomat"),
        _Seed(alias_exchange="walutomat"),
        _Seed(alias_channel="rest"),
    ],
)
async def test_unusable_current_metadata_refuses_instead_of_guessing_aliases(
    session: AsyncSession, seed: _Seed
) -> None:
    """Refuse incomplete metadata or wrong-source evidence before a client exists.

    Given: One selected identity has an inactive, disallowed or wrong-source join.
    When: The bounded reader resolves its Kraken WebSocket alias.
    Then: It refuses the incomplete set instead of inventing a wire symbol.
    """
    selected = await _seed(session, seed)

    with pytest.raises(ValueError, match="unambiguous current ticker aliases"):
        await read_selected_kraken_ticker_aliases(
            session, [selected], asyncio.get_running_loop().time() + 1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("identities", [[], [""], [" padded "], ["id"] * (MAX_REFRESH_TARGETS + 1)])
async def test_invalid_alias_scope_never_means_all(
    session: AsyncSession, identities: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject malformed and empty scopes before sending SQL.

    Given: The supplied instrument list is empty, oversized or contains invalid text.
    When: The reader validates its explicit scope.
    Then: It refuses without consulting any catalog rows.
    """

    async def unexpected_query(statement: object) -> NoReturn:
        """Fail if rejected input reaches the database."""
        raise AssertionError("Unexpected alias query")

    monkeypatch.setattr(session, "execute", unexpected_query)
    with pytest.raises(ValueError):
        await read_selected_kraken_ticker_aliases(
            session, identities, asyncio.get_running_loop().time() + 1
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline", [float("nan"), float("inf"), -1.0])
async def test_invalid_or_elapsed_alias_deadline_refuses_before_sql(
    session: AsyncSession, deadline: float, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not start resolution when the caller has no usable time budget.

    Given: A selected identity is paired with a nonfinite or elapsed deadline.
    When: The reader checks the caller's absolute budget.
    Then: It raises without starting a SQL operation.
    """

    async def unexpected_query(statement: object) -> NoReturn:
        """Fail if an exhausted deadline reaches the database."""
        raise AssertionError("Unexpected alias query")

    monkeypatch.setattr(session, "execute", unexpected_query)
    with pytest.raises(ValueError if not math.isfinite(deadline) else TimeoutError):
        await read_selected_kraken_ticker_aliases(session, [_identity("selected")], deadline)


@pytest.mark.asyncio
async def test_blocked_alias_query_consumes_the_existing_deadline(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancel a waiting database operation when the original budget expires.

    Given: The alias query waits without producing rows.
    When: The reader reaches its existing short deadline.
    Then: It reports timeout and does not invent an empty successful mapping.
    """

    async def blocked_query(statement: object) -> NoReturn:
        """Wait until the reader cancels the synthetic driver call."""
        await asyncio.Event().wait()
        raise AssertionError("The blocked query unexpectedly resumed")

    monkeypatch.setattr(session, "execute", blocked_query)
    with pytest.raises(TimeoutError):
        await read_selected_kraken_ticker_aliases(
            session, [_identity("selected")], asyncio.get_running_loop().time() + 0.01
        )
