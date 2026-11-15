"""Tests for the spot anchor bootstrap's owned-order population read.

Most cases run against ``BootstrapSettingsLoader().db_url``: the default isolated
SQLite copy and, when ``DB_URL`` names one, a disposable PostgreSQL witness. Every
row they persist carries one test-owned session id and is deleted afterwards, and
every wallet is fresh, so the shared database is left as it was found. Shapes the
schema itself forbids (a multiply active Instrument, an out-of-enum command
status) are built in a private SQLite file whose index or CHECK is relaxed.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
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
from uuid import uuid7

import pytest
from sqlalchemy import delete
from sqlalchemy import event
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.never_submitted_commands import never_submitted_digest
from snapper.application.portfolio.spot_anchor_ownership import never_submitted_document
from snapper.application.portfolio.spot_anchor_ownership import own_submit_unresolved
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import SpotOwnedOrderPopulation
from snapper.data.repository_types import SpotOwnedSubmitCommandRow

_T0 = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)
_CLOSED_AT = _T0 + timedelta(hours=1)
_NEVER_SETTLED = (
    TradeCommandStatusEnum.CREATED.value,
    TradeCommandStatusEnum.DISPATCHED.value,
    TradeCommandStatusEnum.DIRECT_DISPATCHED.value,
    TradeCommandStatusEnum.ACCEPTED.value,
    TradeCommandStatusEnum.PARTIALLY_FILLED.value,
)
_SETTLED_BY_VENUE_ID = (
    TradeCommandStatusEnum.FILLED.value,
    TradeCommandStatusEnum.CANCELLED.value,
    TradeCommandStatusEnum.EXPIRED.value,
    TradeCommandStatusEnum.REJECTED.value,
    TradeCommandStatusEnum.FAILED.value,
)


@dataclass(frozen=True)
class _Scope:
    """One test's repository, persisted-row session id, and fresh wallets."""

    repository: SQLAlchemyRepository
    session_id: str
    wallet: str
    other_wallet: str


def _new_id() -> str:
    """Return a fresh canonical UUID string."""
    return str(uuid7())


def _instrument(
    scope: _Scope,
    exchange: str,
    *,
    public_id: str | None = None,
    known_to: datetime = KNOWN_TO_MAX,
    timestamp: datetime = _T0,
) -> Instrument:
    """Build one Instrument version owned by the test's session id."""
    return Instrument(
        public_id=public_id or _new_id(),
        symbol_public_id=_new_id(),
        exchange=exchange,
        session_id=scope.session_id,
        sequence_id=1,
        timestamp=timestamp,
        known_to=known_to,
    )


class _VersionOverrides(TypedDict, total=False):
    """Optional scope and version fields a test row may override."""

    wallet_public_id: str
    mode: str
    public_id: str
    known_to: datetime
    timestamp: datetime


class _CommandOverrides(_VersionOverrides, total=False):
    """Optional command fields a test command may override."""

    exchange: str
    command_type: str
    client_order_id: str
    venue_client_id: str
    acked_at: datetime


class _EventOverrides(TypedDict, total=False):
    """Optional venue event fields a test event may override."""

    wallet_public_id: str
    mode: str
    client_order_id: str
    venue_client_id: str
    command_public_id: str
    exchange_order_id: str


def _order(
    scope: _Scope,
    instrument_public_id: str,
    exchange_order_id: str | None,
    **overrides: Unpack[_VersionOverrides],
) -> Order:
    """Build one Order version owned by the test's session id."""
    fields: dict[str, object] = {
        "public_id": _new_id(),
        "mode": "live",
        "wallet_public_id": scope.wallet,
        "timestamp": _T0,
        "known_to": KNOWN_TO_MAX,
        **overrides,
    }
    return Order(
        instrument_public_id=instrument_public_id,
        client_order_id=None,
        exchange_order_id=exchange_order_id,
        created_at=_T0,
        side="buy",
        order_type="market",
        size=1.0,
        status="filled",
        session_id=scope.session_id,
        sequence_id=1,
        **fields,
    )


def _command(
    scope: _Scope,
    status: str,
    exchange_order_id: str | None,
    **overrides: Unpack[_CommandOverrides],
) -> TradeCommand:
    """Build one TradeCommand version owned by the test's session id."""
    fields: dict[str, object] = {
        "public_id": _new_id(),
        "command_type": "submit",
        "exchange": "walutomat",
        "mode": "live",
        "wallet_public_id": scope.wallet,
        "timestamp": _T0,
        "known_to": KNOWN_TO_MAX,
        "client_order_id": _new_id(),
        "venue_client_id": _new_id(),
        **overrides,
    }
    return TradeCommand(
        shard_key=f"{fields['exchange']}.EUR-PLN.{fields['mode']}",
        instrument="EUR-PLN",
        strategy_id="ownership-test",
        side="buy",
        order_type="market",
        quantity=1.0,
        status=status,
        created_at=_T0,
        exchange_order_id=exchange_order_id,
        session_id=scope.session_id,
        sequence_id=1,
        **fields,
    )


def _venue_event(
    scope: _Scope, event_type: str, **overrides: Unpack[_EventOverrides]
) -> VenueEvent:
    """Build one venue event owned by the test's session id."""
    fields: dict[str, object] = {
        "wallet_public_id": scope.wallet,
        "mode": "live",
        **overrides,
    }
    return VenueEvent(
        event_type=event_type,
        shard_key="walutomat.EUR-PLN.live",
        exchange="walutomat",
        instrument="EUR-PLN",
        status="failed",
        error="live trading interlocked",
        received_at=_T0,
        session_id=scope.session_id,
        sequence_id=1,
        timestamp=_T0,
        known_to=KNOWN_TO_MAX,
        **fields,
    )


async def _persist(scope: _Scope, *rows: Instrument | Order | TradeCommand | VenueEvent) -> None:
    """Commit the given rows in one session."""
    async with scope.repository.session() as s:
        s.add_all(rows)
        await s.commit()


async def _read(scope: _Scope) -> tuple[frozenset[str], bool]:
    """Read the Walutomat population of the test wallet and gate it."""
    population = await scope.repository.get_spot_anchor_owned_order_population(
        scope.wallet, "walutomat", "live"
    )
    return population["owned_order_ids"], own_submit_unresolved(population)


def _configured_url() -> str:
    """Return the configured test database URL."""
    return BootstrapSettingsLoader().db_url


@pytest.fixture
async def configured() -> AsyncIterator[_Scope]:
    """Yield a scope on the configured database and delete its rows afterwards."""
    scope = _Scope(
        repository=SQLAlchemyRepository(_configured_url()),
        session_id=_new_id(),
        wallet=_new_id(),
        other_wallet=_new_id(),
    )
    try:
        yield scope
    finally:
        async with scope.repository.session() as s:
            for model in (TradeCommand, Order, Instrument, VenueEvent):
                await s.execute(delete(model).where(model.session_id == scope.session_id))
            await s.commit()
        await scope.repository.engine.dispose()


@pytest.fixture
async def private_sqlite(tmp_path: Path) -> AsyncIterator[_Scope]:
    """Yield a scope on a private SQLite file whose schema a test may relax."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'ownership.db'}")
    await repository.create_all()
    try:
        yield _Scope(
            repository=repository,
            session_id=_new_id(),
            wallet=_new_id(),
            other_wallet=_new_id(),
        )
    finally:
        await repository.engine.dispose()


async def test_the_population_is_scoped_to_wallet_mode_and_exchange(configured: _Scope) -> None:
    """Other wallets, modes, and exchanges never enter the population.

    Given: A Walutomat Order and a filled submit command of the wallet in live
        mode, plus a Kraken Order of the wallet, a Walutomat Order of another
        wallet and of paper mode, and commands of every other scope or type.
    When: The Walutomat population is read.
    Then: ``S``, the resolved Orders, and the commands hold only the in-scope
        rows, and the gate passes.
    """
    walutomat = _instrument(configured, "walutomat")
    kraken = _instrument(configured, "kraken")
    owned = _order(configured, walutomat.public_id, "WA-1")
    await _persist(
        configured,
        walutomat,
        kraken,
        owned,
        _order(configured, kraken.public_id, "KR-1"),
        _order(configured, walutomat.public_id, "WA-2", wallet_public_id=configured.other_wallet),
        _order(configured, walutomat.public_id, "WA-3", mode="paper"),
        _command(configured, "filled", "WA-1"),
        _command(configured, "dispatched", None, exchange="kraken"),
        _command(configured, "dispatched", None, wallet_public_id=configured.other_wallet),
        _command(configured, "dispatched", None, mode="paper"),
        _command(configured, "dispatched", None, command_type="cancel"),
    )
    population = await configured.repository.get_spot_anchor_owned_order_population(
        configured.wallet, "walutomat", "live"
    )
    assert population["owned_order_ids"] == frozenset({"WA-1"})
    assert population["resolved_orders"] == (
        {"order_public_id": owned.public_id, "exchange_order_id": "WA-1"},
    )
    assert population["unresolved_lineage"] == ()
    assert population["blocked_order_ids"] == frozenset()
    assert population["other_exchange_unidentified_orders"] == ()
    assert [command["status"] for command in population["commands"]] == ["filled"]
    assert own_submit_unresolved(population) is False


async def test_create_commands_are_in_scope_like_submits(configured: _Scope) -> None:
    """A REST ``create`` command is read exactly like an engine ``submit``.

    Given: A resolved Walutomat Order and a still-created ``create`` command.
    When: The Walutomat population is read.
    Then: The command is present and the gate refuses.
    """
    instrument = _instrument(configured, "walutomat")
    await _persist(
        configured,
        instrument,
        _order(configured, instrument.public_id, "WA-1"),
        _command(configured, "created", None, command_type="create"),
    )
    owned, unresolved = await _read(configured)
    assert owned == frozenset({"WA-1"})
    assert unresolved is True


async def test_a_missing_instrument_blocks_with_its_venue_id(configured: _Scope) -> None:
    """An Order whose Instrument has no version survives the left join as a fact.

    Given: A Walutomat Order with id ``WA-1`` whose instrument id names nothing.
    When: The Walutomat population is read.
    Then: An ``instrument_absent`` fact keeps ``WA-1`` blocked and out of ``S``.
    """
    await _persist(configured, _order(configured, _new_id(), "WA-1"))
    population = await configured.repository.get_spot_anchor_owned_order_population(
        configured.wallet, "walutomat", "live"
    )
    assert [fact["fact"] for fact in population["unresolved_lineage"]] == ["instrument_absent"]
    assert population["blocked_order_ids"] == frozenset({"WA-1"})
    assert population["owned_order_ids"] == frozenset()
    assert own_submit_unresolved(population) is True


async def test_a_dangling_instrument_blocks(configured: _Scope) -> None:
    """An Order whose Instrument has only closed versions is dangling.

    Given: A Walutomat Order whose Instrument's only version is closed.
    When: The Walutomat population is read.
    Then: An ``instrument_dangling`` fact blocks the gate.
    """
    retired = _instrument(configured, "walutomat", known_to=_CLOSED_AT)
    await _persist(configured, retired, _order(configured, retired.public_id, "WA-1"))
    population = await configured.repository.get_spot_anchor_owned_order_population(
        configured.wallet, "walutomat", "live"
    )
    assert [fact["fact"] for fact in population["unresolved_lineage"]] == ["instrument_dangling"]
    assert own_submit_unresolved(population) is True


async def test_an_instrument_whose_versions_disagree_conflicts(configured: _Scope) -> None:
    """An Instrument whose closed version names another exchange conflicts.

    Given: A Walutomat Order whose Instrument's sentinel tip says Walutomat while
        an older closed version of the same Instrument said Kraken.
    When: The Walutomat population is read.
    Then: An ``instrument_conflicting`` fact blocks instead of trusting the tip.
    """
    public_id = _new_id()
    await _persist(
        configured,
        _instrument(configured, "kraken", public_id=public_id, known_to=_CLOSED_AT),
        _instrument(configured, "walutomat", public_id=public_id, timestamp=_CLOSED_AT),
        _order(configured, public_id, "WA-1"),
    )
    population = await configured.repository.get_spot_anchor_owned_order_population(
        configured.wallet, "walutomat", "live"
    )
    assert [fact["fact"] for fact in population["unresolved_lineage"]] == ["instrument_conflicting"]
    assert population["blocked_order_ids"] == frozenset({"WA-1"})


async def test_a_non_canonical_instrument_exchange_conflicts(configured: _Scope) -> None:
    """A padded exchange key is never read as another exchange.

    Given: An Order whose only Instrument carries a padded ``walutomat `` key.
    When: The Walutomat population is read.
    Then: An ``instrument_conflicting`` fact blocks rather than excluding it.
    """
    padded = _instrument(configured, "walutomat ")
    await _persist(configured, padded, _order(configured, padded.public_id, "WA-1"))
    population = await configured.repository.get_spot_anchor_owned_order_population(
        configured.wallet, "walutomat", "live"
    )
    assert [fact["fact"] for fact in population["unresolved_lineage"]] == ["instrument_conflicting"]


async def test_a_null_venue_order_id_refuses(configured: _Scope) -> None:
    """A resolved Walutomat Order without a venue order id is unresolved.

    Given: A resolved Walutomat Order whose venue order id is missing.
    When: The Walutomat population is read.
    Then: The Order is listed as resolved, ``S`` stays empty, and the gate refuses.
    """
    instrument = _instrument(configured, "walutomat")
    await _persist(configured, instrument, _order(configured, instrument.public_id, None))
    population = await configured.repository.get_spot_anchor_owned_order_population(
        configured.wallet, "walutomat", "live"
    )
    assert [order["exchange_order_id"] for order in population["resolved_orders"]] == [None]
    assert population["owned_order_ids"] == frozenset()
    assert own_submit_unresolved(population) is True


async def test_a_null_venue_order_id_on_another_exchange_refuses(configured: _Scope) -> None:
    """A resolved Kraken Order without a venue order id still refuses Walutomat.

    Given: A resolved Walutomat Order with its venue order id and its filled
        command, plus a resolved Kraken Order of the same wallet and mode whose
        venue order id is missing.
    When: The Walutomat population is read.
    Then: ``S`` holds only the Walutomat id, the Kraken Order is recorded as an
        unidentified Order of another exchange, and the gate refuses.
    """
    walutomat = _instrument(configured, "walutomat")
    kraken = _instrument(configured, "kraken")
    unidentified = _order(configured, kraken.public_id, None)
    await _persist(
        configured,
        walutomat,
        kraken,
        _order(configured, walutomat.public_id, "WA-1"),
        unidentified,
        _command(configured, "filled", "WA-1"),
    )
    population = await configured.repository.get_spot_anchor_owned_order_population(
        configured.wallet, "walutomat", "live"
    )
    assert population["owned_order_ids"] == frozenset({"WA-1"})
    assert population["other_exchange_unidentified_orders"] == (unidentified.public_id,)
    assert own_submit_unresolved(population) is True


async def test_history_versions_are_ignored(configured: _Scope) -> None:
    """Closed Order and command versions never enter the population.

    Given: An Order whose closed versions carry no id and an old id, and whose
        sentinel tip carries ``WA-1``, plus a command whose closed version was
        still created and whose sentinel tip is filled with ``WA-1``.
    When: The Walutomat population is read.
    Then: ``S`` is exactly ``WA-1``, one command is read, and the gate passes.
    """
    instrument = _instrument(configured, "walutomat")
    order_public_id = _new_id()
    command_public_id = _new_id()
    await _persist(
        configured,
        instrument,
        _order(
            configured,
            instrument.public_id,
            None,
            public_id=order_public_id,
            known_to=_T0 + timedelta(minutes=1),
        ),
        _order(
            configured,
            instrument.public_id,
            "WA-OLD",
            public_id=order_public_id,
            known_to=_CLOSED_AT,
            timestamp=_T0 + timedelta(minutes=1),
        ),
        _order(
            configured,
            instrument.public_id,
            "WA-1",
            public_id=order_public_id,
            timestamp=_CLOSED_AT,
        ),
        _command(configured, "created", None, public_id=command_public_id, known_to=_CLOSED_AT),
        _command(configured, "filled", "WA-1", public_id=command_public_id, timestamp=_CLOSED_AT),
    )
    population = await configured.repository.get_spot_anchor_owned_order_population(
        configured.wallet, "walutomat", "live"
    )
    assert population["owned_order_ids"] == frozenset({"WA-1"})
    assert len(population["resolved_orders"]) == 1
    assert [command["status"] for command in population["commands"]] == ["filled"]
    assert own_submit_unresolved(population) is False


@pytest.mark.parametrize("status", _NEVER_SETTLED)
async def test_a_live_command_status_refuses_on_the_database(
    configured: _Scope, status: str
) -> None:
    """Every never-settled status read from the database refuses.

    Given: A resolved Walutomat Order ``WA-1`` and a command in a live status
        naming ``WA-1``.
    When: The Walutomat population is read and gated.
    Then: The status is read verbatim and the gate refuses.

    Args:
        configured: The configured-database scope.
        status: A status the closed table never settles.
    """
    instrument = _instrument(configured, "walutomat")
    await _persist(
        configured,
        instrument,
        _order(configured, instrument.public_id, "WA-1"),
        _command(configured, status, "WA-1"),
    )
    owned, unresolved = await _read(configured)
    assert owned == frozenset({"WA-1"})
    assert unresolved is True


@pytest.mark.parametrize("status", _SETTLED_BY_VENUE_ID)
async def test_a_terminal_command_settles_only_with_its_id_in_s(
    configured: _Scope, status: str
) -> None:
    """A terminal status settles with its id in ``S`` and refuses without it.

    Given: A resolved Walutomat Order ``WA-1``, one terminal command naming it,
        and the same terminal status on a second command with no venue id.
    When: The population is read before and after the second command exists.
    Then: The first read passes and the second refuses — no pre-venue proof
        settles a command without a venue id in ``S``.

    Args:
        configured: The configured-database scope.
        status: A terminal status.
    """
    instrument = _instrument(configured, "walutomat")
    await _persist(
        configured,
        instrument,
        _order(configured, instrument.public_id, "WA-1"),
        _command(configured, status, "WA-1"),
    )
    assert (await _read(configured))[1] is False
    await _persist(configured, _command(configured, status, None))
    assert (await _read(configured))[1] is True


async def test_an_out_of_enum_status_is_read_verbatim_and_refuses(
    private_sqlite: _Scope,
) -> None:
    """A status outside the closed enum reaches the gate and fails closed.

    Given: A private SQLite file with CHECK constraints relaxed, a resolved
        Walutomat Order ``WA-1``, and a command whose status is not an enum value.
    When: The Walutomat population is read.
    Then: The unknown status is read verbatim and the gate refuses.
    """
    instrument = _instrument(private_sqlite, "walutomat")
    async with private_sqlite.repository.session() as s:
        await s.execute(text("PRAGMA ignore_check_constraints = ON"))
        s.add_all(
            [
                instrument,
                _order(private_sqlite, instrument.public_id, "WA-1"),
                _command(private_sqlite, "settled_elsewhere", "WA-1"),
            ]
        )
        await s.commit()
    population = await private_sqlite.repository.get_spot_anchor_owned_order_population(
        private_sqlite.wallet, "walutomat", "live"
    )
    assert [command["status"] for command in population["commands"]] == ["settled_elsewhere"]
    assert own_submit_unresolved(population) is True


async def test_a_multiply_active_instrument_survives_the_left_join(
    private_sqlite: _Scope,
) -> None:
    """Two sentinel-tip Instruments for one id keep the Order unresolved.

    Given: A private SQLite file without the active-public-id index, holding two
        sentinel-tip Walutomat versions of one Instrument and an Order on it.
    When: The Walutomat population is read.
    Then: An ``instrument_multiply_active`` fact blocks the Order's venue id.
    """
    async with private_sqlite.repository.session() as s:
        await s.execute(text("DROP INDEX ix_instruments_public_id"))
        await s.commit()
    public_id = _new_id()
    await _persist(
        private_sqlite,
        _instrument(private_sqlite, "walutomat", public_id=public_id),
        _instrument(private_sqlite, "walutomat", public_id=public_id),
        _order(private_sqlite, public_id, "WA-1"),
    )
    population = await private_sqlite.repository.get_spot_anchor_owned_order_population(
        private_sqlite.wallet, "walutomat", "live"
    )
    assert [fact["fact"] for fact in population["unresolved_lineage"]] == [
        "instrument_multiply_active"
    ]
    assert population["blocked_order_ids"] == frozenset({"WA-1"})


async def test_a_malformed_wallet_identity_raises(private_sqlite: _Scope) -> None:
    """A wallet identity that is not a UUID is refused like other reconciliation reads.

    Given: A malformed wallet identity.
    When: The population is read.
    Then: ``ValueError`` is raised.
    """
    with pytest.raises(ValueError, match="wallet identity is invalid"):
        await private_sqlite.repository.get_spot_anchor_owned_order_population(
            "not-a-wallet", "walutomat", "live"
        )


async def test_postgresql_reads_open_one_repeatable_read_only_transaction(
    private_sqlite: _Scope,
) -> None:
    """On PostgreSQL the snapshot is pinned before the first read.

    Given: A repository whose dialect reports PostgreSQL and a recording session.
    When: The population is read.
    Then: One explicit transaction is opened and its first statement sets
        ``REPEATABLE READ, READ ONLY``, followed by the four reads of a scope
        without never-submitted candidates.
    """
    session = AsyncMock(begin=MagicMock(return_value=AsyncMock()))
    session.execute = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=[])))
    repository = private_sqlite.repository
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
        population = await repository.get_spot_anchor_owned_order_population(
            private_sqlite.wallet, "walutomat", "live"
        )
    statement = session.execute.await_args_list[0].args[0]
    assert str(statement) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert session.execute.await_count == 5
    session.begin.assert_called_once_with()
    assert population["owned_order_ids"] == frozenset()
    assert population["commands"] == ()


async def test_sqlite_reads_open_one_explicit_database_transaction(
    private_sqlite: _Scope,
) -> None:
    """On SQLite an explicit ``BEGIN`` precedes the three reads.

    Given: A SQLite repository and a recording session.
    When: The population is read.
    Then: One transaction scope is opened and its first statement is a plain
        ``BEGIN``, followed by the four reads of a scope without never-submitted
        candidates, because the driver's legacy transaction control issues none
        before a ``SELECT``.
    """
    session = AsyncMock(begin=MagicMock(return_value=AsyncMock()))
    session.execute = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=[])))
    repository = private_sqlite.repository
    with patch.object(repository, "session") as session_context:
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        population = await repository.get_spot_anchor_owned_order_population(
            private_sqlite.wallet, "walutomat", "live"
        )
    statement = session.execute.await_args_list[0].args[0]
    assert str(statement) == "BEGIN"
    assert session.execute.await_count == 5
    session.begin.assert_called_once_with()
    assert population["owned_order_ids"] == frozenset()


async def test_the_population_reads_one_snapshot(
    configured: _Scope, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A command committed between the Order and command reads stays invisible.

    Runs on the configured database: the isolated SQLite copy by default, and
    the PostgreSQL witness when ``DB_URL`` names one. On SQLite it proves the
    explicit ``BEGIN``: without it each ``SELECT`` reads its own cut, and the
    later command read would see the new command.

    Given: A resolved Walutomat Order ``WA-1`` with a filled command, and a
        dispatched in-scope command committed by another connection after the
        population's Order read.
    When: The population is read, then read again.
    Then: The first read still passes the gate from its snapshot, and the second
        read sees the new command and refuses.
    """
    instrument = _instrument(configured, "walutomat")
    await _persist(
        configured,
        instrument,
        _order(configured, instrument.public_id, "WA-1"),
        _command(configured, "filled", "WA-1"),
    )
    original = SQLAlchemyRepository._spot_owned_instrument_versions

    async def versions_then_commit(
        s: AsyncSession, wallet_public_id: str, mode: str
    ) -> dict[str, frozenset[str]]:
        """Read the versions, then commit a new command from another session."""
        versions = await original(s, wallet_public_id, mode)
        await _persist(configured, _command(configured, "dispatched", None))
        return versions

    monkeypatch.setattr(
        configured.repository, "_spot_owned_instrument_versions", versions_then_commit
    )
    assert (await _read(configured))[1] is False
    monkeypatch.undo()
    assert (await _read(configured))[1] is True


def _candidate_history(scope: _Scope, public_id: str, client_order_id: str) -> list[TradeCommand]:
    """Build a create that was dispatched and then failed before the venue, in three versions."""
    common: _CommandOverrides = {
        "public_id": public_id,
        "command_type": "create",
        "client_order_id": client_order_id,
        "venue_client_id": client_order_id,
    }
    return [
        _command(scope, "created", None, known_to=_T0 + timedelta(seconds=1), **common),
        _command(
            scope,
            "dispatched",
            None,
            timestamp=_T0 + timedelta(seconds=1),
            known_to=_T0 + timedelta(seconds=2),
            **common,
        ),
        _command(scope, "failed", None, timestamp=_T0 + timedelta(seconds=2), **common),
    ]


async def _population(scope: _Scope) -> SpotOwnedOrderPopulation:
    """Read the Walutomat population of the test wallet."""
    return await scope.repository.get_spot_anchor_owned_order_population(
        scope.wallet, "walutomat", "live"
    )


def _only_command(population: SpotOwnedOrderPopulation) -> SpotOwnedSubmitCommandRow:
    """Return the population's single in-scope command."""
    assert len(population["commands"]) == 1
    return population["commands"][0]


async def test_a_never_submitted_candidate_carries_its_whole_facts(configured: _Scope) -> None:
    """A failed create without a venue id reads its history, linked events, Orders and audit.

    Given: A create in three versions (created, dispatched, failed), venue events
        linked by client order id in another wallet and mode, by venue client id
        alone and by command public id alone, an unrelated event, and an Order
        of another wallet carrying the client order id.
    When: The population is read.
    Then: The candidate lists every version with its open-ended flag, exactly the
        three linked events in id order, one Order, and the tip's audit context.
    """
    public_id = _new_id()
    client_order_id = _new_id()
    history = _candidate_history(configured, public_id, client_order_id)
    by_client_id = _venue_event(
        configured,
        "order_interlock_blocked",
        client_order_id=client_order_id,
        wallet_public_id=configured.other_wallet,
        mode="paper",
    )
    by_venue_client_id = _venue_event(configured, "order_rejected", venue_client_id=client_order_id)
    by_command_id = _venue_event(configured, "order_rejected", command_public_id=public_id)
    unrelated = _venue_event(configured, "order_rejected", client_order_id=_new_id())
    instrument = _instrument(configured, "walutomat")
    other_wallet_order = _order(
        configured, instrument.public_id, "WA-9", wallet_public_id=configured.other_wallet
    )
    other_wallet_order.client_order_id = client_order_id
    await _persist(
        configured,
        *history,
        by_client_id,
        by_venue_client_id,
        by_command_id,
        unrelated,
        instrument,
        other_wallet_order,
    )
    command = _only_command(await _population(configured))
    candidate = command["candidate"]
    assert candidate is not None
    assert command["row_id"] == history[2].id
    assert command["client_order_id"] == client_order_id
    assert command["venue_client_id"] == client_order_id
    assert command["acked_at"] is None
    assert [
        (version["row_id"], version["status"], version["open_ended"])
        for version in candidate["versions"]
    ] == [
        (history[0].id, "created", False),
        (history[1].id, "dispatched", False),
        (history[2].id, "failed", True),
    ]
    assert [event["id"] for event in candidate["venue_events"]] == sorted(
        [by_client_id.id, by_venue_client_id.id, by_command_id.id]
    )
    assert candidate["venue_events"][-1]["received_at"] == _T0
    assert candidate["order_row_count"] == 1
    assert candidate["audit"]["instrument"] == "EUR-PLN"
    assert candidate["audit"]["created_at"] == _T0
    assert candidate["audit"]["quantity"] == 1.0


@pytest.mark.parametrize(
    "status, exchange_order_id, acked, client_order_id",
    [
        pytest.param("filled", "WA-1", False, None, id="settled_status"),
        pytest.param("failed", "WA-1", False, None, id="venue_id"),
        pytest.param("rejected", None, True, None, id="acknowledged"),
        pytest.param("rejected", None, False, "  ", id="blank_client_id"),
    ],
)
async def test_only_never_submitted_candidates_carry_facts(
    configured: _Scope,
    status: str,
    exchange_order_id: str | None,
    acked: bool,
    client_order_id: str | None,
) -> None:
    """A command that cannot be never-submitted gets no candidate read.

    Given: One in-scope command that is settled, carries a venue id, was
        acknowledged, or has a blank client order id.
    When: The population is read.
    Then: Its candidate is ``None``.

    Args:
        configured: The test scope.
        status: The command's status.
        exchange_order_id: Its venue order id.
        acked: Whether it carries an acknowledgement time.
        client_order_id: Its client order id, or ``None`` for a fresh one.
    """
    overrides: _CommandOverrides = {}
    if acked:
        overrides["acked_at"] = _T0
    if client_order_id is not None:
        overrides["client_order_id"] = client_order_id
    await _persist(configured, _command(configured, status, exchange_order_id, **overrides))
    assert _only_command(await _population(configured))["candidate"] is None


async def test_retired_commands_are_found_within_the_scope(configured: _Scope) -> None:
    """A command without an open-ended version in scope is retired and refuses.

    Given: A fully closed in-scope history; closed in-scope versions whose
        open-ended successors moved to Kraken, to another wallet, to the paper
        mode, or to the cancel command type; a closed version with an open
        in-scope successor; and a closed history of another wallet.
    When: The population is read.
    Then: Exactly the fully closed and the four moved public ids are retired,
        sorted, and the gate refuses.
    """
    closed_at: _VersionOverrides = {"known_to": _CLOSED_AT}
    fully_closed = _new_id()
    moved = _new_id()
    moved_wallet = _new_id()
    moved_mode = _new_id()
    moved_type = _new_id()
    still_open = _new_id()
    other_wallet = _new_id()
    successors: list[TradeCommand] = []
    for public_id, successor in (
        (moved_wallet, _CommandOverrides(wallet_public_id=configured.other_wallet)),
        (moved_mode, _CommandOverrides(mode="paper")),
        (moved_type, _CommandOverrides(command_type="cancel")),
    ):
        successors.append(
            _command(configured, "dispatched", None, public_id=public_id, **closed_at)
        )
        successors.append(
            _command(
                configured,
                "dispatched",
                None,
                public_id=public_id,
                timestamp=_CLOSED_AT,
                **successor,
            )
        )
    await _persist(
        configured,
        *successors,
        _command(configured, "dispatched", None, public_id=fully_closed, **closed_at),
        _command(configured, "dispatched", None, public_id=moved, **closed_at),
        _command(
            configured,
            "dispatched",
            None,
            public_id=moved,
            exchange="kraken",
            timestamp=_CLOSED_AT,
        ),
        _command(configured, "dispatched", None, public_id=still_open, **closed_at),
        _command(configured, "filled", "WA-1", public_id=still_open, timestamp=_CLOSED_AT),
        _command(
            configured,
            "dispatched",
            None,
            public_id=other_wallet,
            wallet_public_id=configured.other_wallet,
            **closed_at,
        ),
    )
    population = await _population(configured)
    assert population["retired_command_public_ids"] == tuple(
        sorted([fully_closed, moved, moved_wallet, moved_mode, moved_type])
    )
    assert own_submit_unresolved(population) is True


async def test_a_pinned_candidate_is_excluded_until_its_facts_change(configured: _Scope) -> None:
    """The exclusion settles the exact reviewed facts and nothing that differs.

    Given: The interlock-failed create with its one event, no Order and no other
        command, and its document digest pinned.
    When: The population is read, a late event is linked, and it is read again.
    Then: The first read excludes it and the gate passes; the second excludes
        nothing and the gate refuses.
    """
    public_id = _new_id()
    client_order_id = _new_id()
    await _persist(
        configured,
        *_candidate_history(configured, public_id, client_order_id),
        _venue_event(configured, "order_interlock_blocked", client_order_id=client_order_id),
    )
    document = never_submitted_document(_only_command(await _population(configured)))
    assert document is not None
    pinned = frozenset({never_submitted_digest(document)})
    with patch("snapper.data.repository.NEVER_SUBMITTED_COMMAND_DIGESTS", pinned):
        before = await _population(configured)
        await _persist(
            configured,
            _venue_event(configured, "order_accepted", client_order_id=client_order_id),
        )
        after = await _population(configured)
    assert before["excluded_command_public_ids"] == frozenset({public_id})
    assert before["excluded_client_order_ids"] == frozenset({client_order_id})
    assert own_submit_unresolved(before) is False
    assert after["excluded_command_public_ids"] == frozenset()
    assert own_submit_unresolved(after) is True


async def test_the_candidate_facts_share_the_population_snapshot(
    configured: _Scope, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An event committed after the snapshot opened stays out of the candidate.

    Runs on the configured database: the isolated SQLite copy by default, and
    the PostgreSQL witness when ``DB_URL`` names one.

    Given: The interlock-failed create with its one event, and a second linked
        event committed by another connection after the population's Instrument
        read.
    When: The population is read, then read again.
    Then: The first read's candidate holds one event, and the second holds two.
    """
    public_id = _new_id()
    client_order_id = _new_id()
    await _persist(
        configured,
        *_candidate_history(configured, public_id, client_order_id),
        _venue_event(configured, "order_interlock_blocked", client_order_id=client_order_id),
    )
    original = SQLAlchemyRepository._spot_owned_instrument_versions

    async def versions_then_commit(
        s: AsyncSession, wallet_public_id: str, mode: str
    ) -> dict[str, frozenset[str]]:
        """Read the versions, then commit a linked event from another session."""
        versions = await original(s, wallet_public_id, mode)
        await _persist(
            configured,
            _venue_event(configured, "order_accepted", command_public_id=public_id),
        )
        return versions

    monkeypatch.setattr(
        configured.repository, "_spot_owned_instrument_versions", versions_then_commit
    )
    first = _only_command(await _population(configured))["candidate"]
    monkeypatch.undo()
    second = _only_command(await _population(configured))["candidate"]
    assert first is not None
    assert second is not None
    assert len(first["venue_events"]) == 1
    assert len(second["venue_events"]) == 2


async def test_candidate_facts_are_batched_and_attributed_per_candidate(configured: _Scope) -> None:
    """Several candidates share three reads, and each gets exactly its own facts.

    Given: Two interlock-failed creates, an event linked to the first by client
        order id and to the second by command public id, an event of the second
        alone, and an Order carrying the first's client order id.
    When: The population is read.
    Then: Each candidate lists its own three versions, the shared event appears
        once in each, the lone event only in the second, and the Order counts
        only for the first.
    """
    first_id, first_client = _new_id(), _new_id()
    second_id, second_client = _new_id(), _new_id()
    shared = _venue_event(
        configured,
        "order_interlock_blocked",
        client_order_id=first_client,
        command_public_id=second_id,
    )
    lone = _venue_event(configured, "order_interlock_blocked", client_order_id=second_client)
    instrument = _instrument(configured, "walutomat")
    order = _order(configured, instrument.public_id, "WA-7")
    order.client_order_id = first_client
    await _persist(
        configured,
        *_candidate_history(configured, first_id, first_client),
        *_candidate_history(configured, second_id, second_client),
        shared,
        lone,
        instrument,
        order,
    )
    population = await _population(configured)
    by_id = {command["public_id"]: command["candidate"] for command in population["commands"]}
    first, second = by_id[first_id], by_id[second_id]
    assert first is not None
    assert second is not None
    assert [version["status"] for version in first["versions"]] == [
        "created",
        "dispatched",
        "failed",
    ]
    assert [version["status"] for version in second["versions"]] == [
        "created",
        "dispatched",
        "failed",
    ]
    assert [event["id"] for event in first["venue_events"]] == [shared.id]
    assert [event["id"] for event in second["venue_events"]] == sorted([shared.id, lone.id])
    assert first["order_row_count"] == 1
    assert second["order_row_count"] == 0


async def test_the_candidate_reads_do_not_grow_with_the_candidates(configured: _Scope) -> None:
    """The population issues the same number of statements for one or three candidates.

    Given: One interlock-failed create, then two more in the same scope.
    When: The population is read before and after the two are added, counting
        the statements sent to the database.
    Then: Both reads send the same number of statements.
    """
    statements: list[str] = []

    def count(*args: object) -> None:
        """Record one statement sent through the engine."""
        statements.append(str(args[2]))

    await _persist(configured, *_candidate_history(configured, _new_id(), _new_id()))
    event.listen(configured.repository.engine.sync_engine, "before_cursor_execute", count)
    try:
        await _population(configured)
        one_candidate = len(statements)
        await _persist(
            configured,
            *_candidate_history(configured, _new_id(), _new_id()),
            *_candidate_history(configured, _new_id(), _new_id()),
        )
        statements.clear()
        await _population(configured)
        three_candidates = len(statements)
    finally:
        event.remove(configured.repository.engine.sync_engine, "before_cursor_execute", count)
    assert three_candidates == one_candidate
