"""Tests of the clockless, join-free reads of the cancel-target repair (G3-C)."""

import json
from collections.abc import AsyncIterator
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest

from snapper.data import cancel_target_repair_reads as reads
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.cancel_target_repair import CANCELED_AT
from tests.helpers.cancel_target_repair import CREATED_AT
from tests.helpers.cancel_target_repair import KRAKEN_SHARD
from tests.helpers.cancel_target_repair import OTHER_WALLET
from tests.helpers.cancel_target_repair import SESSION
from tests.helpers.cancel_target_repair import WALLET
from tests.helpers.cancel_target_repair import add_rows
from tests.helpers.cancel_target_repair import checkpoint
from tests.helpers.cancel_target_repair import command
from tests.helpers.cancel_target_repair import execution
from tests.helpers.cancel_target_repair import paired_execution_leg
from tests.helpers.cancel_target_repair import seed_kraken_pair
from tests.helpers.cancel_target_repair import sqlite_repository
from tests.helpers.cancel_target_repair import venue_event

_FUTURE = CREATED_AT + timedelta(days=3650)


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide an empty SQLite repository owned by one test."""
    async with sqlite_repository(tmp_path / "reads.db") as created:
        yield created


@pytest.mark.asyncio
async def test_active_command_is_found_even_when_stamped_in_the_future(
    repository: SQLAlchemyRepository,
) -> None:
    """The clockless read finds an active row whatever its timestamp.

    Given: An active command stamped ten years ahead,
    When: It is read by public id,
    Then: It is found, and an unknown id gives None.
    """
    future = command(
        "00000000-0000-7000-8000-0000000000e9",
        command_type="cancel",
        status="dispatched",
        client_order_id="cid-future",
        created_at=_FUTURE,
    )
    await add_rows(repository, [future])
    found = await reads.get_active_trade_command(repository, future.public_id)
    assert found is not None and found["status"] == "dispatched"
    missing = await reads.get_active_trade_command(
        repository, "00000000-0000-7000-8000-0000000000ee"
    )
    assert missing is None


@pytest.mark.asyncio
async def test_order_versions_include_closed_versions_in_id_order(
    repository: SQLAlchemyRepository,
) -> None:
    """Every version of the order is returned, the closed one first.

    Given: A seeded order with a closed open version and an active canceled one,
    When: Its versions are read by client order id,
    Then: Both come back in id order with their known_to.
    """
    pair = await seed_kraken_pair(repository)
    versions = await reads.get_order_versions_by_client_order_id(repository, pair.client_order_id)
    assert [version["status"] for version in versions] == ["open", "canceled"]
    assert versions[0]["known_to"] == CANCELED_AT
    assert versions[1]["exchange_order_id"] == "OX-1"
    assert [(version["order_type"], version["side"], version["size"]) for version in versions] == [
        ("limit", "buy", 5.0),
        ("limit", "buy", 5.0),
    ]


@pytest.mark.asyncio
async def test_executions_are_found_without_any_order_row(
    repository: SQLAlchemyRepository,
) -> None:
    """An execution is found by order public id even when no order row exists.

    Given: An execution whose order has no row at all,
    When: The executions of that order are read,
    Then: The row is returned with its evidence fields; no ids returns nothing.
    """
    orphan = "00000000-0000-7000-8000-0000000000d9"
    await add_rows(repository, [execution(orphan, exec_id="E-9")])
    rows = await reads.get_executions_by_order_public_ids(repository, [orphan])
    assert [(row["order_public_id"], row["exec_id"]) for row in rows] == [(orphan, "E-9")]
    assert await reads.get_executions_by_order_public_ids(repository, []) == []


@pytest.mark.asyncio
async def test_venue_events_and_open_commands_and_wallets(
    repository: SQLAlchemyRepository,
) -> None:
    """The shard reads return events by id, open commands only, and sorted wallets.

    Given: A seeded pair, an event of another wallet on the shard, a terminal command,
        and a checkpoint of the other wallet,
    When: The events, the open commands and the wallets are read,
    Then: Each read returns exactly what its contract names.
    """
    pair = await seed_kraken_pair(repository)
    await add_rows(
        repository,
        [
            venue_event("cid-other", event_type="order_accepted", wallet_public_id=OTHER_WALLET),
            command(
                "00000000-0000-7000-8000-0000000000e7",
                command_type="create",
                status="filled",
                client_order_id="cid-done",
            ),
            checkpoint(
                open_command_ids=json.dumps([pair.create_id]), wallet_public_id=OTHER_WALLET
            ),
        ],
    )
    events = await reads.get_venue_events_for_client_order_id(repository, pair.client_order_id)
    assert [event["event_type"] for event in events] == ["order_accepted"]
    open_ids = [
        row["public_id"] for row in await reads.get_open_commands_on_shard(repository, KRAKEN_SHARD)
    ]
    assert open_ids == [pair.create_id]
    assert await reads.get_shard_wallets(repository, KRAKEN_SHARD) == sorted([WALLET, OTHER_WALLET])
    active = await reads.get_active_checkpoint(repository, KRAKEN_SHARD)
    assert active is not None and active["wallet_public_id"] == OTHER_WALLET
    assert await reads.get_active_checkpoint(repository, "kraken.none.live") is None


@pytest.mark.asyncio
async def test_venue_events_can_match_an_exchange_order_id(
    repository: SQLAlchemyRepository,
) -> None:
    """The margin evidence read also finds an event whose client id is unrelated.

    Given: A seeded pair and a second event with another client id but its exchange order id,
    When: Events are read with that exchange order id,
    Then: Both events come back in id order without duplication.
    """
    pair = await seed_kraken_pair(repository)
    event = venue_event("cid-other", event_type="fill_observed")
    event.exchange_order_id = "OX-1"
    event.fill_size = 1.0
    await add_rows(repository, [event])
    events = await reads.get_venue_events_for_client_order_id(
        repository, pair.client_order_id, ["OX-1"]
    )
    assert [event["event_type"] for event in events] == ["order_accepted", "fill_observed"]


@pytest.mark.asyncio
async def test_paired_leg_existence_matches_either_order_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """The existence read matches client and exchange ids and returns false for no row.

    Given: No paired leg, then one leg with both order identities,
    When: Existence is checked by either identity,
    Then: The empty read is false and both matching reads are true.
    """
    assert not await reads.has_paired_execution_leg(repository, "cid-leg", [])
    leg = paired_execution_leg(client_order_id="cid-leg", exchange_order_id="OX-leg")
    await add_rows(repository, [leg])
    assert await reads.has_paired_execution_leg(repository, "cid-leg", [])
    assert await reads.has_paired_execution_leg(repository, "cid-other", ["OX-leg"])


@pytest.mark.asyncio
async def test_command_versions_are_returned_in_id_order(
    repository: SQLAlchemyRepository,
) -> None:
    """Every version of a command is returned, oldest first.

    Given: A command with a closed version and an active one,
    When: Its versions are read,
    Then: Both come back, closed first.
    """
    public_id = "00000000-0000-7000-8000-0000000000e8"
    first = command(public_id, command_type="create", status="dispatched", client_order_id="c8")
    first.known_to = CANCELED_AT
    await add_rows(repository, [first])
    await add_rows(
        repository,
        [command(public_id, command_type="create", status="accepted", client_order_id="c8")],
    )
    versions = await reads.get_trade_command_versions(repository, public_id)
    assert [version["status"] for version in versions] == ["dispatched", "accepted"]


async def _seed_versions_closed_in_the_future(repository: SQLAlchemyRepository) -> str:
    """Seed a command and a checkpoint, each with an active version and one closed ten years ahead.

    Returns:
        The command's public id.
    """
    public_id = "00000000-0000-7000-8000-0000000000e6"
    closed_command = command(
        public_id, command_type="create", status="accepted", client_order_id="c6"
    )
    closed_command.known_to = _FUTURE
    closed_checkpoint = checkpoint(wallet_public_id=OTHER_WALLET)
    closed_checkpoint.known_to = _FUTURE
    await add_rows(
        repository,
        [
            closed_command,
            command(public_id, command_type="create", status="filled", client_order_id="c6"),
            closed_checkpoint,
            checkpoint(),
        ],
    )
    return public_id


def _every_version_open(known_to: datetime) -> bool:
    """Accept every loaded version as open-ended, to reach the duplicate-row guards.

    Returns:
        Always True.
    """
    return known_to is not None


@pytest.mark.asyncio
async def test_versions_closed_in_the_future_are_never_active(
    repository: SQLAlchemyRepository,
) -> None:
    """A finite known_to later than now passes the SQL range but is not an active version.

    Given: A command and a checkpoint, each with an active version and a version
        closed ten years ahead,
    When: The active rows and the shard's open commands are read,
    Then: Only the open-ended versions come back.
    """
    public_id = await _seed_versions_closed_in_the_future(repository)
    found = await reads.get_active_trade_command(repository, public_id)
    assert found is not None and found["status"] == "filled"
    assert await reads.get_open_commands_on_shard(repository, KRAKEN_SHARD) == []
    active = await reads.get_active_checkpoint(repository, KRAKEN_SHARD)
    assert active is not None and active["wallet_public_id"] == WALLET


@pytest.mark.asyncio
async def test_several_active_rows_raise_instead_of_picking_one(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A read that finds more than one active row raises.

    Given: The same rows, with the open-ended proof made to accept every version,
    When: The active command and the active checkpoint are read,
    Then: Both raise RuntimeError.
    """
    public_id = await _seed_versions_closed_in_the_future(repository)
    monkeypatch.setattr(reads, "is_open_ended_version", _every_version_open)
    with pytest.raises(RuntimeError, match="several active versions"):
        await reads.get_active_trade_command(repository, public_id)
    with pytest.raises(RuntimeError, match="several active checkpoints"):
        await reads.get_active_checkpoint(repository, KRAKEN_SHARD)


@pytest.mark.asyncio
async def test_a_sessions_terminal_is_read_back_by_its_session_only(
    repository: SQLAlchemyRepository,
) -> None:
    """The read-back finds the order_terminal a session wrote, and nothing for another session.

    Given: An order_accepted and an order_terminal of one client order id, both by the
        seeded session,
    When: The terminal is read back by that session and by another one,
    Then: The seeded session finds the terminal, and the other session finds None.
    """
    await add_rows(
        repository,
        [
            venue_event("cid-t", event_type="order_accepted", status="open"),
            venue_event("cid-t", event_type="order_terminal", status="canceled"),
        ],
    )
    found = await reads.get_terminal_written_by_session(repository, "cid-t", SESSION)
    assert found is not None
    assert (found["event_type"], found["status"]) == ("order_terminal", "canceled")
    other = "00000000-0000-7000-8000-0000000000a2"
    assert await reads.get_terminal_written_by_session(repository, "cid-t", other) is None
