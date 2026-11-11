"""Tests for the pure owned-order population and unresolved-submit gate."""

import pytest

from snapper.application.portfolio.spot_anchor_ownership import authoritative_execution_order_ids
from snapper.application.portfolio.spot_anchor_ownership import build_owned_order_population
from snapper.application.portfolio.spot_anchor_ownership import is_venue_order_id
from snapper.application.portfolio.spot_anchor_ownership import own_submit_unresolved
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository_types import SpotExecutionWitnessRow
from snapper.data.repository_types import SpotOwnedOrderLineageFactKind
from snapper.data.repository_types import SpotOwnedOrderPopulation
from snapper.data.repository_types import SpotOwnedOrderSourceRow
from snapper.data.repository_types import SpotOwnedSubmitCommandRow

_INSTRUMENT = "00000000-0000-7000-8000-000000000a01"
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


def _order_row(
    row_id: int,
    exchange_order_id: str | None,
    instrument_exchanges: tuple[str, ...] = ("walutomat",),
    *,
    instrument_public_id: str = _INSTRUMENT,
) -> SpotOwnedOrderSourceRow:
    """Build one left-joined sentinel-tip Order source row."""
    return {
        "order_row_id": row_id,
        "order_public_id": f"00000000-0000-7000-8000-{row_id:012d}",
        "instrument_public_id": instrument_public_id,
        "exchange_order_id": exchange_order_id,
        "instrument_exchanges": instrument_exchanges,
    }


def _command(status: str, exchange_order_id: str | None) -> SpotOwnedSubmitCommandRow:
    """Build one in-scope submit command row."""
    return {
        "public_id": "00000000-0000-7000-8000-00000000c001",
        "status": status,
        "exchange_order_id": exchange_order_id,
    }


def _population(
    order_rows: list[SpotOwnedOrderSourceRow],
    commands: list[SpotOwnedSubmitCommandRow] | None = None,
) -> SpotOwnedOrderPopulation:
    """Classify rows whose Instruments all have one Walutomat version."""
    return build_owned_order_population(
        "walutomat",
        order_rows,
        {_INSTRUMENT: frozenset({"walutomat"})},
        [] if commands is None else commands,
    )


@pytest.mark.parametrize(
    "value, usable",
    [(None, False), ("", False), ("   ", False), ("O1", True), (" O1", True)],
)
def test_only_a_non_blank_string_is_a_venue_order_id(value: str | None, usable: bool) -> None:
    """A blank order id counts exactly like a missing one.

    Given: A verbatim order id value.
    When: Its usability is checked.
    Then: Only a non-blank string is a usable venue order id.

    Args:
        value: The verbatim order id.
        usable: Whether it is expected to be usable.
    """
    assert is_venue_order_id(value) is usable


def test_a_resolved_order_of_the_exchange_contributes_to_the_owned_set() -> None:
    """A resolved Walutomat Order's venue order id belongs to ``S``.

    Given: One sentinel-tip Order whose single sentinel-tip Instrument is
        Walutomat with one consistent version.
    When: The population is built for Walutomat.
    Then: Its venue order id is in ``S`` and no lineage fact is recorded.
    """
    population = _population([_order_row(1, "O1")])
    assert population["owned_order_ids"] == frozenset({"O1"})
    assert population["resolved_orders"] == (
        {"order_public_id": "00000000-0000-7000-8000-000000000001", "exchange_order_id": "O1"},
    )
    assert population["unresolved_lineage"] == ()
    assert population["blocked_order_ids"] == frozenset()


def test_a_resolved_order_of_another_exchange_is_excluded() -> None:
    """A resolved Kraken Order neither enters ``S`` nor blocks Walutomat.

    Given: One sentinel-tip Order resolved to a consistent Kraken Instrument.
    When: The population is built for Walutomat.
    Then: It is excluded from every part of the population.
    """
    population = build_owned_order_population(
        "walutomat",
        [_order_row(1, "K1", ("kraken",))],
        {_INSTRUMENT: frozenset({"kraken"})},
        [],
    )
    assert population["owned_order_ids"] == frozenset()
    assert population["resolved_orders"] == ()
    assert population["unresolved_lineage"] == ()
    assert population["other_exchange_unidentified_orders"] == ()
    assert own_submit_unresolved(population) is False


@pytest.mark.parametrize("exchange_order_id", [None, "", "  "])
def test_a_resolved_order_of_another_exchange_without_a_venue_order_id_refuses(
    exchange_order_id: str | None,
) -> None:
    """Every sentinel-tip Order without a venue order id refuses, whatever its exchange.

    Given: One sentinel-tip Order resolved to a consistent Kraken Instrument
        whose venue order id is missing or blank.
    When: The population is built for Walutomat and the gate runs.
    Then: The Order stays out of ``S`` and the resolved Orders, is recorded as
        an unidentified Order of another exchange, and the gate refuses.

    Args:
        exchange_order_id: The missing or blank venue order id.
    """
    population = build_owned_order_population(
        "walutomat",
        [_order_row(1, exchange_order_id, ("kraken",))],
        {_INSTRUMENT: frozenset({"kraken"})},
        [],
    )
    assert population["owned_order_ids"] == frozenset()
    assert population["resolved_orders"] == ()
    assert population["other_exchange_unidentified_orders"] == (
        "00000000-0000-7000-8000-000000000001",
    )
    assert own_submit_unresolved(population) is True


@pytest.mark.parametrize(
    "instrument_exchanges, versions, expected",
    [
        pytest.param((), None, "instrument_absent", id="absent"),
        pytest.param((), frozenset({"walutomat"}), "instrument_dangling", id="dangling"),
        pytest.param(
            ("walutomat", "walutomat"),
            frozenset({"walutomat"}),
            "instrument_multiply_active",
            id="multiply_active",
        ),
        pytest.param(
            ("walutomat",),
            frozenset({"walutomat", "kraken"}),
            "instrument_conflicting",
            id="versions_disagree",
        ),
        pytest.param(
            ("walutomat ",),
            frozenset({"walutomat "}),
            "instrument_conflicting",
            id="padded_exchange",
        ),
        pytest.param(("",), frozenset({""}), "instrument_conflicting", id="blank_exchange"),
    ],
)
def test_an_unresolved_instrument_projection_is_a_blocking_lineage_fact(
    instrument_exchanges: tuple[str, ...],
    versions: frozenset[str] | None,
    expected: SpotOwnedOrderLineageFactKind,
) -> None:
    """Missing, dangling, multiply active, or conflicting lineage blocks.

    Given: One sentinel-tip Order whose Instrument projection does not resolve.
    When: The population is built for Walutomat.
    Then: The named lineage fact is recorded, its venue order id is blocked and
        kept out of ``S``, and the gate refuses.

    Args:
        instrument_exchanges: The sentinel-tip Instrument exchanges the join matched.
        versions: The exchanges of every Instrument version, or ``None`` for none.
        expected: The expected lineage fact.
    """
    instrument_versions = {} if versions is None else {_INSTRUMENT: versions}
    population = build_owned_order_population(
        "walutomat",
        [_order_row(1, "O1", instrument_exchanges)],
        instrument_versions,
        [],
    )
    assert [fact["fact"] for fact in population["unresolved_lineage"]] == [expected]
    assert population["unresolved_lineage"][0]["exchange_order_id"] == "O1"
    assert population["blocked_order_ids"] == frozenset({"O1"})
    assert population["owned_order_ids"] == frozenset()
    assert own_submit_unresolved(population) is True


def test_blocked_ids_and_the_owned_set_skip_missing_and_blank_ids() -> None:
    """A missing or blank venue order id is never an owned or blocked id.

    Given: Resolved Orders with a missing and a blank id, and unresolved Orders
        with a missing and a blank id.
    When: The population is built.
    Then: Neither ``S`` nor the blocked ids contain them, while the resolved
        Orders and lineage facts still list every Order.
    """
    population = build_owned_order_population(
        "walutomat",
        [
            _order_row(1, None),
            _order_row(2, " "),
            _order_row(3, None, (), instrument_public_id="00000000-0000-7000-8000-000000000a09"),
            _order_row(4, "", (), instrument_public_id="00000000-0000-7000-8000-000000000a09"),
        ],
        {_INSTRUMENT: frozenset({"walutomat"})},
        [],
    )
    assert population["owned_order_ids"] == frozenset()
    assert population["blocked_order_ids"] == frozenset()
    assert len(population["resolved_orders"]) == 2
    assert len(population["unresolved_lineage"]) == 2


def test_an_empty_snapshot_leaves_nothing_unresolved() -> None:
    """A scope with no Orders and no commands is not unresolved.

    Given: An empty population.
    When: The gate runs.
    Then: It does not refuse.
    """
    assert own_submit_unresolved(_population([])) is False


@pytest.mark.parametrize("exchange_order_id", [None, "", "  "])
def test_a_resolved_order_without_a_venue_order_id_is_unresolved(
    exchange_order_id: str | None,
) -> None:
    """Every sentinel-tip Order without a usable venue order id is unresolved.

    Given: One resolved Walutomat Order whose venue order id is missing or blank.
    When: The gate runs.
    Then: It refuses.

    Args:
        exchange_order_id: The missing or blank venue order id.
    """
    assert own_submit_unresolved(_population([_order_row(1, exchange_order_id)])) is True


@pytest.mark.parametrize("status", _NEVER_SETTLED)
def test_a_live_command_status_is_never_settled(status: str) -> None:
    """A non-terminal command status is unresolved even with its id in ``S``.

    Given: A resolved Order ``O1`` in ``S`` and a command in a live status that
        names ``O1``.
    When: The gate runs.
    Then: It refuses.

    Args:
        status: A status the closed table never settles.
    """
    population = _population([_order_row(1, "O1")], [_command(status, "O1")])
    assert own_submit_unresolved(population) is True


@pytest.mark.parametrize("status", _SETTLED_BY_VENUE_ID)
def test_a_terminal_command_with_its_id_in_s_is_settled(status: str) -> None:
    """A terminal command whose id one resolved Order carries is settled.

    Given: A resolved Order ``O1`` in ``S`` and a terminal command naming ``O1``.
    When: The gate runs.
    Then: It does not refuse.

    Args:
        status: A status the venue id may settle.
    """
    population = _population([_order_row(1, "O1")], [_command(status, "O1")])
    assert own_submit_unresolved(population) is False


@pytest.mark.parametrize("exchange_order_id", [None, "", "O2"])
@pytest.mark.parametrize("status", _SETTLED_BY_VENUE_ID)
def test_a_terminal_command_without_its_id_in_s_is_unresolved(
    status: str, exchange_order_id: str | None
) -> None:
    """A terminal command with a missing, blank, or foreign id is unresolved.

    Given: A resolved Order ``O1`` in ``S`` and a terminal command whose venue
        order id is missing, blank, or outside ``S`` (no pre-venue proof exists).
    When: The gate runs.
    Then: It refuses.

    Args:
        status: A terminal status.
        exchange_order_id: A venue order id that cannot settle the command.
    """
    population = _population([_order_row(1, "O1")], [_command(status, exchange_order_id)])
    assert own_submit_unresolved(population) is True


def test_a_status_outside_the_enum_is_never_settled() -> None:
    """An unknown command status fails closed.

    Given: A resolved Order ``O1`` in ``S`` and a command whose status is not a
        member of the closed enum.
    When: The gate runs.
    Then: It refuses.
    """
    population = _population([_order_row(1, "O1")], [_command("settled_elsewhere", "O1")])
    assert own_submit_unresolved(population) is True


def test_a_venue_id_carried_by_two_resolved_orders_does_not_settle() -> None:
    """A terminal command whose id two resolved Orders carry has no exact lineage.

    Given: Two resolved Walutomat Orders carrying ``O1`` and a filled command
        naming ``O1``.
    When: The gate runs.
    Then: It refuses.
    """
    population = _population(
        [_order_row(1, "O1"), _order_row(2, "O1")],
        [_command(TradeCommandStatusEnum.FILLED.value, "O1")],
    )
    assert own_submit_unresolved(population) is True


def _witness_row(scope_sequence: int, row_id: int) -> SpotExecutionWitnessRow:
    """Build one sealed-prefix execution linked to the Order of ``row_id``."""
    return {
        "scope_sequence": scope_sequence,
        "exec_id": f"wal-{scope_sequence}",
        "order_public_id": f"00000000-0000-7000-8000-{row_id:012d}",
    }


def test_authoritative_ids_resolve_only_unique_resolved_orders() -> None:
    """Each execution resolves through exactly one resolved Order of the exchange.

    Given: Executions linked to a resolved Order, an Order without a venue id, a
        duplicated Order public id, and an Order absent from the population.
    When: The authoritative venue order ids are resolved.
    Then: Only the uniquely resolved Order yields its id; the rest yield ``None``.
    """
    population = _population(
        [
            _order_row(1, "O1"),
            _order_row(2, None),
            _order_row(3, "O3"),
            {**_order_row(4, "O4"), "order_public_id": _order_row(3, None)["order_public_id"]},
        ]
    )
    resolved = authoritative_execution_order_ids(
        [_witness_row(1, 1), _witness_row(2, 2), _witness_row(3, 3), _witness_row(4, 9)],
        population,
    )
    assert resolved == {1: "O1", 2: None, 3: None, 4: None}
