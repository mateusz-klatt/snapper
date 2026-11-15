"""Tests for the pure owned-order population and unresolved-submit gate."""

from collections.abc import Callable
from datetime import UTC
from datetime import datetime

import pytest

from snapper.application.portfolio.never_submitted_commands import NEVER_SUBMITTED_DOCUMENT_SCHEMA
from snapper.application.portfolio.never_submitted_commands import never_submitted_digest
from snapper.application.portfolio.spot_anchor_ownership import authoritative_execution_order_ids
from snapper.application.portfolio.spot_anchor_ownership import build_owned_order_population
from snapper.application.portfolio.spot_anchor_ownership import is_venue_order_id
from snapper.application.portfolio.spot_anchor_ownership import never_submitted_document
from snapper.application.portfolio.spot_anchor_ownership import own_submit_unresolved
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository_types import SpotCommandVersionRow
from snapper.data.repository_types import SpotExecutionWitnessRow
from snapper.data.repository_types import SpotNeverSubmittedAudit
from snapper.data.repository_types import SpotNeverSubmittedCandidate
from snapper.data.repository_types import SpotNeverSubmittedEventRow
from snapper.data.repository_types import SpotOwnedOrderLineageFactKind
from snapper.data.repository_types import SpotOwnedOrderPopulation
from snapper.data.repository_types import SpotOwnedOrderSourceRow
from snapper.data.repository_types import SpotOwnedSubmitCommandRow
from snapper.data.repository_types import SpotSubmitCommandRead

_INSTRUMENT = "00000000-0000-7000-8000-000000000a01"
_GOLDEN_DIGEST = "06dfc06879b979ec664b37f87317fa974cb0673415dd74ab066c99428c03357e"
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
    """Build one in-scope submit command row that is not a never-submitted candidate."""
    return {
        "public_id": "00000000-0000-7000-8000-00000000c001",
        "status": status,
        "exchange_order_id": exchange_order_id,
        "row_id": 1,
        "command_type": "create",
        "client_order_id": "00000000-0000-7000-8000-00000000d001",
        "venue_client_id": "00000000-0000-7000-8000-00000000d001",
        "wallet_public_id": "00000000-0000-7000-8000-00000000e001",
        "mode": "live",
        "exchange": "walutomat",
        "acked_at": None,
        "candidate": None,
    }


def _read(
    commands: list[SpotOwnedSubmitCommandRow] | None = None,
    retired_public_ids: tuple[str, ...] = (),
) -> SpotSubmitCommandRead:
    """Bundle in-scope commands and retired public ids into one command read."""
    return {
        "commands": () if commands is None else tuple(commands),
        "retired_public_ids": retired_public_ids,
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
        _read(commands),
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
        _read(),
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
        _read(),
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
        _read(),
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
        _read(),
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


_NS_PUBLIC_ID = "00000000-0000-7000-8000-00000000c101"
_NS_CLIENT_ID = "00000000-0000-7000-8000-00000000d101"
_NS_WALLET = "00000000-0000-7000-8000-00000000e001"
_ACKED_AT = datetime(2026, 7, 19, 15, 24, 31, tzinfo=UTC)


def _version(
    row_id: int,
    status: str,
    *,
    open_ended: bool = False,
    exchange_order_id: str | None = None,
    acked_at: datetime | None = None,
) -> SpotCommandVersionRow:
    """Build one version of a never-submitted candidate command."""
    return {
        "row_id": row_id,
        "status": status,
        "exchange_order_id": exchange_order_id,
        "acked_at": acked_at,
        "open_ended": open_ended,
    }


def _event(
    event_id: int, event_type: str, *, exchange_order_id: str | None = None
) -> SpotNeverSubmittedEventRow:
    """Build one venue event linked to a never-submitted candidate command."""
    return {
        "id": event_id,
        "event_type": event_type,
        "command_public_id": None,
        "client_order_id": _NS_CLIENT_ID,
        "exchange_order_id": exchange_order_id,
        "status": "failed",
        "error": "live trading interlocked",
        "received_at": datetime(2026, 7, 19, 15, 24, 30, tzinfo=UTC),
    }


def _audit() -> SpotNeverSubmittedAudit:
    """Build the unhashed review context of a candidate."""
    return {
        "created_at": datetime(2026, 7, 19, 15, 24, 29, tzinfo=UTC),
        "dispatched_at": datetime(2026, 7, 19, 15, 24, 30, tzinfo=UTC),
        "terminal_at": datetime(2026, 7, 19, 15, 24, 30, tzinfo=UTC),
        "attempt_count": 1,
        "last_error": "live trading interlocked",
        "instrument": "EUR-PLN",
        "side": "buy",
        "order_type": "limit",
        "quantity": 10.0,
        "price": 4.25,
    }


def _never_submitted(
    status: str = TradeCommandStatusEnum.FAILED.value,
    event_type: str = "order_interlock_blocked",
) -> SpotOwnedSubmitCommandRow:
    """Build a qualifying never-submitted candidate: created, dispatched, then terminal."""
    candidate: SpotNeverSubmittedCandidate = {
        "versions": (
            _version(10, TradeCommandStatusEnum.CREATED.value),
            _version(11, TradeCommandStatusEnum.DISPATCHED.value),
            _version(12, status, open_ended=True),
        ),
        "venue_events": (_event(7, event_type),),
        "order_row_count": 0,
        "audit": _audit(),
    }
    return {
        "public_id": _NS_PUBLIC_ID,
        "status": status,
        "exchange_order_id": None,
        "row_id": 12,
        "command_type": "create",
        "client_order_id": _NS_CLIENT_ID,
        "venue_client_id": _NS_CLIENT_ID,
        "wallet_public_id": _NS_WALLET,
        "mode": "live",
        "exchange": "walutomat",
        "acked_at": None,
        "candidate": candidate,
    }


def _with_candidate(
    command: SpotOwnedSubmitCommandRow, candidate: SpotNeverSubmittedCandidate
) -> SpotOwnedSubmitCommandRow:
    """Return the command with its candidate facts replaced."""
    return {**command, "candidate": candidate}


def _candidate(command: SpotOwnedSubmitCommandRow) -> SpotNeverSubmittedCandidate:
    """Return a command's candidate facts, which the fixtures always set."""
    candidate = command["candidate"]
    assert candidate is not None
    return candidate


def _replace_versions(
    command: SpotOwnedSubmitCommandRow, versions: tuple[SpotCommandVersionRow, ...]
) -> SpotOwnedSubmitCommandRow:
    """Return the command with its version history replaced."""
    return _with_candidate(command, {**_candidate(command), "versions": versions})


def _replace_events(
    command: SpotOwnedSubmitCommandRow, events: tuple[SpotNeverSubmittedEventRow, ...]
) -> SpotOwnedSubmitCommandRow:
    """Return the command with its linked venue events replaced."""
    return _with_candidate(command, {**_candidate(command), "venue_events": events})


def _pinned(command: SpotOwnedSubmitCommandRow) -> frozenset[str]:
    """Return a pinned set holding exactly the command's document digest."""
    document = never_submitted_document(command)
    assert document is not None
    return frozenset({never_submitted_digest(document)})


@pytest.mark.parametrize(
    "status, event_type",
    [
        (TradeCommandStatusEnum.FAILED.value, "order_interlock_blocked"),
        (TradeCommandStatusEnum.FAILED.value, "order_breaker_open"),
        (TradeCommandStatusEnum.REJECTED.value, "order_rejected"),
    ],
)
def test_each_proof_class_yields_a_never_submitted_document(status: str, event_type: str) -> None:
    """A pre-venue failure and a venue refusal each render a document.

    Given: A qualifying candidate in each allowed status and event pairing.
    When: Its never-submitted document is rendered.
    Then: It carries exactly the bound keys and no datetime.

    Args:
        status: The candidate's terminal status.
        event_type: Its one linked venue event.
    """
    document = never_submitted_document(_never_submitted(status, event_type))
    assert document == {
        "schema": NEVER_SUBMITTED_DOCUMENT_SCHEMA,
        "command_public_id": _NS_PUBLIC_ID,
        "command_type": "create",
        "client_order_id": _NS_CLIENT_ID,
        "venue_client_id": _NS_CLIENT_ID,
        "exchange": "walutomat",
        "wallet_public_id": _NS_WALLET,
        "mode": "live",
        "status": status,
        "versions": [
            {"row_id": 10, "status": "created"},
            {"row_id": 11, "status": "dispatched"},
            {"row_id": 12, "status": status},
        ],
        "venue_events": [
            {
                "id": 7,
                "event_type": event_type,
                "command_public_id": None,
                "client_order_id": _NS_CLIENT_ID,
                "exchange_order_id": None,
                "status": "failed",
                "error": "live trading interlocked",
            }
        ],
        "order_row_count": 0,
    }


def test_the_document_digest_is_pinned_to_its_canonical_form() -> None:
    """The canonical form of a document cannot drift without a test failing.

    Given: The interlock-failed fixture.
    When: Its document digest is computed.
    Then: It equals the golden digest.
    """
    document = never_submitted_document(_never_submitted())
    assert document is not None
    assert never_submitted_digest(document) == _GOLDEN_DIGEST


def _set_status(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Move the command to ``expired``, which no exclusion accepts."""
    return {**command, "status": TradeCommandStatusEnum.EXPIRED.value}


def _set_venue_id(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Give the command a venue order id."""
    return {**command, "exchange_order_id": "O9"}


def _set_acked(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Give the command an acknowledgement time."""
    return {**command, "acked_at": _ACKED_AT}


def _blank_client_id(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Blank the command's client order id and venue client id alike."""
    return {**command, "client_order_id": " ", "venue_client_id": " "}


def _other_venue_client_id(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Give the command a venue client id different from its client order id."""
    return {**command, "venue_client_id": "00000000-0000-7000-8000-00000000d999"}


def _no_candidate(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Drop the command's candidate facts."""
    return {**command, "candidate": None}


def _order_row_exists(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Record an Order carrying the command's client order id."""
    return _with_candidate(command, {**_candidate(command), "order_row_count": 1})


def _no_versions(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Drop every version of the command."""
    return _replace_versions(command, ())


def _version_with_venue_id(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Give an earlier version a venue order id."""
    versions = _candidate(command)["versions"]
    return _replace_versions(
        command, (versions[0], _version(11, "dispatched", exchange_order_id="O9"), versions[2])
    )


def _version_acked(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Give an earlier version an acknowledgement time."""
    versions = _candidate(command)["versions"]
    return _replace_versions(
        command, (versions[0], _version(11, "dispatched", acked_at=_ACKED_AT), versions[2])
    )


def _versions_out_of_order(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """List the versions out of row-id order."""
    versions = _candidate(command)["versions"]
    return _replace_versions(command, (versions[1], versions[0], versions[2]))


def _versions_duplicated(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """List one version twice."""
    versions = _candidate(command)["versions"]
    return _replace_versions(command, (versions[0], versions[0], versions[2]))


def _tip_closed(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Close the last version."""
    versions = _candidate(command)["versions"]
    return _replace_versions(command, (versions[0], versions[1], _version(12, command["status"])))


def _two_open_versions(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Leave an earlier version open-ended as well."""
    versions = _candidate(command)["versions"]
    return _replace_versions(
        command, (versions[0], _version(11, "dispatched", open_ended=True), versions[2])
    )


def _tip_of_another_row(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Make the last version a row other than the command's own."""
    return {**command, "row_id": 13}


def _tip_status_differs(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Give the last version a status other than the command's."""
    versions = _candidate(command)["versions"]
    return _replace_versions(
        command, (versions[0], versions[1], _version(12, "rejected", open_ended=True))
    )


def _acknowledged_earlier(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Put an earlier version in a status reached only after acknowledgement."""
    versions = _candidate(command)["versions"]
    return _replace_versions(command, (versions[0], _version(11, "accepted"), versions[2]))


def _no_events(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Drop every linked venue event."""
    return _replace_events(command, ())


def _two_events(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Link a second venue event."""
    events = _candidate(command)["venue_events"]
    return _replace_events(command, (events[0], _event(8, "order_interlock_blocked")))


def _event_with_venue_id(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Give the one linked event a venue order id."""
    return _replace_events(command, (_event(7, "order_interlock_blocked", exchange_order_id="O9"),))


def _wrong_event_for_failed(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Pair a ``failed`` command with a generic rejection."""
    return _replace_events(command, (_event(7, "order_rejected"),))


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(_set_status, id="status_expired"),
        pytest.param(_set_venue_id, id="command_venue_id"),
        pytest.param(_set_acked, id="command_acked"),
        pytest.param(_blank_client_id, id="blank_client_id"),
        pytest.param(_other_venue_client_id, id="venue_client_id_differs"),
        pytest.param(_no_candidate, id="no_candidate"),
        pytest.param(_order_row_exists, id="order_row"),
        pytest.param(_no_versions, id="no_versions"),
        pytest.param(_version_with_venue_id, id="version_venue_id"),
        pytest.param(_version_acked, id="version_acked"),
        pytest.param(_versions_out_of_order, id="versions_out_of_order"),
        pytest.param(_versions_duplicated, id="versions_duplicated"),
        pytest.param(_tip_closed, id="tip_closed"),
        pytest.param(_two_open_versions, id="two_open_versions"),
        pytest.param(_tip_of_another_row, id="tip_of_another_row"),
        pytest.param(_tip_status_differs, id="tip_status_differs"),
        pytest.param(_acknowledged_earlier, id="earlier_version_post_ack"),
        pytest.param(_no_events, id="no_events"),
        pytest.param(_two_events, id="two_events"),
        pytest.param(_event_with_venue_id, id="event_venue_id"),
        pytest.param(_wrong_event_for_failed, id="failed_with_generic_rejection"),
    ],
)
def test_a_candidate_breaking_any_condition_yields_no_document(
    mutate: Callable[[SpotOwnedSubmitCommandRow], SpotOwnedSubmitCommandRow],
) -> None:
    """Every single violation of the never-submitted proof leaves no document.

    Given: The interlock-failed fixture changed in exactly one fact.
    When: Its never-submitted document is rendered.
    Then: There is none, so it can never be excluded.

    Args:
        mutate: The single change.
    """
    assert never_submitted_document(mutate(_never_submitted())) is None


def test_a_rejected_command_with_a_pre_venue_event_yields_no_document() -> None:
    """A ``rejected`` command carrying the interlock's event does not qualify.

    Given: A ``rejected`` candidate whose one event is ``order_interlock_blocked``.
    When: Its never-submitted document is rendered.
    Then: There is none.
    """
    command = _never_submitted(TradeCommandStatusEnum.REJECTED.value, "order_interlock_blocked")
    assert never_submitted_document(command) is None


def test_a_pinned_command_is_excluded_and_the_gate_passes() -> None:
    """A reviewed exclusion settles a command that the status table cannot.

    Given: A resolved Order ``O1``, a filled command naming it, and the
        interlock-failed candidate whose digest is pinned.
    When: The population is built and the gate runs.
    Then: The candidate's ids are listed as excluded and the gate passes.
    """
    candidate = _never_submitted()
    population = build_owned_order_population(
        "walutomat",
        [_order_row(1, "O1")],
        {_INSTRUMENT: frozenset({"walutomat"})},
        _read([_command(TradeCommandStatusEnum.FILLED.value, "O1"), candidate]),
        _pinned(candidate),
    )
    assert population["excluded_command_public_ids"] == frozenset({_NS_PUBLIC_ID})
    assert population["excluded_client_order_ids"] == frozenset({_NS_CLIENT_ID})
    assert own_submit_unresolved(population) is False


def test_an_unpinned_candidate_stays_unresolved() -> None:
    """Without a pinned digest a never-submitted candidate still refuses.

    Given: The interlock-failed candidate and the default, empty pinned set.
    When: The population is built and the gate runs.
    Then: Nothing is excluded and the gate refuses.
    """
    population = _population([], [_never_submitted()])
    assert population["excluded_command_public_ids"] == frozenset()
    assert population["excluded_client_order_ids"] == frozenset()
    assert own_submit_unresolved(population) is True


def _new_version(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Append a later version, as any write to the command would."""
    versions = _candidate(command)["versions"]
    tip = _version(13, command["status"], open_ended=True)
    return {
        **_replace_versions(command, (*versions[:2], _version(12, command["status"]), tip)),
        "row_id": 13,
    }


def _new_event(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Link a later venue event, as a late venue answer would."""
    return _replace_events(command, (_event(9, "order_interlock_blocked"),))


def _other_wallet(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Move the command to another wallet."""
    return {**command, "wallet_public_id": "00000000-0000-7000-8000-00000000e999"}


def _other_mode(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Move the command to the paper mode."""
    return {**command, "mode": "paper"}


def _other_exchange(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Move the command to another exchange."""
    return {**command, "exchange": "kraken"}


def _other_error(command: SpotOwnedSubmitCommandRow) -> SpotOwnedSubmitCommandRow:
    """Change the recorded error of the one linked event."""
    event = _candidate(command)["venue_events"][0]
    return _replace_events(command, ({**event, "error": "something else"},))


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(_new_version, id="new_version"),
        pytest.param(_new_event, id="new_event"),
        pytest.param(_other_wallet, id="wallet"),
        pytest.param(_other_mode, id="mode"),
        pytest.param(_other_exchange, id="exchange"),
        pytest.param(_other_error, id="event_error"),
        pytest.param(_order_row_exists, id="order_row"),
        pytest.param(_set_acked, id="acked"),
    ],
)
def test_any_change_after_review_voids_the_exclusion(
    mutate: Callable[[SpotOwnedSubmitCommandRow], SpotOwnedSubmitCommandRow],
) -> None:
    """A pinned digest no longer matches once any bound fact changes.

    Given: The digest of the interlock-failed candidate pinned, and the
        candidate then changed in one fact.
    When: The population is built and the gate runs.
    Then: Nothing is excluded and the gate refuses.

    Args:
        mutate: The single change after review.
    """
    pinned = _pinned(_never_submitted())
    population = build_owned_order_population(
        "walutomat",
        [],
        {},
        _read([mutate(_never_submitted())]),
        pinned,
    )
    assert population["excluded_command_public_ids"] == frozenset()
    assert own_submit_unresolved(population) is True


def test_a_retired_command_refuses_even_when_everything_else_settles() -> None:
    """An in-scope command without an open-ended version in scope refuses.

    Given: A resolved Order ``O1`` with a filled command naming it, and one
        retired public id.
    When: The population is built and the gate runs.
    Then: The retired id is carried through and the gate refuses.
    """
    population = build_owned_order_population(
        "walutomat",
        [_order_row(1, "O1")],
        {_INSTRUMENT: frozenset({"walutomat"})},
        _read(
            [_command(TradeCommandStatusEnum.FILLED.value, "O1")],
            retired_public_ids=("00000000-0000-7000-8000-00000000c777",),
        ),
    )
    assert population["retired_command_public_ids"] == ("00000000-0000-7000-8000-00000000c777",)
    assert own_submit_unresolved(population) is True
