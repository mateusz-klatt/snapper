"""Tests for the venue account-truth SCD2 storage slice (PnL Phase 3).

Pins the fail-closed contract of the REWORKED venue account-truth DAL, where
the repository — never the caller — owns coherence and provenance:

* :meth:`SQLAlchemyRepository._venue_account_rollup_status` DERIVES the
  coherent roll-up from the two component statuses: ``observed`` only when the
  balance is observed AND positions are observed or structurally absent,
  ``simulated`` from a simulated balance, ``unsupported`` from a wholly
  unsupported venue, and ``error`` for every combination that would otherwise
  over-claim truth a component did not provide.
* :meth:`SQLAlchemyRepository.record_venue_account_snapshot` takes ONE attempt
  row, appends the append-only observation and SCD2-materializes the state in
  ONE transaction. Each component resolves THREE ways: a FRESH read (balance
  observed/simulated, positions observed) writes the attempt's payload sourced
  to the new observation; a TRANSIENT ``error`` WITH a predecessor RETAINS the
  last good payload, its clock, and its true source id from the LOCKED
  predecessor so a blip never blanks a still-displayable last-known value; and
  everything else — structural absence (``not_applicable``/``unsupported``) or
  an ``error`` with no predecessor — CLEARS the component to NULL. Clearing on
  structural absence is critical: an ``observed`` balance with
  ``not_applicable`` positions rolls up to authoritative ``observed``, so a
  lingering stale position payload there would be served as fiction. Balance and
  positions are INDEPENDENT reads, so provenance is tracked PER COMPONENT
  (``balance_payload_source_observation_id`` /
  ``position_payload_source_observation_id``), while (NOT NULL)
  ``current_attempt_observation_id`` always points at the latest attempt.
* A lagging bus clock is clamped to ``max(bus_time, existing.timestamp)`` so a
  successor never travels back in time, and a lost first-insert unique race is
  retried exactly once against the real partial-unique index.
* :meth:`SQLAlchemyRepository.get_venue_account_states` scopes an empty wallet
  list to nothing and a ``None`` filter to every wallet (admin-unscoped),
  returns only sentinel-active rows ordered by ``(exchange, mode)``, excludes
  closed rows, and is wallet-scoped.
* The model DB CHECK constraints reject an ``observed`` roll-up whose balance,
  positions, or authority window do not back it, an observed/simulated balance
  or observed positions with a NULL JSON payload (or a positions read missing
  its timestamp), a payload JSON divorced from its source observation id (the
  two must be both-null or both-non-null), a freshly observed/simulated balance
  or freshly observed positions whose payload source is not THIS attempt, a
  simulated status on a live row, invalid status vocabularies, a timestamp-less
  observed balance, a non-``native_only`` valuation, an uppercase exchange, and
  a mode outside ``live``/``paper`` — on both the state and the observation
  plane.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import VenueAccountObservation
from snapper.data.models import VenueAccountState
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import VenueAccountAttemptRow

_WALLET = "00000000-0000-7000-8000-000000000001"
_OTHER_WALLET = "00000000-0000-7000-8000-000000000002"
_SESSION = "00000000-0000-7000-8000-0000000000aa"
_T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)

_UNSET = object()


def _attempt_row(
    bus_time: datetime,
    *,
    wallet_public_id: str = _WALLET,
    exchange: str = "kraken",
    mode: str = "live",
    balance_status: str = "observed",
    position_status: str = "observed",
    valuation_status: str = "native_only",
    balances_json: str | None = '{"USD": 100.0}',
    open_positions_json: str | None = "[]",
    balance_observed_at: datetime | None = None,
    position_observed_at: datetime | None = None,
    authoritative_until: datetime | None | object = _UNSET,
    error: str | None = None,
    sequence_id: int = 1,
) -> VenueAccountAttemptRow:
    """Build one raw account-observation attempt row with sane defaults.

    The repository derives the roll-up status, the payload-source id, and any
    retained payload; the caller supplies only the raw outcome of a single
    poll attempt, so this fixture carries NO ``sync_status``,
    ``attempt_status``, or observation ids — drift against the TypedDict fails
    static checking.

    Args:
        bus_time: Attempt timestamp and SCD2 effective time.
        wallet_public_id: Full wallet identity (UUID).
        exchange: Lowercase venue name.
        mode: Trading mode (live/paper).
        balance_status: Per-component balance status.
        position_status: Per-component position status.
        valuation_status: Phase-3 valuation status (``native_only``).
        balances_json: Fresh native balances blob or honest NULL.
        open_positions_json: Fresh native positions blob or honest NULL.
        balance_observed_at: Balance observation clock; defaults to
            ``bus_time`` (ignored by the repo unless the balance is fresh).
        position_observed_at: Position observation clock; defaults to
            ``bus_time`` (ignored unless the position is fresh).
        authoritative_until: Fresh balance authority window; defaults to
            ``bus_time + 5m`` so a freshly observed balance satisfies the
            observed-authority CHECK. Pass ``None`` to omit it.
        error: Failure detail or honest NULL.
        sequence_id: Monotonic per-attempt sequence.

    Returns:
        Complete TYPED attempt row accepted by
        ``record_venue_account_snapshot``.
    """
    resolved_balance_at = bus_time if balance_observed_at is None else balance_observed_at
    resolved_position_at = bus_time if position_observed_at is None else position_observed_at
    resolved_authoritative = (
        bus_time + timedelta(minutes=5) if authoritative_until is _UNSET else authoritative_until
    )
    assert resolved_authoritative is None or isinstance(resolved_authoritative, datetime)
    return {
        "wallet_public_id": wallet_public_id,
        "exchange": exchange,
        "mode": mode,
        "balance_status": balance_status,
        "position_status": position_status,
        "valuation_status": valuation_status,
        "balances_json": balances_json,
        "open_positions_json": open_positions_json,
        "balance_observed_at": resolved_balance_at,
        "position_observed_at": resolved_position_at,
        "authoritative_until": resolved_authoritative,
        "error": error,
        "session_id": _SESSION,
        "sequence_id": sequence_id,
        "bus_time": bus_time,
    }


async def _make_repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Create a throwaway on-disk SQLite repository with the full schema.

    An on-disk file (not ``:memory:``) so a second repository handle can act
    as a concurrent rival against the same database in the retry test.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        Repository bound to a fresh database.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'venue.db'}")
    await repo.create_all()
    return repo


async def _all_states(repo: SQLAlchemyRepository) -> list[VenueAccountState]:
    """Return every account-state row ordered by id.

    Args:
        repo: Repository under test.

    Returns:
        All state rows, historical and active.
    """
    async with repo.session() as s:
        result = await s.execute(select(VenueAccountState).order_by(VenueAccountState.id))
        return list(result.scalars().all())


async def _all_observations(repo: SQLAlchemyRepository) -> list[VenueAccountObservation]:
    """Return every observation row ordered by id.

    Args:
        repo: Repository under test.

    Returns:
        All append-only observation rows.
    """
    async with repo.session() as s:
        result = await s.execute(
            select(VenueAccountObservation).order_by(VenueAccountObservation.id)
        )
        return list(result.scalars().all())


async def _active_state(repo: SQLAlchemyRepository) -> VenueAccountState:
    """Return the single sentinel-active state row, asserting there is one.

    Args:
        repo: Repository under test.

    Returns:
        The lone active state row.
    """
    active = [r for r in await _all_states(repo) if r.known_to == KNOWN_TO_MAX]
    assert len(active) == 1
    return active[0]


@pytest.mark.parametrize(
    ("balance_status", "position_status", "expected"),
    [
        ("observed", "observed", "observed"),
        ("observed", "not_applicable", "observed"),
        ("observed", "error", "error"),
        ("observed", "unsupported", "error"),
        ("simulated", "not_applicable", "simulated"),
        ("simulated", "observed", "simulated"),
        ("unsupported", "unsupported", "unsupported"),
        ("unsupported", "not_applicable", "unsupported"),
        ("unsupported", "observed", "error"),
        ("error", "observed", "error"),
    ],
)
def test_rollup_status_covers_every_branch(
    balance_status: str, position_status: str, expected: str
) -> None:
    """The derived roll-up never over-claims truth a component withheld.

    Given: each meaningful (balance, position) component pairing,
    When: the fail-closed roll-up is derived,
    Then: ``observed`` requires an observed balance AND observed-or-absent
        positions; ``simulated`` follows the balance; ``unsupported`` requires
        both unsupported-or-absent; and every other pairing — a balance
        observed while positions errored, an unsupported balance with observed
        positions, an errored balance — collapses to ``error``.
    """
    assert (
        SQLAlchemyRepository._venue_account_rollup_status(balance_status, position_status)
        == expected
    )


async def test_first_snapshot_observed_futures_inserts_observation_and_state(
    tmp_path: Path,
) -> None:
    """A fresh observed balance and observed positions materialize one state.

    Given: an empty database and a futures attempt that observed both the
        balance and open positions,
    When: record_venue_account_snapshot runs once,
    Then: exactly one observation and one active state exist; the derived
        sync_status is ``observed``; current_attempt_observation_id and BOTH
        per-component payload-source ids equal the new observation id (balance
        and positions were both freshly observed); the authority window and
        fresh balances survive.
    """
    repo = await _make_repo(tmp_path)
    authoritative = _T0 + timedelta(minutes=5)
    state_id = await repo.record_venue_account_snapshot(
        _attempt_row(
            _T0,
            exchange="kraken_futures",
            balances_json='{"USD": 100.0}',
            open_positions_json='[{"symbol": "PF_XBTUSD"}]',
            authoritative_until=authoritative,
        )
    )
    assert state_id > 0
    observations = await _all_observations(repo)
    assert len(observations) == 1
    obs_id = observations[0].id
    state = await _active_state(repo)
    assert state.timestamp == _T0
    assert state.public_id
    assert state.sync_status == "observed"
    assert state.current_attempt_observation_id == obs_id
    assert state.balance_payload_source_observation_id == obs_id
    assert state.position_payload_source_observation_id == obs_id
    assert state.authoritative_until == authoritative
    assert state.balances_json == '{"USD": 100.0}'
    assert state.open_positions_json == '[{"symbol": "PF_XBTUSD"}]'


async def test_first_snapshot_observed_spot_uses_not_applicable_positions(
    tmp_path: Path,
) -> None:
    """A spot balance observed with structurally-absent positions is observed.

    Given: an empty database and a spot attempt that observed the balance
        while positions are ``not_applicable``,
    When: record_venue_account_snapshot runs,
    Then: the derived sync_status is ``observed`` and open_positions_json is
        NULL — a not_applicable position is never invented into a payload.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_venue_account_snapshot(
        _attempt_row(
            _T0,
            position_status="not_applicable",
            open_positions_json=None,
        )
    )
    state = await _active_state(repo)
    assert state.sync_status == "observed"
    assert state.position_status == "not_applicable"
    assert state.open_positions_json is None
    assert state.position_observed_at is None
    assert state.position_payload_source_observation_id is None
    assert state.balances_json == '{"USD": 100.0}'


async def test_first_snapshot_simulated_paper_records_fiction(tmp_path: Path) -> None:
    """A simulated paper balance materializes a simulated state with a payload.

    Given: an empty database and a paper attempt whose balance is
        ``simulated`` (fiction, never reconciled) with not_applicable
        positions,
    When: record_venue_account_snapshot runs,
    Then: the derived sync_status is ``simulated``, the simulated balances and
        their observation clock are stored (simulated counts as fresh), and
        the payload provenance points at the new observation.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_venue_account_snapshot(
        _attempt_row(
            _T0,
            mode="paper",
            balance_status="simulated",
            position_status="not_applicable",
            balances_json='{"USD": 1000.0}',
            open_positions_json=None,
        )
    )
    obs_id = (await _all_observations(repo))[0].id
    state = await _active_state(repo)
    assert state.sync_status == "simulated"
    assert state.balances_json == '{"USD": 1000.0}'
    assert state.balance_observed_at == _T0
    assert state.balance_payload_source_observation_id == obs_id


async def test_second_snapshot_closes_predecessor_and_keeps_public_id(
    tmp_path: Path,
) -> None:
    """A later snapshot SCD2-transitions the same identity in place.

    Given: an existing active observed state for an identity,
    When: a later observed snapshot arrives for the SAME (wallet, exchange,
        mode),
    Then: the predecessor closes at the new bus_time, exactly one active row
        remains, the successor reuses the predecessor's public_id and carries
        the new observation id, and two observations are appended.
    """
    repo = await _make_repo(tmp_path)
    t1 = _T0
    t2 = _T0 + timedelta(seconds=30)
    await repo.record_venue_account_snapshot(_attempt_row(t1))
    await repo.record_venue_account_snapshot(_attempt_row(t2, balances_json='{"USD": 250.0}'))
    states = await _all_states(repo)
    observations = await _all_observations(repo)
    assert len(observations) == 2
    assert len(states) == 2
    old, new = states
    assert old.known_to == t2
    assert new.known_to == KNOWN_TO_MAX
    assert new.public_id == old.public_id
    assert new.timestamp == t2
    assert new.balances_json == '{"USD": 250.0}'
    assert new.current_attempt_observation_id == observations[1].id


async def test_error_attempt_retains_predecessor_balance_and_provenance(
    tmp_path: Path,
) -> None:
    """A retained balance keeps its TRUE source id and never reads as fresh.

    Given: a first observed snapshot (observation #1) whose balances become
        the good payload source,
    When: a follow-up ERROR attempt arrives — balance and positions errored,
        no fresh JSON — for the SAME identity,
    Then: the successor's sync_status is ``error``;
        current_attempt_observation_id points at the NEW (error) observation;
        balance_payload_source_observation_id stays observation #1 and DIFFERS
        from the current attempt; and the retained balances, balance_observed_at,
        and authoritative_until are observation #1's values, never NULL and never
        the error attempt's — a retained payload never masquerades as fresh.
    """
    repo = await _make_repo(tmp_path)
    t1 = _T0
    t2 = _T0 + timedelta(seconds=45)
    good_authoritative = t1 + timedelta(minutes=5)
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t1,
            balances_json='{"USD": 100.0}',
            balance_observed_at=t1,
            authoritative_until=good_authoritative,
        )
    )
    obs1_id = (await _all_observations(repo))[0].id
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t2,
            balance_status="error",
            position_status="error",
            balances_json=None,
            open_positions_json=None,
            authoritative_until=None,
            error="venue timeout",
        )
    )
    observations = await _all_observations(repo)
    assert len(observations) == 2
    new_obs_id = observations[1].id
    state = await _active_state(repo)
    assert state.sync_status == "error"
    assert state.current_attempt_observation_id == new_obs_id
    assert state.balance_payload_source_observation_id == obs1_id
    assert state.balance_payload_source_observation_id != state.current_attempt_observation_id
    assert state.balances_json == '{"USD": 100.0}'
    assert state.balance_observed_at == t1
    assert state.authoritative_until == good_authoritative


async def test_error_attempt_without_predecessor_stores_honest_nulls(
    tmp_path: Path,
) -> None:
    """A first-ever error attempt has nothing to retain and blanks the payload.

    Given: an empty database and an ERROR attempt (no predecessor to retain
        from),
    When: record_venue_account_snapshot runs,
    Then: sync_status is ``error`` and every retain-eligible field —
        balances_json, balance_observed_at, BOTH per-component payload-source
        ids, authoritative_until — is NULL, never fabricated.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_venue_account_snapshot(
        _attempt_row(
            _T0,
            balance_status="error",
            position_status="error",
            balances_json=None,
            open_positions_json=None,
            authoritative_until=None,
            error="auth rejected",
        )
    )
    state = await _active_state(repo)
    assert state.sync_status == "error"
    assert state.balances_json is None
    assert state.balance_observed_at is None
    assert state.balance_payload_source_observation_id is None
    assert state.position_payload_source_observation_id is None
    assert state.authoritative_until is None


async def test_position_error_retains_predecessor_positions_while_balance_fresh(
    tmp_path: Path,
) -> None:
    """Components retain independently: fresh balance, retained positions.

    Given: a first snapshot observing both balance and positions
        (observation #1),
    When: a follow-up snapshot freshly observes the balance but the position
        read ERRORS,
    Then: the balance payload, its clock, and its payload-source id are the NEW
        attempt's while the open positions, their clock, and the
        position_payload_source_observation_id are RETAINED from observation #1
        — the two components never share fate, and the errored position read can
        never blank a still-displayable last-known position.
    """
    repo = await _make_repo(tmp_path)
    t1 = _T0
    t2 = _T0 + timedelta(seconds=20)
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t1,
            exchange="kraken_futures",
            balances_json='{"USD": 100.0}',
            open_positions_json='[{"symbol": "PF_XBTUSD"}]',
        )
    )
    obs1_id = (await _all_observations(repo))[0].id
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t2,
            exchange="kraken_futures",
            balance_status="observed",
            position_status="error",
            balances_json='{"USD": 222.0}',
            open_positions_json=None,
            error="positions endpoint 500",
        )
    )
    new_obs_id = (await _all_observations(repo))[1].id
    state = await _active_state(repo)
    assert state.sync_status == "error"
    assert state.balances_json == '{"USD": 222.0}'
    assert state.balance_observed_at == t2
    assert state.balance_payload_source_observation_id == new_obs_id
    assert state.open_positions_json == '[{"symbol": "PF_XBTUSD"}]'
    assert state.position_observed_at == t1
    assert state.position_payload_source_observation_id == obs1_id


async def test_independent_per_component_payload_provenance(tmp_path: Path) -> None:
    """A fresh balance and a retained position carry DISTINCT source ids (F3).

    Given: state #1 freshly observing BOTH the balance and open positions
        (observation #1 backs both components), then state #2 freshly observing
        the balance while the position read ERRORS (observation #2 is fresh for
        the balance only),
    When: record_venue_account_snapshot materializes each state,
    Then: on state #1 both payload-source ids equal observation #1; on state #2
        balance_payload_source_observation_id equals the NEW observation #2 (the
        balance is fresh) while position_payload_source_observation_id stays
        observation #1 (positions retained), the two DIFFER, and the retained
        open positions and their clock are observation #1's values — proving a
        fresh balance is never conflated with stale positions.
    """
    repo = await _make_repo(tmp_path)
    t1 = _T0
    t2 = _T0 + timedelta(seconds=15)
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t1,
            exchange="kraken_futures",
            balances_json='{"USD": 100.0}',
            open_positions_json='[{"symbol": "PF_XBTUSD"}]',
        )
    )
    obs1_id = (await _all_observations(repo))[0].id
    state_one = await _active_state(repo)
    assert state_one.balance_payload_source_observation_id == obs1_id
    assert state_one.position_payload_source_observation_id == obs1_id
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t2,
            exchange="kraken_futures",
            balance_status="observed",
            position_status="error",
            balances_json='{"USD": 250.0}',
            open_positions_json=None,
            error="positions endpoint 500",
        )
    )
    obs2_id = (await _all_observations(repo))[1].id
    state_two = await _active_state(repo)
    assert state_two.balance_payload_source_observation_id == obs2_id
    assert state_two.position_payload_source_observation_id == obs1_id
    assert (
        state_two.balance_payload_source_observation_id
        != state_two.position_payload_source_observation_id
    )
    assert state_two.open_positions_json == '[{"symbol": "PF_XBTUSD"}]'
    assert state_two.position_observed_at == t1


async def test_not_applicable_positions_clear_prior_observed_positions(
    tmp_path: Path,
) -> None:
    """Structural absence CLEARS a prior observed position — the Codex hole.

    Given: state #1 freshly observing both the balance and open positions
        (futures), then a follow-up whose balance is freshly observed but whose
        positions are now ``not_applicable``,
    When: record_venue_account_snapshot materializes state #2,
    Then: the positions are CLEARED (open_positions_json, position_observed_at,
        position_payload_source_observation_id all NULL) even though a
        predecessor existed — retain is transient-error-only, never structural
        absence. sync_status is ``observed`` (observed balance + not_applicable
        positions rolls up to authoritative truth), and the balance stays fresh;
        a lingering stale position on an authoritative row would be fiction.
    """
    repo = await _make_repo(tmp_path)
    t1 = _T0
    t2 = _T0 + timedelta(seconds=15)
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t1,
            exchange="kraken_futures",
            balances_json='{"USD": 100.0}',
            open_positions_json='[{"symbol": "PF_XBTUSD"}]',
        )
    )
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t2,
            exchange="kraken_futures",
            balance_status="observed",
            position_status="not_applicable",
            balances_json='{"USD": 250.0}',
            open_positions_json=None,
        )
    )
    obs2_id = (await _all_observations(repo))[1].id
    state = await _active_state(repo)
    assert state.sync_status == "observed"
    assert state.balances_json == '{"USD": 250.0}'
    assert state.balance_payload_source_observation_id == obs2_id
    assert state.open_positions_json is None
    assert state.position_observed_at is None
    assert state.position_payload_source_observation_id is None


async def test_unsupported_positions_clear_prior_observed_positions(
    tmp_path: Path,
) -> None:
    """An unsupported positions read CLEARS a prior observed position.

    Given: state #1 freshly observing both the balance and open positions, then
        a follow-up whose balance is freshly observed but whose positions are
        now ``unsupported``,
    When: record_venue_account_snapshot materializes state #2,
    Then: the positions are CLEARED (all three position fields NULL) — an
        unsupported read is structural absence, not a transient blip to retain —
        and sync_status is ``error`` (observed balance + unsupported positions
        cannot roll up to observed).
    """
    repo = await _make_repo(tmp_path)
    t1 = _T0
    t2 = _T0 + timedelta(seconds=15)
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t1,
            exchange="kraken_futures",
            balances_json='{"USD": 100.0}',
            open_positions_json='[{"symbol": "PF_XBTUSD"}]',
        )
    )
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t2,
            exchange="kraken_futures",
            balance_status="observed",
            position_status="unsupported",
            balances_json='{"USD": 250.0}',
            open_positions_json=None,
        )
    )
    state = await _active_state(repo)
    assert state.sync_status == "error"
    assert state.balances_json == '{"USD": 250.0}'
    assert state.open_positions_json is None
    assert state.position_observed_at is None
    assert state.position_payload_source_observation_id is None


async def test_unsupported_balance_clears_prior_observed_balance(
    tmp_path: Path,
) -> None:
    """An unsupported balance read CLEARS a prior observed balance and window.

    Given: state #1 freshly observing a spot balance, then a follow-up whose
        balance is now ``unsupported`` (structural absence) with not_applicable
        positions,
    When: record_venue_account_snapshot materializes state #2,
    Then: the balance is CLEARED (balances_json, balance_observed_at,
        balance_payload_source_observation_id, authoritative_until all NULL) even
        with a predecessor — unsupported is structural absence, never retained —
        and sync_status is ``unsupported``.
    """
    repo = await _make_repo(tmp_path)
    t1 = _T0
    t2 = _T0 + timedelta(seconds=15)
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t1,
            position_status="not_applicable",
            open_positions_json=None,
        )
    )
    await repo.record_venue_account_snapshot(
        _attempt_row(
            t2,
            balance_status="unsupported",
            position_status="not_applicable",
            balances_json=None,
            open_positions_json=None,
            authoritative_until=None,
        )
    )
    state = await _active_state(repo)
    assert state.sync_status == "unsupported"
    assert state.balances_json is None
    assert state.balance_observed_at is None
    assert state.balance_payload_source_observation_id is None
    assert state.authoritative_until is None


async def test_snapshot_clamps_lagging_bus_clock(tmp_path: Path) -> None:
    """A bus_time older than the stored timestamp is clamped forward.

    Given: an active state stamped at t1,
    When: a snapshot arrives with bus_time BEFORE t1 (the clock-skew class),
    Then: the successor is stamped at t1 (never travelling back), the
        predecessor closes at t1, and exactly one row stays active carrying
        the fresh balance.
    """
    repo = await _make_repo(tmp_path)
    t1 = _T0
    skewed = _T0 - timedelta(seconds=30)
    await repo.record_venue_account_snapshot(_attempt_row(t1))
    await repo.record_venue_account_snapshot(_attempt_row(skewed, balances_json='{"USD": 5.0}'))
    states = await _all_states(repo)
    assert len(states) == 2
    old, new = states
    assert old.known_to == t1
    assert new.timestamp == t1
    assert new.known_to == KNOWN_TO_MAX
    active = [r for r in states if r.known_to == KNOWN_TO_MAX]
    assert len(active) == 1
    assert active[0].balances_json == '{"USD": 5.0}'


async def test_snapshot_retries_lost_first_insert_race(tmp_path: Path) -> None:
    """A lost partial-unique first-insert race re-reads and closes the winner.

    Given: a concurrent rival commits the winning active state while the first
        write pass believed the identity was empty, so flushing a conflicting
        active row dies on the REAL partial unique index (exercising
        failed-transaction rollback and the single retry),
    When: record_venue_account_snapshot retries,
    Then: the retry finds and closes the committed winner, the successor
        carries the WINNER's public_id and the retry attempt's fresh balance,
        and exactly one active row remains.
    """
    repo = await _make_repo(tmp_path)
    rival = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'venue.db'}")
    t1 = _T0
    t2 = _T0 + timedelta(seconds=1)
    original = repo._write_venue_account_snapshot
    attempts: list[int] = []

    async def flaky(s: AsyncSession, attempt: VenueAccountAttemptRow) -> int:
        """Lose the race once, then defer to the real write on retry.

        Args:
            s: Open session owned by the caller.
            attempt: The raw account-observation attempt.

        Returns:
            The new active state row id on the retry pass.
        """
        attempts.append(1)
        if len(attempts) == 1:
            await rival.record_venue_account_snapshot(
                _attempt_row(t1, balances_json='{"USD": 999.0}')
            )
            s.add(
                VenueAccountState(
                    wallet_public_id=_WALLET,
                    exchange="kraken",
                    mode="live",
                    sync_status="error",
                    balance_status="error",
                    position_status="error",
                    valuation_status="native_only",
                    current_attempt_observation_id=1,
                    session_id=_SESSION,
                    sequence_id=2,
                    timestamp=t1,
                    known_to=KNOWN_TO_MAX,
                )
            )
            await s.flush()
            raise AssertionError("partial unique index must have rejected the insert")
        return await original(s, attempt)

    with patch.object(repo, "_write_venue_account_snapshot", side_effect=flaky):
        new_id = await repo.record_venue_account_snapshot(
            _attempt_row(t2, balances_json='{"USD": 1.0}')
        )
    assert new_id > 0
    assert len(attempts) == 2
    states = await _all_states(repo)
    assert len(states) == 2
    winner, successor = states
    assert winner.balances_json == '{"USD": 999.0}'
    assert winner.known_to == t2
    assert successor.known_to == KNOWN_TO_MAX
    assert successor.public_id == winner.public_id
    assert successor.balances_json == '{"USD": 1.0}'


async def test_get_states_empty_wallet_list_scopes_to_nothing(tmp_path: Path) -> None:
    """An empty wallet list returns an empty list (scoped to nothing).

    Given: a database that DOES hold an active state,
    When: get_venue_account_states runs with an empty wallet list,
    Then: it returns [] — the ``.in_([])`` filter matches no wallet, never an
        unfiltered scan that could leak rows.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_venue_account_snapshot(_attempt_row(_T0))
    assert await repo.get_venue_account_states([]) == []


async def test_get_states_none_returns_all_unscoped(tmp_path: Path) -> None:
    """A ``None`` wallet filter returns every active state (admin-unscoped).

    Given: active states for two different wallets,
    When: get_venue_account_states runs with ``None`` (no wallet filter),
    Then: rows for BOTH wallets come back — mirroring get_positions, the
        admin-unscoped view is the only path that reaches an unfiltered read
        (resolve_target_wallets returns None only for an admin with no scope).
    """
    repo = await _make_repo(tmp_path)
    other_wallet = "019e873c-d060-762d-8cee-5fde40095131"
    await repo.record_venue_account_snapshot(_attempt_row(_T0))
    await repo.record_venue_account_snapshot(_attempt_row(_T0, wallet_public_id=other_wallet))
    rows = await repo.get_venue_account_states(None)
    wallets = {r["wallet_public_id"] for r in rows}
    assert _WALLET in wallets
    assert other_wallet in wallets


async def test_get_states_returns_active_rows_ordered_by_exchange_then_mode(
    tmp_path: Path,
) -> None:
    """Active rows come back ordered by exchange then mode.

    Given: three active states for one wallet across two exchanges and two
        modes written out of order,
    When: get_venue_account_states runs for that wallet,
    Then: the rows are ordered by exchange then mode
        (coinbase/live, kraken/live, kraken/paper).
    """
    repo = await _make_repo(tmp_path)
    await repo.record_venue_account_snapshot(
        _attempt_row(
            _T0,
            exchange="kraken",
            mode="paper",
            balance_status="simulated",
            position_status="not_applicable",
            open_positions_json=None,
        )
    )
    await repo.record_venue_account_snapshot(
        _attempt_row(
            _T0,
            exchange="coinbase",
            position_status="not_applicable",
            open_positions_json=None,
        )
    )
    await repo.record_venue_account_snapshot(
        _attempt_row(
            _T0,
            exchange="kraken",
            position_status="not_applicable",
            open_positions_json=None,
        )
    )
    rows = await repo.get_venue_account_states([_WALLET])
    assert [(r["exchange"], r["mode"]) for r in rows] == [
        ("coinbase", "live"),
        ("kraken", "live"),
        ("kraken", "paper"),
    ]


async def test_get_states_excludes_closed_rows(tmp_path: Path) -> None:
    """A closed (non-sentinel known_to) row is never returned.

    Given: a wallet with one active state and one manually-closed state
        version (known_to moved off the sentinel, no active successor),
    When: get_venue_account_states runs for that wallet,
    Then: only the active identity is returned — the closed version stays
        queryable via history but is absent from the live surface.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_venue_account_snapshot(
        _attempt_row(
            _T0, exchange="kraken", position_status="not_applicable", open_positions_json=None
        )
    )
    async with repo.session() as s:
        s.add(
            VenueAccountState(
                wallet_public_id=_WALLET,
                exchange="coinbase",
                mode="live",
                sync_status="observed",
                balance_status="observed",
                position_status="observed",
                valuation_status="native_only",
                balances_json='{"USD": 100.0}',
                open_positions_json="[]",
                balance_observed_at=_T0,
                position_observed_at=_T0,
                current_attempt_observation_id=1,
                balance_payload_source_observation_id=1,
                position_payload_source_observation_id=1,
                authoritative_until=_T0 + timedelta(minutes=5),
                session_id=_SESSION,
                sequence_id=1,
                timestamp=_T0,
                known_to=_T0 + timedelta(minutes=1),
            )
        )
        await s.commit()
    rows = await repo.get_venue_account_states([_WALLET])
    assert [(r["exchange"], r["mode"]) for r in rows] == [("kraken", "live")]


async def test_get_states_is_wallet_scoped(tmp_path: Path) -> None:
    """Reads are scoped to the requested wallets only.

    Given: active states for two different wallets,
    When: get_venue_account_states runs for exactly one of them,
    Then: only that wallet's row is returned; the other wallet is excluded
        even though its row is active.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_venue_account_snapshot(_attempt_row(_T0, wallet_public_id=_WALLET))
    await repo.record_venue_account_snapshot(_attempt_row(_T0, wallet_public_id=_OTHER_WALLET))
    rows = await repo.get_venue_account_states([_WALLET])
    assert len(rows) == 1
    assert rows[0]["wallet_public_id"] == _WALLET


async def _add_state(repo: SQLAlchemyRepository, **overrides: object) -> None:
    """Insert one VenueAccountState via the ORM, committing the session.

    Builds a fully valid active observed state row and applies the caller's
    field overrides so a single test can violate exactly one CHECK constraint.

    Args:
        repo: Repository under test.
        overrides: Column values overriding the valid baseline.

    Returns:
        None. Raises IntegrityError when an override trips a CHECK.
    """
    values: dict[str, object] = {
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "sync_status": "observed",
        "balance_status": "observed",
        "position_status": "observed",
        "valuation_status": "native_only",
        "balances_json": '{"USD": 100.0}',
        "open_positions_json": "[]",
        "balance_observed_at": _T0,
        "position_observed_at": _T0,
        "current_attempt_observation_id": 1,
        "balance_payload_source_observation_id": 1,
        "position_payload_source_observation_id": 1,
        "authoritative_until": _T0 + timedelta(minutes=5),
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": _T0,
        "known_to": KNOWN_TO_MAX,
    }
    values.update(overrides)
    async with repo.session() as s:
        s.add(VenueAccountState(**values))
        await s.commit()


async def _add_observation(repo: SQLAlchemyRepository, **overrides: object) -> None:
    """Insert one VenueAccountObservation via the ORM, committing the session.

    Builds a fully valid observed attempt row and applies the caller's field
    overrides so a single test can violate exactly one observation CHECK.

    Args:
        repo: Repository under test.
        overrides: Column values overriding the valid baseline.

    Returns:
        None. Raises IntegrityError when an override trips a CHECK.
    """
    values: dict[str, object] = {
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "attempt_status": "observed",
        "balance_status": "observed",
        "position_status": "observed",
        "balances_json": '{"USD": 100.0}',
        "open_positions_json": "[]",
        "balance_observed_at": _T0,
        "position_observed_at": _T0,
        "error": None,
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": _T0,
        "known_to": KNOWN_TO_MAX,
    }
    values.update(overrides)
    async with repo.session() as s:
        s.add(VenueAccountObservation(**values))
        await s.commit()


async def test_state_rejects_observed_rollup_without_observed_balance(
    tmp_path: Path,
) -> None:
    """An observed roll-up requires the balance component observed.

    Given: an ``observed`` state whose balance_status is ``error``,
    When: the row is flushed,
    Then: ck_venue_account_states_observed_balance raises IntegrityError — the
        roll-up can never claim truth the balance did not provide.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, sync_status="observed", balance_status="error")


async def test_state_rejects_observed_rollup_with_errored_position(
    tmp_path: Path,
) -> None:
    """An observed roll-up requires positions observed or absent.

    Given: an ``observed`` state whose position_status is ``error``,
    When: the row is flushed,
    Then: ck_venue_account_states_observed_position raises IntegrityError — an
        errored position read can never ride an observed account.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, sync_status="observed", position_status="error")


async def test_state_rejects_observed_rollup_without_authority_window(
    tmp_path: Path,
) -> None:
    """An observed roll-up must carry an authority window.

    Given: an ``observed`` state whose authoritative_until is NULL,
    When: the row is flushed,
    Then: ck_venue_account_states_observed_authority raises IntegrityError —
        without a window the read layer cannot expire the row and would serve
        it as live truth forever.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, sync_status="observed", authoritative_until=None)


async def test_state_rejects_observed_balance_without_json(tmp_path: Path) -> None:
    """An observed balance must carry its JSON payload.

    Given: a state row whose balance_status is ``observed`` but balances_json is
        NULL (a materially empty snapshot posing as authoritative truth),
    When: the row is flushed,
    Then: ck_venue_account_states_balance_json_present raises IntegrityError — a
        genuinely empty account is an empty object, never NULL.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, balance_status="observed", balances_json=None)


async def test_state_rejects_simulated_balance_without_json(tmp_path: Path) -> None:
    """A simulated balance must also carry its JSON payload.

    Given: a paper state row whose balance_status is ``simulated`` but
        balances_json is NULL,
    When: the row is flushed,
    Then: ck_venue_account_states_balance_json_present raises IntegrityError —
        the JSON-present guard covers simulated as well as observed balances.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(
            repo,
            mode="paper",
            sync_status="simulated",
            balance_status="simulated",
            balances_json=None,
        )


async def test_state_rejects_observed_position_without_json(tmp_path: Path) -> None:
    """An observed positions component must carry its JSON payload.

    Given: an ``observed`` state whose position_status is ``observed`` but
        open_positions_json is NULL,
    When: the row is flushed,
    Then: ck_venue_account_states_position_observed_present raises
        IntegrityError — an empty book is an empty array, never NULL.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, position_status="observed", open_positions_json=None)


async def test_state_rejects_observed_position_without_observed_at(
    tmp_path: Path,
) -> None:
    """An observed positions component must carry its observation timestamp.

    Given: an ``observed`` state whose position_status is ``observed`` but
        position_observed_at is NULL,
    When: the row is flushed,
    Then: ck_venue_account_states_position_observed_present raises
        IntegrityError — observed positions without a clock are never served.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, position_status="observed", position_observed_at=None)


async def test_state_rejects_null_current_attempt_observation_id(
    tmp_path: Path,
) -> None:
    """Every state must reference the attempt that produced it.

    Given: an otherwise-valid state row whose current_attempt_observation_id is
        NULL,
    When: the row is flushed,
    Then: the NOT NULL constraint raises IntegrityError — a state can never
        exist without the observation it was materialized from.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, current_attempt_observation_id=None)


async def test_state_rejects_balance_json_without_source(tmp_path: Path) -> None:
    """A displayed balance payload cannot exist without its provenance.

    Given: an ``observed`` state carrying balances_json but a NULL
        balance_payload_source_observation_id,
    When: the row is flushed,
    Then: ck_venue_account_states_balance_payload_source raises IntegrityError —
        JSON and its source id are both-null or both-non-null.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, balance_payload_source_observation_id=None)


async def test_state_rejects_balance_source_without_json(tmp_path: Path) -> None:
    """A balance source id cannot point at a NULL payload.

    Given: an ``error`` state with a NULL balances_json but a non-NULL
        balance_payload_source_observation_id (forged provenance),
    When: the row is flushed,
    Then: ck_venue_account_states_balance_payload_source raises IntegrityError.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(
            repo,
            sync_status="error",
            balance_status="error",
            balances_json=None,
            balance_payload_source_observation_id=1,
        )


async def test_state_rejects_position_json_without_source(tmp_path: Path) -> None:
    """A displayed positions payload cannot exist without its provenance.

    Given: an ``observed`` state carrying open_positions_json but a NULL
        position_payload_source_observation_id,
    When: the row is flushed,
    Then: ck_venue_account_states_position_payload_source raises IntegrityError.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, position_payload_source_observation_id=None)


async def test_state_rejects_fresh_balance_source_not_current_attempt(
    tmp_path: Path,
) -> None:
    """A freshly observed balance must be sourced to THIS attempt.

    Given: an ``observed`` state whose balance_payload_source_observation_id does
        not equal current_attempt_observation_id,
    When: the row is flushed,
    Then: ck_venue_account_states_balance_fresh_source raises IntegrityError — a
        fresh read can never be attributed to an earlier observation.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, balance_payload_source_observation_id=999)


async def test_state_rejects_fresh_position_source_not_current_attempt(
    tmp_path: Path,
) -> None:
    """A freshly observed positions component must be sourced to THIS attempt.

    Given: an ``observed`` state whose position_payload_source_observation_id
        does not equal current_attempt_observation_id,
    When: the row is flushed,
    Then: ck_venue_account_states_position_fresh_source raises IntegrityError.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, position_payload_source_observation_id=999)


async def test_state_rejects_simulated_status_on_live_row(tmp_path: Path) -> None:
    """A simulated status can only ride a paper row.

    Given: a LIVE state row whose sync_status is ``simulated`` (a valid value,
        but fiction on a live account),
    When: the row is flushed,
    Then: ck_venue_account_states_simulated_paper raises IntegrityError — a
        live account is never simulated.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, mode="live", sync_status="simulated")


async def test_state_rejects_invalid_sync_status(tmp_path: Path) -> None:
    """An out-of-vocabulary sync_status is rejected by the CHECK.

    Given: a state row whose sync_status is not one of the four allowed
        values,
    When: the row is flushed,
    Then: ck_venue_account_states_sync_status raises IntegrityError.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, sync_status="garbage")


async def test_state_rejects_invalid_position_status(tmp_path: Path) -> None:
    """An out-of-vocabulary position_status is rejected by the CHECK.

    Given: a state row whose position_status is not one of the four allowed
        values,
    When: the row is flushed,
    Then: ck_venue_account_states_position_status raises IntegrityError.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, position_status="garbage")


async def test_state_rejects_observed_balance_without_observed_at(
    tmp_path: Path,
) -> None:
    """An observed balance must carry its observation timestamp.

    Given: a state row with balance_status ``observed`` but a NULL
        balance_observed_at,
    When: the row is flushed,
    Then: ck_venue_account_states_balance_observed_at raises IntegrityError — a
        timestamp-less observed balance is indistinguishable from a
        fabrication.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, balance_status="observed", balance_observed_at=None)


async def test_state_rejects_non_native_valuation_status(tmp_path: Path) -> None:
    """Phase 3 valuation_status is pinned to native_only.

    Given: a state row whose valuation_status is a USD value (Phase 5
        territory),
    When: the row is flushed,
    Then: ck_venue_account_states_valuation_status raises IntegrityError — zero
        USD math is allowed in Phase 3.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, valuation_status="usd")


async def test_state_rejects_uppercase_exchange(tmp_path: Path) -> None:
    """A mixed-case exchange is rejected by the lowercase CHECK.

    Given: a state row whose exchange is ``Kraken``,
    When: the row is flushed,
    Then: ck_venue_account_states_exchange_lower raises IntegrityError.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, exchange="Kraken")


async def test_state_rejects_mode_outside_live_paper(tmp_path: Path) -> None:
    """A mode outside live/paper is rejected by the CHECK.

    Given: a state row whose mode is ``margin``,
    When: the row is flushed,
    Then: ck_venue_account_states_mode raises IntegrityError.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_state(repo, mode="margin")


async def test_observation_rejects_simulated_attempt_on_live_row(
    tmp_path: Path,
) -> None:
    """A simulated observation attempt can only ride a paper row.

    Given: a LIVE observation whose attempt_status is ``simulated``,
    When: the row is flushed,
    Then: ck_venue_account_obs_simulated_paper raises IntegrityError — a live
        account attempt is never fiction.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_observation(repo, mode="live", attempt_status="simulated")


async def test_observation_rejects_observed_attempt_without_observed_balance(
    tmp_path: Path,
) -> None:
    """An observed attempt roll-up requires the balance component observed.

    Given: an ``observed`` observation whose balance_status is ``error``,
    When: the row is flushed,
    Then: ck_venue_account_obs_observed_balance raises IntegrityError — the
        append-only plane enforces the same coherence as the state roll-up.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_observation(repo, attempt_status="observed", balance_status="error")


async def test_observation_rejects_observed_balance_without_json(
    tmp_path: Path,
) -> None:
    """An observed balance on the observation plane must carry its JSON.

    Given: an observation whose balance_status is ``observed`` but balances_json
        is NULL,
    When: the row is flushed,
    Then: ck_venue_account_obs_balance_json_present raises IntegrityError — the
        append-only plane enforces the same JSON-present guard as the state.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_observation(repo, balance_status="observed", balances_json=None)


async def test_observation_rejects_observed_position_without_json(
    tmp_path: Path,
) -> None:
    """An observed positions component on the observation plane needs its JSON.

    Given: an observation whose position_status is ``observed`` but
        open_positions_json is NULL,
    When: the row is flushed,
    Then: ck_venue_account_obs_position_observed_present raises IntegrityError.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_observation(repo, position_status="observed", open_positions_json=None)


async def test_observation_rejects_invalid_attempt_status(tmp_path: Path) -> None:
    """An out-of-vocabulary attempt_status is rejected on the observation.

    Given: an observation row whose attempt_status is not one of the four
        allowed values,
    When: the row is flushed,
    Then: ck_venue_account_obs_attempt_status raises IntegrityError — the
        append-only plane enforces its own vocabulary independent of the state
        roll-up.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(IntegrityError):
        await _add_observation(repo, attempt_status="garbage")
