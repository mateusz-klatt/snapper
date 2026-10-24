"""Require database-current temporal proof despite retained nonexpired ORM identities."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import tzinfo
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade import unit_snapshot_route
from snapper.application.trade import unit_snapshot_submission
from snapper.core.execution_route_authority import ExecutionRouteSelection
from snapper.core.execution_unit_snapshot import SnapshotCommandMaterial
from snapper.core.financial_account_owner import FINANCIAL_OWNER_SETTING_KEY
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.core.financial_account_owner import canonical_financial_account_owner_policy
from snapper.data import execution_route_authority_store
from snapper.data import execution_unit_admission_store
from snapper.data import execution_unit_catalog
from snapper.data import financial_account_owner_policy_store
from snapper.data import financial_account_owner_policy_writer
from snapper.data.execution_route_authority_store import RouteAuthorityBindingError
from snapper.data.execution_route_authority_store import require_current_route_authority
from snapper.data.execution_unit_catalog import UnitCatalogLookup
from snapper.data.execution_unit_catalog import load_unit_catalog
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import durable_command_material
from snapper.data.financial_account_owner_policy_store import load_current_financial_owner_policy
from snapper.data.financial_account_owner_policy_writer import _lock_reserved_policy
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Setting
from snapper.data.models import Symbol
from snapper.data.models import TradeCommand
from snapper.data.models import VenueAccountState
from snapper.data.models import Wallet
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.unit_admission import AdmissionScenario
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import seed_admission_scenario

_NOW = datetime(2026, 10, 24, 12, tzinfo=UTC)
_FUTURE_END = _NOW + timedelta(days=1)
_SUCCESSOR_AT = _NOW - timedelta(seconds=1)

type RetainedRow = Wallet | VenueAccountState | Instrument | Symbol | InstrumentSpec | Setting | TradeCommand
type RetainedModel = (
    type[Wallet]
    | type[VenueAccountState]
    | type[Instrument]
    | type[Symbol]
    | type[InstrumentSpec]
    | type[Setting]
    | type[TradeCommand]
)


class _Clock:
    """Control only the readers' wall clock, retaining actual database and identity-map behavior."""

    @classmethod
    def now(cls, zone: tzinfo | None = None) -> datetime:
        """Return the fixed actual interpretation instant in the requested timezone.

        Args:
            zone: Requested timezone or the existing aware fixture timezone.

        Returns:
            The deterministic actual interpretation instant.
        """
        return _NOW if zone is None else _NOW.astimezone(zone)


@pytest.fixture
async def scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[AdmissionScenario]:
    """Create genuine file-backed account/catalog evidence and an explicit empty owner policy.

    Args:
        tmp_path: Private directory containing the isolated SQLite database.
        monkeypatch: Reader clock substitution without changing ORM results or session behavior.

    Yields:
        Real source and route evidence under the unmodified schema and database guards.
    """
    for module in (
        unit_snapshot_route,
        unit_snapshot_submission,
        execution_route_authority_store,
        execution_unit_admission_store,
        execution_unit_catalog,
        financial_account_owner_policy_store,
        financial_account_owner_policy_writer,
    ):
        monkeypatch.setattr(module, "datetime", _Clock)
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'identity-map.db'}")
    try:
        await repository.create_all()
        value = await seed_admission_scenario(repository, _NOW, source="kraken_futures")
        async with repository.session_factory() as session:
            session.add(
                Setting(
                    public_id=admission_id(600),
                    session_id=admission_id(99),
                    sequence_id=600,
                    timestamp=_NOW - timedelta(hours=1),
                    key=FINANCIAL_OWNER_SETTING_KEY,
                    value=canonical_financial_account_owner_policy(
                        FinancialAccountOwnerPolicy(assignments=())
                    ),
                    category="financial_owner",
                    is_encrypted=False,
                    updated_by="isolated-identity-map-fixture",
                )
            )
            await session.commit()
        yield value
    finally:
        await repository.engine.dispose()


async def _close_from_independent_session(
    repository: SQLAlchemyRepository, cached: RetainedRow, with_successor: bool
) -> int | None:
    """Commit a real external close while the reader retains its original ORM identity.

    Args:
        repository: Real repository whose factory creates an independent writer session.
        cached: Strong reader-owned reference used only to identify the physical database row.
        with_successor: Whether the writer also commits a genuine exact-current successor.

    Returns:
        The successor's physical identity, or absence when only finite history remains.
    """
    async with repository.session_factory() as writer:
        stored: RetainedRow | None = await writer.get(type(cached), cached.id)
        assert stored is not None
        assert stored.known_to == KNOWN_TO_MAX
        fields: dict[str, object] = {
            column.key: getattr(stored, column.key)
            for column in type(stored).__table__.columns
            if column.key != "id"
        }
        stored.known_to = _FUTURE_END
        await writer.flush()
        successor_id: int | None = None
        if with_successor:
            fields.update(
                timestamp=_SUCCESSOR_AT,
                known_to=KNOWN_TO_MAX,
                sequence_id=stored.sequence_id + 10000,
            )
            successor = type(stored)(**fields)
            writer.add(successor)
            await writer.flush()
            successor_id = successor.id
        await writer.commit()
    return successor_id


async def _prove_cached_end_disagrees_with_database(
    reader: AsyncSession, cached: RetainedRow
) -> None:
    """Establish fresh database visibility independently of the retained entity attribute.

    Args:
        reader: Original committed reader session with expiration disabled.
        cached: Strong ORM reference retained across the independent writer commit.
    """
    assert reader.sync_session.expire_on_commit is False
    row_model = type(cached)
    actual_end: datetime = (
        await reader.execute(select(row_model.known_to).where(row_model.id == cached.id))
    ).scalar_one()
    assert actual_end == _FUTURE_END
    assert _NOW < actual_end < KNOWN_TO_MAX
    assert cached.known_to == KNOWN_TO_MAX


@pytest.mark.parametrize("with_successor", (False, True), ids=("finite-only", "with-successor"))
@pytest.mark.parametrize(
    ("row_model", "reason"),
    (
        pytest.param(Wallet, "route_wallet_missing", id="wallet"),
        pytest.param(VenueAccountState, "route_current_account_state_missing", id="account-state"),
    ),
)
async def test_route_current_proof_does_not_trust_a_retained_open_entity(
    scenario: AdmissionScenario, row_model: RetainedModel, reason: str, with_successor: bool
) -> None:
    """Use database-current route ownership rather than a prior transaction's cached sentinel.

    Given: A committed reader retains MAX while another session closes that exact row.
    When: Real route verification runs against the new database snapshot.
    Then: Finite-only evidence is refused and an actual current successor stays usable.

    Args:
        scenario: Genuine selected account route and underlying wallet metadata.
        row_model: Wallet or account-state entity retained by the original reader.
        reason: Expected refusal when no actual current successor exists.
        with_successor: Whether the independent writer also commits a current row.
    """
    async with scenario.repository.session_factory() as reader:
        cached: RetainedRow = (await reader.execute(select(row_model))).scalar_one()
        assert cached.known_to == KNOWN_TO_MAX
        await reader.commit()
        await _close_from_independent_session(scenario.repository, cached, with_successor)
        await _prove_cached_end_disagrees_with_database(reader, cached)
        if not with_successor:
            with pytest.raises(RouteAuthorityBindingError, match=reason):
                await require_current_route_authority(reader, scenario.route, _NOW)
            assert cached.known_to == KNOWN_TO_MAX
            return
        actual = await require_current_route_authority(reader, scenario.route, _NOW)
        assert cached.known_to == KNOWN_TO_MAX
        assert actual == scenario.authority


@pytest.mark.parametrize("with_successor", (False, True), ids=("finite-only", "with-successor"))
@pytest.mark.parametrize("row_model", (Instrument, Symbol, InstrumentSpec))
async def test_current_catalog_uses_database_end_for_each_join_participant(
    scenario: AdmissionScenario, row_model: RetainedModel, with_successor: bool
) -> None:
    """Keep current catalog joins independent of cached pre-close ORM attributes.

    Given: A strongly retained catalog entity stays MAX after a second session closes it.
    When: The original reader loads actual current catalog lineage.
    Then: It refuses finite-only lineage or selects the genuine successor without ambiguity.

    Args:
        scenario: Real joined instrument, symbol and specification lineage.
        row_model: Join participant retained in the reader's identity map.
        with_successor: Whether the actual database also contains a current successor.
    """
    async with scenario.repository.session_factory() as reader:
        cached: RetainedRow = (await reader.execute(select(row_model))).scalar_one()
        assert cached.known_to == KNOWN_TO_MAX
        await reader.commit()
        successor_id = await _close_from_independent_session(
            scenario.repository, cached, with_successor
        )
        await _prove_cached_end_disagrees_with_database(reader, cached)
        lookup = UnitCatalogLookup(exchange="kraken_futures", native_symbol="BTC-USD")
        if not with_successor:
            with pytest.raises(SnapshotBindingError, match="unit_catalog_lineage_missing"):
                await load_unit_catalog(reader, lookup, _NOW, current=True)
            assert cached.known_to == KNOWN_TO_MAX
            return
        actual = await load_unit_catalog(reader, lookup, _NOW, current=True)
        assert cached.known_to == KNOWN_TO_MAX
        selected = next(
            row
            for row in (actual.instrument, actual.symbol, actual.spec)
            if isinstance(row, row_model)
        )
        assert selected.id == successor_id
        assert selected.id != cached.id
        assert selected.known_to == KNOWN_TO_MAX


@pytest.mark.parametrize("with_successor", (False, True), ids=("finite-only", "with-successor"))
async def test_financial_policy_reader_does_not_reauthorize_a_cached_revision(
    scenario: AdmissionScenario, with_successor: bool
) -> None:
    """Rebind financial owner policy to its actual current database revision.

    Given: A policy object remains cached as MAX after an independent committed closure.
    When: The original reader locks and loads the actual current policy.
    Then: Finite-only ownership refuses and an actual successor binds its new provenance.

    Args:
        scenario: Real isolated repository with one explicit empty revocation policy.
        with_successor: Whether the writer also commits a new exact-current policy revision.
    """
    async with scenario.repository.session_factory() as reader:
        cached = (
            await reader.execute(select(Setting).where(Setting.key == FINANCIAL_OWNER_SETTING_KEY))
        ).scalar_one()
        assert cached.known_to == KNOWN_TO_MAX
        await reader.commit()
        await _close_from_independent_session(scenario.repository, cached, with_successor)
        await _prove_cached_end_disagrees_with_database(reader, cached)
        if not with_successor:
            with pytest.raises(
                FinancialAccountOwnerUnavailableError,
                match="financial_owner_current_setting_missing_or_ambiguous",
            ):
                await load_current_financial_owner_policy(reader, _NOW)
            assert cached.known_to == KNOWN_TO_MAX
            return
        actual = await load_current_financial_owner_policy(reader, _NOW)
        assert cached.known_to == KNOWN_TO_MAX
        assert actual.reference.timestamp == _SUCCESSOR_AT
        assert actual.reference.sequence_id == 10600
        assert actual.policy == FinancialAccountOwnerPolicy(assignments=())


@pytest.mark.parametrize("with_successor", (False, True), ids=("finite-only", "with-successor"))
async def test_financial_policy_writer_lock_uses_actual_current_revision(
    scenario: AdmissionScenario, with_successor: bool
) -> None:
    """Never let a retained closed identity impersonate the writer's active policy lock.

    Given: Another session commits the policy close after the reader committed its preload.
    When: The real reserved-policy writer boundary reacquires current ownership.
    Then: It returns actual absence or the exact new physical successor, never the cached row.

    Args:
        scenario: Real policy rows under their unmodified unique-current schema constraints.
        with_successor: Whether an actual current successor is committed by the other session.
    """
    async with scenario.repository.session_factory() as reader:
        cached = (
            await reader.execute(select(Setting).where(Setting.key == FINANCIAL_OWNER_SETTING_KEY))
        ).scalar_one()
        assert cached.known_to == KNOWN_TO_MAX
        await reader.commit()
        successor_id = await _close_from_independent_session(
            scenario.repository, cached, with_successor
        )
        await _prove_cached_end_disagrees_with_database(reader, cached)
        actual = await _lock_reserved_policy(reader)
        assert cached.known_to == KNOWN_TO_MAX
        if not with_successor:
            assert actual is None
            return
        assert actual is not None
        assert actual.id == successor_id
        assert actual.id != cached.id
        assert actual.timestamp == _SUCCESSOR_AT


@pytest.mark.parametrize("with_successor", (False, True), ids=("finite-only", "with-successor"))
async def test_exact_observer_cycle_refuses_cached_future_closed_physical_identity(
    scenario: AdmissionScenario, with_successor: bool
) -> None:
    """Prove current observer eligibility from the database even for one exact physical row.

    Given: A retained observer state stays cached MAX after another session closes its row.
    When: Its old physical id is presented as the actual current observer response.
    Then: The closed original refuses even if a separate valid successor now exists.

    Args:
        scenario: Genuine observer response, selected client and isolated account-state rows.
        with_successor: Whether a different physical current response is committed alongside it.
    """
    authority = scenario.authority
    selection = ExecutionRouteSelection(
        scope=authority.scope,
        credential_public_id=authority.credential_public_id,
        credential_timestamp=authority.credential_timestamp,
        public_api_origin=authority.public_api_origin,
    )
    async with scenario.repository.session_factory() as reader:
        cached = (await reader.execute(select(VenueAccountState))).scalar_one()
        assert cached.known_to == KNOWN_TO_MAX
        await reader.commit()
        successor_id = await _close_from_independent_session(
            scenario.repository, cached, with_successor
        )
        await _prove_cached_end_disagrees_with_database(reader, cached)
        with pytest.raises(
            RouteAuthorityBindingError, match="actual_observer_state_identity_unproven"
        ):
            await unit_snapshot_route._actual_observer_response(reader, selection, cached.id)
        assert cached.known_to == KNOWN_TO_MAX
        if with_successor:
            assert successor_id is not None
            state, observation = await unit_snapshot_route._actual_observer_response(
                reader, selection, successor_id
            )
            assert cached.known_to == KNOWN_TO_MAX
            assert state.id == successor_id
            assert state.id != cached.id
            assert state.known_to == KNOWN_TO_MAX
            assert observation.id == state.current_attempt_observation_id


async def _read_current_command_from_retained_session(
    reader: AsyncSession,
    scenario: AdmissionScenario,
    request: SnapshotCommandMaterial,
    boundary: Literal["submission", "admission"],
) -> TradeCommand:
    """Run the actual command selector in the session retaining the old physical entity.

    Args:
        reader: Original committed session whose identity map retains the closed command.
        scenario: Genuine outgoing frame and selected execution route.
        request: Original durable request material, without lifecycle fields.
        boundary: Producer or consumer current-command proof boundary to exercise.

    Returns:
        The real current command selected independently from retained ORM eligibility.
    """
    if boundary == "submission":
        return await unit_snapshot_submission._current_command(
            reader, scenario.order, scenario.route, _NOW
        )
    return await execution_unit_admission_store._current_command(reader, request, _NOW)


@pytest.mark.parametrize("with_successor", (False, True), ids=("finite-only", "with-successor"))
@pytest.mark.parametrize("boundary", ("submission", "admission"))
async def test_command_current_proof_ignores_cached_pre_close_eligibility(
    scenario: AdmissionScenario,
    boundary: Literal["submission", "admission"],
    with_successor: bool,
) -> None:
    """Bind each actual command selector to fresh database eligibility without clobbering history.

    Given: A strongly retained command remains cached MAX after a second session closes it.
    When: The original session rechecks current command ownership through the real reader.
    Then: Finite-only material refuses and a genuine successor is selected without ambiguity.

    Args:
        scenario: Real durable CREATE, outgoing request and selected execution scope.
        boundary: Submission producer or retained-admission current-command reader.
        with_successor: Whether the other session also commits a real current command version.
    """
    async with scenario.repository.session_factory() as reader:
        cached = (await reader.execute(select(TradeCommand))).scalar_one()
        assert cached.known_to == KNOWN_TO_MAX
        request = durable_command_material(cached)
        await reader.commit()
        successor_id = await _close_from_independent_session(
            scenario.repository, cached, with_successor
        )
        await _prove_cached_end_disagrees_with_database(reader, cached)
        if not with_successor:
            with pytest.raises(SnapshotBindingError, match=f"{boundary}_current_command_missing"):
                await _read_current_command_from_retained_session(
                    reader, scenario, request, boundary
                )
            assert cached.known_to == KNOWN_TO_MAX
            return
        actual = await _read_current_command_from_retained_session(
            reader, scenario, request, boundary
        )
        assert cached.known_to == KNOWN_TO_MAX
        assert actual.id == successor_id
        assert actual.id != cached.id
        assert actual.known_to == KNOWN_TO_MAX
        assert durable_command_material(actual) == request
