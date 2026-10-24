"""Preserve current-row SQL cardinality in the presence of future-closed history."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import tzinfo
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade import unit_snapshot_submission
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_unit_snapshot import SnapshotCommandMaterial
from snapper.data import execution_route_authority_store
from snapper.data import execution_unit_admission_store
from snapper.data import execution_unit_catalog
from snapper.data import native_account_read_store
from snapper.data.execution_route_authority_store import RouteAuthorityBindingError
from snapper.data.execution_route_authority_store import require_current_route_authority
from snapper.data.execution_unit_catalog import UnitCatalogLookup
from snapper.data.execution_unit_catalog import load_unit_catalog
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import durable_command_material
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
from snapper.data.models import TradeCommand
from snapper.data.models import VenueAccountState
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.native_account_read_store import NativeAccountReadBindingError
from snapper.data.native_account_read_store import begin_native_account_read
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.temporal_selection import one_or_none
from tests.helpers.unit_admission import AdmissionScenario
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import seed_admission_scenario

_NOW = datetime(2026, 10, 24, 12, tzinfo=UTC)
_FUTURE_END = _NOW + timedelta(days=1)

type TemporalRow = (
    Wallet
    | WalletCredential
    | VenueAccountState
    | Instrument
    | Symbol
    | InstrumentSpec
    | TradeCommand
)
type TemporalModel = (
    type[Wallet]
    | type[WalletCredential]
    | type[VenueAccountState]
    | type[Instrument]
    | type[Symbol]
    | type[InstrumentSpec]
    | type[TradeCommand]
)


class _Clock:
    """Use a fixed actual clock while retaining genuine SQL predicates and stored rows."""

    @classmethod
    def now(cls, zone: tzinfo | None = None) -> datetime:
        """Return the deterministic interpretation instant in the requested zone.

        Args:
            zone: Requested aware timezone, or the default fixture timezone.

        Returns:
            The same actual instant for every current-row proof.
        """
        return _NOW if zone is None else _NOW.astimezone(zone)


@pytest.fixture
async def scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[AdmissionScenario]:
    """Seed actual route, account, credential, command and catalog rows in private SQLite.

    Args:
        tmp_path: Per-test directory for the real isolated database.
        monkeypatch: Clock substitutions without replacing SQL or repository behavior.

    Yields:
        A genuine admission scenario with its unmodified schema and database guards.
    """
    for module in (
        unit_snapshot_submission,
        execution_route_authority_store,
        execution_unit_admission_store,
        execution_unit_catalog,
        native_account_read_store,
    ):
        monkeypatch.setattr(module, "datetime", _Clock)
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'cardinality.db'}")
    try:
        await repository.create_all()
        yield await seed_admission_scenario(repository, _NOW, source="kraken_futures")
    finally:
        await repository.engine.dispose()


async def _retain_future_closed_version(
    scenario: AdmissionScenario, row_model: TemporalModel, keep_current: bool
) -> int:
    """Retain a real finite closure, optionally beside the original exact-current row.

    Args:
        scenario: Actual isolated repository whose initially current row is retained.
        row_model: Temporal table whose current-row query must preserve cardinality.
        keep_current: Whether to keep the sentinel row beside a finite predecessor.

    Returns:
        The original current row's physical identity for independent selection checks.
    """
    async with scenario.repository.session() as session:
        row: TemporalRow = (await session.execute(select(row_model))).scalar_one()
        current_id = row.id
        if keep_current:
            fields: dict[str, object] = {
                column.key: getattr(row, column.key)
                for column in row_model.__table__.columns
                if column.key != "id"
            }
            fields.update(
                timestamp=row.timestamp - timedelta(microseconds=1),
                known_to=_FUTURE_END,
                sequence_id=row.sequence_id + 10000,
            )
            session.add(row_model(**fields))
        else:
            row.known_to = _FUTURE_END
        await session.commit()
    async with scenario.repository.session() as session:
        candidates: int = (
            await session.execute(
                select(func.count()).select_from(row_model).where(row_model.known_to > _NOW)
            )
        ).scalar_one()
        assert candidates == (2 if keep_current else 1)
    return current_id


@pytest.mark.parametrize("keep_current", (True, False), ids=("with-current", "finite-only"))
@pytest.mark.parametrize(
    ("row_model", "reason"),
    (
        pytest.param(Wallet, "route_wallet_missing", id="wallet"),
        pytest.param(WalletCredential, "selected_credential_revision_missing", id="credential"),
        pytest.param(VenueAccountState, "route_current_account_state_missing", id="account-state"),
    ),
)
async def test_route_authority_filters_closed_history_before_cardinality(
    scenario: AdmissionScenario, row_model: TemporalModel, reason: str, keep_current: bool
) -> None:
    """Require the exact-current route rather than counting every future-visible version.

    Given: A future-closed wallet, credential or state and optionally its current successor.
    When: The real route authority reader resolves current ownership and source evidence.
    Then: The successor remains usable, while a finite-only candidate is refused.

    Args:
        scenario: Real isolated account route and source observations.
        row_model: Route-related temporal table containing the finite version.
        reason: Boundary-specific refusal expected when no exact-current row remains.
        keep_current: Whether a genuine current successor also remains available.
    """
    await _retain_future_closed_version(scenario, row_model, keep_current)
    async with scenario.repository.session() as session:
        if not keep_current:
            with pytest.raises(RouteAuthorityBindingError, match=reason):
                await require_current_route_authority(session, scenario.route, _NOW)
            return
        selected = await require_current_route_authority(session, scenario.route, _NOW)
    assert selected == scenario.authority


@pytest.mark.parametrize("keep_current", (True, False), ids=("with-current", "finite-only"))
@pytest.mark.parametrize("row_model", (Instrument, Symbol, InstrumentSpec))
async def test_catalog_join_filters_each_closed_participant_before_cardinality(
    scenario: AdmissionScenario, row_model: TemporalModel, keep_current: bool
) -> None:
    """Bind the actual current catalog instead of multiplying joined historical versions.

    Given: One future-closed join participant and optionally its exact-current counterpart.
    When: Current catalog evidence is loaded through the real three-table join.
    Then: Only current lineage is returned, or missing lineage is explicitly refused.

    Args:
        scenario: Real instrument, symbol and specification rows.
        row_model: Catalog join participant carrying the finite historical version.
        keep_current: Whether the exact-current participant remains available.
    """
    await _retain_future_closed_version(scenario, row_model, keep_current)
    lookup = UnitCatalogLookup(exchange="kraken_futures", native_symbol="BTC-USD")
    async with scenario.repository.session() as session:
        if not keep_current:
            with pytest.raises(SnapshotBindingError, match="unit_catalog_lineage_missing"):
                await load_unit_catalog(session, lookup, _NOW, current=True)
            return
        observation = await load_unit_catalog(session, lookup, _NOW, current=True)
    assert observation.instrument.public_id == admission_id(2)
    assert observation.symbol.public_id == admission_id(1)
    assert observation.spec.public_id == admission_id(4)
    assert all(
        row.known_to == KNOWN_TO_MAX
        for row in (observation.instrument, observation.symbol, observation.spec)
    )


async def test_historical_catalog_query_preserves_actual_range_ambiguity(
    scenario: AdmissionScenario,
) -> None:
    """Do not silently choose a current row when historical range evidence is ambiguous.

    Given: Two genuine specification rows both cover the requested historical horizon.
    When: Historical catalog lineage is requested without a current-only requirement.
    Then: The original SQLAlchemy cardinality failure remains explicit.

    Args:
        scenario: Actual catalog into which the finite predecessor is retained.
    """
    await _retain_future_closed_version(scenario, InstrumentSpec, keep_current=True)
    lookup = UnitCatalogLookup(exchange="kraken_futures", native_symbol="BTC-USD")
    async with scenario.repository.session() as session:
        with pytest.raises(MultipleResultsFound):
            await load_unit_catalog(session, lookup, _NOW, current=False)


@pytest.mark.parametrize("keep_current", (True, False), ids=("with-current", "finite-only"))
@pytest.mark.parametrize(
    ("row_model", "reason"),
    (
        pytest.param(Wallet, "native_read_live_wallet_missing", id="wallet"),
        pytest.param(WalletCredential, "native_read_selected_credential_missing", id="credential"),
    ),
)
async def test_native_read_selects_only_current_public_account_metadata(
    scenario: AdmissionScenario, row_model: TemporalModel, reason: str, keep_current: bool
) -> None:
    """Allocate a real native read only from unclosed wallet and credential ownership.

    Given: Retained future-closed metadata with or without an exact-current selection.
    When: The real fenced native read writer verifies the public client selection.
    Then: One valid current attempt is allocated, or finite-only ownership is refused.

    Args:
        scenario: Real Futures wallet and selected public credential revision.
        row_model: Wallet or credential table containing the finite version.
        reason: Expected explicit refusal when current ownership is absent.
        keep_current: Whether a genuine current selection remains available.
    """
    authority = scenario.authority
    assert authority.credential_public_id is not None
    assert authority.credential_timestamp is not None
    assert authority.public_api_origin is not None
    selection = ExecutionClientSelection(
        wallet_public_id=authority.scope.wallet_public_id,
        exchange=authority.scope.exchange,
        mode=authority.scope.mode,
        environment=authority.scope.environment,
        credential_public_id=authority.credential_public_id,
        credential_timestamp=authority.credential_timestamp,
        public_api_origin=authority.public_api_origin,
    )
    await _retain_future_closed_version(scenario, row_model, keep_current)
    identity = ExecutionEvidenceIdentity(
        public_id=admission_id(500), session_id=admission_id(99), sequence_id=500
    )
    if not keep_current:
        with pytest.raises(NativeAccountReadBindingError, match=reason):
            await begin_native_account_read(scenario.repository, selection, identity)
        return
    attempt = await begin_native_account_read(scenario.repository, selection, identity)
    assert attempt.public_id == identity.public_id
    assert attempt.payload.client_selection == selection
    assert attempt.payload.family_sequence == 1


async def _read_current_command(
    session: AsyncSession,
    scenario: AdmissionScenario,
    request: SnapshotCommandMaterial,
    reader: Literal["submission", "admission"],
) -> TradeCommand:
    """Exercise each actual command selector with its own boundary contract.

    Args:
        session: Caller-owned real SQLite transaction.
        scenario: Actual outgoing execution frame and retained selected route.
        request: Original durable request material without lifecycle fields.
        reader: Admission producer or retained-witness consumer boundary.

    Returns:
        The independently selected actual current durable command.
    """
    if reader == "submission":
        return await unit_snapshot_submission._current_command(
            session, scenario.order, scenario.route, _NOW
        )
    return await execution_unit_admission_store._current_command(session, request, _NOW)


@pytest.mark.parametrize("keep_current", (True, False), ids=("with-current", "finite-only"))
@pytest.mark.parametrize("reader", ("submission", "admission"))
async def test_command_readers_ignore_future_closed_request_versions(
    scenario: AdmissionScenario, reader: Literal["submission", "admission"], keep_current: bool
) -> None:
    """Keep both command proof boundaries independent from historical SQL cardinality.

    Given: Unchanged durable command material in a finite version and optional current row.
    When: Each actual submission/admission reader selects its current command.
    Then: It returns the exact-current physical row or refuses finite-only history.

    Args:
        scenario: Actual durable command and matching outgoing frame.
        reader: Current-command proof boundary being exercised.
        keep_current: Whether the exact-current command row remains present.
    """
    async with scenario.repository.session() as session:
        command = (await session.execute(select(TradeCommand))).scalar_one()
        request = durable_command_material(command)
    current_id = await _retain_future_closed_version(scenario, TradeCommand, keep_current)
    async with scenario.repository.session() as session:
        if not keep_current:
            with pytest.raises(SnapshotBindingError, match=f"{reader}_current_command_missing"):
                await _read_current_command(session, scenario, request, reader)
            return
        selected = await _read_current_command(session, scenario, request, reader)
        assert selected.id == current_id
        assert selected.known_to == KNOWN_TO_MAX
        assert durable_command_material(selected) == request


def test_filtered_cardinality_accepts_no_candidate() -> None:
    """Represent absence without fabricating an eligible temporal row.

    Given: No candidate survives exact-current filtering.
    When: Filtered cardinality is resolved.
    Then: Missing evidence remains explicitly absent.
    """
    assert one_or_none(()) is None


def test_filtered_cardinality_preserves_the_sole_candidate_identity() -> None:
    """Keep the actual surviving row object rather than copying or replacing it.

    Given: Exactly one eligible candidate survives filtering.
    When: Filtered cardinality is resolved.
    Then: Its original object identity is returned unchanged.
    """
    candidate = object()

    assert one_or_none((candidate,)) is candidate


def test_filtered_cardinality_refuses_multiple_surviving_candidates() -> None:
    """Preserve the original database ambiguity contract after temporal filtering.

    Given: Two independently eligible candidates survive the exact-current proof.
    When: The scalar cardinality boundary resolves them.
    Then: SQLAlchemy ambiguity is raised instead of selecting an arbitrary candidate.
    """
    with pytest.raises(MultipleResultsFound):
        one_or_none((object(), object()))


async def test_submission_refuses_two_genuinely_open_commands_for_the_same_client_identity(
    scenario: AdmissionScenario,
) -> None:
    """Preserve SQL ambiguity for distinct current intents sharing a nonunique client identity.

    Given: Two actual CREATE rows have different logical UUIDs but the same venue client identity.
    When: The real submission selector reads both exact-open database candidates.
    Then: It raises SQLAlchemy ambiguity without selecting either command or weakening any guard.

    Args:
        scenario: Actual isolated repository with its original durable CREATE and matching frame.
    """
    async with scenario.repository.session() as session:
        original = (await session.execute(select(TradeCommand))).scalar_one()
        original_public_id = original.public_id
        fields: dict[str, object] = {
            column.key: getattr(original, column.key)
            for column in TradeCommand.__table__.columns
            if column.key != "id"
        }
        fields.update(
            public_id=admission_id(9000),
            idempotency_key=None,
            sequence_id=original.sequence_id + 20000,
        )
        session.add(TradeCommand(**fields))
        await session.commit()
    async with scenario.repository.session() as session:
        candidates = (
            await session.execute(
                select(TradeCommand.public_id, TradeCommand.known_to).where(
                    TradeCommand.client_order_id == scenario.order.client_order_id,
                    TradeCommand.exchange == scenario.route.scope.exchange,
                    TradeCommand.command_type.in_(("create", "submit")),
                    TradeCommand.timestamp <= _NOW,
                )
            )
        ).all()
        assert len(candidates) == 2
        assert {candidate.public_id for candidate in candidates} == {
            original_public_id,
            admission_id(9000),
        }
        assert all(candidate.known_to == KNOWN_TO_MAX for candidate in candidates)
        with pytest.raises(MultipleResultsFound):
            await unit_snapshot_submission._current_command(
                session, scenario.order, scenario.route, _NOW
            )
