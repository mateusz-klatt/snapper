"""Actual observer-cycle identity, retained response refusal and selected-route capture."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import tzinfo
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy import select

import snapper.application.trade.unit_snapshot_route as producer
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import ExecutionRouteSelection
from snapper.data.execution_route_authority_store import RouteAuthorityBindingError
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.models import VenueAccountState
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.unit_admission import AdmissionScenario
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import seed_admission_scenario

_NOW = datetime(2026, 10, 23, 6, tzinfo=UTC)


class _Clock:
    """Keep the producer clock local without patching any global standard-library module."""

    @staticmethod
    def now(timezone: tzinfo | None = None) -> datetime:
        """Return a bounded actual capture instant after the original observation."""
        at = _NOW + timedelta(seconds=1)
        return at if timezone is None else at.astimezone(timezone)


@pytest.fixture
async def scenario(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> AsyncIterator[AdmissionScenario]:
    """Provide a real current observer response and selected credential metadata."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'admission.db'}")
    monkeypatch.setattr(producer, "datetime", _Clock)
    try:
        await repository.create_all()
        yield await seed_admission_scenario(repository, _NOW)
    finally:
        await repository.engine.dispose()


def _selection(scenario: AdmissionScenario) -> ExecutionRouteSelection:
    """Carry public metadata from the actual client rather than another credential lookup."""
    authority = scenario.authority
    return ExecutionRouteSelection(
        scope=authority.scope,
        credential_public_id=authority.credential_public_id,
        credential_timestamp=authority.credential_timestamp,
        public_api_origin=authority.public_api_origin,
    )


def _identity() -> ExecutionEvidenceIdentity:
    """Give the observer capture an independent immutable record identity."""
    return ExecutionEvidenceIdentity(
        public_id=admission_id(194), session_id=admission_id(99), sequence_id=194
    )


async def _state_id(scenario: AdmissionScenario) -> int:
    """Read the exact actual state identity returned by the isolated observer cycle."""
    async with scenario.repository.session() as session:
        return (await session.execute(select(VenueAccountState.id))).scalar_one()


async def test_same_observer_cycle_capture_preserves_source_clocks_and_material(
    scenario: AdmissionScenario,
) -> None:
    """Actual route capture cannot refresh the response's own balance time or authority window.

    Given: A just-persisted observer cycle through the actual selected client,
    When: The producer records its account-route witness,
    Then: The exact source response and bounded authority window survive unchanged.
    """
    result = await producer.capture_observed_execution_route(
        scenario.repository, _selection(scenario), await _state_id(scenario), _identity()
    )
    assert result.result.created is True
    assert result.result.public_id == admission_id(194)
    assert result.reference.authority_public_id == result.result.public_id
    assert result.reference.route_revision == scenario.route.route_revision
    assert result.payload.balance_receipt_json == scenario.authority.balance_receipt_json
    assert result.payload.balance_observed_at == scenario.authority.balance_observed_at
    assert result.payload.valid_until == scenario.authority.valid_until
    assert result.payload.observed_at == _NOW + timedelta(seconds=1)
    assert result.payload.venue_account_id is None


@pytest.mark.parametrize("condition", ("missing", "closed", "crossed"))
async def test_observer_cycle_identity_must_be_actual_current_selected_scope(
    scenario: AdmissionScenario, condition: str
) -> None:
    """A caller cannot substitute an arbitrary older or foreign account response.

    Given: A missing, closed or foreign-wallet state id,
    When: It is presented as the actual observer cycle,
    Then: Route capture refuses rather than selecting any last-good balance.
    """
    state_id = await _state_id(scenario)
    if condition == "missing":
        state_id += 1000
    else:
        async with scenario.repository.session() as session:
            state = (await session.execute(select(VenueAccountState))).scalar_one()
            if condition == "closed":
                state.known_to = _NOW - timedelta(seconds=1)
            else:
                state.wallet_public_id = admission_id(77)
            await session.commit()
    with pytest.raises(RouteAuthorityBindingError, match="actual_observer_state_identity_unproven"):
        await producer.capture_observed_execution_route(
            scenario.repository, _selection(scenario), state_id, _identity()
        )


async def test_retained_balance_never_masquerades_as_the_actual_new_response(
    scenario: AdmissionScenario,
) -> None:
    """A failed new attempt cannot use a prior successful payload as a new client witness.

    Given: A state pointing at an older retained balance source,
    When: Its current attempt id is offered to route capture,
    Then: The retained response is refused without extending its authority.
    """
    state_id = await _state_id(scenario)
    async with scenario.repository.session() as session:
        state = (await session.execute(select(VenueAccountState))).scalar_one()
        state.sync_status = "error"
        state.balance_status = "error"
        state.current_attempt_observation_id += 1000
        await session.commit()
    with pytest.raises(RouteAuthorityBindingError, match="response_not_fresh"):
        await producer.capture_observed_execution_route(
            scenario.repository, _selection(scenario), state_id, _identity()
        )


async def test_missing_durable_response_is_not_invented_from_current_state(
    scenario: AdmissionScenario,
) -> None:
    """A corrupted current-state pointer cannot manufacture a successful account observation.

    Given: Consistent-looking state pointers whose actual observation is absent,
    When: The source witness is captured,
    Then: Missing durable evidence is explicit and no new authority commits.
    """
    state_id = await _state_id(scenario)
    async with scenario.repository.session() as session:
        state = (await session.execute(select(VenueAccountState))).scalar_one()
        state.current_attempt_observation_id += 1000
        state.balance_payload_source_observation_id = state.current_attempt_observation_id
        await session.commit()
    with pytest.raises(RouteAuthorityBindingError, match="actual_observer_response_missing"):
        await producer.capture_observed_execution_route(
            scenario.repository, _selection(scenario), state_id, _identity()
        )
    async with scenario.repository.session() as session:
        count = (
            await session.execute(select(func.count()).select_from(ExecutionRouteAuthority))
        ).scalar_one()
    assert count == 1


async def test_missing_source_authority_window_is_not_filled_with_a_default(
    scenario: AdmissionScenario,
) -> None:
    """Only an actual source validity window can bound the captured financial authority.

    Given: A degraded state with no authoritative window,
    When: Route capture copies its actual source evidence,
    Then: Missing validity is refused rather than replaced with a fresh default TTL.
    """
    state_id = await _state_id(scenario)
    async with scenario.repository.session() as session:
        state = (await session.execute(select(VenueAccountState))).scalar_one()
        state.sync_status = "error"
        state.balance_status = "error"
        state.authoritative_until = None
        await session.commit()
    with pytest.raises(ValidationError):
        await producer.capture_observed_execution_route(
            scenario.repository, _selection(scenario), state_id, _identity()
        )
