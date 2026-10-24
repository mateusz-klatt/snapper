"""Actual executor/client identity binding and same-response authority integration."""

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import select

from snapper.config.credentials import CredentialVersion
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionRouteSelection
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import VenueAccountState
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.contracts import ExecutionApiRoute
from snapper.infrastructure.exchanges.contracts import ExecutionObservationContext
from snapper.infrastructure.exchanges.contracts import NativeBalanceEntry
from snapper.infrastructure.exchanges.contracts import NativeBalanceObservation
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.messaging.executors import base as executor_module
from snapper.messaging.executors.base import ExchangeExecutorService
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import seed_admission_scenario
from tests.infrastructure.exchanges.test_exchange_base import DummyExchangeClient

_NOW = datetime(2026, 10, 23, 9, tzinfo=UTC)


class _Client(DummyExchangeClient):
    """Exercise the real client base over a clearly isolated transport test double."""

    route: ExecutionApiRoute | None = ExecutionApiRoute("production", "https://api.kraken.com")

    def execution_api_route(self) -> ExecutionApiRoute | None:
        """Return the explicitly injected isolated transport endpoint."""
        return self.route

    async def read_native_balances(self) -> list[NativeBalanceEntry]:
        """Return a native test response without any implicit account-route authority."""
        return [NativeBalanceEntry("USD", 10.0, 10.0, 0.0)]


class _Executor(ExchangeExecutorService[_Client]):
    """Use actual executor integration with isolated client endpoints and database."""

    def _get_exchange_name(self) -> OrderExchange:
        """Return the exact selected client venue."""
        return ExchangeEnum.KRAKEN

    def _create_exchange_client(self) -> _Client:
        """Create a transport double without credentials, network or subscriptions."""
        return _Client(exchange_name="kraken")


def _credential() -> CredentialVersion:
    """Supply only the public selected credential revision."""
    return CredentialVersion(admission_id(2), admission_id(1), "kraken", "api_key_secret", _NOW)


def _executor(repository: SQLAlchemyRepository) -> _Executor:
    """Initialize real lifecycle state with isolated settings and an actual source writer."""
    with patch.object(executor_module, "get_settings", return_value=MagicMock()):
        executor = _Executor(wallet_public_id=admission_id(1))
    executor.repository = repository
    executor.exchange_client = _Client(exchange_name="kraken")
    executor._credential_version = _credential()
    return executor


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated physical schema including immutable financial receipt guards."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'executor.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


@pytest.mark.parametrize("missing", ("wallet", "credential", "route"))
def test_unproven_actual_client_metadata_never_yields_selection(missing: str) -> None:
    """Incomplete actual client provenance cannot produce an execution selection.

    Given: A client missing its wallet, selected credential revision or proven transport route,
    When: The executor asks for its actual client selection,
    Then: It returns no selection and the default adapter supplies no invented route.
    """
    client = _Client(exchange_name="kraken")
    if missing != "wallet":
        client.bind_execution_identity(
            admission_id(1), None if missing == "credential" else _credential()
        )
    if missing == "route":
        client.route = None
    assert client.execution_client_selection() is None
    assert DummyExchangeClient().execution_api_route() is None


@pytest.mark.parametrize("field", ("wallet_public_id", "exchange"))
def test_foreign_public_credential_cannot_be_bound_to_a_client(field: str) -> None:
    """A credential from another wallet or venue cannot label this instantiated client.

    Given: Public credential metadata whose wallet or exchange contradicts the client,
    When: The executor tries to bind that credential revision,
    Then: Binding refuses and no execution selection becomes available.
    """
    credential = _credential()
    changed = replace(
        credential, **{field: admission_id(33) if field == "wallet_public_id" else "paper"}
    )
    client = _Client(exchange_name="kraken")
    with pytest.raises(ValueError, match="scope_mismatch"):
        client.bind_execution_identity(admission_id(1), changed)
    assert client.execution_client_selection() is None


def test_client_identity_cannot_be_relabelled_after_instantiation() -> None:
    """An existing SDK identity cannot be relabelled with a later public credential revision.

    Given: A client already bound to one actual credential revision,
    When: The same binding repeats and a different credential identifier is then supplied,
    Then: Repetition is harmless but relabelling refuses and the original selection remains.
    """
    client = _Client(exchange_name="kraken")
    original = _credential()
    client.bind_execution_identity(admission_id(1), original)
    client.bind_execution_identity(admission_id(1), original)
    with pytest.raises(ValueError, match="already_bound"):
        client.bind_execution_identity(
            admission_id(1), replace(original, public_id=admission_id(4))
        )
    selection = client.execution_client_selection()
    assert selection is not None
    assert selection.credential_public_id == original.public_id


def test_simulated_client_has_an_explicit_distinct_environment() -> None:
    """Paper execution retains an explicit simulated environment without remote identity claims.

    Given: A local Paper client with an explicit simulated route and wallet,
    When: Its actual execution selection is projected,
    Then: Mode and environment are paper and remote credential/origin fields remain absent.
    """
    client = _Client(exchange_name="paper")
    client.route = ExecutionApiRoute("paper", None)
    client.bind_execution_identity(admission_id(1), None)
    selection = client.execution_client_selection()
    assert selection is not None
    assert selection.environment == selection.mode == "paper"
    assert selection.public_api_origin is selection.credential_public_id is None


async def test_installed_executor_sink_commits_actual_callback_before_return(
    repository: SQLAlchemyRepository,
) -> None:
    """The installed executor sink commits actual callback evidence before acknowledging capture.

    Given: A real initialized executor, isolated database and uninterpretable private observation,
    When: Its client awaits the installed durable capture callback,
    Then: The returned UUID identifies a committed receipt with the selected wallet and credential.
    """
    executor = _executor(repository)
    executor._install_execution_observation_capture()
    client = executor._require_exchange_client()
    source = PrivateExecutionObservation(
        ExecutionObservationContext("unknown", "executions", _NOW), "opaque_text", "bad finance"
    )
    public_id = await client._capture_private_execution_observation(source)
    async with repository.session() as session:
        receipt = (await session.execute(select(AccountingRawReceipt))).scalar_one()
    observation = RawAccountingObservation.model_validate_json(receipt.canonical_payload)
    assert receipt.public_id == public_id
    assert observation.scope.wallet_public_id == executor.wallet_public_id
    assert observation.scope.environment == "production"
    assert observation.credential_public_id == _credential().public_id


async def test_legacy_single_balance_read_never_manufactures_authority() -> None:
    """A legacy balance response remains unscoped evidence rather than account authority.

    Given: A client whose native reader returns balances without a same-response route proof,
    When: The base observation wrapper performs that read,
    Then: Its entries survive and no account-route authority is manufactured.
    """
    client = _Client(exchange_name="kraken")
    result = await client.read_native_balance_observation()
    assert result == NativeBalanceObservation([NativeBalanceEntry("USD", 10.0, 10.0, 0.0)])
    assert result.route is None


async def test_same_actual_observer_cycle_is_captured_and_failed_next_scope_clears_it(
    repository: SQLAlchemyRepository,
) -> None:
    """A later scope failure invalidates authority captured from an earlier actual observer cycle.

    Given: A persisted native observation bound to the selected actual client and route,
    When: The same observer cycle is captured and a subsequent transport route disagrees,
    Then: The first proof references its actual observation and the failed capture clears it.
    """
    scenario = await seed_admission_scenario(repository, datetime.now(UTC))
    executor = _executor(repository)
    actual = client_selection_from_authority(scenario.authority)
    assert actual.credential_public_id is not None
    assert actual.credential_timestamp is not None
    executor.wallet_public_id = actual.wallet_public_id
    executor._credential_version = CredentialVersion(
        actual.credential_public_id,
        actual.wallet_public_id,
        actual.exchange,
        "api_key_secret",
        actual.credential_timestamp,
    )
    executor._install_execution_observation_capture()
    selection = ExecutionRouteSelection(
        scope=scenario.authority.scope,
        credential_public_id=actual.credential_public_id,
        credential_timestamp=actual.credential_timestamp,
        public_api_origin=actual.public_api_origin,
    )
    async with repository.session() as session:
        state_id = (await session.execute(select(VenueAccountState.id))).scalar_one()
    await executor._capture_actual_balance_route(repository, state_id, selection)
    captured = executor._current_execution_route()
    assert captured is not None
    assert (
        captured.payload.account_observation_public_id
        == scenario.authority.account_observation_public_id
    )
    executor._require_exchange_client().route = ExecutionApiRoute(
        "demo", "https://demo.example.com"
    )
    await executor._capture_actual_balance_route(repository, state_id, selection)
    assert executor._current_execution_route() is None
