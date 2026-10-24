"""Keep valuation, executor selection and private ownership boundaries fail closed."""

from collections.abc import Awaitable
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import snapper.application.portfolio.futures_reconciliation as reconciliation
import snapper.application.pricing.usd_converter as pricing
import snapper.infrastructure.exchanges.implementations.kraken_futures as futures
from snapper.application.pricing.usd_converter import PriceUnavailableError
from snapper.application.pricing.usd_converter import USDConverter
from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionRouteSelection
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import InstrumentValuationMetadataRow
from snapper.infrastructure.exchanges.contracts import ExecutionApiRoute
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_ETL_VERSION
from snapper.messaging.executors import base as executor_module
from tests.application.portfolio.test_futures_reconciliation import _NOW as _RECONCILIATION_TIME
from tests.application.portfolio.test_futures_reconciliation import _spec as _reconciliation_spec
from tests.application.pricing.test_derivative_usd_converter import _INSTRUMENT
from tests.application.pricing.test_derivative_usd_converter import _NOW as _PRICE_TIME
from tests.application.pricing.test_derivative_usd_converter import _market
from tests.application.pricing.test_derivative_usd_converter import _spec as _valuation_spec
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.unit_admission import admission_id
from tests.messaging.executors.test_executor_execution_evidence import _Client
from tests.messaging.executors.test_executor_execution_evidence import _credential
from tests.messaging.executors.test_executor_execution_evidence import _Executor
from tests.messaging.executors.test_executor_execution_evidence import _executor


def _certified_boundary(spec: InstrumentSpecRow, evaluated_at: datetime) -> bool:
    """Model an upstream certificate without granting the caller incompatible units.

    Args:
        spec: Contradictory catalog boundary document.
        evaluated_at: Actual explicit evaluation clock.

    Returns:
        The upstream certificate result whose local unit checks remain mandatory.
    """
    assert spec["unit_certified"] and evaluated_at.utcoffset() is not None
    return True


def test_reconciliation_refuses_noncontract_units_despite_upstream_certificate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Require the reconciler's own quantity convention.

    Given: An upstream certificate accepts a specification with base-asset units.
    When: Contract-position comparison selects its certified lot step.
    Then: The reconciler refuses incompatible units instead of comparing unlike quantities.

    Args:
        monkeypatch: Isolated upstream certification boundary override.
    """
    monkeypatch.setattr(reconciliation, "is_effective_unit_certified", _certified_boundary)
    spec = _reconciliation_spec(quantity_unit="base_asset")
    assert reconciliation._certified_lot_step(spec, _RECONCILIATION_TIME, Decimal(1)) == (
        None,
        "mismatched_unit",
    )


def test_contract_valuation_refuses_unknown_native_units_despite_upstream_certificate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep a certificate from inventing a native quantity conversion.

    Given: Explicit payoff metadata names an unsupported native quantity unit.
    When: An upstream certificate accepts it but monetary interpretation runs independently.
    Then: No contract model or spot fallback is produced.

    Args:
        monkeypatch: Isolated upstream certification boundary override.
    """
    monkeypatch.setattr(pricing, "is_effective_unit_certified", _certified_boundary)
    spec = _reconciliation_spec(contract_size=Decimal(1))
    spec["spec_version"] = f"{FUTURES_SPEC_ETL_VERSION}:boundary"
    spec["native_order_quantity_unit"] = "unproven_native_unit"
    spec["contract_payoff_model"] = "inverse"
    spec["contract_value_currency"] = "USD"
    spec["settlement_currency"] = "BTC"
    metadata = InstrumentValuationMetadataRow(
        exchange="kraken_futures", base="BTC", quote="USD", spec=spec
    )
    with pytest.raises(PriceUnavailableError, match="contract_valuation_unavailable"):
        pricing._contract_model(metadata, _RECONCILIATION_TIME, "unproven-contract")


@pytest.mark.parametrize("change", ("missing", "quote"))
async def test_price_identity_change_between_actual_reads_never_enters_cache(
    monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    """Refuse a market identity that disappears or changes during valuation.

    Given: Real SQL resolves a USD quote and snapshot before a later metadata read changes.
    When: Public valuation reaches the contradictory metadata boundary.
    Then: It reports missing instrument identity and a subsequent genuine SQL read succeeds.

    Args:
        monkeypatch: Scoped final metadata-read boundary override.
        change: Disappearance or quote-currency replacement at that boundary.
    """
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    try:
        await _market(repository, _valuation_spec("inverse"))
        metadata = await repository.get_active_instrument_valuation_metadata(_INSTRUMENT)
        assert metadata is not None
        metadata["quote"] = "EUR"
        converter = USDConverter(repository, now=lambda: _PRICE_TIME)
        with monkeypatch.context() as scoped:
            scoped.setattr(
                repository,
                "get_active_instrument_valuation_metadata",
                AsyncMock(return_value=None if change == "missing" else metadata),
            )
            with pytest.raises(PriceUnavailableError, match="instrument_not_found"):
                await converter.to_usd(_INSTRUMENT, Decimal(1))
        assert await converter.to_usd(_INSTRUMENT, Decimal(1)) == Decimal(1)
    finally:
        await repository.engine.dispose()


@pytest.mark.parametrize("failure", ("scope", "client", "client_revision", "credential"))
async def test_executor_balance_response_requires_its_actual_selected_credential(
    failure: Literal["scope", "client", "client_revision", "credential"],
) -> None:
    """Refuse a response whose scope, actual client or selected credential differs.

    Given: A real executor and client carry an explicit isolated credential revision.
    When: One response identity coordinate no longer matches that exact selection.
    Then: The relevant authority guard refuses before the response can grant a route.

    Args:
        failure: Independent executor, actual-client or selected-credential boundary.
    """
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    try:
        executor = _executor(repository)
        client = executor._require_exchange_client()
        if failure != "client":
            client.bind_execution_identity(admission_id(1), _credential())
        selection = ExecutionRouteSelection(
            scope=ExecutionAccountScope(
                wallet_public_id=admission_id(3) if failure == "scope" else admission_id(1),
                exchange="kraken",
                mode="live",
                environment="production",
                margin_account="spot.cash",
            ),
            credential_public_id=(
                admission_id(4) if failure == "client_revision" else _credential().public_id
            ),
            credential_timestamp=_credential().timestamp,
            public_api_origin="https://api.kraken.com",
        )
        if failure == "credential":
            executor._credential_version = None
        expected = {
            "scope": "executor_mismatch",
            "client": "client_mismatch",
            "client_revision": "client_mismatch",
            "credential": "selected_credential_mismatch",
        }
        if failure == "client_revision":
            actual = client.execution_client_selection()
            assert actual is not None
            assert actual != client_selection_from_authority(selection)
        with pytest.raises(ValueError, match=expected[failure]):
            executor._require_selected_balance_route(selection)
    finally:
        await repository.engine.dispose()


class _PaperRouteExecutor(_Executor):
    """Exercise the real route guard with a coherent isolated paper client selection."""

    def _get_exchange_name(self) -> OrderExchange:
        """Return the actual simulated venue for the executor's scope comparison."""
        return ExchangeEnum.PAPER

    def _create_exchange_client(self) -> _Client:
        """Build a local client whose actual public route is explicitly simulated."""
        client = _Client(exchange_name="paper")
        client.route = ExecutionApiRoute("paper", None)
        return client


def test_matching_paper_client_route_requires_no_authenticated_credential_revision() -> None:
    """Accept a matching simulated route without inventing authenticated venue credentials.

    Given: A real base executor and client carry the same explicit paper wallet selection.
    When: The matching simulated balance response is checked without a selected credential.
    Then: The paper route passes without applying the authenticated credential requirement.
    """
    with patch.object(executor_module, "get_settings", return_value=MagicMock()):
        executor = _PaperRouteExecutor(wallet_public_id=admission_id(1))
    client = executor._create_exchange_client()
    client.bind_execution_identity(admission_id(1), None)
    executor.exchange_client = client
    executor._credential_version = None
    selection = ExecutionRouteSelection(
        scope=ExecutionAccountScope(
            wallet_public_id=admission_id(1),
            exchange="paper",
            mode="paper",
            environment="paper",
            margin_account="spot.cash",
        ),
        credential_public_id=None,
        credential_timestamp=None,
        public_api_origin=None,
    )
    assert client.execution_client_selection() == client_selection_from_authority(selection)
    executor._require_selected_balance_route(selection)
    assert executor._credential_version is None


async def test_private_sink_cannot_be_replaced_and_unstarted_runtime_has_no_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve one installed frame writer and refuse absent physical runtime.

    Given: A genuine selected client has an installed original capture factory.
    When: Another factory attempts replacement before any private socket exists.
    Then: Replacement refuses, the first factory remains, and no physical runtime is invented.

    Args:
        tmp_path: Isolated actual SQLite and signed-HTTP source directory.
        monkeypatch: Scoped source transport and clock lifetime.
    """

    async def capture(observation: PrivateExecutionObservation) -> str | None:
        """Preserve the explicit unused child boundary without creating source authority."""
        assert observation is not None
        return None

    def factory(
        ticket: object | None, wire: bytes | None
    ) -> Callable[[PrivateExecutionObservation], Awaitable[str | None]]:
        """Expose the original uninvoked frame capture factory."""
        assert ticket is None and wire is None
        return capture

    def replacement(
        ticket: object | None, wire: bytes | None
    ) -> Callable[[PrivateExecutionObservation], Awaitable[str | None]]:
        """Expose a distinct competing writer without invoking either producer."""
        return factory(ticket, wire)

    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        source.client.set_private_frame_observation_sink(factory)
        with pytest.raises(RuntimeError, match="another writer"):
            source.client.set_private_frame_observation_sink(replacement)
        assert source.client._private_frame_sink_factory is factory
        with pytest.raises(RuntimeError, match="runtime is unavailable"):
            source.client._current_private_runtime_selection()


async def test_rebinding_the_same_actual_prepared_owner_preserves_its_live_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep an idempotent pre-socket handoff from closing its own issued token.

    Given: A real signed key/accounts preparation commits its actual same-operation proof.
    When: The selected client receives the same issued owner twice before socket startup.
    Then: Its token remains usable and still names the original committed proof.

    Args:
        tmp_path: Isolated actual SQLite and signed-HTTP source directory.
        monkeypatch: Scoped source transport and clock lifetime.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        try:
            token = prepared.require_token()
            original = token.require_proof_reference()
            source.client.bind_prepared_private_owner(token)
            source.client.bind_prepared_private_owner(token)
            assert token.require_proof_reference() == original
            assert source.client._prepared_private_source_token is token
        finally:
            source.client._clear_prepared_private_owner()
            prepared.close()


async def test_unresolved_poisoned_private_close_blocks_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain physical ownership until the close boundary proves release.

    Given: An authenticated client holds a poisoned private socket whose close is unresolved.
    When: Reconnection attempts to replace that socket.
    Then: It refuses and preserves the original slot without constructing another SDK client.

    Args:
        monkeypatch: Isolated SDK lifecycle boundary overrides without network startup.
    """
    client = futures.KrakenFuturesExchangeClient(
        api_key="synthetic-key", api_secret="c3ludGhldGljLXNlY3JldA=="
    )
    poisoned = MagicMock(spec=futures.FuturesWSClient)
    poisoned.exception_occur = True
    poisoned.close = AsyncMock()
    constructor = MagicMock()
    monkeypatch.setattr(futures, "FuturesWSClient", constructor)
    monkeypatch.setattr(futures, "authenticated_private_close_resolved", lambda actual: False)
    client._private_ws_client = poisoned
    try:
        with pytest.raises(RuntimeError, match="close remains unresolved"):
            await client._ensure_private_ws_connected()
        poisoned.close.assert_awaited_once()
        constructor.assert_not_called()
        assert client._private_ws_client is poisoned
    finally:
        client._private_ws_client = None
        await client.disconnect()
