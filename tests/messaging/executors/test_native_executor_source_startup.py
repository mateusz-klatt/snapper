"""Exercise real executor startup through native signed source preparation and client cleanup."""

from pathlib import Path
from urllib.parse import urlsplit

import pytest
from sqlalchemy import select

from snapper.config.credentials import CredentialVersion
from snapper.core.json_types import JsonObject
from snapper.core.types import OrderExchange
from snapper.data.models import FuturesInventoryReadAttempt
from snapper.data.models import FuturesInventoryReadOutcome
from snapper.data.models import NativeAccountIdentityProof
from snapper.data.models import NativeAccountReadOutcome
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyTokenError
from snapper.messaging.executors.base import ExchangeExecutorService
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_scenario


class _ReachedRecoveryError(RuntimeError):
    """Stop at the actual recovery boundary before any order or private-stream task starts."""


class _SourceStartupExecutor(ExchangeExecutorService[KrakenFuturesExchangeClient]):
    """Retain actual executor startup while supplying an isolated database and transport."""

    def __init__(self, scenario: NativeAuthorityScenario) -> None:
        """Select the same actual client and public credential version as the local HTTP fixture."""
        super().__init__(scenario.selection.wallet_public_id)
        self.scenario = scenario
        self.recovery_reached = False
        self.observed_token: PreparedKeyToken | None = None

    def _create_exchange_client(self) -> KrakenFuturesExchangeClient:
        """Return the real SDK client whose signed requests use the local adapter.

        Returns:
            Original selected client without replacing preparation, context entry or cleanup.
        """
        return self.scenario.client

    def _get_exchange_name(self) -> OrderExchange:
        """Return the real Futures route used by this executor.

        Returns:
            The actual Futures exchange identifier.
        """
        return "kraken_futures"

    async def _initialize_settings(self) -> None:
        """Supply the real isolated repository without starting an unrelated settings subscriber."""
        self.repository = self.scenario.repository

    async def _resolve_credentials(self, exchange_name: OrderExchange) -> None:
        """Retain the actual fixture's selected public credential revision without decrypting secrets."""
        selected = self.scenario.selection
        assert exchange_name == selected.exchange
        assert selected.credential_public_id is not None
        assert selected.credential_timestamp is not None
        self._credential_version = CredentialVersion(
            public_id=selected.credential_public_id,
            wallet_public_id=selected.wallet_public_id,
            exchange=selected.exchange,
            credential_type="api_key_secret",
            timestamp=selected.credential_timestamp,
        )

    def _setup_zmq_sockets(self, exchange_name: OrderExchange) -> None:
        """Avoid unrelated network endpoints before this witness stops at recovery."""
        assert exchange_name == "kraken_futures"

    async def _recover_pending_orders(self, exchange_name: OrderExchange) -> None:
        """Observe committed preparation at the actual startup boundary, then prevent task creation."""
        self.recovery_reached = True
        client = self._require_exchange_client()
        assert exchange_name == "kraken_futures"
        assert self.running
        assert self._client_context_active
        assert client._market_client is not None
        assert client._private_ws_client is None
        assert client._private_frame_sink_factory is not None
        self.observed_token = client._prepared_private_source_token
        assert self.observed_token is not None
        proof = self.observed_token.require_proof_reference()
        async with self.scenario.repository.session() as session:
            identities = (await session.scalars(select(NativeAccountIdentityProof))).all()
            outcomes = (await session.scalars(select(NativeAccountReadOutcome))).all()
            inventory_attempts = (
                await session.scalars(
                    select(FuturesInventoryReadAttempt).order_by(
                        FuturesInventoryReadAttempt.purpose
                    )
                )
            ).all()
            inventory_outcomes = (await session.scalars(select(FuturesInventoryReadOutcome))).all()
        assert len(identities) == len(outcomes) == 1
        assert identities[0].public_id == proof.public_id
        assert outcomes[0].result_kind == "captured"
        assert [(row.purpose, row.key_proof_public_id) for row in inventory_attempts] == [
            ("kraken_futures.open_orders", proof.public_id),
            ("kraken_futures.open_positions", proof.public_id),
        ]
        assert {
            (row.purpose, row.attempt_public_id, row.result_kind) for row in inventory_outcomes
        } == {(row.purpose, row.public_id, "captured") for row in inventory_attempts}
        raise _ReachedRecoveryError("Actual recovery boundary reached")


def _closed_public_markets() -> JsonObject:
    """Keep actual client connection local while leaving signed private requests unchanged.

    Returns:
        Empty public market inventory, which this startup ordering witness never consumes.
    """
    return {}


@pytest.mark.asyncio
async def test_actual_start_commits_owner_before_recovery_and_closes_on_unwind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Given a real selected client, when startup reaches recovery, then ownership already committed.

    Given:
        Actual native HTTP producers and an isolated SQL repository behind the real executor lifecycle.
    When:
        Startup connects the client and reaches recovery before creating any loop tasks.
    Then:
        Safe account/key evidence and the installed frame sink exist, and unwind closes the token.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as scenario:
        monkeypatch.setattr(scenario.client._ccxt_client, "load_markets", _closed_public_markets)
        executor = _SourceStartupExecutor(scenario)
        try:
            with pytest.raises(_ReachedRecoveryError, match="Actual recovery boundary reached"):
                await executor.start()
            assert executor.recovery_reached
            assert [urlsplit(request.url or "").path for request in scenario.adapter.calls] == [
                "/api/auth/v1/api-keys/v3/check",
                "/derivatives/api/v3/accounts",
                "/derivatives/api/v3/openorders",
                "/derivatives/api/v3/openpositions",
            ]
            assert scenario.adapter.proofs_at_accounts_send == [1]
            assert scenario.client._private_ws_client is None
            assert scenario.client._prepared_private_source_token is None
            assert not executor._client_context_active
            assert executor.observed_token is not None
            with pytest.raises(PreparedKeyTokenError, match="prepared_key_token_closed"):
                executor.observed_token.require_proof_reference()
        finally:
            await executor.stop()


@pytest.mark.asyncio
async def test_actual_start_failed_accounts_never_reaches_recovery_or_private_tasks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Given a failed actual accounts read, when startup prepares ownership, then recovery never starts.

    Given:
        A successful signed key proof followed by a real local HTTP accounts transport failure.
    When:
        The actual connected executor attempts to install its native private source owner.
    Then:
        The read failure is retained, running remains false, and private sockets are never created.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as scenario:
        scenario.adapter.fail_accounts = True
        monkeypatch.setattr(scenario.client._ccxt_client, "load_markets", _closed_public_markets)
        executor = _SourceStartupExecutor(scenario)
        try:
            with pytest.raises(ValueError, match="private_owner_preparation_unsuccessful"):
                await executor.start()
            assert not executor.recovery_reached
            assert not executor.running
            assert not executor._client_context_active
            assert scenario.client._private_ws_client is None
            assert scenario.client._prepared_private_source_token is None
            assert len(scenario.adapter.calls) == 2
            assert [urlsplit(request.url or "").path for request in scenario.adapter.calls] == [
                "/api/auth/v1/api-keys/v3/check",
                "/derivatives/api/v3/accounts",
            ]
            async with scenario.repository.session() as session:
                identities = (await session.scalars(select(NativeAccountIdentityProof))).all()
                outcomes = (await session.scalars(select(NativeAccountReadOutcome))).all()
            assert len(identities) == len(outcomes) == 1
            assert outcomes[0].result_kind == "failed"
        finally:
            await executor.stop()
