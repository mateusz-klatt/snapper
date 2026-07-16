"""Tests for the wallet credential management routes.

Covers:

- ``GET /wallets/{id}/credentials`` — list summaries (no encrypted
  payload in the response).
- ``POST /wallets/{id}/credentials`` — create with Fernet encryption
  + payload field validation + 409 conflict mapping.
- ``POST /wallets/{id}/credentials/{cid}/rotate`` — SCD2 rotation
  + 404 when source credential is missing.

All tests invoke the handlers directly with mocked repository +
mocked encryption service to isolate the route logic.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from pydantic import ValidationError

from snapper.api.schemas.multi_tenant import CreateCredentialBody
from snapper.api.schemas.multi_tenant import CreateCredentialCommand
from snapper.api.schemas.multi_tenant import RotateCredentialBody
from snapper.api.schemas.multi_tenant import RotateCredentialCommand
from snapper.api.schemas.multi_tenant import SetCredentialReconciliationMethodBody
from snapper.api.schemas.multi_tenant import SetCredentialReconciliationMethodCommand
from snapper.application.portfolio.reconciliation_methods import RealPortfolioReconciliationMethod
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import CredentialConflictError
from snapper.data.repository import CredentialNotFoundError
from snapper.data.repository import ReconciliationMethodImmutableError
from snapper.data.repository_types import PortfolioReconciliationMethodConfigRow
from snapper.data.repository_types import WalletCredentialRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.credential_routes import create_credential
from snapper.server.credential_routes import list_credentials
from snapper.server.credential_routes import rotate_credential
from snapper.server.credential_routes import set_credential_reconciliation_method


def _cred_row(
    public_id: str = "cred-1",
    wallet_public_id: str = "wallet-42",
    exchange: str = "kraken",
    credential_type: str = "api_key_secret",
    label: str | None = "main key",
) -> WalletCredentialRow:
    """Minimal ``WalletCredentialRow`` fixture."""
    return WalletCredentialRow(
        public_id=public_id,
        wallet_public_id=wallet_public_id,
        exchange=exchange,
        credential_type=credential_type,
        encrypted_payload="gAAAAABencrypted",
        label=label,
        timestamp=datetime.now(UTC),
        session_id="test-sid",
        sequence_id=1,
    )


def _method_config_row(
    method: RealPortfolioReconciliationMethod = "futures_position",
) -> PortfolioReconciliationMethodConfigRow:
    """Minimal active method-config row fixture."""
    return PortfolioReconciliationMethodConfigRow(
        wallet_public_id="wallet-42",
        exchange="kraken_futures",
        mode="live",
        method=method,
        classified_after_observation_id=None,
        public_id="method-config-1",
        timestamp=datetime.now(UTC),
        session_id="test-sid",
        sequence_id=2,
    )


def _method_command(
    method: RealPortfolioReconciliationMethod,
) -> SetCredentialReconciliationMethodCommand:
    """Build one reconciliation-method command envelope."""
    return SetCredentialReconciliationMethodCommand(
        session_id="test-sid",
        sequence_id=1,
        public_id="cmd-pid",
        timestamp=datetime.now(UTC),
        payload=SetCredentialReconciliationMethodBody(reconciliation_method=method),
    )


def _make_request() -> Request:
    """Return a ``Request`` mock with a real ``SequenceTracker``."""
    mock_request = MagicMock(spec=Request)
    mock_request.app.state.rest_tracker = SequenceTracker()
    return mock_request


def _admin_principal() -> AuthPrincipal:
    return AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id="00000000-0000-7000-8000-000000000099",
    )


class TestListCredentials:
    """Behaviour of ``list_credentials`` GET handler."""

    @pytest.mark.asyncio
    async def test_returns_summaries_without_encrypted_payload(self) -> None:
        """Response carries label/exchange/type but NOT encrypted_payload.

        Given: A wallet with one credential row,
        When: ``list_credentials`` is called,
        Then: The summary projection has no ``encrypted_payload`` field
            and the response payload count is 1.
        """
        mock_repo = AsyncMock()
        mock_repo.list_wallet_credentials_for_wallet = AsyncMock(return_value=[_cred_row()])

        result = await list_credentials(
            request=_make_request(),
            _principal=_admin_principal(),
            wallet_public_id="wallet-42",
            repo=mock_repo,
        )

        assert result.count == 1
        summary = result.payload[0]
        assert summary.exchange == "kraken"
        assert summary.credential_type == "api_key_secret"
        assert summary.label == "main key"
        assert not hasattr(summary, "encrypted_payload")

    @pytest.mark.asyncio
    async def test_empty_wallet_returns_empty_list(self) -> None:
        """Wallet with no credentials returns an empty list."""
        mock_repo = AsyncMock()
        mock_repo.list_wallet_credentials_for_wallet = AsyncMock(return_value=[])

        result = await list_credentials(
            request=_make_request(),
            _principal=_admin_principal(),
            wallet_public_id="wallet-42",
            repo=mock_repo,
        )

        assert result.count == 0


class TestCreateCredential:
    """Behaviour of ``create_credential`` POST handler."""

    @pytest.mark.asyncio
    async def test_happy_path_encrypts_and_inserts(self) -> None:
        """Successful create encrypts the payload and returns a summary.

        Given: A valid api_key_secret command with plaintext fields,
        When: ``create_credential`` is called,
        Then: The encryption service is called with the JSON-serialized
            payload, the repo receives the ciphertext, and the response
            returns a ``CredentialSummary`` (no payload).
        """
        mock_repo = AsyncMock()
        mock_repo.create_wallet_credential = AsyncMock(return_value=_cred_row())
        mock_encryption = MagicMock()
        mock_encryption.encrypt.return_value = "gAAAAABencrypted"

        command = CreateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=CreateCredentialBody(
                exchange="kraken",
                credential_type="api_key_secret",
                reconciliation_method="spot_execution_replay",
                credential_payload={"api_key": "k", "api_secret": "s"},
                label="main key",
            ),
        )
        with patch(
            "snapper.server.credential_routes.get_encryption_service",
            return_value=mock_encryption,
        ):
            result = await create_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        mock_encryption.encrypt.assert_called_once()
        mock_repo.create_wallet_credential.assert_awaited_once()
        call_kwargs = mock_repo.create_wallet_credential.await_args.kwargs
        assert call_kwargs["encrypted_payload"] == "gAAAAABencrypted"
        assert call_kwargs["wallet_public_id"] == "wallet-42"
        assert call_kwargs["reconciliation_method"] == "spot_execution_replay"
        assert result.payload.exchange == "kraken"

    @pytest.mark.asyncio
    async def test_missing_payload_fields_rejected_with_400(self) -> None:
        """api_key_secret missing api_secret -> 400."""
        mock_repo = AsyncMock()
        mock_encryption = MagicMock()
        command = CreateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=CreateCredentialBody(
                exchange="kraken",
                credential_type="api_key_secret",
                reconciliation_method="unclassified",
                credential_payload={"api_key": "k"},
                label=None,
            ),
        )
        with (
            patch(
                "snapper.server.credential_routes.get_encryption_service",
                return_value=mock_encryption,
            ),
            pytest.raises(HTTPException) as excinfo,
        ):
            await create_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "api_secret" in str(excinfo.value.detail)
        mock_repo.create_wallet_credential.assert_not_called()

    @pytest.mark.asyncio
    async def test_duplicate_exchange_maps_to_409(self) -> None:
        """CredentialConflictError -> HTTP 409."""
        mock_repo = AsyncMock()
        mock_repo.create_wallet_credential = AsyncMock(
            side_effect=CredentialConflictError(
                wallet_public_id="wallet-42",
                exchange="kraken",
                reason="duplicate",
            )
        )
        mock_encryption = MagicMock()
        mock_encryption.encrypt.return_value = "gAAAAABencrypted"
        command = CreateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=CreateCredentialBody(
                exchange="kraken",
                credential_type="api_key_secret",
                reconciliation_method="spot_execution_replay",
                credential_payload={"api_key": "k", "api_secret": "s"},
                label=None,
            ),
        )
        with (
            patch(
                "snapper.server.credential_routes.get_encryption_service",
                return_value=mock_encryption,
            ),
            pytest.raises(HTTPException) as excinfo,
        ):
            await create_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_409_CONFLICT

    @pytest.mark.asyncio
    async def test_paper_credential_requires_initial_balance(self) -> None:
        """Paper type missing initial_balance -> 400."""
        mock_repo = AsyncMock()
        command = CreateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=CreateCredentialBody(
                exchange="paper",
                credential_type="paper",
                reconciliation_method="unclassified",
                credential_payload={},
                label=None,
            ),
        )
        mock_encryption = MagicMock()
        with (
            patch(
                "snapper.server.credential_routes.get_encryption_service",
                return_value=mock_encryption,
            ),
            pytest.raises(HTTPException) as excinfo,
        ):
            await create_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "initial_balance" in str(excinfo.value.detail)

    @pytest.mark.asyncio
    async def test_paper_unclassified_creation_inserts_no_method_config(self) -> None:
        """Paper creation accepts only the no-config unclassified selection."""
        row = _cred_row(exchange="paper", credential_type="paper")
        mock_repo = AsyncMock()
        mock_repo.create_wallet_credential = AsyncMock(return_value=row)
        mock_encryption = MagicMock()
        mock_encryption.encrypt.return_value = "gAAAAABencrypted"
        command = CreateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=CreateCredentialBody(
                exchange="paper",
                credential_type="paper",
                reconciliation_method="unclassified",
                credential_payload={"initial_balance": "1000"},
                label=None,
            ),
        )

        with patch(
            "snapper.server.credential_routes.get_encryption_service",
            return_value=mock_encryption,
        ):
            result = await create_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        assert result.payload.exchange == "paper"
        kwargs = mock_repo.create_wallet_credential.await_args.kwargs
        assert kwargs["reconciliation_method"] == "unclassified"

    @pytest.mark.asyncio
    async def test_rejects_real_method_outside_concrete_adapter_policy(self) -> None:
        """Kraken Futures cannot be configured with a spot method."""
        mock_repo = AsyncMock()
        command = CreateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=CreateCredentialBody(
                exchange="kraken_futures",
                credential_type="api_key_secret",
                reconciliation_method="spot_execution_replay",
                credential_payload={"api_key": "k", "api_secret": "s"},
                label=None,
            ),
        )

        with pytest.raises(HTTPException) as excinfo:
            await create_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST
        mock_repo.create_wallet_credential.assert_not_called()

    @pytest.mark.asyncio
    async def test_exchange_lookalike_cannot_inherit_registered_policy(self) -> None:
        """A futures-looking unknown exchange remains unreviewed and rejects real methods."""
        mock_repo = AsyncMock()
        command = CreateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=CreateCredentialBody(
                exchange="kraken_futures_v2",
                credential_type="api_key_secret",
                reconciliation_method="futures_position",
                credential_payload={"api_key": "k", "api_secret": "s"},
                label=None,
            ),
        )

        with pytest.raises(HTTPException) as excinfo:
            await create_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.asyncio
    async def test_unknown_adapter_accepts_explicit_unclassified_without_config(self) -> None:
        """Unreviewed adapters can remain explicitly unclassified at creation."""
        row = _cred_row(exchange="future_venue")
        mock_repo = AsyncMock()
        mock_repo.create_wallet_credential = AsyncMock(return_value=row)
        mock_encryption = MagicMock()
        mock_encryption.encrypt.return_value = "gAAAAABencrypted"
        command = CreateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=CreateCredentialBody(
                exchange="future_venue",
                credential_type="api_key_secret",
                reconciliation_method="unclassified",
                credential_payload={"api_key": "k", "api_secret": "s"},
                label=None,
            ),
        )

        with patch(
            "snapper.server.credential_routes.get_encryption_service",
            return_value=mock_encryption,
        ):
            result = await create_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        assert result.payload.exchange == "future_venue"
        kwargs = mock_repo.create_wallet_credential.await_args.kwargs
        assert kwargs["reconciliation_method"] == "unclassified"

    @pytest.mark.asyncio
    async def test_paper_rejects_live_method(self) -> None:
        """Paper credential creation cannot persist a live method config."""
        mock_repo = AsyncMock()
        command = CreateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=CreateCredentialBody(
                exchange="paper",
                credential_type="paper",
                reconciliation_method="futures_position",
                credential_payload={"initial_balance": "1000"},
                label=None,
            ),
        )

        with pytest.raises(HTTPException) as excinfo:
            await create_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.asyncio
    async def test_paper_type_and_venue_must_agree(self) -> None:
        """A paper credential type cannot disguise a live exchange identity."""
        mock_repo = AsyncMock()
        command = CreateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=CreateCredentialBody(
                exchange="kraken",
                credential_type="paper",
                reconciliation_method="unclassified",
                credential_payload={"initial_balance": "1000"},
                label=None,
            ),
        )

        with pytest.raises(HTTPException) as excinfo:
            await create_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.asyncio
    async def test_atomic_method_conflict_maps_to_409(self) -> None:
        """A history conflict from atomic credential provisioning returns conflict."""
        mock_repo = AsyncMock()
        mock_repo.create_wallet_credential = AsyncMock(
            side_effect=ReconciliationMethodImmutableError("method history conflicts")
        )
        mock_encryption = MagicMock()
        mock_encryption.encrypt.return_value = "gAAAAABencrypted"
        command = CreateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=CreateCredentialBody(
                exchange="kraken_futures",
                credential_type="api_key_secret",
                reconciliation_method="futures_position",
                credential_payload={"api_key": "k", "api_secret": "s"},
                label=None,
            ),
        )

        with (
            patch(
                "snapper.server.credential_routes.get_encryption_service",
                return_value=mock_encryption,
            ),
            pytest.raises(HTTPException) as excinfo,
        ):
            await create_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_409_CONFLICT


class TestSetCredentialReconciliationMethod:
    """Behaviour of the administrative reconciliation-method PUT handler."""

    @pytest.mark.asyncio
    async def test_happy_path_persists_live_method_and_returns_config(self) -> None:
        """A policy-approved method is written with live account mode."""
        credential = _cred_row(exchange="kraken_futures")
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(return_value=credential)
        mock_repo.set_portfolio_reconciliation_method_config = AsyncMock(
            return_value=_method_config_row()
        )

        result = await set_credential_reconciliation_method(
            request=_make_request(),
            _principal=_admin_principal(),
            _csrf=None,
            wallet_public_id="wallet-42",
            credential_public_id="cred-1",
            command=_method_command("futures_position"),
            repo=mock_repo,
        )

        kwargs = mock_repo.set_portfolio_reconciliation_method_config.await_args.kwargs
        assert kwargs["wallet_public_id"] == "wallet-42"
        assert kwargs["exchange"] == "kraken_futures"
        assert kwargs["mode"] == "live"
        assert kwargs["method"] == "futures_position"
        assert result.payload.public_id == "method-config-1"
        assert result.payload.method == "futures_position"

    @pytest.mark.asyncio
    async def test_missing_or_wrong_wallet_credential_returns_404(self) -> None:
        """The path wallet must own the active credential being classified."""
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(return_value=_cred_row())

        with pytest.raises(HTTPException) as excinfo:
            await set_credential_reconciliation_method(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-other",
                credential_public_id="cred-1",
                command=_method_command("spot_execution_replay"),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND
        mock_repo.set_portfolio_reconciliation_method_config.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_active_credential_returns_404(self) -> None:
        """A closed or absent credential cannot receive configuration."""
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as excinfo:
            await set_credential_reconciliation_method(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                credential_public_id="cred-missing",
                command=_method_command("futures_position"),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND

    @pytest.mark.asyncio
    async def test_policy_rejects_method_before_repository_write(self) -> None:
        """Kraken Futures rejects a spot method at the administrative surface."""
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(
            return_value=_cred_row(exchange="kraken_futures")
        )

        with pytest.raises(HTTPException) as excinfo:
            await set_credential_reconciliation_method(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                credential_public_id="cred-1",
                command=_method_command("spot_execution_replay"),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST
        mock_repo.set_portfolio_reconciliation_method_config.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_exchange_rejects_every_real_method(self) -> None:
        """Unregistered adapters have an empty administrative allowed set."""
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(
            return_value=_cred_row(exchange="kraken_futures_v2")
        )

        with pytest.raises(HTTPException) as excinfo:
            await set_credential_reconciliation_method(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                credential_public_id="cred-1",
                command=_method_command("futures_position"),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.asyncio
    async def test_paper_credential_is_rejected(self) -> None:
        """The administrative endpoint never creates paper method configs."""
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(
            return_value=_cred_row(exchange="paper", credential_type="paper")
        )

        with pytest.raises(HTTPException) as excinfo:
            await set_credential_reconciliation_method(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                credential_public_id="cred-1",
                command=_method_command("futures_position"),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST
        mock_repo.set_portfolio_reconciliation_method_config.assert_not_called()

    @pytest.mark.asyncio
    async def test_immutable_method_change_maps_to_409(self) -> None:
        """Durable history conflicts surface as HTTP conflict."""
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(return_value=_cred_row(exchange="kraken"))
        mock_repo.set_portfolio_reconciliation_method_config = AsyncMock(
            side_effect=ReconciliationMethodImmutableError("method is immutable")
        )

        with pytest.raises(HTTPException) as excinfo:
            await set_credential_reconciliation_method(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                credential_public_id="cred-1",
                command=_method_command("margin_ledger_replay"),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_409_CONFLICT

    def test_request_schema_rejects_unclassified(self) -> None:
        """Existing-credential PUT accepts only real methods."""
        with pytest.raises(ValidationError):
            SetCredentialReconciliationMethodBody.model_validate(
                {"reconciliation_method": "unclassified"}
            )


class TestRotateCredential:
    """Behaviour of ``rotate_credential`` POST handler."""

    @pytest.mark.asyncio
    async def test_happy_path_encrypts_and_rotates(self) -> None:
        """Successful rotation returns the new credential summary.

        Given: An existing active credential,
        When: ``rotate_credential`` is called with new payload,
        Then: The encryption service encrypts the new payload, the
            repo's rotate method is called with the ciphertext,
            and the response wraps the new row's summary.
        """
        new_row = _cred_row(public_id="cred-2", label="rotated key")
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(
            return_value=_cred_row(credential_type="api_key_secret")
        )
        mock_repo.rotate_wallet_credential = AsyncMock(return_value=new_row)
        mock_encryption = MagicMock()
        mock_encryption.encrypt.return_value = "gAAAAABnewciphertext"

        command = RotateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=RotateCredentialBody(
                credential_payload={"api_key": "new_k", "api_secret": "new_s"},
                label="rotated key",
            ),
        )
        with patch(
            "snapper.server.credential_routes.get_encryption_service",
            return_value=mock_encryption,
        ):
            result = await rotate_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                credential_public_id="cred-1",
                command=command,
                repo=mock_repo,
            )

        mock_encryption.encrypt.assert_called_once()
        mock_repo.rotate_wallet_credential.assert_awaited_once()
        call_kwargs = mock_repo.rotate_wallet_credential.await_args.kwargs
        assert call_kwargs["encrypted_payload"] == "gAAAAABnewciphertext"
        assert call_kwargs["credential_public_id"] == "cred-1"
        assert result.payload.label == "rotated key"

    @pytest.mark.asyncio
    async def test_missing_credential_maps_to_404(self) -> None:
        """Missing credential -> HTTP 404 from the pre-check lookup.

        Given: ``get_active_credential_by_id`` returns None,
        When: ``rotate_credential`` is called,
        Then: 404 is raised before the repo rotation is attempted.
        """
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(return_value=None)
        mock_repo.rotate_wallet_credential = AsyncMock()
        mock_encryption = MagicMock()
        mock_encryption.encrypt.return_value = "gAAAAABencrypted"

        command = RotateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=RotateCredentialBody(
                credential_payload={"api_key": "k", "api_secret": "s"},
                label=None,
            ),
        )
        with (
            patch(
                "snapper.server.credential_routes.get_encryption_service",
                return_value=mock_encryption,
            ),
            pytest.raises(HTTPException) as excinfo,
        ):
            await rotate_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                credential_public_id="cred-missing",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND

    @pytest.mark.asyncio
    async def test_concurrent_close_during_rotation_maps_to_404(self) -> None:
        """Race: pre-check passes but repo rotation raises CredentialNotFoundError.

        Given: ``get_active_credential_by_id`` returns a row (pre-check passes)
            but the credential is concurrently closed before the repo rotate,
        When: ``rotate_credential`` is called,
        Then: The CredentialNotFoundError from the repo is caught and mapped
            to HTTP 404 (defense-in-depth for concurrent rotation).
        """
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(
            return_value=_cred_row(credential_type="api_key_secret")
        )
        mock_repo.rotate_wallet_credential = AsyncMock(
            side_effect=CredentialNotFoundError("concurrently closed")
        )
        mock_encryption = MagicMock()
        mock_encryption.encrypt.return_value = "gAAAAABencrypted"
        command = RotateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=RotateCredentialBody(
                credential_payload={"api_key": "k", "api_secret": "s"},
                label=None,
            ),
        )
        with (
            patch(
                "snapper.server.credential_routes.get_encryption_service",
                return_value=mock_encryption,
            ),
            pytest.raises(HTTPException) as excinfo,
        ):
            await rotate_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                credential_public_id="cred-1",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND
        mock_repo.rotate_wallet_credential.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rotation_validates_payload_against_existing_type(self) -> None:
        """Rotation rejects incomplete payload for the existing credential_type.

        Given: An api_key_secret credential exists,
        When: ``rotate_credential`` is called with a payload missing
            ``api_secret``,
        Then: HTTPException 400 is raised before the repo rotation is
            attempted.
        """
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(
            return_value=_cred_row(credential_type="api_key_secret")
        )
        mock_repo.rotate_wallet_credential = AsyncMock()
        mock_encryption = MagicMock()
        command = RotateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=RotateCredentialBody(
                credential_payload={"api_key": "only-key"},
                label=None,
            ),
        )
        with (
            patch(
                "snapper.server.credential_routes.get_encryption_service",
                return_value=mock_encryption,
            ),
            pytest.raises(HTTPException) as excinfo,
        ):
            await rotate_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                credential_public_id="cred-1",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "api_secret" in str(excinfo.value.detail)
        mock_repo.rotate_wallet_credential.assert_not_called()

    @pytest.mark.asyncio
    async def test_rotation_validates_paper_credential_fields(self) -> None:
        """Rotation rejects empty payload for a paper credential.

        Given: A paper credential exists,
        When: ``rotate_credential`` is called with an empty payload,
        Then: HTTPException 400 is raised with ``initial_balance`` in detail.
        """
        mock_repo = AsyncMock()
        mock_repo.get_active_credential_by_id = AsyncMock(
            return_value=_cred_row(credential_type="paper")
        )
        mock_repo.rotate_wallet_credential = AsyncMock()
        mock_encryption = MagicMock()
        command = RotateCredentialCommand(
            session_id="test-sid",
            sequence_id=1,
            public_id="cmd-pid",
            timestamp=datetime.now(UTC),
            payload=RotateCredentialBody(
                credential_payload={},
                label=None,
            ),
        )
        with (
            patch(
                "snapper.server.credential_routes.get_encryption_service",
                return_value=mock_encryption,
            ),
            pytest.raises(HTTPException) as excinfo,
        ):
            await rotate_credential(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                wallet_public_id="wallet-42",
                credential_public_id="cred-1",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "initial_balance" in str(excinfo.value.detail)
