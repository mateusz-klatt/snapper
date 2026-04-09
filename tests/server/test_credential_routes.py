"""Tests for the Phase 0d wallet credential management routes.

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

from snapper.api.schemas.multi_tenant import CreateCredentialBody
from snapper.api.schemas.multi_tenant import CreateCredentialCommand
from snapper.api.schemas.multi_tenant import RotateCredentialBody
from snapper.api.schemas.multi_tenant import RotateCredentialCommand
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import CredentialConflictError
from snapper.data.repository import CredentialNotFoundError
from snapper.data.repository_types import WalletCredentialRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.credential_routes import create_credential
from snapper.server.credential_routes import list_credentials
from snapper.server.credential_routes import rotate_credential


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
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        mock_encryption.encrypt.assert_called_once()
        mock_repo.create_wallet_credential.assert_awaited_once()
        call_kwargs = mock_repo.create_wallet_credential.await_args.kwargs
        assert call_kwargs["encrypted_payload"] == "gAAAAABencrypted"
        assert call_kwargs["wallet_public_id"] == "wallet-42"
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
                wallet_public_id="wallet-42",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "initial_balance" in str(excinfo.value.detail)


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
        """CredentialNotFoundError -> HTTP 404."""
        mock_repo = AsyncMock()
        mock_repo.rotate_wallet_credential = AsyncMock(
            side_effect=CredentialNotFoundError("not found")
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
                wallet_public_id="wallet-42",
                credential_public_id="cred-missing",
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND
