"""Repository-level tests for the Phase 0d wallet credential helpers.

Covers ``create_wallet_credential``, ``rotate_wallet_credential``,
and ``list_wallet_credentials_for_wallet``. Runs against an on-disk
SQLite database with the full schema materialised via ``create_all``.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy.exc import IntegrityError

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Wallet
from snapper.data.repository import CredentialConflictError
from snapper.data.repository import CredentialNotFoundError
from snapper.data.repository import SQLAlchemyRepository


async def _seed_wallet(repo: SQLAlchemyRepository) -> str:
    """Insert a wallet and return its ``public_id``."""
    base_ts = datetime.now(UTC) - timedelta(minutes=5)
    async with repo.session() as s:
        wallet = Wallet(
            label="default",
            description=None,
            is_paper=False,
            session_id="test-session",
            sequence_id=1,
            timestamp=base_ts,
            known_to=KNOWN_TO_MAX,
        )
        s.add(wallet)
        await s.commit()
        await s.refresh(wallet)
        return wallet.public_id


@pytest.fixture
async def repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Disposable on-disk SQLite repository with the full schema."""
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/credentials.db")
    await r.create_all()
    return r


class TestCreateWalletCredential:
    """Behaviour of ``create_wallet_credential``."""

    @pytest.mark.asyncio
    async def test_insert_returns_row_without_decrypting(self, repo: SQLAlchemyRepository) -> None:
        """A first credential insert returns a populated row.

        Given: An existing wallet,
        When: ``create_wallet_credential`` is called with an encrypted payload,
        Then: The returned row carries the ciphertext verbatim (the repo
            never decrypts), along with exchange + type + label.
        """
        wallet_id = await _seed_wallet(repo)
        now = datetime.now(UTC)

        row = await repo.create_wallet_credential(
            wallet_public_id=wallet_id,
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload="gAAAAABencrypted",
            label="main key",
            session_id="test-session",
            sequence_id=10,
            timestamp=now,
        )

        assert row["wallet_public_id"] == wallet_id
        assert row["exchange"] == "kraken"
        assert row["credential_type"] == "api_key_secret"
        assert row["encrypted_payload"] == "gAAAAABencrypted"
        assert row["label"] == "main key"
        assert row["public_id"]

    @pytest.mark.asyncio
    async def test_duplicate_wallet_exchange_raises_conflict(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Two active credentials on the same (wallet, exchange) conflict.

        Given: An existing credential for ``(wallet, kraken)``,
        When: A second insert reuses the same pair,
        Then: ``CredentialConflictError`` is raised.
        """
        wallet_id = await _seed_wallet(repo)
        base_ts = datetime.now(UTC)
        await repo.create_wallet_credential(
            wallet_public_id=wallet_id,
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload="gAAAAABfirst",
            label=None,
            session_id="test-session",
            sequence_id=10,
            timestamp=base_ts,
        )

        with pytest.raises(CredentialConflictError) as excinfo:
            await repo.create_wallet_credential(
                wallet_public_id=wallet_id,
                exchange="kraken",
                credential_type="api_key_secret",
                encrypted_payload="gAAAAABsecond",
                label=None,
                session_id="test-session",
                sequence_id=11,
                timestamp=base_ts + timedelta(microseconds=1),
            )

        assert excinfo.value.exchange == "kraken"

    @pytest.mark.asyncio
    async def test_non_unique_integrity_error_reraises(self, repo: SQLAlchemyRepository) -> None:
        """A non-unique IntegrityError re-raises unhandled."""
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        orig = Exception("NOT NULL constraint failed: wallet_credentials.exchange")
        mock_session.commit = AsyncMock(
            side_effect=IntegrityError(statement="INSERT", params={}, orig=orig)
        )
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        with (
            patch.object(repo, "session", return_value=mock_ctx),
            pytest.raises(IntegrityError),
        ):
            await repo.create_wallet_credential(
                wallet_public_id="w",
                exchange="",
                credential_type="paper",
                encrypted_payload="x",
                label=None,
                session_id="s",
                sequence_id=99,
                timestamp=datetime.now(UTC),
            )


class TestRotateWalletCredential:
    """Behaviour of ``rotate_wallet_credential``."""

    @pytest.mark.asyncio
    async def test_rotation_closes_old_and_inserts_new(self, repo: SQLAlchemyRepository) -> None:
        """Rotation creates a new row and closes the old one.

        Given: An existing active credential,
        When: ``rotate_wallet_credential`` is called,
        Then: The returned row carries the new encrypted payload,
            and the old row is no longer returned by ``get_active_credential``.
        """
        wallet_id = await _seed_wallet(repo)
        base_ts = datetime.now(UTC) - timedelta(minutes=1)
        old = await repo.create_wallet_credential(
            wallet_public_id=wallet_id,
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload="gAAAAABold",
            label="old key",
            session_id="test-session",
            sequence_id=10,
            timestamp=base_ts,
        )

        rotate_ts = datetime.now(UTC)
        new_row = await repo.rotate_wallet_credential(
            credential_public_id=old["public_id"],
            encrypted_payload="gAAAAABnew",
            label="rotated key",
            session_id="test-session",
            sequence_id=11,
            timestamp=rotate_ts,
        )

        assert new_row["encrypted_payload"] == "gAAAAABnew"
        assert new_row["label"] == "rotated key"
        assert new_row["public_id"] != old["public_id"]
        active = await repo.get_active_credential("kraken", wallet_id, datetime.now(UTC))
        assert active is not None
        assert active["public_id"] == new_row["public_id"]

    @pytest.mark.asyncio
    async def test_missing_credential_raises_not_found(self, repo: SQLAlchemyRepository) -> None:
        """Rotating a non-existent credential raises ``CredentialNotFoundError``."""
        with pytest.raises(CredentialNotFoundError):
            await repo.rotate_wallet_credential(
                credential_public_id="00000000-0000-7000-8000-000000000000",
                encrypted_payload="gAAAAABx",
                label=None,
                session_id="test-session",
                sequence_id=99,
                timestamp=datetime.now(UTC),
            )

    @pytest.mark.asyncio
    async def test_rotation_preserves_label_when_none(self, repo: SQLAlchemyRepository) -> None:
        """Passing ``label=None`` to rotate preserves the existing label.

        Given: A credential with ``label='main key'``,
        When: ``rotate_wallet_credential`` is called with ``label=None``,
        Then: The new row inherits the existing label.
        """
        wallet_id = await _seed_wallet(repo)
        base_ts = datetime.now(UTC) - timedelta(minutes=1)
        old = await repo.create_wallet_credential(
            wallet_public_id=wallet_id,
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload="gAAAAABold",
            label="main key",
            session_id="test-session",
            sequence_id=10,
            timestamp=base_ts,
        )

        new_row = await repo.rotate_wallet_credential(
            credential_public_id=old["public_id"],
            encrypted_payload="gAAAAABnew",
            label=None,
            session_id="test-session",
            sequence_id=11,
            timestamp=datetime.now(UTC),
        )

        assert new_row["label"] == "main key"


class TestListWalletCredentialsForWallet:
    """Behaviour of ``list_wallet_credentials_for_wallet``."""

    @pytest.mark.asyncio
    async def test_returns_only_credentials_for_given_wallet(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Credentials for a different wallet are excluded.

        Given: Two wallets each with one credential,
        When: ``list_wallet_credentials_for_wallet`` is called for the first,
        Then: Only the first wallet's credential is returned.
        """
        base_ts = datetime.now(UTC) - timedelta(minutes=1)
        wallet_id = await _seed_wallet(repo)
        async with repo.session() as s:
            wallet2 = Wallet(
                label="other",
                description=None,
                is_paper=False,
                session_id="test-session",
                sequence_id=2,
                timestamp=base_ts,
                known_to=KNOWN_TO_MAX,
            )
            s.add(wallet2)
            await s.commit()
            await s.refresh(wallet2)
            wallet2_id = wallet2.public_id

        await repo.create_wallet_credential(
            wallet_public_id=wallet_id,
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload="gAAAAABw1",
            label=None,
            session_id="test-session",
            sequence_id=10,
            timestamp=base_ts,
        )
        await repo.create_wallet_credential(
            wallet_public_id=wallet2_id,
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload="gAAAAABw2",
            label=None,
            session_id="test-session",
            sequence_id=11,
            timestamp=base_ts + timedelta(microseconds=1),
        )

        rows = await repo.list_wallet_credentials_for_wallet(wallet_id, datetime.now(UTC))

        assert len(rows) == 1
        assert rows[0]["wallet_public_id"] == wallet_id
