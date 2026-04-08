"""Tests for the per-wallet ``CredentialResolver``.

Plan 0 Phase 0b Section 4.2. Verifies the resolver pulls an active
``wallet_credentials`` row from the repository, decrypts the JSON
envelope using the project's Fernet encryption service, and surfaces
``CredentialNotFoundError`` when no row matches.

These tests run against a real on-disk SQLite repository so the
temporal ``where_active`` filter, partial unique index, and
``ck_wallet_credentials_exchange_lower`` CHECK constraint are exercised
end-to-end.
"""

import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from snapper.config.credentials import CredentialNotFoundError
from snapper.config.credentials import CredentialResolver
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import WalletCredential
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.security.encryption import SettingsEncryptionService


@pytest.fixture
async def repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Disposable on-disk SQLite repository with the full schema."""
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/credentials.db")
    await r.create_all()
    return r


@pytest.fixture
def encryption() -> SettingsEncryptionService:
    """Stable Fernet encryption service for the test session."""
    return SettingsEncryptionService("test-master-password")


async def _seed_credential(
    repo: SQLAlchemyRepository,
    encryption: SettingsEncryptionService,
    *,
    wallet_public_id: str,
    exchange: str,
    payload: dict[str, str | float],
    credential_type: str = "api_key_secret",
) -> str:
    """Insert one active wallet credential and return its public_id."""
    encrypted = encryption.encrypt(json.dumps(payload))
    async with repo.session() as s:
        row = WalletCredential(
            wallet_public_id=wallet_public_id,
            exchange=exchange,
            credential_type=credential_type,
            encrypted_payload=encrypted,
            encryption_key_id="test-master-key",
            label=None,
            session_id="test-session",
            sequence_id=1,
            timestamp=datetime.now(UTC) - timedelta(minutes=1),
            known_to=KNOWN_TO_MAX,
        )
        s.add(row)
        await s.commit()
        await s.refresh(row)
        return row.public_id


class TestCredentialResolverSuccess:
    """Happy-path tests for ``CredentialResolver.get_credentials``."""

    @pytest.mark.asyncio
    async def test_decrypts_api_key_secret_payload(
        self,
        repo: SQLAlchemyRepository,
        encryption: SettingsEncryptionService,
    ) -> None:
        """An api_key_secret envelope is decoded into a string-valued dict.

        Given: A seeded encrypted credential with api_key + api_secret,
        When: ``get_credentials`` is invoked,
        Then: A dict with the original key/value pairs is returned.
        """
        await _seed_credential(
            repo,
            encryption,
            wallet_public_id="00000000-0000-7000-8000-0000000000a1",
            exchange="kraken",
            payload={"api_key": "kraken-key-abc", "api_secret": "kraken-secret-xyz"},
        )
        resolver = CredentialResolver(repo, encryption_service=encryption)
        creds = await resolver.get_credentials(
            exchange="kraken",
            wallet_public_id="00000000-0000-7000-8000-0000000000a1",
        )
        assert creds == {"api_key": "kraken-key-abc", "api_secret": "kraken-secret-xyz"}

    @pytest.mark.asyncio
    async def test_paper_payload_stringifies_numeric_values(
        self,
        repo: SQLAlchemyRepository,
        encryption: SettingsEncryptionService,
    ) -> None:
        """Paper credentials with numeric initial_balance round-trip as strings.

        Given: A paper-mode credential whose payload carries a float
            ``initial_balance``,
        When: ``get_credentials`` is invoked,
        Then: The numeric value is stringified so callers do not need
            to know the per-credential-type schema.
        """
        await _seed_credential(
            repo,
            encryption,
            wallet_public_id="00000000-0000-7000-8000-0000000000a2",
            exchange="paper",
            payload={"initial_balance": 10000.0},
            credential_type="paper",
        )
        resolver = CredentialResolver(repo, encryption_service=encryption)
        creds = await resolver.get_credentials(
            exchange="paper",
            wallet_public_id="00000000-0000-7000-8000-0000000000a2",
        )
        assert creds == {"initial_balance": "10000.0"}

    @pytest.mark.asyncio
    async def test_exchange_lookup_is_case_insensitive(
        self,
        repo: SQLAlchemyRepository,
        encryption: SettingsEncryptionService,
    ) -> None:
        """An uppercase exchange parameter still resolves the lowercase row.

        Given: A credential row stored with exchange='kraken' (the CHECK
            constraint forces lowercase),
        When: ``get_credentials`` is invoked with exchange='KRAKEN',
        Then: The row is still found because the resolver normalizes
            the exchange parameter to lowercase before the DB lookup.
        """
        await _seed_credential(
            repo,
            encryption,
            wallet_public_id="00000000-0000-7000-8000-0000000000a3",
            exchange="kraken",
            payload={"api_key": "k", "api_secret": "s"},
        )
        resolver = CredentialResolver(repo, encryption_service=encryption)
        creds = await resolver.get_credentials(
            exchange="KRAKEN",
            wallet_public_id="00000000-0000-7000-8000-0000000000a3",
        )
        assert creds["api_key"] == "k"

    @pytest.mark.asyncio
    async def test_explicit_as_of_timestamp_round_trips(
        self,
        repo: SQLAlchemyRepository,
        encryption: SettingsEncryptionService,
    ) -> None:
        """An explicit ``as_of`` timestamp is honored by the temporal query.

        Given: A credential row inserted at ``t0 - 1m``,
        When: ``get_credentials`` is invoked with ``as_of=t0``,
        Then: The row is returned because it is active at ``t0``.
        """
        await _seed_credential(
            repo,
            encryption,
            wallet_public_id="00000000-0000-7000-8000-0000000000a4",
            exchange="kraken",
            payload={"api_key": "k", "api_secret": "s"},
        )
        resolver = CredentialResolver(repo, encryption_service=encryption)
        now = datetime.now(UTC)
        creds = await resolver.get_credentials(
            exchange="kraken",
            wallet_public_id="00000000-0000-7000-8000-0000000000a4",
            as_of=now,
        )
        assert creds == {"api_key": "k", "api_secret": "s"}


class TestCredentialResolverErrors:
    """Failure-path tests."""

    @pytest.mark.asyncio
    async def test_missing_credential_raises_not_found(
        self,
        repo: SQLAlchemyRepository,
        encryption: SettingsEncryptionService,
    ) -> None:
        """No matching row raises ``CredentialNotFoundError``.

        Given: A repository with no credentials seeded,
        When: ``get_credentials`` is invoked,
        Then: ``CredentialNotFoundError`` is raised carrying the
            requested ``exchange`` and ``wallet_public_id`` so the
            executor log makes the missing seed obvious.
        """
        resolver = CredentialResolver(repo, encryption_service=encryption)
        with pytest.raises(CredentialNotFoundError) as excinfo:
            await resolver.get_credentials(
                exchange="KRAKEN",
                wallet_public_id="00000000-0000-7000-8000-0000000000ff",
            )
        assert excinfo.value.exchange == "kraken"
        assert excinfo.value.wallet_public_id == "00000000-0000-7000-8000-0000000000ff"

    @pytest.mark.asyncio
    async def test_default_encryption_service_falls_back_to_singleton(
        self,
        repo: SQLAlchemyRepository,
        encryption: SettingsEncryptionService,
    ) -> None:
        """Omitting ``encryption_service`` uses ``get_encryption_service``.

        Given: A patched ``get_encryption_service`` returning a stable
            instance and one seeded credential,
        When: ``CredentialResolver`` is constructed without an explicit
            encryption service and ``get_credentials`` is invoked,
        Then: The resolver decrypts the payload via the singleton
            returned by the patched factory (verified by the successful
            round-trip and the factory call assertion).
        """
        await _seed_credential(
            repo,
            encryption,
            wallet_public_id="00000000-0000-7000-8000-0000000000a5",
            exchange="kraken",
            payload={"api_key": "fallback-key", "api_secret": "fallback-secret"},
        )
        with patch(
            "snapper.config.credentials.get_encryption_service",
            return_value=encryption,
        ) as mock_factory:
            resolver = CredentialResolver(repo)
            creds = await resolver.get_credentials(
                exchange="kraken",
                wallet_public_id="00000000-0000-7000-8000-0000000000a5",
            )
        mock_factory.assert_called_once()
        assert creds == {"api_key": "fallback-key", "api_secret": "fallback-secret"}


class TestListActiveWalletCredentials:
    """Phase 0c.2 ``Repository.list_active_wallet_credentials`` coverage.

    The dynamic per-wallet executor spawner consumes this list at boot
    to discover the ``(exchange, wallet)`` pairs that need a dedicated
    executor instance. The tests verify ordering, temporal filtering,
    and the empty-list contract on a fresh DB.
    """

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_credentials(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Fresh DB returns no credentials.

        Given: A SQLAlchemyRepository with the schema created but no
            wallet_credentials rows,
        When: ``list_active_wallet_credentials`` is called,
        Then: An empty list is returned so the spawner skips dynamic
            spawn entirely.
        """
        rows = await repo.list_active_wallet_credentials(as_of=datetime.now(UTC))
        assert rows == []

    @pytest.mark.asyncio
    async def test_returns_active_rows_sorted_by_exchange_and_wallet(
        self,
        repo: SQLAlchemyRepository,
        encryption: SettingsEncryptionService,
    ) -> None:
        """Active rows are returned ordered by (exchange, wallet_public_id).

        Given: Three credential rows seeded out of order across two
            exchanges and two wallets,
        When: ``list_active_wallet_credentials`` is called,
        Then: All three rows come back ordered by exchange then wallet
            so the spawner produces deterministic process names.
        """
        await _seed_credential(
            repo,
            encryption,
            wallet_public_id="00000000-0000-7000-8000-0000000000c2",
            exchange="kraken",
            payload={"api_key": "k2", "api_secret": "s2"},
        )
        await _seed_credential(
            repo,
            encryption,
            wallet_public_id="00000000-0000-7000-8000-0000000000c1",
            exchange="paper",
            payload={"initial_balance": "5000.0"},
            credential_type="paper",
        )
        await _seed_credential(
            repo,
            encryption,
            wallet_public_id="00000000-0000-7000-8000-0000000000c1",
            exchange="kraken",
            payload={"api_key": "k1", "api_secret": "s1"},
        )
        rows = await repo.list_active_wallet_credentials(as_of=datetime.now(UTC))
        assert len(rows) == 3
        ordering = [(row["exchange"], row["wallet_public_id"]) for row in rows]
        assert ordering == [
            ("kraken", "00000000-0000-7000-8000-0000000000c1"),
            ("kraken", "00000000-0000-7000-8000-0000000000c2"),
            ("paper", "00000000-0000-7000-8000-0000000000c1"),
        ]

    @pytest.mark.asyncio
    async def test_skips_closed_rows(
        self,
        repo: SQLAlchemyRepository,
        encryption: SettingsEncryptionService,
    ) -> None:
        """SCD2-closed rows are excluded from the active list.

        Given: One active credential and one credential whose
            ``known_to`` has been closed (SCD2 historical row),
        When: ``list_active_wallet_credentials`` is called at a time
            after the close,
        Then: Only the active row is returned, exercising the
            ``where_active`` predicate on the bitemporal table.
        """
        await _seed_credential(
            repo,
            encryption,
            wallet_public_id="00000000-0000-7000-8000-0000000000d1",
            exchange="kraken",
            payload={"api_key": "active", "api_secret": "active"},
        )
        async with repo.session() as s:
            historical = WalletCredential(
                wallet_public_id="00000000-0000-7000-8000-0000000000d2",
                exchange="kraken",
                credential_type="api_key_secret",
                encrypted_payload=encryption.encrypt(
                    json.dumps({"api_key": "old", "api_secret": "old"})
                ),
                encryption_key_id="test-master-key",
                label=None,
                session_id="test-session",
                sequence_id=1,
                timestamp=datetime.now(UTC) - timedelta(hours=2),
                known_to=datetime.now(UTC) - timedelta(hours=1),
            )
            s.add(historical)
            await s.commit()
        rows = await repo.list_active_wallet_credentials(as_of=datetime.now(UTC))
        assert len(rows) == 1
        assert rows[0]["wallet_public_id"] == "00000000-0000-7000-8000-0000000000d1"
