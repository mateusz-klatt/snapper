"""Repository-level tests for the wallet credential helpers.

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

from snapper.core.wallet_short import compute_legacy_wallet_short
from snapper.core.wallet_short import compute_wallet_short
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Wallet
from snapper.data.repository import CredentialConflictError
from snapper.data.repository import CredentialNotFoundError
from snapper.data.repository import SQLAlchemyRepository

_CANONICAL_WALLET = "abcdefab-cdef-7abc-8def-abcdefabcdef"


async def _seed_wallet(repo: SQLAlchemyRepository, public_id: str | None = None) -> str:
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
        if public_id is not None:
            wallet.public_id = public_id
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
            reconciliation_method="unclassified",
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
            reconciliation_method="unclassified",
        )

        s5778_value_1 = timedelta(microseconds=1)
        with pytest.raises(CredentialConflictError) as excinfo:
            await repo.create_wallet_credential(
                wallet_public_id=wallet_id,
                exchange="kraken",
                credential_type="api_key_secret",
                encrypted_payload="gAAAAABsecond",
                label=None,
                session_id="test-session",
                sequence_id=11,
                timestamp=base_ts + s5778_value_1,
                reconciliation_method="unclassified",
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
            patch.object(repo, "_begin_portfolio_reconciliation_write", new=AsyncMock()),
            patch.object(repo, "_acquire_wallet_advisory_lock", new=AsyncMock()),
            patch.object(repo, "_require_active_portfolio_wallet", new=AsyncMock()),
            patch.object(
                repo,
                "_load_active_portfolio_reconciliation_method_config",
                new=AsyncMock(return_value=None),
            ),
            pytest.raises(IntegrityError),
        ):
            await repo.create_wallet_credential(
                wallet_public_id="00000000-0000-7000-8000-000000000001",
                exchange="",
                credential_type="paper",
                encrypted_payload="x",
                label=None,
                session_id="s",
                sequence_id=99,
                timestamp=datetime.now(UTC),
                reconciliation_method="unclassified",
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
            reconciliation_method="unclassified",
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
        s5778_value_1 = datetime.now(UTC)
        with pytest.raises(CredentialNotFoundError):
            await repo.rotate_wallet_credential(
                credential_public_id="00000000-0000-7000-8000-000000000000",
                encrypted_payload="gAAAAABx",
                label=None,
                session_id="test-session",
                sequence_id=99,
                timestamp=s5778_value_1,
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
            reconciliation_method="unclassified",
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


class TestGetActiveCredentialById:
    """Behaviour of ``get_active_credential_by_id``."""

    @pytest.mark.asyncio
    async def test_returns_row_when_active(self, repo: SQLAlchemyRepository) -> None:
        """Active credential is returned by its public_id.

        Given: An active credential row,
        When: ``get_active_credential_by_id`` is called with its public_id,
        Then: The matching ``WalletCredentialRow`` is returned.
        """
        wallet_id = await _seed_wallet(repo)
        base_ts = datetime.now(UTC) - timedelta(minutes=1)
        created = await repo.create_wallet_credential(
            wallet_public_id=wallet_id,
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload="gAAAAABfoo",
            label="test key",
            session_id="test-session",
            sequence_id=10,
            timestamp=base_ts,
            reconciliation_method="unclassified",
        )

        result = await repo.get_active_credential_by_id(created["public_id"], datetime.now(UTC))

        assert result is not None
        assert result["public_id"] == created["public_id"]
        assert result["exchange"] == "kraken"

    @pytest.mark.asyncio
    async def test_returns_none_when_missing(self, repo: SQLAlchemyRepository) -> None:
        """Non-existent public_id returns None."""
        result = await repo.get_active_credential_by_id(
            "00000000-0000-7000-8000-000000000000", datetime.now(UTC)
        )

        assert result is None


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
            reconciliation_method="unclassified",
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
            reconciliation_method="unclassified",
        )

        rows = await repo.list_wallet_credentials_for_wallet(wallet_id, datetime.now(UTC))

        assert len(rows) == 1
        assert rows[0]["wallet_public_id"] == wallet_id


class TestCredentialWalletUuidReadCanonicalization:
    """Wallet-filtered credential reads share writer UUID canonicalization."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "wallet_alias",
        [
            _CANONICAL_WALLET.upper(),
            _CANONICAL_WALLET.replace("-", ""),
        ],
    )
    async def test_alias_spelling_finds_canonically_stored_credential(
        self,
        repo: SQLAlchemyRepository,
        wallet_alias: str,
    ) -> None:
        """Uppercase and hyphenless UUID reads find the canonical row."""
        await _seed_wallet(repo, _CANONICAL_WALLET)
        created = await repo.create_wallet_credential(
            wallet_public_id=_CANONICAL_WALLET,
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload="gAAAAABcanonical",
            label=None,
            session_id="test-session",
            sequence_id=10,
            timestamp=datetime.now(UTC) - timedelta(minutes=1),
            reconciliation_method="unclassified",
        )
        as_of = datetime.now(UTC)

        active = await repo.get_active_credential("kraken", wallet_alias, as_of)
        listed = await repo.list_wallet_credentials_for_wallet(wallet_alias, as_of)

        assert active == created
        assert listed == [created]
        assert created["wallet_public_id"] == _CANONICAL_WALLET

    @pytest.mark.asyncio
    async def test_malformed_wallet_is_rejected_by_both_filtered_reads(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Both credential readers reject an unparseable wallet identity."""
        as_of = datetime.now(UTC)
        with pytest.raises(ValueError, match="reconciliation wallet identity is invalid"):
            await repo.get_active_credential("kraken", "not-a-wallet-uuid", as_of)
        with pytest.raises(ValueError, match="reconciliation wallet identity is invalid"):
            await repo.list_wallet_credentials_for_wallet("not-a-wallet-uuid", as_of)


class TestResolveWalletPublicIdByShort:
    """Temporal wallet-short resolution for checkpoint recovery."""

    @pytest.mark.asyncio
    async def test_resolves_canonical_short(self, repo: SQLAlchemyRepository) -> None:
        """Canonical last-12 wallet-short resolves through active credentials.

        Given: A wallet with an active credential at the query time,
        When: The canonical wallet-short is resolved,
        Then: The wallet public ID is returned.
        """
        wallet_id = "018f0000-0000-7000-8000-abcdefabcdef"
        base_ts = datetime.now(UTC) - timedelta(minutes=1)
        await _seed_wallet(repo, wallet_id)
        await repo.create_wallet_credential(
            wallet_public_id=wallet_id,
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload="gAAAAABcanonical",
            label=None,
            session_id="test-session",
            sequence_id=10,
            timestamp=base_ts,
            reconciliation_method="unclassified",
        )

        result = await repo.resolve_wallet_public_id_by_short(
            compute_wallet_short(wallet_id),
            datetime.now(UTC),
        )

        assert result == wallet_id

    @pytest.mark.asyncio
    async def test_resolves_legacy_short(self, repo: SQLAlchemyRepository) -> None:
        """Legacy first-12 wallet-short remains recoverable.

        Given: A wallet whose persisted shard key used the old first-12 alias,
        When: The legacy wallet-short is resolved,
        Then: The wallet public ID is returned.
        """
        wallet_id = "018f1111-2222-7333-8444-555566667777"
        base_ts = datetime.now(UTC) - timedelta(minutes=1)
        await _seed_wallet(repo, wallet_id)
        await repo.create_wallet_credential(
            wallet_public_id=wallet_id,
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload="gAAAAABlegacy",
            label=None,
            session_id="test-session",
            sequence_id=10,
            timestamp=base_ts,
            reconciliation_method="unclassified",
        )

        result = await repo.resolve_wallet_public_id_by_short(
            compute_legacy_wallet_short(wallet_id),
            datetime.now(UTC),
        )

        assert result == wallet_id

    @pytest.mark.asyncio
    async def test_honors_temporal_activation(self, repo: SQLAlchemyRepository) -> None:
        """Wallet-short resolution reads credentials at the supplied time.

        Given: A credential that becomes active at ``base_ts``,
        When: The resolver reads before and after that timestamp,
        Then: Only the later read resolves the wallet.
        """
        wallet_id = "018f9999-0000-7000-8000-999999999999"
        base_ts = datetime.now(UTC)
        await _seed_wallet(repo, wallet_id)
        await repo.create_wallet_credential(
            wallet_public_id=wallet_id,
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload="gAAAAABtemporal",
            label=None,
            session_id="test-session",
            sequence_id=10,
            timestamp=base_ts,
            reconciliation_method="unclassified",
        )
        wallet_short = compute_wallet_short(wallet_id)

        before = await repo.resolve_wallet_public_id_by_short(
            wallet_short,
            base_ts - timedelta(microseconds=1),
        )
        after = await repo.resolve_wallet_public_id_by_short(
            wallet_short,
            base_ts + timedelta(microseconds=1),
        )

        assert before is None
        assert after == wallet_id

    def test_canonical_match_preempts_earlier_legacy_candidate(self) -> None:
        """Canonical matches win even after an earlier legacy candidate.

        Given: Ordered wallet IDs where the first row has a legacy alias
            equal to the second row's canonical short,
        When: The in-memory resolver scans the rows,
        Then: The canonical owner is returned.
        """
        legacy_candidate = "abcdefab-cdef-7000-8000-111111111111"
        canonical_owner = "018f0000-0000-7000-8000-abcdefabcdef"

        result = SQLAlchemyRepository._wallet_public_id_from_short(
            compute_wallet_short(canonical_owner),
            [legacy_candidate, canonical_owner],
        )

        assert result == canonical_owner

    def test_first_legacy_match_wins_when_alias_is_duplicated(self) -> None:
        """Duplicate legacy aliases keep the first deterministic owner.

        Given: Two ordered wallet IDs with the same legacy first-12 alias,
        When: The resolver scans both rows,
        Then: The first row remains the legacy owner.
        """
        first = "abcdefab-cdef-7000-8000-111111111111"
        second = "abcdefab-cdef-7000-8000-222222222222"

        result = SQLAlchemyRepository._wallet_public_id_from_short(
            compute_legacy_wallet_short(first),
            [first, second],
        )

        assert result == first
