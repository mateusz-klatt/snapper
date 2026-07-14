"""Tests for atomic credential and reconciliation-method provisioning."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioReconciliationMethodConfig
from snapper.data.models import PortfolioReconciliationObservation
from snapper.data.models import PortfolioReconciliationState
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.repository import ReconciliationMethodImmutableError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow

_NOW = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
_WALLET = "00000000-0000-7000-8000-000000000101"
_SESSION = "00000000-0000-7000-8000-000000000201"


async def _make_repo(tmp_path: Path, name: str) -> SQLAlchemyRepository:
    """Create one isolated full-schema repository."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / name}")
    await repository.create_all()
    return repository


async def _seed_wallet(repository: SQLAlchemyRepository) -> None:
    """Insert one active live wallet."""
    async with repository.session() as session:
        session.add(
            Wallet(
                label="credential-atomicity-wallet",
                description=None,
                is_paper=False,
                public_id=_WALLET,
                session_id=_SESSION,
                sequence_id=1,
                timestamp=_NOW - timedelta(hours=1),
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()


async def _create_credential(
    repository: SQLAlchemyRepository,
    reconciliation_method: str,
    *,
    exchange: str = "kraken_futures",
    credential_type: str = "api_key_secret",
) -> None:
    """Create one credential through the atomic DAL boundary."""
    await repository.create_wallet_credential(
        wallet_public_id=_WALLET,
        exchange=exchange,
        credential_type=credential_type,
        encrypted_payload="encrypted-payload",
        label="main",
        session_id=_SESSION,
        sequence_id=2,
        timestamp=_NOW,
        reconciliation_method=reconciliation_method,
    )


def _evaluation(sequence_id: int) -> PortfolioReconciliationEvaluationRow:
    """Build one non-full futures evaluation for the config race."""
    return {
        "wallet_public_id": _WALLET,
        "exchange": "kraken_futures",
        "mode": "live",
        "method": "futures_position",
        "evaluation_status": "incomplete",
        "venue_account_state_public_id": None,
        "venue_account_observation_id": None,
        "account_authoritative_until": None,
        "source_watermark_kind": None,
        "source_watermark": None,
        "anchor_public_id": None,
        "expected_json": None,
        "actual_json": None,
        "difference_json": None,
        "tolerance_json": None,
        "error": None,
        "session_id": _SESSION,
        "sequence_id": sequence_id,
        "bus_time": _NOW + timedelta(seconds=sequence_id),
    }


async def test_real_method_credential_creates_config_in_same_transaction(
    tmp_path: Path,
) -> None:
    """A real method produces both one credential and one active config."""
    repository = await _make_repo(tmp_path, "credential-real-method.db")
    await _seed_wallet(repository)
    await _create_credential(repository, "futures_position")
    active = await repository.get_active_portfolio_reconciliation_method_config(
        _WALLET,
        "kraken_futures",
        "live",
    )
    credentials = await repository.list_wallet_credentials_for_wallet(_WALLET, _NOW)
    assert len(credentials) == 1
    assert credentials[0]["exchange"] == "kraken_futures"
    assert active is not None
    assert active["method"] == "futures_position"
    assert active["session_id"] == credentials[0]["session_id"]
    assert active["sequence_id"] == credentials[0]["sequence_id"]


async def test_unclassified_credential_creates_no_config(tmp_path: Path) -> None:
    """An explicit unclassified choice persists only the credential."""
    repository = await _make_repo(tmp_path, "credential-unclassified.db")
    await _seed_wallet(repository)
    await _create_credential(repository, "unclassified")
    credentials = await repository.list_wallet_credentials_for_wallet(_WALLET, _NOW)
    active = await repository.get_active_portfolio_reconciliation_method_config(
        _WALLET,
        "kraken_futures",
        "live",
    )
    assert len(credentials) == 1
    assert active is None


@pytest.mark.parametrize(
    ("exchange", "credential_type", "method", "message"),
    [
        (
            "paper",
            "api_key_secret",
            "futures_position",
            "paper credentials cannot have",
        ),
        (
            "kraken",
            "paper",
            "spot_execution_replay",
            "paper credentials cannot have",
        ),
        (
            "kraken",
            "api_key_secret",
            "invented_method",
            "credential reconciliation method is invalid",
        ),
    ],
)
async def test_invalid_or_paper_real_method_credential_is_rejected(
    tmp_path: Path,
    exchange: str,
    credential_type: str,
    method: str,
    message: str,
) -> None:
    """Paper classifications and invalid method values fail before persistence."""
    repository = await _make_repo(tmp_path, f"credential-invalid-{exchange}-{method}.db")
    await _seed_wallet(repository)
    with pytest.raises(ValueError, match=message):
        await _create_credential(
            repository,
            method,
            exchange=exchange,
            credential_type=credential_type,
        )
    credentials = await repository.list_wallet_credentials_for_wallet(_WALLET, _NOW)
    assert credentials == []


async def test_existing_config_rejects_unclassified_credential(tmp_path: Path) -> None:
    """Unclassified credential creation cannot contradict an active config."""
    repository = await _make_repo(tmp_path, "credential-existing-config.db")
    await _seed_wallet(repository)
    await repository.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        session_id=_SESSION,
        sequence_id=2,
        timestamp=_NOW - timedelta(seconds=1),
    )
    with pytest.raises(ReconciliationMethodImmutableError, match="unclassified credential"):
        await _create_credential(repository, "unclassified")
    credentials = await repository.list_wallet_credentials_for_wallet(_WALLET, _NOW)
    assert credentials == []


async def test_commit_failure_rolls_back_credential_and_config(tmp_path: Path) -> None:
    """A failed commit leaves neither half of real-method provisioning."""
    repository = await _make_repo(tmp_path, "credential-commit-failure.db")
    await _seed_wallet(repository)

    async def fail_commit(session: AsyncSession) -> None:
        """Roll back flushed rows and model a non-unique commit failure."""
        await session.rollback()
        raise IntegrityError("COMMIT", {}, Exception("forced commit failure"))

    with (
        patch.object(AsyncSession, "commit", fail_commit),
        pytest.raises(IntegrityError, match="forced commit failure"),
    ):
        await _create_credential(repository, "futures_position")
    async with repository.session() as session:
        credential_count = await session.scalar(select(func.count(WalletCredential.id)))
        config_count = await session.scalar(
            select(func.count(PortfolioReconciliationMethodConfig.id))
        )
    assert credential_count == 0
    assert config_count == 0


async def test_cross_repository_config_and_evaluation_race_fails_closed(
    tmp_path: Path,
) -> None:
    """SQLite serialization permits only config-consistent race outcomes."""
    database = tmp_path / "credential-config-race.db"
    first = SQLAlchemyRepository(f"sqlite+aiosqlite:///{database}")
    second = SQLAlchemyRepository(f"sqlite+aiosqlite:///{database}")
    await first.create_all()
    await _seed_wallet(first)
    config_write = first.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        session_id=_SESSION,
        sequence_id=2,
        timestamp=_NOW,
    )
    evaluation_write = second.record_portfolio_reconciliation(_evaluation(3))
    race_results = cast(
        list[object],
        await asyncio.gather(
            config_write,
            evaluation_write,
            return_exceptions=True,
        ),
    )
    config_result: object = race_results[0]
    evaluation_result: object = race_results[1]
    assert not isinstance(config_result, BaseException)
    active = await first.get_active_portfolio_reconciliation_method_config(
        _WALLET,
        "kraken_futures",
        "live",
    )
    assert active is not None
    assert active["method"] == "futures_position"
    async with first.session() as session:
        observation_count = await session.scalar(
            select(func.count(PortfolioReconciliationObservation.id))
        )
        state_count = await session.scalar(select(func.count(PortfolioReconciliationState.id)))
    if isinstance(evaluation_result, BaseException):
        assert isinstance(evaluation_result, RuntimeError)
        assert "active method config" in str(evaluation_result)
        assert observation_count == 0
        assert state_count == 0
    else:
        assert isinstance(evaluation_result, int)
        assert observation_count == 1
        assert state_count == 1
