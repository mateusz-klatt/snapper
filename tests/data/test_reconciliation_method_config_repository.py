"""Tests for immutable reconciliation-method configuration and transition guards."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioDriftEpisode
from snapper.data.models import PortfolioReconciliationMethodConfig
from snapper.data.models import PortfolioReconciliationObservation
from snapper.data.models import PortfolioReconciliationState
from snapper.data.models import VenueAccountState
from snapper.data.models import Wallet
from snapper.data.repository import ReconciliationMethodImmutableError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _canonicalize_reconciliation_wallet_public_id
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow

_NOW = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
_WALLET = "00000000-0000-7000-8000-000000000101"
_ACCOUNT = "00000000-0000-7000-8000-000000000201"
_SESSION = "00000000-0000-7000-8000-000000000301"
_EPISODE = "00000000-0000-7000-8000-000000000401"
_ALPHA_WALLET = "abcdefab-cdef-7abc-8def-abcdefabcdef"


@pytest.mark.parametrize(
    "wallet_public_id",
    [
        _ALPHA_WALLET,
        _ALPHA_WALLET.upper(),
        _ALPHA_WALLET.replace("-", ""),
    ],
)
def test_reconciliation_wallet_public_id_is_canonicalized(wallet_public_id: str) -> None:
    """Canonical, uppercase, and hyphenless UUID spellings converge."""
    assert _canonicalize_reconciliation_wallet_public_id(wallet_public_id) == _ALPHA_WALLET


def test_invalid_reconciliation_wallet_public_id_is_rejected() -> None:
    """An unparseable reconciliation wallet identity raises ValueError."""
    with pytest.raises(ValueError, match="reconciliation wallet identity is invalid"):
        _canonicalize_reconciliation_wallet_public_id("not-a-wallet-uuid")


async def _make_repo(tmp_path: Path, name: str) -> SQLAlchemyRepository:
    """Create one isolated full-schema repository."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / name}")
    await repository.create_all()
    return repository


async def _seed_wallet(
    repository: SQLAlchemyRepository,
    wallet_public_id: str = _WALLET,
) -> None:
    """Insert the active live wallet required by both locked writers."""
    async with repository.session() as session:
        session.add(
            Wallet(
                label="s4a-live-wallet",
                description=None,
                is_paper=False,
                public_id=wallet_public_id,
                session_id=_SESSION,
                sequence_id=1,
                timestamp=_NOW - timedelta(hours=1),
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()


@pytest.mark.parametrize(
    "wallet_alias",
    [_ALPHA_WALLET.upper(), _ALPHA_WALLET.replace("-", "")],
)
async def test_active_config_read_canonicalizes_wallet_aliases(
    tmp_path: Path,
    wallet_alias: str,
) -> None:
    """Alias-spelled reads find a canonically stored active method config.

    Given: A canonical alphabetic wallet and active reconciliation method,
    When: The config read uses its uppercase or hyphenless wallet spelling,
    Then: The canonical active config is returned on SQLite.
    """
    repository = await _make_repo(tmp_path, "aliased-active-config.db")
    await _seed_wallet(repository, _ALPHA_WALLET)
    await repository.set_portfolio_reconciliation_method_config(
        wallet_public_id=_ALPHA_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        session_id=_SESSION,
        sequence_id=2,
        timestamp=_NOW,
    )

    config = await repository.get_active_portfolio_reconciliation_method_config(
        wallet_alias,
        "kraken_futures",
        "live",
    )

    assert config is not None
    assert config["wallet_public_id"] == _ALPHA_WALLET


async def test_active_config_read_rejects_malformed_wallet_identity(
    tmp_path: Path,
) -> None:
    """A malformed config wallet identity raises the exact shared ValueError.

    Given: A reconciliation method-config repository,
    When: The active-config read receives a non-UUID wallet identity,
    Then: The DAL rejects it with the writer-compatible canonicalization error.
    """
    repository = await _make_repo(tmp_path, "malformed-active-config.db")

    with pytest.raises(ValueError) as exc_info:
        await repository.get_active_portfolio_reconciliation_method_config(
            "not-a-wallet-uuid",
            "kraken_futures",
            "live",
        )

    assert str(exc_info.value) == "reconciliation wallet identity is invalid"


async def _seed_drift_episode(repository: SQLAlchemyRepository) -> None:
    """Insert one valid open episode without adding method-bearing history."""
    async with repository.session() as session:
        session.add(
            PortfolioDriftEpisode(
                wallet_public_id=_WALLET,
                exchange="kraken_futures",
                mode="live",
                status="open",
                opened_at=_NOW,
                closed_at=None,
                trigger_observation_id=1,
                last_observation_id=1,
                details_source_observation_id=1,
                latest_full_mismatch_count=3,
                resolution_reason=None,
                closed_by_user_public_id=None,
                closed_by_operator_public_id=None,
                rebase_anchor_public_id=None,
                public_id=_EPISODE,
                session_id=_SESSION,
                sequence_id=2,
                timestamp=_NOW,
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()


async def _configure(
    repository: SQLAlchemyRepository,
    method: str,
    *,
    sequence_id: int = 2,
    timestamp: datetime = _NOW - timedelta(minutes=10),
) -> str:
    """Persist one real method and return its stable logical identity."""
    row = await repository.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method=method,
        session_id=_SESSION,
        sequence_id=sequence_id,
        timestamp=timestamp,
    )
    return str(row["public_id"])


def _evaluation(
    method: str,
    status: str,
    *,
    sequence_id: int,
    full: bool = False,
) -> PortfolioReconciliationEvaluationRow:
    """Build one method-scoped S1 evaluation with optional full evidence."""
    bus_time = _NOW + timedelta(seconds=sequence_id)
    return {
        "wallet_public_id": _WALLET,
        "exchange": "kraken_futures",
        "mode": "live",
        "method": method,
        "evaluation_status": status,
        "venue_account_state_public_id": _ACCOUNT if full else None,
        "venue_account_observation_id": 41 if full else None,
        "account_authoritative_until": bus_time + timedelta(minutes=5) if full else None,
        "source_watermark_kind": "venue_event_id" if full else None,
        "source_watermark": sequence_id if full else None,
        "anchor_public_id": None,
        "expected_json": '{"quantity":"1"}' if full else None,
        "actual_json": '{"quantity":"2"}' if full else None,
        "difference_json": '{"quantity":"1"}' if full else None,
        "tolerance_json": '{"quantity":"0"}' if full else None,
        "error": (
            "margin_ledger_replay_not_implemented"
            if method == "margin_ledger_replay"
            else "reconciliation_method_unclassified" if method == "unclassified" else None
        ),
        "session_id": _SESSION,
        "sequence_id": sequence_id,
        "bus_time": bus_time,
    }


async def _active_state(repository: SQLAlchemyRepository) -> PortfolioReconciliationState:
    """Load the single active reconciliation state."""
    async with repository.session() as session:
        row = (
            (
                await session.execute(
                    select(PortfolioReconciliationState).where(
                        PortfolioReconciliationState.wallet_public_id == _WALLET,
                        PortfolioReconciliationState.exchange == "kraken_futures",
                        PortfolioReconciliationState.mode == "live",
                        PortfolioReconciliationState.known_to == KNOWN_TO_MAX,
                    )
                )
            )
            .scalars()
            .one()
        )
        return row


async def _delete_configs(repository: SQLAlchemyRepository) -> None:
    """Remove config rows to model pre-S4a history without classification."""
    async with repository.session() as session:
        await session.execute(delete(PortfolioReconciliationMethodConfig))
        await session.commit()


async def test_venue_account_state_version_returns_closed_row_or_none(
    tmp_path: Path,
) -> None:
    """Exact version reads include closed rows and return None for absent ids."""
    repository = await _make_repo(tmp_path, "venue-account-version.db")
    async with repository.session() as session:
        state = VenueAccountState(
            wallet_public_id=_WALLET,
            exchange="kraken_futures",
            mode="live",
            sync_status="error",
            balance_status="error",
            position_status="error",
            valuation_status="native_only",
            current_attempt_observation_id=1,
            error="venue unavailable",
            public_id=_ACCOUNT,
            session_id=_SESSION,
            sequence_id=1,
            timestamp=_NOW - timedelta(minutes=2),
            known_to=_NOW - timedelta(minutes=1),
        )
        session.add(state)
        await session.flush()
        state_id = int(state.id)
        await session.commit()
    found = await repository.get_venue_account_state_version(state_id)
    missing = await repository.get_venue_account_state_version(state_id + 1)
    assert found is not None
    assert found["public_id"] == _ACCOUNT
    assert missing is None


async def test_non_sqlite_reconciliation_write_skips_begin_immediate(
    tmp_path: Path,
) -> None:
    """Non-SQLite writers leave transaction startup to their native dialect."""
    repository = SQLAlchemyRepository(
        f"sqlite+aiosqlite:///{tmp_path / 'non-sqlite-write-preamble.db'}"
    )
    session = AsyncMock(add=MagicMock())
    with patch.object(
        SQLAlchemyRepository,
        "dialect_name",
        new_callable=PropertyMock,
        return_value="postgresql",
    ):
        await repository._begin_portfolio_reconciliation_write(session)
    session.execute.assert_not_awaited()


async def test_classification_captures_active_unclassified_observation_id(
    tmp_path: Path,
) -> None:
    """Classification durably names the active unclassified predecessor."""
    repository = await _make_repo(tmp_path, "capture-unclassified-predecessor.db")
    await _seed_wallet(repository)
    await repository.record_portfolio_reconciliation(
        _evaluation("unclassified", "incomplete", sequence_id=2)
    )
    predecessor = await _active_state(repository)
    config = await repository.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        session_id=_SESSION,
        sequence_id=3,
        timestamp=_NOW,
    )
    assert config["classified_after_observation_id"] == predecessor.current_observation_id
    async with repository.session() as session:
        stored = (
            (
                await session.execute(
                    select(PortfolioReconciliationMethodConfig).where(
                        PortfolioReconciliationMethodConfig.known_to == KNOWN_TO_MAX
                    )
                )
            )
            .scalars()
            .one()
        )
    assert stored.classified_after_observation_id == predecessor.current_observation_id


async def test_classification_without_prior_state_captures_null(tmp_path: Path) -> None:
    """A classification with no causal predecessor persists a null lineage id."""
    repository = await _make_repo(tmp_path, "capture-without-state.db")
    await _seed_wallet(repository)
    config = await repository.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        session_id=_SESSION,
        sequence_id=2,
        timestamp=_NOW,
    )
    active = await repository.get_active_portfolio_reconciliation_method_config(
        _WALLET,
        "kraken_futures",
        "live",
    )
    assert config["classified_after_observation_id"] is None
    assert active == config


async def test_same_method_replay_does_not_refresh_captured_observation_id(
    tmp_path: Path,
) -> None:
    """An idempotent re-PUT preserves the classification-time predecessor id."""
    repository = await _make_repo(tmp_path, "capture-idempotent-replay.db")
    await _seed_wallet(repository)
    await repository.record_portfolio_reconciliation(
        _evaluation("unclassified", "incomplete", sequence_id=2)
    )
    predecessor = await _active_state(repository)
    first = await repository.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        session_id=_SESSION,
        sequence_id=3,
        timestamp=_NOW,
    )
    await repository.record_portfolio_reconciliation(
        _evaluation("futures_position", "incomplete", sequence_id=4)
    )
    successor = await _active_state(repository)
    replay = await repository.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        session_id=_SESSION,
        sequence_id=99,
        timestamp=_NOW + timedelta(minutes=1),
    )
    assert successor.current_observation_id != predecessor.current_observation_id
    assert first["classified_after_observation_id"] == predecessor.current_observation_id
    assert replay["classified_after_observation_id"] == predecessor.current_observation_id
    assert replay["sequence_id"] == 3


async def test_pre_history_method_remint_captures_null(tmp_path: Path) -> None:
    """A history-free method correction re-mints with no predecessor lineage."""
    repository = await _make_repo(tmp_path, "capture-pre-history-remint.db")
    await _seed_wallet(repository)
    first = await repository.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        session_id=_SESSION,
        sequence_id=2,
        timestamp=_NOW - timedelta(minutes=1),
    )
    successor = await repository.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="spot_execution_replay",
        session_id=_SESSION,
        sequence_id=3,
        timestamp=_NOW,
    )
    assert first["classified_after_observation_id"] is None
    assert successor["classified_after_observation_id"] is None


async def test_first_config_lookup_and_same_method_replay_are_idempotent(
    tmp_path: Path,
) -> None:
    """The same active method preserves one row and identity after history."""
    repository = await _make_repo(tmp_path, "idempotent.db")
    await _seed_wallet(repository)
    public_id = await _configure(repository, "futures_position")
    await repository.record_portfolio_reconciliation(
        _evaluation("futures_position", "incomplete", sequence_id=3)
    )
    replay = await repository.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        session_id=_SESSION,
        sequence_id=99,
        timestamp=_NOW,
    )
    active = await repository.get_active_portfolio_reconciliation_method_config(
        _WALLET,
        "kraken_futures",
        "live",
    )
    assert replay["public_id"] == public_id
    assert replay["sequence_id"] == 2
    assert active == replay
    async with repository.session() as session:
        rows = (await session.execute(select(PortfolioReconciliationMethodConfig))).scalars().all()
    assert len(rows) == 1


async def test_pre_history_method_change_versions_and_retains_public_id(
    tmp_path: Path,
) -> None:
    """A pre-history correction closes and inserts one stable SCD2 identity."""
    repository = await _make_repo(tmp_path, "pre-history-change.db")
    await _seed_wallet(repository)
    first_at = _NOW - timedelta(minutes=10)
    second_at = _NOW - timedelta(minutes=5)
    public_id = await _configure(
        repository,
        "futures_position",
        timestamp=first_at,
    )
    successor = await repository.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="spot_execution_replay",
        session_id=_SESSION,
        sequence_id=3,
        timestamp=second_at,
    )
    async with repository.session() as session:
        rows = (
            (
                await session.execute(
                    select(PortfolioReconciliationMethodConfig).order_by(
                        PortfolioReconciliationMethodConfig.id
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 2
    assert rows[0].method == "futures_position"
    assert rows[0].known_to == second_at
    assert rows[1].method == "spot_execution_replay"
    assert rows[1].known_to == KNOWN_TO_MAX
    assert rows[0].public_id == public_id
    assert rows[1].public_id == public_id
    assert successor["public_id"] == public_id


async def test_post_history_method_change_is_immutable(tmp_path: Path) -> None:
    """Any observation history freezes a different active method."""
    repository = await _make_repo(tmp_path, "immutable.db")
    await _seed_wallet(repository)
    await _configure(repository, "futures_position")
    await repository.record_portfolio_reconciliation(
        _evaluation("futures_position", "incomplete", sequence_id=3)
    )
    with pytest.raises(ReconciliationMethodImmutableError):
        await repository.set_portfolio_reconciliation_method_config(
            wallet_public_id=_WALLET,
            exchange="kraken_futures",
            mode="live",
            method="spot_execution_replay",
            session_id=_SESSION,
            sequence_id=4,
            timestamp=_NOW,
        )


async def test_first_config_rejects_episode_history_without_method_rows(
    tmp_path: Path,
) -> None:
    """Episode-only history cannot be assigned a method without matching state."""
    repository = await _make_repo(tmp_path, "episode-only-history.db")
    await _seed_wallet(repository)
    await _seed_drift_episode(repository)
    with pytest.raises(
        ReconciliationMethodImmutableError,
        match="drift episode history has no matching reconciliation state",
    ):
        await _configure(repository, "futures_position")


async def test_first_config_rejects_unclassified_history_with_episode(
    tmp_path: Path,
) -> None:
    """An episode makes otherwise bare unclassified history immutable."""
    repository = await _make_repo(tmp_path, "unclassified-episode-history.db")
    await _seed_wallet(repository)
    await repository.record_portfolio_reconciliation(
        _evaluation("unclassified", "incomplete", sequence_id=2)
    )
    await _seed_drift_episode(repository)
    with pytest.raises(
        ReconciliationMethodImmutableError,
        match="unclassified reconciliation history contains retained evidence",
    ):
        await _configure(repository, "futures_position", sequence_id=3)


async def test_first_config_accepts_matching_legacy_real_method_history(
    tmp_path: Path,
) -> None:
    """Matching pre-config real-method history permits explicit provisioning."""
    repository = await _make_repo(tmp_path, "matching-legacy.db")
    await _seed_wallet(repository)
    await _configure(repository, "futures_position")
    await repository.record_portfolio_reconciliation(
        _evaluation("futures_position", "incomplete", sequence_id=3)
    )
    await _delete_configs(repository)
    public_id = await _configure(
        repository,
        "futures_position",
        sequence_id=4,
        timestamp=_NOW,
    )
    active = await repository.get_active_portfolio_reconciliation_method_config(
        _WALLET,
        "kraken_futures",
        "live",
    )
    assert active is not None
    assert active["public_id"] == public_id
    assert active["method"] == "futures_position"


async def test_mixed_legacy_method_history_rejects_first_config(tmp_path: Path) -> None:
    """Contradictory pre-config state and observation methods fail closed."""
    repository = await _make_repo(tmp_path, "mixed-legacy.db")
    await _seed_wallet(repository)
    await _configure(repository, "futures_position")
    await repository.record_portfolio_reconciliation(
        _evaluation("futures_position", "incomplete", sequence_id=3)
    )
    await _delete_configs(repository)
    async with repository.session() as session:
        await session.execute(
            update(PortfolioReconciliationState)
            .where(PortfolioReconciliationState.known_to == KNOWN_TO_MAX)
            .values(method="spot_execution_replay")
        )
        await session.commit()
    with pytest.raises(ReconciliationMethodImmutableError):
        await _configure(
            repository,
            "futures_position",
            sequence_id=4,
            timestamp=_NOW,
        )


async def test_valid_unclassified_history_accepts_first_config_then_freezes(
    tmp_path: Path,
) -> None:
    """Safe unclassified history permits one real classification and no change."""
    repository = await _make_repo(tmp_path, "unclassified-first.db")
    await _seed_wallet(repository)
    await repository.record_portfolio_reconciliation(
        _evaluation("unclassified", "incomplete", sequence_id=2)
    )
    await repository.record_portfolio_reconciliation(
        _evaluation("unclassified", "error", sequence_id=3)
    )
    await _configure(
        repository,
        "futures_position",
        sequence_id=4,
        timestamp=_NOW,
    )
    await repository.record_portfolio_reconciliation(
        _evaluation("futures_position", "incomplete", sequence_id=5)
    )
    state = await _active_state(repository)
    assert state.method == "futures_position"
    with pytest.raises(ReconciliationMethodImmutableError):
        await _configure(
            repository,
            "spot_execution_replay",
            sequence_id=6,
            timestamp=_NOW + timedelta(seconds=1),
        )


async def test_real_evaluation_must_match_active_config(tmp_path: Path) -> None:
    """The S1 transaction rejects a real method contradicting its config."""
    repository = await _make_repo(tmp_path, "config-mismatch.db")
    await _seed_wallet(repository)
    await _configure(repository, "futures_position")
    with pytest.raises(RuntimeError, match="conflicts with active method config"):
        await repository.record_portfolio_reconciliation(
            _evaluation("spot_execution_replay", "incomplete", sequence_id=3)
        )


async def test_unclassified_evaluation_rejects_existing_config(tmp_path: Path) -> None:
    """An active real config makes every unclassified evaluation contradictory."""
    repository = await _make_repo(tmp_path, "configured-unclassified.db")
    await _seed_wallet(repository)
    await _configure(repository, "futures_position")
    with pytest.raises(RuntimeError, match="unclassified reconciliation conflicts"):
        await repository.record_portfolio_reconciliation(
            _evaluation("unclassified", "incomplete", sequence_id=3)
        )


async def test_stale_unclassified_evaluation_after_classification_drops_idempotently(
    tmp_path: Path,
) -> None:
    """A pre-classification unclassified tuple that lost the race is dropped, not raised.

    Given: an active real-method state at a newer sequence after the operator
        classified the wallet,
    When: a strictly older unclassified evaluation (produced before the config
        existed) finally arrives out of order,
    Then: it is ignored idempotently - the active state is untouched, nothing
        enters storage, and no spurious config-conflict error is raised.

    Args:
        tmp_path: Pytest temporary directory.
    """
    repository = await _make_repo(tmp_path, "stale-unclassified.db")
    await _seed_wallet(repository)
    await _configure(repository, "futures_position")
    newer_id = await repository.record_portfolio_reconciliation(
        _evaluation("futures_position", "incomplete", sequence_id=5)
    )
    before = await _active_state(repository)
    stale_id = await repository.record_portfolio_reconciliation(
        _evaluation("unclassified", "incomplete", sequence_id=4)
    )
    after = await _active_state(repository)
    assert stale_id == newer_id == before.id == after.id
    assert after.method == "futures_position"
    assert after.sequence_id == 5
    async with repository.session() as session:
        observations = (
            (await session.execute(select(PortfolioReconciliationObservation))).scalars().all()
        )
    assert len(observations) == 1
    assert observations[0].method == "futures_position"


@pytest.mark.parametrize(
    ("prior_status", "prior_full"),
    [
        pytest.param("incomplete", False, id="without-retained-detail"),
        pytest.param("mismatched", True, id="with-retained-detail-and-streak"),
    ],
)
async def test_unclassified_cannot_replace_real_state_without_config(
    tmp_path: Path,
    prior_status: str,
    prior_full: bool,
) -> None:
    """A missing config cannot authorize erasing a real-method state."""
    repository = await _make_repo(tmp_path, f"unclassified-over-real-{prior_status}.db")
    await _seed_wallet(repository)
    await _configure(repository, "futures_position")
    await repository.record_portfolio_reconciliation(
        _evaluation(
            "futures_position",
            prior_status,
            sequence_id=3,
            full=prior_full,
        )
    )
    before = await _active_state(repository)
    if prior_full:
        assert before.last_full_observation_id is not None
        assert before.detail_source_observation_id is not None
        assert before.consecutive_full_mismatches == 1
    else:
        assert before.last_full_observation_id is None
        assert before.detail_source_observation_id is None
        assert before.consecutive_full_mismatches == 0
    await _delete_configs(repository)
    assert (
        await repository.get_active_portfolio_reconciliation_method_config(
            _WALLET,
            "kraken_futures",
            "live",
        )
        is None
    )
    with pytest.raises(RuntimeError, match="method transition is invalid"):
        await repository.record_portfolio_reconciliation(
            _evaluation("unclassified", "incomplete", sequence_id=4)
        )
    after = await _active_state(repository)
    assert after.id == before.id
    assert after.method == "futures_position"
    assert after.consecutive_full_mismatches == before.consecutive_full_mismatches


async def test_real_method_cannot_replace_different_real_state(tmp_path: Path) -> None:
    """A matching incoming config cannot override a different real state method."""
    repository = await _make_repo(tmp_path, "real-over-real.db")
    await _seed_wallet(repository)
    await _configure(repository, "futures_position")
    await repository.record_portfolio_reconciliation(
        _evaluation("futures_position", "incomplete", sequence_id=3)
    )
    async with repository.session() as session:
        await session.execute(
            update(PortfolioReconciliationMethodConfig)
            .where(PortfolioReconciliationMethodConfig.known_to == KNOWN_TO_MAX)
            .values(method="spot_execution_replay")
        )
        await session.commit()
    with pytest.raises(RuntimeError, match="method transition is invalid"):
        await repository.record_portfolio_reconciliation(
            _evaluation("spot_execution_replay", "incomplete", sequence_id=4)
        )
    state = await _active_state(repository)
    assert state.method == "futures_position"


async def test_margin_method_accepts_only_error_and_retains_no_evidence(
    tmp_path: Path,
) -> None:
    """The DAL rejects incomplete margin rows and stores only bare error state."""
    repository = await _make_repo(tmp_path, "margin-error-only.db")
    await _seed_wallet(repository)
    await _configure(repository, "margin_ledger_replay")
    with pytest.raises(RuntimeError, match="cannot carry full evidence"):
        await repository.record_portfolio_reconciliation(
            _evaluation("margin_ledger_replay", "error", sequence_id=2, full=True)
        )
    with pytest.raises(RuntimeError, match="permits only error status"):
        await repository.record_portfolio_reconciliation(
            _evaluation("margin_ledger_replay", "incomplete", sequence_id=3)
        )
    await repository.record_portfolio_reconciliation(
        _evaluation("margin_ledger_replay", "error", sequence_id=4)
    )
    await repository.record_portfolio_reconciliation(
        _evaluation("margin_ledger_replay", "error", sequence_id=5)
    )
    state = await _active_state(repository)
    assert state.method == "margin_ledger_replay"
    assert state.current_evaluation_status == "error"
    assert state.last_full_observation_id is None
    assert state.detail_source_observation_id is None
    assert state.consecutive_full_mismatches == 0
    assert state.open_drift_episode_public_id is None
    assert state.expected_json is None
    assert state.actual_json is None


@pytest.mark.parametrize(
    ("exchange", "mode", "method"),
    [
        ("Kraken_Futures", "live", "futures_position"),
        ("kraken_futures", "paper", "futures_position"),
        ("kraken_futures", "live", "unclassified"),
    ],
)
async def test_config_writer_rejects_invalid_identity_or_nonreal_method(
    tmp_path: Path,
    exchange: str,
    mode: str,
    method: str,
) -> None:
    """The config plane accepts only canonical live identities and real methods."""
    repository = await _make_repo(tmp_path, f"invalid-config-{mode}-{method}.db")
    await _seed_wallet(repository)
    with pytest.raises(ValueError):
        await repository.set_portfolio_reconciliation_method_config(
            wallet_public_id=_WALLET,
            exchange=exchange,
            mode=mode,
            method=method,
            session_id=_SESSION,
            sequence_id=2,
            timestamp=_NOW,
        )


async def test_both_locked_writers_fail_closed_without_active_wallet(
    tmp_path: Path,
) -> None:
    """Neither config nor evaluation writes proceed for an absent wallet."""
    repository = await _make_repo(tmp_path, "absent-wallet.db")
    with pytest.raises(RuntimeError, match="active wallet is absent or ambiguous"):
        await _configure(repository, "futures_position")
    with pytest.raises(RuntimeError, match="active wallet is absent or ambiguous"):
        await repository.record_portfolio_reconciliation(
            _evaluation("unclassified", "incomplete", sequence_id=2)
        )


async def test_active_config_lookup_fails_closed_on_duplicate_rows(tmp_path: Path) -> None:
    """Corrupt duplicate active configs are detected and exposed as unclassified."""
    repository = await _make_repo(tmp_path, "duplicate-active-config.db")
    await _seed_wallet(repository)
    await _configure(repository, "futures_position")
    async with repository.session() as session:
        await session.execute(
            text("DROP INDEX uq_portfolio_reconciliation_method_configs_identity")
        )
        session.add(
            PortfolioReconciliationMethodConfig(
                wallet_public_id=_WALLET,
                exchange="kraken_futures",
                mode="live",
                method="spot_execution_replay",
                public_id="00000000-0000-7000-8000-000000000901",
                session_id=_SESSION,
                sequence_id=99,
                timestamp=_NOW,
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()
    assert (
        await repository.get_active_portfolio_reconciliation_method_config(
            _WALLET,
            "kraken_futures",
            "live",
        )
        is None
    )


async def test_active_config_lookup_fails_closed_on_corrupt_method(tmp_path: Path) -> None:
    """A CHECK-bypassed invalid method is detected and exposed as unclassified."""
    repository = await _make_repo(tmp_path, "corrupt-active-config.db")
    await _seed_wallet(repository)
    await _configure(repository, "futures_position")
    async with repository.session() as session:
        await session.execute(text("PRAGMA ignore_check_constraints = ON"))
        await session.execute(
            update(PortfolioReconciliationMethodConfig)
            .where(PortfolioReconciliationMethodConfig.known_to == KNOWN_TO_MAX)
            .values(method="corrupt_method")
        )
        await session.execute(text("PRAGMA ignore_check_constraints = OFF"))
        await session.commit()
    assert (
        await repository.get_active_portfolio_reconciliation_method_config(
            _WALLET,
            "kraken_futures",
            "live",
        )
        is None
    )
