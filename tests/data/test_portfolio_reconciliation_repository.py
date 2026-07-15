"""Tests for the portfolio reconciliation storage plane (PnL Phase 4 S1).

The repository owns mismatch streaks, stable drift-episode identities,
retained full-evidence provenance, clock clamping, concurrency serialization,
and the atomic observation/state/episode transaction. The schema independently
rejects every row-local contradiction on SQLite and emits equivalent
PostgreSQL migration DDL.
"""

import asyncio
import importlib
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from io import StringIO
from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioDriftEpisode
from snapper.data.models import PortfolioReconciliationMethodConfig
from snapper.data.models import PortfolioReconciliationObservation
from snapper.data.models import PortfolioReconciliationState
from snapper.data.models import Wallet
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import SpotReconciliationAnchorRow

_WALLET = "00000000-0000-7000-8000-000000000101"
_OTHER_WALLET = "00000000-0000-7000-8000-000000000102"
_ACCOUNT_STATE = "00000000-0000-7000-8000-000000000201"
_ANCHOR = "00000000-0000-7000-8000-000000000301"
_EPISODE = "00000000-0000-7000-8000-000000000401"
_SESSION = "00000000-0000-7000-8000-000000000501"
_OTHER_SESSION = "00000000-0000-7000-8000-000000000502"
_EARLIER_SESSION = "00000000-0000-7000-8000-000000000500"
_PUBLIC = "00000000-0000-7000-8000-000000000601"
_OTHER_PUBLIC = "00000000-0000-7000-8000-000000000602"
_T0 = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)


def _spot_anchor(
    wallet_public_id: str = _OTHER_WALLET,
    exchange: str = "kraken",
) -> SpotReconciliationAnchorRow:
    """Build the real immutable anchor required by full spot outcomes."""
    return {
        "public_id": _ANCHOR,
        "wallet_public_id": wallet_public_id,
        "exchange": exchange,
        "mode": "live",
        "venue_account_state_public_id": _ACCOUNT_STATE,
        "balance_observation_id": 1,
        "source_watermark_kind": "execution_id",
        "source_watermark": 0,
        "balances_json": '{"USD":"1"}',
        "first_request_started_at": _T0,
        "first_request_completed_at": _T0,
        "second_request_started_at": _T0,
        "second_request_completed_at": _T0,
        "boundary_status": "double_read_equal",
        "inventory_status": "certified_full",
        "margin_status": "cash",
        "provenance": "test",
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": _T0,
    }


def _evaluation(
    bus_time: datetime,
    status: str,
    *,
    wallet_public_id: str = _WALLET,
    exchange: str = "kraken_futures",
    mode: str = "live",
    method: str = "futures_position",
    sequence_id: int = 1,
    expected_json: str | None = '{"quantity": 1}',
    error: str | None = None,
) -> PortfolioReconciliationEvaluationRow:
    """Build one raw evaluation with complete evidence by default.

    Args:
        bus_time: Evaluation and bus timestamp.
        status: Raw evaluation outcome.
        wallet_public_id: Full wallet identity.
        exchange: Lowercase venue name.
        mode: Trading mode.
        method: Reconciliation method.
        sequence_id: Temporal sequence id.
        expected_json: Expected comparison payload.
        error: Optional failure detail.

    Returns:
        Complete typed repository input.
    """
    resolved_error = "venue unavailable" if status == "error" and error is None else error
    full_spot = method == "spot_execution_replay" and status in ("matched", "mismatched")
    return {
        "wallet_public_id": wallet_public_id,
        "exchange": exchange,
        "mode": mode,
        "method": method,
        "evaluation_status": status,
        "venue_account_state_public_id": _ACCOUNT_STATE,
        "venue_account_observation_id": 41,
        "account_authoritative_until": bus_time + timedelta(minutes=5),
        "source_watermark_kind": "execution_id" if full_spot else "venue_event_id",
        "source_watermark": sequence_id,
        "anchor_public_id": _ANCHOR if full_spot else None,
        "expected_json": expected_json,
        "actual_json": '{"quantity": 2}',
        "difference_json": '{"quantity": 1}',
        "tolerance_json": '{"quantity": 0}',
        "error": resolved_error,
        "session_id": _SESSION,
        "sequence_id": sequence_id,
        "bus_time": bus_time,
    }


def _non_full_evaluation(
    bus_time: datetime, status: str, sequence_id: int
) -> PortfolioReconciliationEvaluationRow:
    """Build a non-full evaluation with no full-result evidence.

    Args:
        bus_time: Evaluation and bus timestamp.
        status: Incomplete, unsupported, or error outcome.
        sequence_id: Temporal sequence id.

    Returns:
        Typed repository input carrying no full-result evidence.
    """
    evaluation = _evaluation(bus_time, status, sequence_id=sequence_id)
    evaluation["venue_account_state_public_id"] = None
    evaluation["venue_account_observation_id"] = None
    evaluation["account_authoritative_until"] = None
    evaluation["source_watermark_kind"] = None
    evaluation["source_watermark"] = None
    evaluation["anchor_public_id"] = None
    evaluation["expected_json"] = None
    evaluation["actual_json"] = None
    evaluation["difference_json"] = None
    evaluation["tolerance_json"] = None
    return evaluation


def _retained_non_full_observation_values(status: str, episode_public_id: str) -> dict[str, object]:
    """Build a CHECK-valid retained non-full observation mutation.

    Args:
        status: Incomplete, unsupported, or error outcome.
        episode_public_id: Retained active episode identity.

    Returns:
        Observation values with retained lineage and no full evidence.
    """
    return {
        "evaluation_status": status,
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
        "resulting_full_mismatch_count": 3,
        "drift_episode_public_id": episode_public_id,
        "error": "venue unavailable" if status == "error" else None,
    }


async def _make_repo(tmp_path: Path, name: str = "reconciliation.db") -> SQLAlchemyRepository:
    """Create an on-disk SQLite repository with the full ORM schema.

    Args:
        tmp_path: Pytest temporary directory.
        name: Database filename.

    Returns:
        Repository bound to a fresh SQLite database.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / name}")
    await repo.create_all()
    seeded_at = _T0 - timedelta(days=1)
    async with repo.session() as session:
        session.add_all(
            [
                Wallet(
                    public_id=_WALLET,
                    label="reconciliation-primary",
                    description=None,
                    is_paper=False,
                    session_id=_SESSION,
                    sequence_id=1,
                    timestamp=seeded_at,
                    known_to=KNOWN_TO_MAX,
                ),
                Wallet(
                    public_id=_OTHER_WALLET,
                    label="reconciliation-secondary",
                    description=None,
                    is_paper=False,
                    session_id=_SESSION,
                    sequence_id=2,
                    timestamp=seeded_at,
                    known_to=KNOWN_TO_MAX,
                ),
                PortfolioReconciliationMethodConfig(
                    wallet_public_id=_WALLET,
                    exchange="kraken_futures",
                    mode="live",
                    method="futures_position",
                    session_id=_SESSION,
                    sequence_id=3,
                    timestamp=seeded_at,
                    known_to=KNOWN_TO_MAX,
                ),
                PortfolioReconciliationMethodConfig(
                    wallet_public_id=_OTHER_WALLET,
                    exchange="kraken",
                    mode="live",
                    method="spot_execution_replay",
                    session_id=_SESSION,
                    sequence_id=4,
                    timestamp=seeded_at,
                    known_to=KNOWN_TO_MAX,
                ),
            ]
        )
        await session.commit()
    return repo


async def _observations(
    repo: SQLAlchemyRepository,
) -> list[PortfolioReconciliationObservation]:
    """Return every reconciliation observation in id order.

    Args:
        repo: Repository under test.

    Returns:
        All append-only observations.
    """
    async with repo.session() as session:
        rows = await session.execute(
            select(PortfolioReconciliationObservation).order_by(
                PortfolioReconciliationObservation.id
            )
        )
        return list(rows.scalars().all())


async def _states(repo: SQLAlchemyRepository) -> list[PortfolioReconciliationState]:
    """Return every reconciliation state version in id order.

    Args:
        repo: Repository under test.

    Returns:
        Historical and active state versions.
    """
    async with repo.session() as session:
        rows = await session.execute(
            select(PortfolioReconciliationState).order_by(PortfolioReconciliationState.id)
        )
        return list(rows.scalars().all())


async def _episodes(repo: SQLAlchemyRepository) -> list[PortfolioDriftEpisode]:
    """Return every drift-episode version in id order.

    Args:
        repo: Repository under test.

    Returns:
        Historical and active episode versions.
    """
    async with repo.session() as session:
        rows = await session.execute(
            select(PortfolioDriftEpisode).order_by(PortfolioDriftEpisode.id)
        )
        return list(rows.scalars().all())


async def _active_state(repo: SQLAlchemyRepository) -> PortfolioReconciliationState:
    """Return the only active reconciliation state.

    Args:
        repo: Repository under test.

    Returns:
        Sentinel-active state.
    """
    active = [row for row in await _states(repo) if row.known_to == KNOWN_TO_MAX]
    assert len(active) == 1
    return active[0]


async def test_mismatch_streak_opens_one_stable_episode_on_third(
    tmp_path: Path,
) -> None:
    """First through fourth mismatches derive one stable episode identity.

    Given: no prior reconciliation state,
    When: four full mismatches are recorded,
    Then: counts advance 1, 2, 3, 4; the third mints one episode and the
        fourth versions that same identity with updated evidence.
    """
    repo = await _make_repo(tmp_path)
    for offset in range(4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=offset), "mismatched", sequence_id=offset + 1)
        )
    observations = await _observations(repo)
    episodes = await _episodes(repo)
    state = await _active_state(repo)
    assert [row.resulting_full_mismatch_count for row in observations] == [1, 2, 3, 4]
    assert observations[0].drift_episode_public_id is None
    assert observations[1].drift_episode_public_id is None
    assert observations[2].drift_episode_public_id is not None
    assert observations[2].drift_episode_public_id == observations[3].drift_episode_public_id
    assert len(episodes) == 2
    assert episodes[0].known_to != KNOWN_TO_MAX
    assert episodes[1].known_to == KNOWN_TO_MAX
    assert episodes[0].public_id == episodes[1].public_id
    assert episodes[1].trigger_observation_id == observations[2].id
    assert episodes[1].last_observation_id == observations[3].id
    assert episodes[1].details_source_observation_id == observations[3].id
    assert episodes[1].latest_full_mismatch_count == 4
    assert state.open_drift_episode_public_id == episodes[1].public_id
    assert state.consecutive_full_mismatches == episodes[1].latest_full_mismatch_count
    assert state.last_full_observation_id == episodes[1].last_observation_id
    assert state.detail_source_observation_id == observations[3].id


async def test_drift_transition_lookup_preserves_each_exact_lifecycle_version(
    tmp_path: Path,
) -> None:
    """Exact evaluation lookup preserves committed episode lifecycle versions.

    Given: an episode opens, advances, and resolves across three evaluations,
    When: each evaluation tuple is queried after later versions commit,
    Then: every transition remains available with complete paging evidence.
    """
    repo = await _make_repo(tmp_path)
    assert (
        await repo.get_portfolio_drift_episode_transition(
            _WALLET,
            "kraken_futures",
            "live",
            _SESSION,
            1,
        )
        is None
    )
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    observations = await _observations(repo)
    opened = await repo.get_portfolio_drift_episode_transition(
        _WALLET,
        "kraken_futures",
        "live",
        _SESSION,
        3,
    )
    assert opened is not None
    episode_public_id = opened["public_id"]
    assert opened == {
        "wallet_public_id": _WALLET,
        "exchange": "kraken_futures",
        "mode": "live",
        "status": "open",
        "opened_at": _T0 + timedelta(seconds=3),
        "closed_at": None,
        "trigger_observation_id": observations[2].id,
        "last_observation_id": observations[2].id,
        "latest_full_mismatch_count": 3,
        "resolution_reason": None,
        "public_id": episode_public_id,
        "session_id": _SESSION,
        "sequence_id": 3,
    }
    assert (
        await repo.get_portfolio_drift_episode_transition(
            _WALLET,
            "kraken_futures",
            "live",
            _OTHER_SESSION,
            3,
        )
        is None
    )

    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=4), "mismatched", sequence_id=4)
    )
    assert (
        await repo.get_portfolio_drift_episode_transition(
            _WALLET,
            "kraken_futures",
            "live",
            _SESSION,
            3,
        )
        == opened
    )
    observations = await _observations(repo)
    continued = await repo.get_portfolio_drift_episode_transition(
        _WALLET,
        "kraken_futures",
        "live",
        _SESSION,
        4,
    )
    assert continued is not None
    assert continued["status"] == "open"
    assert continued["public_id"] == episode_public_id
    assert continued["trigger_observation_id"] == observations[2].id
    assert continued["last_observation_id"] == observations[3].id
    assert continued["latest_full_mismatch_count"] == 4
    assert continued["session_id"] == _SESSION
    assert continued["sequence_id"] == 4

    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=5), "matched", sequence_id=5)
    )
    assert (
        await repo.get_portfolio_drift_episode_transition(
            _WALLET,
            "kraken_futures",
            "live",
            _SESSION,
            4,
        )
        == continued
    )
    observations = await _observations(repo)
    resolved = await repo.get_portfolio_drift_episode_transition(
        _WALLET,
        "kraken_futures",
        "live",
        _SESSION,
        5,
    )
    assert resolved == {
        "wallet_public_id": _WALLET,
        "exchange": "kraken_futures",
        "mode": "live",
        "status": "resolved",
        "opened_at": _T0 + timedelta(seconds=3),
        "closed_at": _T0 + timedelta(seconds=5),
        "trigger_observation_id": observations[2].id,
        "last_observation_id": observations[4].id,
        "latest_full_mismatch_count": 4,
        "resolution_reason": "matched",
        "public_id": episode_public_id,
        "session_id": _SESSION,
        "sequence_id": 5,
    }


async def test_match_resolves_episode_and_new_streak_mints_new_identity(
    tmp_path: Path,
) -> None:
    """A full match resolves drift and a later third mismatch mints anew.

    Given: an open episode after three full mismatches,
    When: a full match and then three new full mismatches arrive,
    Then: the old episode becomes resolved, the streak resets, and a distinct
        stable episode identity opens for the new streak.
    """
    repo = await _make_repo(tmp_path)
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    old_identity = (await _active_state(repo)).open_drift_episode_public_id
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=4), "matched", sequence_id=4)
    )
    matched = await _active_state(repo)
    assert matched.consecutive_full_mismatches == 0
    assert matched.open_drift_episode_public_id is None
    resolved = [row for row in await _episodes(repo) if row.known_to == KNOWN_TO_MAX]
    assert len(resolved) == 1
    assert resolved[0].status == "resolved"
    assert resolved[0].resolution_reason == "matched"
    assert resolved[0].closed_at == matched.timestamp
    for sequence in range(5, 8):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    new_identity = (await _active_state(repo)).open_drift_episode_public_id
    assert old_identity is not None
    assert new_identity is not None
    assert new_identity != old_identity


@pytest.mark.parametrize("status", ["incomplete", "error", "unsupported"])
async def test_non_full_evaluation_retains_without_advancing(tmp_path: Path, status: str) -> None:
    """Unavailable evaluations retain prior full evidence without counting.

    Given: one full mismatch with complete comparison evidence,
    When: an incomplete, error, or unsupported evaluation intervenes,
    Then: the current status becomes unavailable while streak, last-full id,
        detail-source id, payload, and authority remain from the prior full
        observation; the following mismatch advances only to two.

    Args:
        tmp_path: Pytest temporary directory.
        status: Non-full outcome under test.
    """
    repo = await _make_repo(tmp_path, f"{status}.db")
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
    first = await _active_state(repo)
    await repo.record_portfolio_reconciliation(
        _evaluation(
            _T0 + timedelta(seconds=1),
            status,
            sequence_id=2,
            expected_json='{"partial": true}',
        )
    )
    retained = await _active_state(repo)
    observations = await _observations(repo)
    assert retained.current_evaluation_status == status
    assert retained.current_observation_id == observations[-1].id
    assert retained.current_observation_id != first.current_observation_id
    assert retained.last_full_observation_id == first.last_full_observation_id
    assert retained.detail_source_observation_id == first.detail_source_observation_id
    assert retained.expected_json == first.expected_json
    assert retained.authoritative_until == first.authoritative_until
    assert retained.consecutive_full_mismatches == 1
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=2), "mismatched", sequence_id=3)
    )
    assert (await _active_state(repo)).consecutive_full_mismatches == 2


async def test_error_during_open_episode_preserves_episode_without_versioning(
    tmp_path: Path,
) -> None:
    """An error during sustained drift preserves the open episode unchanged.

    Given: an open episode at the third consecutive full mismatch,
    When: an errored evaluation arrives before the next full mismatch,
    Then: the observation and state retain count three and the stable episode
        identity, while no new episode lifecycle version is created.
    """
    repo = await _make_repo(tmp_path)
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    before = await _active_state(repo)
    episode_versions = len(await _episodes(repo))
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=4), "error", sequence_id=4)
    )
    after = await _active_state(repo)
    observation = (await _observations(repo))[-1]
    assert after.current_observation_id == observation.id
    assert after.consecutive_full_mismatches == 3
    assert after.open_drift_episode_public_id == before.open_drift_episode_public_id
    assert after.detail_source_observation_id == before.detail_source_observation_id
    assert observation.resulting_full_mismatch_count == 3
    assert observation.drift_episode_public_id == before.open_drift_episode_public_id
    assert len(await _episodes(repo)) == episode_versions


@pytest.mark.parametrize("status", ["incomplete", "error", "unsupported"])
async def test_non_full_open_episode_retention_preserves_genuine_detail_and_resolves(
    tmp_path: Path, status: str
) -> None:
    """Non-full retention leaves genuine episode detail safe to resolve.

    Given: a genuine full-mismatch trigger and detail source for an open episode,
    When: a non-full observation with no full evidence intervenes before a match,
    Then: retained state and episode lineage remain unchanged and resolution
        preserves the genuine mismatch detail rather than the non-full attempt.

    Args:
        tmp_path: Pytest temporary directory.
        status: Non-full outcome under test.
    """
    repo = await _make_repo(tmp_path, f"episode-retention-{status}.db")
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    episode = (await _episodes(repo))[-1]
    genuine_detail_id = episode.details_source_observation_id
    await repo.record_portfolio_reconciliation(
        _non_full_evaluation(_T0 + timedelta(seconds=4), status, 4)
    )
    retained = await _active_state(repo)
    retained_observation = (await _observations(repo))[-1]
    assert retained.current_evaluation_status == status
    assert retained.detail_source_observation_id == genuine_detail_id
    assert retained_observation.resulting_full_mismatch_count == 3
    assert retained_observation.drift_episode_public_id == episode.public_id
    assert retained_observation.expected_json is None
    retained_episode = (await _episodes(repo))[-1]
    assert retained_episode.latest_full_mismatch_count == retained.consecutive_full_mismatches
    assert retained_episode.last_observation_id == retained.last_full_observation_id
    assert retained_episode.details_source_observation_id == genuine_detail_id
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=5), "matched", sequence_id=5)
    )
    observations = await _observations(repo)
    resolved = (await _episodes(repo))[-1]
    genuine_detail = next(row for row in observations if row.id == genuine_detail_id)
    assert resolved.status == "resolved"
    assert resolved.trigger_observation_id == genuine_detail_id
    assert resolved.details_source_observation_id == genuine_detail_id
    assert resolved.last_observation_id == observations[-1].id
    assert genuine_detail.evaluation_status == "mismatched"
    assert genuine_detail.expected_json is not None


async def test_initial_incomplete_has_no_invented_full_truth_and_reads_are_scoped(
    tmp_path: Path,
) -> None:
    """An initial non-full result stores no full detail and reads stay scoped.

    Given: incomplete evaluations for two wallets and an older closed version,
    When: active states are read with empty, wallet, and unscoped filters,
    Then: no full truth is invented, closed rows stay hidden, and projection
        includes every field of only sentinel-active rows in stable ordering.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "incomplete"))
    first = await _active_state(repo)
    assert first.last_full_observation_id is None
    assert first.detail_source_observation_id is None
    assert first.expected_json is None
    assert first.consecutive_full_mismatches == 0
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(milliseconds=500), "incomplete", sequence_id=2)
    )
    current = await _active_state(repo)
    await repo.record_spot_reconciliation_anchor(_spot_anchor())
    await repo.record_portfolio_reconciliation(
        _evaluation(
            _T0 + timedelta(seconds=1),
            "matched",
            wallet_public_id=_OTHER_WALLET,
            exchange="kraken",
            method="spot_execution_replay",
            sequence_id=3,
        )
    )
    assert await repo.get_portfolio_reconciliation_states([]) == []
    scoped = await repo.get_portfolio_reconciliation_states([_WALLET])
    assert len(scoped) == 1
    assert scoped[0]["wallet_public_id"] == _WALLET
    assert scoped[0]["current_evaluation_status"] == "incomplete"
    assert scoped[0]["method"] == "futures_position"
    assert scoped[0]["public_id"] == current.public_id
    assert scoped[0]["timestamp"] == current.timestamp
    unscoped = await repo.get_portfolio_reconciliation_states(None)
    assert [row["exchange"] for row in unscoped] == ["kraken", "kraken_futures"]


async def test_future_clock_is_clamped_for_state_and_episode_versions(tmp_path: Path) -> None:
    """Lagging bus clocks cannot move SCD2 state or episode history backward.

    Given: a third mismatch effective in the future,
    When: a fourth mismatch arrives with an earlier bus clock,
    Then: the observation records its honest bus time while state and episode
        successor timestamps clamp to their predecessor timestamp.
    """
    repo = await _make_repo(tmp_path)
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    predecessor = await _active_state(repo)
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 - timedelta(hours=1), "mismatched", sequence_id=4)
    )
    state = await _active_state(repo)
    observations = await _observations(repo)
    episode = [row for row in await _episodes(repo) if row.known_to == KNOWN_TO_MAX][0]
    assert observations[-1].timestamp == _T0 - timedelta(hours=1)
    assert state.timestamp == predecessor.timestamp
    assert episode.timestamp == predecessor.timestamp


async def test_regressed_trigger_clock_preserves_valid_episode_opening(
    tmp_path: Path,
) -> None:
    """A lagging trigger bus clock retains its genuine clamped opening time.

    Given: two mismatches followed by a third whose bus clock regresses,
    When: the episode opens and a fourth monotonic mismatch versions it,
    Then: opening-time validation derives the predecessor-clamped time from
        the observation log and accepts the genuine lifecycle.

    Args:
        tmp_path: Pytest temporary directory.
    """
    repo = await _make_repo(tmp_path, "regressed-trigger-clock.db")
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=1), "mismatched", sequence_id=1)
    )
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=2), "mismatched", sequence_id=2)
    )
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 - timedelta(hours=1), "mismatched", sequence_id=3)
    )
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=4), "mismatched", sequence_id=4)
    )
    episode = (await _episodes(repo))[-1]
    assert episode.opened_at == _T0 + timedelta(seconds=2)
    assert episode.latest_full_mismatch_count == 4


async def test_concurrent_first_insert_and_third_mismatch_serialize(tmp_path: Path) -> None:
    """SQLite serialization preserves first-insert and episode uniqueness.

    Given: two concurrent writers on an empty identity and later a two-count
        mismatch predecessor,
    When: each pair records concurrently,
    Then: all four evaluations commit as counts 1 through 4 with one active
        state and exactly one stable open episode identity.
    """
    repo = await _make_repo(tmp_path)
    await asyncio.gather(
        repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched", sequence_id=1)),
        repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(microseconds=1), "mismatched", sequence_id=2)
        ),
    )
    assert sorted(row.resulting_full_mismatch_count for row in await _observations(repo)) == [1, 2]
    await asyncio.gather(
        repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=2), "mismatched", sequence_id=3)
        ),
        repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=3), "mismatched", sequence_id=4)
        ),
    )
    observations = await _observations(repo)
    active_states = [row for row in await _states(repo) if row.known_to == KNOWN_TO_MAX]
    active_open = [
        row
        for row in await _episodes(repo)
        if row.known_to == KNOWN_TO_MAX and row.status == "open"
    ]
    assert sorted(row.resulting_full_mismatch_count for row in observations) == [1, 2, 3, 4]
    assert len(active_states) == 1
    assert len(active_open) == 1
    assert observations[2].drift_episode_public_id == observations[3].drift_episode_public_id


async def test_replayed_evaluation_is_idempotent_and_stale_evaluation_is_ignored(
    tmp_path: Path,
) -> None:
    """Replay and ordering guards preserve the newest mismatch streak.

    Given: a mismatch at sequence two,
    When: that exact evaluation is replayed and sequence one arrives later,
    Then: neither input appends evidence, versions state, or advances streak.
    """
    repo = await _make_repo(tmp_path)
    evaluation = _evaluation(_T0, "mismatched", sequence_id=2)
    state_id = await repo.record_portfolio_reconciliation(evaluation)
    replayed_id = await repo.record_portfolio_reconciliation(evaluation)
    stale_id = await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=1), "mismatched", sequence_id=1)
    )
    state = await _active_state(repo)
    assert replayed_id == state_id
    assert stale_id == state_id
    assert state.sequence_id == 2
    assert state.consecutive_full_mismatches == 1
    assert len(await _observations(repo)) == 1
    assert len(await _states(repo)) == 1


async def test_conflicting_same_evaluation_key_fails_closed(tmp_path: Path) -> None:
    """A same-key payload mutation is corruption rather than idempotent replay.

    Given: one committed evaluation key,
    When: different evidence is supplied under the same key,
    Then: the DAL raises without mutating observation or state history.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
    with pytest.raises(RuntimeError, match="conflicting reconciliation evaluation replay"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0, "mismatched", expected_json='{"quantity": 999}')
        )
    assert len(await _observations(repo)) == 1
    assert len(await _states(repo)) == 1


_REPLAY_CONFLICTS: list[tuple[str, object]] = [
    ("method", "spot_execution_replay"),
    ("evaluation_status", "matched"),
    ("venue_account_state_public_id", _OTHER_PUBLIC),
    ("venue_account_observation_id", 42),
    ("account_authoritative_until", _T0 + timedelta(minutes=10)),
    ("source_watermark_kind", "execution_id"),
    ("source_watermark", 99),
    ("anchor_public_id", _ANCHOR),
    ("expected_json", "{}"),
    ("actual_json", "{}"),
    ("difference_json", "{}"),
    ("tolerance_json", "{}"),
    ("error", "forged error"),
    ("bus_time", _T0 + timedelta(seconds=1)),
]


@pytest.mark.parametrize(
    ("field", "value"),
    _REPLAY_CONFLICTS,
    ids=[field for field, _ in _REPLAY_CONFLICTS],
)
async def test_every_conflicting_replay_evidence_field_fails_closed(
    tmp_path: Path, field: str, value: object
) -> None:
    """Every caller-supplied evidence mutation under a used key is rejected.

    Args:
        tmp_path: Pytest temporary directory.
        field: Evaluation evidence field to mutate.
        value: Conflicting replacement value.
    """
    repo = await _make_repo(tmp_path, f"replay-{field}.db")
    evaluation = _evaluation(_T0, "mismatched")
    await repo.record_portfolio_reconciliation(evaluation)
    values: dict[str, object] = dict(evaluation)
    values[field] = value
    replay = cast(PortfolioReconciliationEvaluationRow, values)
    with pytest.raises(RuntimeError, match="conflicting reconciliation evaluation replay"):
        await repo.record_portfolio_reconciliation(replay)
    assert len(await _observations(repo)) == 1


async def test_orphan_replay_observation_fails_closed(tmp_path: Path) -> None:
    """An evaluation observation without its atomic state is rejected.

    Given: a directly forged observation with no active state,
    When: the exact evaluation is replayed,
    Then: the DAL identifies the broken atomic lineage and raises.
    """
    repo = await _make_repo(tmp_path)
    await _insert_observation(repo, {})
    evaluation = _evaluation(_T0, "mismatched", sequence_id=1, expected_json="{}")
    evaluation["venue_account_observation_id"] = 10
    evaluation["source_watermark"] = 10
    evaluation["actual_json"] = "{}"
    evaluation["difference_json"] = "{}"
    evaluation["tolerance_json"] = "{}"
    with pytest.raises(RuntimeError, match="observation has no active state"):
        await repo.record_portfolio_reconciliation(evaluation)


async def test_genuine_first_observation_without_history_succeeds(tmp_path: Path) -> None:
    """An empty account history permits a predecessor-free first write.

    Given: no reconciliation observations or state for the account,
    When: its first full mismatch is recorded,
    Then: the DAL creates count-one evidence and one active state.

    Args:
        tmp_path: Pytest temporary directory.
    """
    repo = await _make_repo(tmp_path, "genuine-first-observation.db")
    state_id = await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
    observations = await _observations(repo)
    state = await _active_state(repo)
    assert state_id == state.id
    assert state.current_observation_id == observations[0].id
    assert state.consecutive_full_mismatches == 1
    assert len(observations) == 1


@pytest.mark.parametrize("state_mutation", ["deleted", "expired"])
async def test_missing_active_predecessor_with_prior_observations_fails_closed(
    tmp_path: Path, state_mutation: str
) -> None:
    """Deleted or expired active state cannot reset an observed streak.

    Given: two committed mismatch observations whose active state is deleted
        or changed to look expired,
    When: the third mismatch arrives,
    Then: predecessor-existence validation rejects the tampered history rather
        than restarting at count one and suppressing its drift episode.

    Args:
        tmp_path: Pytest temporary directory.
        state_mutation: Direct active-state tampering applied under test.
    """
    repo = await _make_repo(tmp_path, f"missing-predecessor-{state_mutation}.db")
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=1), "mismatched", sequence_id=2)
    )
    async with repo.session() as session:
        active = (
            await session.execute(
                select(PortfolioReconciliationState).where(
                    PortfolioReconciliationState.known_to == KNOWN_TO_MAX
                )
            )
        ).scalar_one()
        if state_mutation == "deleted":
            await session.delete(active)
        else:
            active.known_to = _T0 + timedelta(seconds=2)
        await session.commit()
    with pytest.raises(RuntimeError, match="predecessor is missing for prior observation"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=2), "mismatched", sequence_id=3)
        )
    assert len(await _observations(repo)) == 2
    assert await _episodes(repo) == []


async def test_newer_observation_than_predecessor_fails_closed(tmp_path: Path) -> None:
    """A committed observation newer than active state is rejected.

    Given: coherent sequence-one state plus a forged sequence-two observation,
    When: a new sequence-three evaluation arrives,
    Then: the DAL rejects the stale predecessor before deriving new state.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
    evaluation = _evaluation(_T0 + timedelta(seconds=1), "mismatched", sequence_id=2)
    await _insert_observation(
        repo,
        {
            "public_id": _OTHER_PUBLIC,
            "venue_account_observation_id": evaluation["venue_account_observation_id"],
            "account_authoritative_until": evaluation["account_authoritative_until"],
            "source_watermark": evaluation["source_watermark"],
            "expected_json": evaluation["expected_json"],
            "actual_json": evaluation["actual_json"],
            "difference_json": evaluation["difference_json"],
            "tolerance_json": evaluation["tolerance_json"],
            "resulting_full_mismatch_count": 2,
            "drift_episode_public_id": None,
            "sequence_id": 2,
            "timestamp": evaluation["bus_time"],
        },
    )
    with pytest.raises(RuntimeError, match="does not reference the latest observation"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=2), "mismatched", sequence_id=3)
        )
    assert len(await _observations(repo)) == 2


async def test_coherent_state_rewind_to_older_observation_fails_closed(
    tmp_path: Path,
) -> None:
    """A coherent active-state rewind cannot hide the newest observation.

    Given: two genuine mismatches and their internally consistent active state,
    When: that one row is rewound from observation two and count two to the
        genuine observation-one metadata and count one,
    Then: sequence three is rejected because the log still names observation
        two as the latest committed truth.

    Args:
        tmp_path: Pytest temporary directory.
    """
    repo = await _make_repo(tmp_path, "coherent-state-rewind.db")
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
    first_state = await _active_state(repo)
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=1), "mismatched", sequence_id=2)
    )
    async with repo.session() as session:
        await session.execute(
            update(PortfolioReconciliationState)
            .where(PortfolioReconciliationState.known_to == KNOWN_TO_MAX)
            .values(
                current_observation_id=first_state.current_observation_id,
                last_full_observation_id=first_state.last_full_observation_id,
                detail_source_observation_id=first_state.detail_source_observation_id,
                consecutive_full_mismatches=first_state.consecutive_full_mismatches,
                source_watermark=first_state.source_watermark,
                reconciled_at=first_state.reconciled_at,
                authoritative_until=first_state.authoritative_until,
                session_id=first_state.session_id,
                sequence_id=first_state.sequence_id,
                timestamp=first_state.timestamp,
            )
        )
        await session.commit()
    with pytest.raises(RuntimeError, match="does not reference the latest observation"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=2), "mismatched", sequence_id=3)
        )
    assert len(await _observations(repo)) == 2
    assert await _episodes(repo) == []


async def test_newly_appended_lower_key_observation_fails_closed(tmp_path: Path) -> None:
    """Append order independently prevents an older-key orphan from hiding.

    Given: a genuine sequence-two state followed by a directly appended
        same-account observation under an earlier session key,
    When: sequence three arrives under the genuine current session,
    Then: the append-only latest-id guard rejects the stale predecessor even
        though its monotonic key remains greatest.

    Args:
        tmp_path: Pytest temporary directory.
    """
    repo = await _make_repo(tmp_path, "newly-appended-lower-key.db")
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched", sequence_id=2))
    await _insert_observation(
        repo,
        {
            "public_id": _OTHER_PUBLIC,
            "session_id": _EARLIER_SESSION,
            "sequence_id": 1,
            "resulting_full_mismatch_count": 1,
            "drift_episode_public_id": None,
        },
    )
    with pytest.raises(RuntimeError, match="does not reference the latest observation"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=1), "mismatched", sequence_id=3)
        )
    assert len(await _observations(repo)) == 2


@pytest.mark.parametrize(
    ("trigger", "prepare_count"),
    [
        ("portfolio_reconciliation_observations", 0),
        ("portfolio_drift_episodes", 2),
        ("portfolio_reconciliation_states_insert", 0),
        ("portfolio_reconciliation_states_update", 1),
    ],
)
async def test_transaction_failure_leaves_no_orphan_rows(
    tmp_path: Path, trigger: str, prepare_count: int
) -> None:
    """Failure at every write stage rolls the whole transaction back.

    Given: a SQLite trigger aborting observation, episode, state-insert, or
        state-close writes,
    When: the atomic repository writer reaches that stage,
    Then: both attempts fail and no observation, state version, or episode
        from the evaluation survives.

    Args:
        tmp_path: Pytest temporary directory.
        trigger: Write stage to abort.
        prepare_count: Number of committed mismatch predecessors required.
    """
    repo = await _make_repo(tmp_path, f"{trigger}.db")
    for sequence in range(prepare_count):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence + 1)
        )
    before = (len(await _observations(repo)), len(await _states(repo)), len(await _episodes(repo)))
    if trigger.endswith("_insert"):
        table = "portfolio_reconciliation_states"
        action = "INSERT"
    elif trigger.endswith("_update"):
        table = "portfolio_reconciliation_states"
        action = "UPDATE"
    else:
        table = trigger
        action = "INSERT"
    async with repo.session() as session:
        await session.execute(
            text(
                f"CREATE TRIGGER fail_stage BEFORE {action} ON {table} "
                "BEGIN SELECT RAISE(FAIL, 'forced stage failure'); END"
            )
        )
        await session.commit()
    with pytest.raises(IntegrityError):
        await repo.record_portfolio_reconciliation(
            _evaluation(
                _T0 + timedelta(seconds=prepare_count + 1),
                "mismatched",
                sequence_id=prepare_count + 1,
            )
        )
    after = (len(await _observations(repo)), len(await _states(repo)), len(await _episodes(repo)))
    assert after == before


async def test_integrity_race_retries_once_and_commits(tmp_path: Path) -> None:
    """A first-attempt integrity race is retried once with fresh ORM objects.

    Given: the internal writer raises one synthetic integrity failure,
    When: the public atomic writer runs,
    Then: it rolls back, retries exactly once, and commits one coherent result.
    """
    repo = await _make_repo(tmp_path)
    original = repo._write_portfolio_reconciliation
    calls = 0

    async def flaky(session: AsyncSession, evaluation: PortfolioReconciliationEvaluationRow) -> int:
        """Fail the first call and delegate the retry.

        Args:
            session: Open repository session.
            evaluation: Raw evaluation row.

        Returns:
            Delegated state id.

        Raises:
            IntegrityError: On the first call only.
        """
        nonlocal calls
        calls += 1
        if calls == 1:
            raise IntegrityError("insert", {}, RuntimeError("race"))
        return int(await original(session, evaluation))

    with patch.object(repo, "_write_portfolio_reconciliation", side_effect=flaky):
        state_id = await repo.record_portfolio_reconciliation(_evaluation(_T0, "matched"))
    assert calls == 2
    assert state_id == (await _active_state(repo)).id
    assert len(await _observations(repo)) == 1


async def test_operational_stale_snapshot_retries_once_and_commits(tmp_path: Path) -> None:
    """A SQLite stale-snapshot operational failure receives one fresh retry.

    Given: the first internal write raises a synthetic operational race,
    When: the public atomic writer runs,
    Then: rollback starts a fresh transaction and one coherent result commits.
    """
    repo = await _make_repo(tmp_path)
    original = repo._write_portfolio_reconciliation
    calls = 0
    sessions: list[AsyncSession] = []

    async def flaky(session: AsyncSession, evaluation: PortfolioReconciliationEvaluationRow) -> int:
        """Fail the first call with OperationalError and delegate the retry.

        Args:
            session: Open repository session.
            evaluation: Raw evaluation row.

        Returns:
            Delegated state id.

        Raises:
            OperationalError: On the first call only.
        """
        nonlocal calls
        calls += 1
        sessions.append(session)
        if calls == 1:
            raise OperationalError("update", {}, RuntimeError("SQLITE_BUSY_SNAPSHOT"))
        return int(await original(session, evaluation))

    with patch.object(repo, "_write_portfolio_reconciliation", side_effect=flaky):
        state_id = await repo.record_portfolio_reconciliation(_evaluation(_T0, "matched"))
    assert calls == 2
    assert sessions[0] is not sessions[1]
    assert state_id == (await _active_state(repo)).id
    assert len(await _observations(repo)) == 1


async def test_cross_instance_concurrent_writers_retry_without_corruption(tmp_path: Path) -> None:
    """Independent repository instances retry a stale SQLite write once.

    Given: two repository objects sharing empty reconciliation history,
    When: the second observes no predecessor before the first commits,
    Then: its stale write receives one bounded retry and both callers converge
        on one observation and one coherent active state.
    """
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'cross-instance.db'}"
    first = await _make_repo(tmp_path, "cross-instance.db")
    second = SQLAlchemyRepository(database_url)
    assert await _states(first) == []
    original_write = second._write_portfolio_reconciliation
    stale_read_complete = asyncio.Event()
    release_stale_writer = asyncio.Event()
    write_attempts = 0

    async def stale_then_retry(
        _session: AsyncSession, evaluation: PortfolioReconciliationEvaluationRow
    ) -> int:
        """Model SQLite rejecting a snapshot invalidated by another instance.

        Args:
            _session: Open repository session.
            evaluation: Raw evaluation row.

        Returns:
            Delegated state id on retry.

        Raises:
            OperationalError: After the first empty snapshot becomes stale.
        """
        nonlocal write_attempts
        write_attempts += 1
        if write_attempts == 1:
            stale_read_complete.set()
            await release_stale_writer.wait()
            raise OperationalError("insert", {}, RuntimeError("SQLITE_BUSY_SNAPSHOT"))
        return int(await original_write(_session, evaluation))

    with patch.object(
        second,
        "_write_portfolio_reconciliation",
        side_effect=stale_then_retry,
    ):
        second_task = asyncio.create_task(
            second.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
        )
        await stale_read_complete.wait()
        first_state_id = await first.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
        await first.engine.dispose()
        release_stale_writer.set()
        second_state_id = await second_task
    observations = await _observations(second)
    state = await _active_state(second)
    assert write_attempts == 2
    assert [first_state_id, second_state_id] == [state.id, state.id]
    assert state.sequence_id == 1
    assert state.consecutive_full_mismatches == 1
    assert len(observations) == 1
    assert len([row for row in await _states(second) if row.known_to == KNOWN_TO_MAX]) == 1


async def test_paper_evaluation_rolls_back_without_entering_storage(tmp_path: Path) -> None:
    """Paper evaluation evidence can never enter reconciliation storage.

    Given: a complete comparison labeled as paper mode,
    When: the atomic writer is called,
    Then: the live-only CHECK fails closed and no observation, state, or
        episode row survives either transaction attempt.
    """
    repo = await _make_repo(tmp_path)
    with pytest.raises(RuntimeError, match="identity is invalid"):
        await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched", mode="paper"))
    assert await _observations(repo) == []
    assert await _states(repo) == []
    assert await _episodes(repo) == []


@pytest.mark.parametrize(
    ("method", "status", "error", "message"),
    [
        pytest.param(
            "futures_position",
            "pending",
            None,
            "portfolio reconciliation status is incompatible with method",
            id="real-method-status",
        ),
        pytest.param(
            "unclassified",
            "matched",
            None,
            "unclassified reconciliation status is invalid",
            id="unclassified-status",
        ),
        pytest.param(
            "unclassified",
            "incomplete",
            None,
            "unclassified reconciliation cannot carry full evidence",
            id="unclassified-evidence",
        ),
        pytest.param(
            "balance_guess",
            "incomplete",
            None,
            "portfolio reconciliation method is invalid",
            id="unknown-method",
        ),
        pytest.param(
            "futures_position",
            "error",
            " ",
            "error reconciliation requires a non-empty reason",
            id="empty-error-reason",
        ),
    ],
)
async def test_evaluation_validation_rejects_application_level_contradictions(
    tmp_path: Path,
    method: str,
    status: str,
    error: str | None,
    message: str,
) -> None:
    """Invalid method, status, evidence, and error shapes fail before storage.

    Args:
        tmp_path: Pytest temporary directory.
        method: Incoming reconciliation method.
        status: Incoming evaluation status.
        error: Incoming error reason.
        message: Expected application-level rejection.
    """
    repo = await _make_repo(tmp_path)
    evaluation = _evaluation(_T0, status, method=method, error=error)
    with pytest.raises(RuntimeError, match=message):
        await repo.record_portfolio_reconciliation(evaluation)
    assert await _observations(repo) == []
    assert await _states(repo) == []


async def test_different_real_method_cannot_replace_active_state(tmp_path: Path) -> None:
    """A matching incoming config cannot override another real state method."""
    repo = await _make_repo(tmp_path, "different-real-method.db")
    await repo.record_portfolio_reconciliation(_non_full_evaluation(_T0, "incomplete", 4))
    before = await _active_state(repo)
    async with repo.session() as session:
        await session.execute(
            update(PortfolioReconciliationMethodConfig)
            .where(PortfolioReconciliationMethodConfig.known_to == KNOWN_TO_MAX)
            .values(method="spot_execution_replay")
        )
        await session.commit()
    evaluation = _non_full_evaluation(_T0 + timedelta(seconds=1), "incomplete", 5)
    evaluation["method"] = "spot_execution_replay"
    with pytest.raises(RuntimeError, match="method transition is invalid"):
        await repo.record_portfolio_reconciliation(evaluation)
    after = await _active_state(repo)
    assert after.id == before.id
    assert after.method == "futures_position"
    assert len(await _observations(repo)) == 1


async def test_safe_unclassified_state_transitions_to_first_real_method(tmp_path: Path) -> None:
    """Evidence-free unclassified history accepts its first configured method."""
    repo = await _make_repo(tmp_path, "unclassified-transition.db")
    async with repo.session() as session:
        await session.execute(delete(PortfolioReconciliationMethodConfig))
        await session.commit()
    unclassified = _non_full_evaluation(_T0, "incomplete", 4)
    unclassified["method"] = "unclassified"
    await repo.record_portfolio_reconciliation(unclassified)
    await repo.set_portfolio_reconciliation_method_config(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        session_id=_SESSION,
        sequence_id=5,
        timestamp=_T0 + timedelta(seconds=1),
    )
    incoming = _non_full_evaluation(_T0 + timedelta(seconds=2), "incomplete", 6)
    state_id = await repo.record_portfolio_reconciliation(incoming)
    state = await _active_state(repo)
    assert state_id == state.id
    assert state.method == "futures_position"
    assert state.current_evaluation_status == "incomplete"
    assert len(await _observations(repo)) == 2


@pytest.mark.parametrize(
    ("method", "status", "message"),
    [
        pytest.param(
            "unclassified",
            "incomplete",
            "unclassified reconciliation state retains forbidden evidence",
            id="unclassified",
        ),
        pytest.param(
            "margin_ledger_replay",
            "error",
            "margin ledger reconciliation state retains forbidden evidence",
            id="margin-ledger",
        ),
    ],
)
async def test_nonfull_only_state_transition_guard_rejects_retained_full_evidence(
    tmp_path: Path,
    method: str,
    status: str,
    message: str,
) -> None:
    """The transition backstop rejects forbidden evidence on non-full methods.

    Direct validation isolates this cross-row guard because the database CHECK
    rejects persistence of the forged state and predecessor-lineage checks can
    reject a tampered stored row before method-transition validation runs.

    Args:
        tmp_path: Pytest temporary directory.
        method: Forged non-full-only state method.
        status: Compatible current status for that method.
        message: Expected cross-row rejection.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
    existing = await _active_state(repo)
    existing.method = method
    existing.current_evaluation_status = status
    existing.error = "retained evidence corruption" if status == "error" else None
    evaluation = _non_full_evaluation(_T0 + timedelta(seconds=1), status, 2)
    evaluation["method"] = method
    with pytest.raises(RuntimeError, match=message):
        repo._validate_portfolio_reconciliation_method_transition(existing, evaluation, None)


async def test_evaluation_existence_lookup_detects_only_committed_key(tmp_path: Path) -> None:
    """Evaluation existence is false before commit and true for its exact key."""
    repo = await _make_repo(tmp_path, "evaluation-existence.db")
    assert not await repo.has_portfolio_reconciliation_evaluation(
        _WALLET,
        "kraken_futures",
        "live",
        _SESSION,
        1,
    )
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "matched"))
    assert await repo.has_portfolio_reconciliation_evaluation(
        _WALLET,
        "kraken_futures",
        "live",
        _SESSION,
        1,
    )
    assert not await repo.has_portfolio_reconciliation_evaluation(
        _WALLET,
        "kraken_futures",
        "live",
        _SESSION,
        2,
    )


@pytest.mark.parametrize("episode_status", [None, "resolved"])
async def test_missing_or_non_open_episode_fails_closed(
    tmp_path: Path, episode_status: str | None
) -> None:
    """Cross-row episode corruption is rejected before new evidence is stored.

    Given: a state naming a missing or resolved active lifecycle row,
    When: another evaluation is recorded,
    Then: the DAL raises and appends no orphan observation.

    Args:
        tmp_path: Pytest temporary directory.
        episode_status: Whether to insert a resolved lifecycle row or leave it
            missing.
    """
    repo = await _make_repo(tmp_path, f"missing-{episode_status}.db")
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    async with repo.session() as session:
        episode = (
            await session.execute(
                select(PortfolioDriftEpisode).where(PortfolioDriftEpisode.known_to == KNOWN_TO_MAX)
            )
        ).scalar_one()
        if episode_status is None:
            await session.delete(episode)
        else:
            episode.status = "resolved"
            episode.closed_at = _T0 + timedelta(seconds=4)
            episode.resolution_reason = "matched"
        await session.commit()
    with pytest.raises(RuntimeError, match="no open drift episode"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=5), "mismatched", sequence_id=5)
        )
    assert len(await _observations(repo)) == 3


@pytest.mark.parametrize("lineage", ["missing", "foreign"])
async def test_forged_predecessor_observation_lineage_fails_closed(
    tmp_path: Path, lineage: str
) -> None:
    """Missing and cross-account predecessor observations are rejected.

    Args:
        tmp_path: Pytest temporary directory.
        lineage: Forged lineage shape under test.
    """
    repo = await _make_repo(tmp_path, f"lineage-{lineage}.db")
    observation_id = 999
    if lineage == "foreign":
        await _insert_observation(
            repo,
            {
                "wallet_public_id": _OTHER_WALLET,
                "resulting_full_mismatch_count": 1,
                "drift_episode_public_id": None,
            },
        )
        observation_id = 1
    await _insert_state(
        repo,
        {
            "current_observation_id": observation_id,
            "last_full_observation_id": observation_id,
            "detail_source_observation_id": observation_id,
            "consecutive_full_mismatches": 1,
            "open_drift_episode_public_id": None,
        },
    )
    with pytest.raises(RuntimeError, match=f"{lineage} observation"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=1), "mismatched", sequence_id=2)
        )
    assert len(await _states(repo)) == 1


async def test_cross_account_episode_identity_mutation_fails_closed(tmp_path: Path) -> None:
    """A state can never mutate an episode belonging to another account.

    Given: a valid open episode whose wallet identity is forged in place,
    When: the account records another mismatch,
    Then: the DAL rejects the identity mismatch before appending evidence.
    """
    repo = await _make_repo(tmp_path)
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    async with repo.session() as session:
        await session.execute(
            update(PortfolioDriftEpisode)
            .where(PortfolioDriftEpisode.known_to == KNOWN_TO_MAX)
            .values(wallet_public_id=_OTHER_WALLET)
        )
        await session.commit()
    with pytest.raises(RuntimeError, match="episode identity"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=4), "mismatched", sequence_id=4)
        )
    assert len(await _observations(repo)) == 3


@pytest.mark.parametrize("next_status", ["mismatched", "matched"])
async def test_tampered_episode_opened_at_fails_closed_on_next_write(
    tmp_path: Path, next_status: str
) -> None:
    """An episode cannot carry an opening time unrelated to its trigger.

    Given: three genuine mismatches and an open episode whose opening time is
        moved earlier while remaining row-locally CHECK-valid,
    When: the next mismatch versions it or a match resolves it,
    Then: trigger-time validation rejects the episode before evidence is added.

    Args:
        tmp_path: Pytest temporary directory.
        next_status: Full evaluation outcome that would version the episode.
    """
    repo = await _make_repo(tmp_path, f"episode-opened-at-{next_status}.db")
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    async with repo.session() as session:
        await session.execute(
            update(PortfolioDriftEpisode)
            .where(PortfolioDriftEpisode.known_to == KNOWN_TO_MAX)
            .values(opened_at=_T0)
        )
        await session.commit()
    with pytest.raises(RuntimeError, match="opened_at is inconsistent"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=4), next_status, sequence_id=4)
        )
    assert len(await _observations(repo)) == 3
    assert len(await _episodes(repo)) == 1


async def test_resolution_revalidates_episode_opened_at_before_copying(
    tmp_path: Path,
) -> None:
    """Resolution independently validates opening time before copying it.

    Given: a CHECK-valid earlier opening time on a genuine open episode,
    When: the initial episode-lock validation is bypassed during resolution,
    Then: versioning revalidates the trigger time and rolls back the match.

    Args:
        tmp_path: Pytest temporary directory.
    """
    repo = await _make_repo(tmp_path, "episode-resolution-opened-at.db")
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    async with repo.session() as session:
        await session.execute(
            update(PortfolioDriftEpisode)
            .where(PortfolioDriftEpisode.known_to == KNOWN_TO_MAX)
            .values(opened_at=_T0)
        )
        await session.commit()
    tampered_episode = (await _episodes(repo))[-1]
    with (
        patch.object(repo, "_lock_active_drift_episode", return_value=tampered_episode),
        pytest.raises(RuntimeError, match="opened_at is inconsistent"),
    ):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=4), "matched", sequence_id=4)
        )
    assert len(await _observations(repo)) == 3
    assert len(await _episodes(repo)) == 1


@pytest.mark.parametrize(
    ("lineage", "message"),
    [
        ("missing", "missing observation"),
        ("foreign", "foreign observation"),
        ("no_evidence", "detail observation"),
    ],
)
async def test_forged_episode_observation_lineage_fails_closed(
    tmp_path: Path, lineage: str, message: str
) -> None:
    """Episode versions require real account-scoped triggering evidence.

    Given: an open episode whose observation ids are missing, foreign, or do
        not carry sustained-drift evidence,
    When: a later match would resolve and preserve that lineage,
    Then: the DAL rejects the episode before appending or versioning anything.

    Args:
        tmp_path: Pytest temporary directory.
        lineage: Forged episode lineage shape under test.
        message: Expected fail-closed error fragment.
    """
    repo = await _make_repo(tmp_path, f"episode-lineage-{lineage}.db")
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    episode = (await _episodes(repo))[0]
    if lineage == "missing":
        forged_values: dict[str, object] = {
            "trigger_observation_id": 0,
            "details_source_observation_id": 0,
        }
    elif lineage == "foreign":
        observation_values: dict[str, object] = {
            "public_id": _OTHER_PUBLIC,
            "session_id": _OTHER_SESSION,
            "sequence_id": 1,
        }
        observation_values.update(
            {
                "wallet_public_id": _OTHER_WALLET,
                "drift_episode_public_id": episode.public_id,
            }
        )
        await _insert_observation(repo, observation_values)
        forged_observation_id = (await _observations(repo))[-1].id
        forged_values = {
            "trigger_observation_id": forged_observation_id,
            "last_observation_id": forged_observation_id,
            "details_source_observation_id": forged_observation_id,
        }
    else:
        forged_observation_id = (await _observations(repo))[0].id
        async with repo.session() as session:
            await session.execute(
                update(PortfolioReconciliationObservation)
                .where(PortfolioReconciliationObservation.id == forged_observation_id)
                .values(_retained_non_full_observation_values("incomplete", episode.public_id))
            )
            await session.commit()
        forged_values = {
            "trigger_observation_id": forged_observation_id,
            "last_observation_id": forged_observation_id,
            "details_source_observation_id": forged_observation_id,
        }
    async with repo.session() as session:
        await session.execute(
            update(PortfolioDriftEpisode)
            .where(PortfolioDriftEpisode.known_to == KNOWN_TO_MAX)
            .values(forged_values)
        )
        await session.commit()
    observation_count = len(await _observations(repo))
    episode_count = len(await _episodes(repo))
    with pytest.raises(RuntimeError, match=message):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=5), "matched", sequence_id=5)
        )
    assert len(await _observations(repo)) == observation_count
    assert len(await _episodes(repo)) == episode_count


@pytest.mark.parametrize("status", ["incomplete", "error", "unsupported"])
@pytest.mark.parametrize("lineage", ["trigger", "last", "detail"])
async def test_retained_non_full_episode_lineage_fails_closed(
    tmp_path: Path, status: str, lineage: str
) -> None:
    """Retained non-full observations cannot impersonate episode evidence.

    Given: three genuine mismatches and a CHECK-valid non-full observation
        retaining count three plus the episode identity,
    When: the episode trigger, last, or latest detail lineage is redirected to it,
    Then: a match fails closed before appending or resolving the episode.

    Args:
        tmp_path: Pytest temporary directory.
        status: Non-full outcome under test.
        lineage: Episode lineage reference forged under test.
    """
    repo = await _make_repo(tmp_path, f"episode-non-full-{lineage}-{status}.db")
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    episode = (await _episodes(repo))[-1]
    if lineage in ("last", "detail"):
        await repo.record_portfolio_reconciliation(
            _non_full_evaluation(_T0 + timedelta(seconds=4), status, 4)
        )
        forged_observation_id = (await _observations(repo))[-1].id
        episode_values: dict[str, object] = {"last_observation_id": forged_observation_id}
        if lineage == "detail":
            episode_values["details_source_observation_id"] = forged_observation_id
        next_sequence = 5
    else:
        forged_observation_id = (await _observations(repo))[0].id
        async with repo.session() as session:
            await session.execute(
                update(PortfolioReconciliationObservation)
                .where(PortfolioReconciliationObservation.id == forged_observation_id)
                .values(_retained_non_full_observation_values(status, episode.public_id))
            )
            await session.commit()
        episode_values = {"trigger_observation_id": forged_observation_id}
        next_sequence = 4
    async with repo.session() as session:
        await session.execute(
            update(PortfolioDriftEpisode)
            .where(PortfolioDriftEpisode.known_to == KNOWN_TO_MAX)
            .values(episode_values)
        )
        await session.commit()
    observation_count = len(await _observations(repo))
    episode_count = len(await _episodes(repo))
    with pytest.raises(RuntimeError, match=f"{lineage} observation"):
        await repo.record_portfolio_reconciliation(
            _evaluation(
                _T0 + timedelta(seconds=next_sequence),
                "matched",
                sequence_id=next_sequence,
            )
        )
    assert len(await _observations(repo)) == observation_count
    assert len(await _episodes(repo)) == episode_count


async def test_resolution_revalidates_retained_episode_detail_before_copying(
    tmp_path: Path,
) -> None:
    """Resolution independently rejects a retained non-full detail source.

    Given: an error legitimately retaining an open episode count and identity,
        and a tampered episode redirecting its latest detail source to the error,
    When: the initial episode lock result is supplied without its validation,
    Then: resolution re-reads and rejects the error before copying its id.

    Args:
        tmp_path: Pytest temporary directory.
    """
    repo = await _make_repo(tmp_path, "episode-resolution-revalidation.db")
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    await repo.record_portfolio_reconciliation(
        _non_full_evaluation(_T0 + timedelta(seconds=4), "error", 4)
    )
    error_observation_id = (await _observations(repo))[-1].id
    async with repo.session() as session:
        await session.execute(
            update(PortfolioDriftEpisode)
            .where(PortfolioDriftEpisode.known_to == KNOWN_TO_MAX)
            .values(
                last_observation_id=error_observation_id,
                details_source_observation_id=error_observation_id,
            )
        )
        await session.commit()
    tampered_episode = (await _episodes(repo))[-1]
    observation_count = len(await _observations(repo))
    episode_count = len(await _episodes(repo))
    with (
        patch.object(repo, "_lock_active_drift_episode", return_value=tampered_episode),
        pytest.raises(RuntimeError, match="detail observation"),
    ):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=5), "matched", sequence_id=5)
        )
    assert len(await _observations(repo)) == observation_count
    assert len(await _episodes(repo)) == episode_count


async def test_self_consistent_older_episode_lineage_fails_state_cross_validation(
    tmp_path: Path,
) -> None:
    """An independently valid episode cannot lag its owning active state.

    Given: four genuine mismatches followed by an episode forged back to the
        genuine third-mismatch count, last observation, and detail source,
    When: a match would resolve and preserve that independently valid lineage,
    Then: state-to-episode cross-validation rejects it before appending.

    Args:
        tmp_path: Pytest temporary directory.
    """
    repo = await _make_repo(tmp_path, "episode-state-cross-validation.db")
    for sequence in range(1, 5):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    observations = await _observations(repo)
    third_mismatch_id = observations[2].id
    async with repo.session() as session:
        await session.execute(
            update(PortfolioDriftEpisode)
            .where(PortfolioDriftEpisode.known_to == KNOWN_TO_MAX)
            .values(
                latest_full_mismatch_count=3,
                last_observation_id=third_mismatch_id,
                details_source_observation_id=third_mismatch_id,
            )
        )
        await session.commit()
    episode_count = len(await _episodes(repo))
    with pytest.raises(RuntimeError, match="lineage does not match reconciliation state"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=5), "matched", sequence_id=5)
        )
    assert len(await _observations(repo)) == 4
    assert len(await _episodes(repo)) == episode_count


async def test_resolution_rechecks_episode_state_consistency_before_copying(
    tmp_path: Path,
) -> None:
    """Resolution independently rejects stale but genuine episode lineage.

    Given: four genuine mismatches and a self-consistent episode forged back
        to the genuine third mismatch,
    When: the initial episode-lock validation is bypassed during a match,
    Then: resolution rechecks against state before copying count and detail.

    Args:
        tmp_path: Pytest temporary directory.
    """
    repo = await _make_repo(tmp_path, "episode-resolution-state-revalidation.db")
    for sequence in range(1, 5):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    observations = await _observations(repo)
    third_mismatch_id = observations[2].id
    async with repo.session() as session:
        await session.execute(
            update(PortfolioDriftEpisode)
            .where(PortfolioDriftEpisode.known_to == KNOWN_TO_MAX)
            .values(
                latest_full_mismatch_count=3,
                last_observation_id=third_mismatch_id,
                details_source_observation_id=third_mismatch_id,
            )
        )
        await session.commit()
    tampered_episode = (await _episodes(repo))[-1]
    episode_count = len(await _episodes(repo))
    with (
        patch.object(repo, "_lock_active_drift_episode", return_value=tampered_episode),
        pytest.raises(RuntimeError, match="lineage does not match reconciliation state"),
    ):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=5), "matched", sequence_id=5)
        )
    assert len(await _observations(repo)) == 4
    assert len(await _episodes(repo)) == episode_count


async def test_forged_current_observation_metadata_fails_closed(tmp_path: Path) -> None:
    """State metadata cannot diverge from its real current observation.

    Given: one valid state whose session identity is forged in place,
    When: a newer evaluation arrives,
    Then: lineage validation rejects it before appending evidence.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
    async with repo.session() as session:
        await session.execute(
            update(PortfolioReconciliationState)
            .where(PortfolioReconciliationState.known_to == KNOWN_TO_MAX)
            .values(session_id=_OTHER_SESSION)
        )
        await session.commit()
    with pytest.raises(RuntimeError, match="current observation metadata"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=1), "mismatched", sequence_id=2)
        )
    assert len(await _observations(repo)) == 1


async def test_forged_last_full_outcome_fails_closed(tmp_path: Path) -> None:
    """Last-full outcome must agree with the referenced full observation.

    Given: mismatch detail retained beneath an error state,
    When: both current count and state outcome are forged into a coherent row,
    Then: cross-row validation still rejects the false last-full outcome.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=1), "error", sequence_id=2)
    )
    async with repo.session() as session:
        state = (
            await session.execute(
                select(PortfolioReconciliationState).where(
                    PortfolioReconciliationState.known_to == KNOWN_TO_MAX
                )
            )
        ).scalar_one()
        await session.execute(
            update(PortfolioReconciliationObservation)
            .where(PortfolioReconciliationObservation.id == state.current_observation_id)
            .values(resulting_full_mismatch_count=0)
        )
        state.consecutive_full_mismatches = 0
        state.last_full_outcome = "matched"
        await session.commit()
    with pytest.raises(RuntimeError, match="last-full observation"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=2), "mismatched", sequence_id=3)
        )
    assert len(await _observations(repo)) == 2


@pytest.mark.parametrize(
    "tampering",
    [
        {"resulting_full_mismatch_count": 99},
        {"drift_episode_public_id": _OTHER_PUBLIC},
    ],
)
async def test_forged_retained_full_lineage_fails_closed(
    tmp_path: Path, tampering: dict[str, object]
) -> None:
    """Retained full evidence must agree with the state's drift lineage.

    Given: three mismatches open an episode, a non-full observation retains
        the third mismatch, and its retained count or episode id is forged,
    When: the next full mismatch would version the open episode,
    Then: predecessor validation rejects the forged retained evidence first.

    Args:
        tmp_path: Pytest temporary directory.
        tampering: CHECK-valid retained-observation mutation under test.
    """
    repo = await _make_repo(tmp_path)
    for sequence in range(1, 4):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=sequence), "mismatched", sequence_id=sequence)
        )
    await repo.record_portfolio_reconciliation(
        _evaluation(_T0 + timedelta(seconds=4), "error", sequence_id=4)
    )
    state = await _active_state(repo)
    async with repo.session() as session:
        await session.execute(
            update(PortfolioReconciliationObservation)
            .where(PortfolioReconciliationObservation.id == state.last_full_observation_id)
            .values(tampering)
        )
        await session.commit()
    with pytest.raises(RuntimeError, match="last-full observation"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=5), "mismatched", sequence_id=5)
        )
    assert len(await _observations(repo)) == 4
    assert len(await _episodes(repo)) == 1


async def test_forged_detail_observation_payload_fails_closed(tmp_path: Path) -> None:
    """Retained state evidence must equal its detail-source observation.

    Given: one valid mismatch state whose expected JSON is forged in place,
    When: a newer evaluation arrives,
    Then: detail lineage validation rejects the payload substitution.
    """
    repo = await _make_repo(tmp_path)
    await repo.record_portfolio_reconciliation(_evaluation(_T0, "mismatched"))
    async with repo.session() as session:
        await session.execute(
            update(PortfolioReconciliationState)
            .where(PortfolioReconciliationState.known_to == KNOWN_TO_MAX)
            .values(expected_json='{"quantity": 999}')
        )
        await session.commit()
    with pytest.raises(RuntimeError, match="detail observation"):
        await repo.record_portfolio_reconciliation(
            _evaluation(_T0 + timedelta(seconds=1), "mismatched", sequence_id=2)
        )
    assert len(await _observations(repo)) == 1


def _temporal_values() -> dict[str, object]:
    """Build valid temporal columns for direct constraint tests.

    Returns:
        Shared temporal ORM values.
    """
    return {
        "public_id": _PUBLIC,
        "session_id": _SESSION,
        "sequence_id": 1,
        "timestamp": _T0,
        "known_to": KNOWN_TO_MAX,
    }


async def _insert_observation(repo: SQLAlchemyRepository, overrides: dict[str, object]) -> None:
    """Insert one observation directly for CHECK tests.

    Args:
        repo: Repository under test.
        overrides: Column overrides applied to a valid mismatched row.

    Returns:
        None.
    """
    values: dict[str, object] = {
        "wallet_public_id": _WALLET,
        "exchange": "kraken_futures",
        "mode": "live",
        "method": "futures_position",
        "evaluation_status": "mismatched",
        "venue_account_state_public_id": _ACCOUNT_STATE,
        "venue_account_observation_id": 10,
        "account_authoritative_until": _T0 + timedelta(minutes=5),
        "source_watermark_kind": "venue_event_id",
        "source_watermark": 10,
        "anchor_public_id": None,
        "expected_json": "{}",
        "actual_json": "{}",
        "difference_json": "{}",
        "tolerance_json": "{}",
        "resulting_full_mismatch_count": 3,
        "drift_episode_public_id": _EPISODE,
        "error": None,
        **_temporal_values(),
    }
    values.update(overrides)
    async with repo.session() as session:
        session.add(PortfolioReconciliationObservation(**values))
        await session.commit()


def _empty_state_detail() -> dict[str, object]:
    """Build the all-null no-full-detail state fragment.

    Returns:
        State values with no full evaluation provenance or payload.
    """
    return {
        "last_full_observation_id": None,
        "last_full_outcome": None,
        "detail_source_observation_id": None,
        "anchor_public_id": None,
        "venue_account_state_public_id": None,
        "venue_account_observation_id": None,
        "source_watermark_kind": None,
        "source_watermark": None,
        "expected_json": None,
        "actual_json": None,
        "difference_json": None,
        "tolerance_json": None,
        "reconciled_at": None,
        "authoritative_until": None,
    }


async def _insert_state(repo: SQLAlchemyRepository, overrides: dict[str, object]) -> None:
    """Insert one reconciliation state directly for CHECK/index tests.

    Args:
        repo: Repository under test.
        overrides: Column overrides applied to a valid drift state.

    Returns:
        None.
    """
    values: dict[str, object] = {
        "wallet_public_id": _WALLET,
        "exchange": "kraken_futures",
        "mode": "live",
        "method": "futures_position",
        "current_evaluation_status": "mismatched",
        "current_observation_id": 10,
        "last_full_observation_id": 10,
        "last_full_outcome": "mismatched",
        "detail_source_observation_id": 10,
        "consecutive_full_mismatches": 3,
        "open_drift_episode_public_id": _EPISODE,
        "anchor_public_id": None,
        "venue_account_state_public_id": _ACCOUNT_STATE,
        "venue_account_observation_id": 9,
        "source_watermark_kind": "venue_event_id",
        "source_watermark": 10,
        "expected_json": "{}",
        "actual_json": "{}",
        "difference_json": "{}",
        "tolerance_json": "{}",
        "reconciled_at": _T0,
        "authoritative_until": _T0 + timedelta(minutes=5),
        "error": None,
        **_temporal_values(),
    }
    values.update(overrides)
    async with repo.session() as session:
        session.add(PortfolioReconciliationState(**values))
        await session.commit()


async def _insert_episode(repo: SQLAlchemyRepository, overrides: dict[str, object]) -> None:
    """Insert one drift episode directly for CHECK/index tests.

    Args:
        repo: Repository under test.
        overrides: Column overrides applied to a valid open episode.

    Returns:
        None.
    """
    values: dict[str, object] = {
        "wallet_public_id": _WALLET,
        "exchange": "kraken_futures",
        "mode": "live",
        "status": "open",
        "opened_at": _T0,
        "closed_at": None,
        "trigger_observation_id": 3,
        "last_observation_id": 4,
        "details_source_observation_id": 4,
        "latest_full_mismatch_count": 4,
        "resolution_reason": None,
        "closed_by_user_public_id": None,
        "closed_by_operator_public_id": None,
        "rebase_anchor_public_id": None,
        **_temporal_values(),
    }
    values.update(overrides)
    async with repo.session() as session:
        session.add(PortfolioDriftEpisode(**values))
        await session.commit()


_OBSERVATION_VIOLATIONS: list[tuple[str, dict[str, object]]] = [
    ("exchange_lower", {"exchange": "Kraken"}),
    ("mode", {"mode": "paper"}),
    ("method", {"method": "balance_guess"}),
    ("status", {"evaluation_status": "available"}),
    ("count", {"resulting_full_mismatch_count": -1}),
    ("full_evidence", {"expected_json": None}),
    (
        "matched",
        {
            "evaluation_status": "matched",
            "resulting_full_mismatch_count": 0,
            "drift_episode_public_id": None,
            "error": "forged error",
        },
    ),
    (
        "mismatched",
        {
            "evaluation_status": "mismatched",
            "resulting_full_mismatch_count": 0,
            "drift_episode_public_id": None,
        },
    ),
    ("episode_threshold", {"resulting_full_mismatch_count": 2}),
    (
        "spot_anchor",
        {"method": "spot_execution_replay", "anchor_public_id": None},
    ),
    ("watermark_pair", {"source_watermark_kind": None}),
    (
        "error_text",
        {
            "evaluation_status": "error",
            "error": " ",
        },
    ),
    (
        "error_length",
        {
            "evaluation_status": "incomplete",
            "error": "x" * 513,
        },
    ),
]


@pytest.mark.parametrize(
    ("name", "overrides"),
    _OBSERVATION_VIOLATIONS,
    ids=[name for name, _ in _OBSERVATION_VIOLATIONS],
)
async def test_observation_check_constraints_reject_every_contradiction(
    tmp_path: Path, name: str, overrides: dict[str, object]
) -> None:
    """Every observation CHECK rejects its representative contradiction.

    Args:
        tmp_path: Pytest temporary directory.
        name: Constraint case name.
        overrides: Invalid direct-row overrides.
    """
    repo = await _make_repo(tmp_path, f"observation-{name}.db")
    with pytest.raises(IntegrityError):
        await _insert_observation(repo, overrides)


_STATE_VIOLATIONS: list[tuple[str, dict[str, object]]] = [
    ("exchange_lower", {"exchange": "Kraken"}),
    ("mode", {"mode": "paper"}),
    ("method", {"method": "balance_guess"}),
    ("current_status", {"current_evaluation_status": "available"}),
    ("last_outcome", {"last_full_outcome": "incomplete"}),
    ("count", {"consecutive_full_mismatches": -1}),
    ("detail", {"expected_json": None}),
    ("detail_null_last_full", {"last_full_observation_id": None}),
    (
        "detail_anchor_without_source",
        {
            **_empty_state_detail(),
            "current_evaluation_status": "incomplete",
            "consecutive_full_mismatches": 0,
            "open_drift_episode_public_id": None,
            "anchor_public_id": _ANCHOR,
        },
    ),
    ("current_full", {"current_observation_id": 11}),
    (
        "null_full_lineage",
        {
            **_empty_state_detail(),
            "current_evaluation_status": "mismatched",
            "consecutive_full_mismatches": 0,
            "open_drift_episode_public_id": None,
        },
    ),
    (
        "matched",
        {
            "current_evaluation_status": "matched",
            "last_full_outcome": "matched",
            "consecutive_full_mismatches": 0,
            "open_drift_episode_public_id": None,
            "error": "forged error",
        },
    ),
    ("mismatched", {"consecutive_full_mismatches": 0}),
    ("spot_anchor", {"method": "spot_execution_replay"}),
    (
        "no_full",
        {
            **_empty_state_detail(),
            "current_evaluation_status": "incomplete",
            "consecutive_full_mismatches": 1,
            "open_drift_episode_public_id": None,
        },
    ),
    ("episode", {"consecutive_full_mismatches": 2}),
    (
        "error_text",
        {"current_evaluation_status": "error", "error": ""},
    ),
    (
        "error_length",
        {"current_evaluation_status": "incomplete", "error": "x" * 513},
    ),
]


@pytest.mark.parametrize(
    ("name", "overrides"),
    _STATE_VIOLATIONS,
    ids=[name for name, _ in _STATE_VIOLATIONS],
)
async def test_state_check_constraints_reject_every_contradiction(
    tmp_path: Path, name: str, overrides: dict[str, object]
) -> None:
    """Every reconciliation-state CHECK rejects a contradictory row.

    Args:
        tmp_path: Pytest temporary directory.
        name: Constraint case name.
        overrides: Invalid direct-row overrides.
    """
    repo = await _make_repo(tmp_path, f"state-{name}.db")
    with pytest.raises(IntegrityError):
        await _insert_state(repo, overrides)


_EPISODE_VIOLATIONS: list[tuple[str, dict[str, object]]] = [
    ("exchange_lower", {"exchange": "Kraken"}),
    ("mode", {"mode": "paper"}),
    ("status", {"status": "closed"}),
    ("observation_order", {"last_observation_id": 2}),
    ("detail_before_trigger", {"details_source_observation_id": 2}),
    ("detail_after_last", {"details_source_observation_id": 5}),
    ("mismatch_count", {"latest_full_mismatch_count": 2}),
    ("open", {"closed_at": _T0}),
    ("resolved", {"status": "resolved"}),
    (
        "resolved_closed_before_opened",
        {
            "status": "resolved",
            "closed_at": _T0 - timedelta(seconds=1),
            "resolution_reason": "matched",
        },
    ),
    (
        "rebased",
        {
            "status": "rebased",
            "closed_at": _T0,
            "resolution_reason": "operator_rebase",
        },
    ),
    (
        "rebased_closed_before_opened",
        {
            "status": "rebased",
            "closed_at": _T0 - timedelta(seconds=1),
            "resolution_reason": "operator_rebase",
            "closed_by_operator_public_id": _OTHER_WALLET,
            "rebase_anchor_public_id": _ANCHOR,
        },
    ),
    (
        "rebased_two_closers",
        {
            "status": "rebased",
            "closed_at": _T0 + timedelta(seconds=1),
            "resolution_reason": "operator_rebase",
            "closed_by_user_public_id": _WALLET,
            "closed_by_operator_public_id": _OTHER_WALLET,
            "rebase_anchor_public_id": _ANCHOR,
        },
    ),
    (
        "rebased_null_reason",
        {
            "status": "rebased",
            "closed_at": _T0 + timedelta(seconds=1),
            "resolution_reason": None,
            "closed_by_operator_public_id": _OTHER_WALLET,
            "rebase_anchor_public_id": _ANCHOR,
        },
    ),
]


@pytest.mark.parametrize(
    ("name", "overrides"),
    _EPISODE_VIOLATIONS,
    ids=[name for name, _ in _EPISODE_VIOLATIONS],
)
async def test_episode_check_constraints_reject_every_contradiction(
    tmp_path: Path, name: str, overrides: dict[str, object]
) -> None:
    """Every drift-episode CHECK rejects a contradictory lifecycle row.

    Args:
        tmp_path: Pytest temporary directory.
        name: Constraint case name.
        overrides: Invalid direct-row overrides.
    """
    repo = await _make_repo(tmp_path, f"episode-{name}.db")
    with pytest.raises(IntegrityError):
        await _insert_episode(repo, overrides)


async def test_resolved_and_rebased_episode_shapes_are_valid(tmp_path: Path) -> None:
    """Both legal closed lifecycle shapes satisfy the episode constraints.

    Given: complete matched-resolution and operator-rebase provenance,
    When: the rows are inserted directly,
    Then: both valid closed lifecycle versions commit.
    """
    repo = await _make_repo(tmp_path)
    await _insert_episode(
        repo,
        {
            "status": "resolved",
            "closed_at": _T0 + timedelta(seconds=1),
            "resolution_reason": "matched",
            "known_to": _T0 + timedelta(seconds=2),
        },
    )
    await _insert_episode(
        repo,
        {
            "status": "rebased",
            "closed_at": _T0 + timedelta(seconds=3),
            "resolution_reason": "operator_rebase",
            "closed_by_operator_public_id": _OTHER_WALLET,
            "rebase_anchor_public_id": _ANCHOR,
            "public_id": _EPISODE,
        },
    )
    assert len(await _episodes(repo)) == 2


async def test_partial_unique_indexes_reject_duplicate_active_rows(tmp_path: Path) -> None:
    """Active state, public-id, and open-episode uniqueness is DB-enforced.

    Given: one active state and one active open episode,
    When: duplicate identity or active public-id rows are inserted,
    Then: the partial unique indexes reject each contradiction while closed
        history with the same stable public id remains legal.
    """
    repo = await _make_repo(tmp_path)
    await _insert_observation(repo, {})
    with pytest.raises(IntegrityError):
        await _insert_observation(repo, {"public_id": _OTHER_PUBLIC})
    await _insert_state(repo, {})
    with pytest.raises(IntegrityError):
        await _insert_state(repo, {"public_id": _EPISODE})
    with pytest.raises(IntegrityError):
        await _insert_state(repo, {"wallet_public_id": _OTHER_WALLET})
    await _insert_episode(repo, {})
    with pytest.raises(IntegrityError):
        await _insert_episode(repo, {"public_id": _EPISODE})
    with pytest.raises(IntegrityError):
        await _insert_episode(
            repo,
            {"wallet_public_id": _OTHER_WALLET, "exchange": "kraken"},
        )


def test_postgresql_migration_shape_and_downgrade() -> None:
    """Revision 0022 emits dual-dialect PostgreSQL schema and clean drops.

    Given: an offline PostgreSQL Alembic context,
    When: revision 0022 upgrades and downgrades,
    Then: native UUID, BIGINT watermark, all three tables, CHECK constraints,
        and both partial predicates appear before reverse-order drops.
    """
    migration = importlib.import_module(
        "snapper.data.migrations.versions.0022_portfolio_reconciliation"
    )
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    operations = Operations(context)
    with patch.object(migration, "op", operations):
        migration.upgrade()
        migration.downgrade()
    ddl = output.getvalue()
    assert migration.revision == "0022"
    assert migration.down_revision == "0021"
    assert "CREATE TABLE portfolio_reconciliation_observations" in ddl
    assert "CREATE TABLE portfolio_reconciliation_states" in ddl
    assert "CREATE TABLE portfolio_drift_episodes" in ddl
    assert "UUID NOT NULL" in ddl
    assert "source_watermark BIGINT" in ddl
    assert "method VARCHAR(32) NOT NULL" in ddl
    assert "WHERE known_to = '9999-12-31T23:59:59+00:00'" in ddl
    assert "WHERE status = 'open' AND known_to = '9999-12-31T23:59:59+00:00'" in ddl
    assert "ck_portfolio_recon_obs_full_evidence" in ddl
    assert "ck_portfolio_recon_states_detail" in ddl
    assert "ck_portfolio_recon_states_spot_anchor" in ddl
    assert "uq_portfolio_reconciliation_observations_evaluation" in ddl
    assert "ck_portfolio_drift_detail_order" in ddl
    assert "ck_portfolio_drift_closed_order" in ddl
    assert "ck_portfolio_drift_rebased" in ddl
    assert "DROP TABLE portfolio_drift_episodes" in ddl
