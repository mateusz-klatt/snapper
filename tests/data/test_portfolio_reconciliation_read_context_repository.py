"""Tests for batched portfolio reconciliation read contexts."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from sqlalchemy import event
from sqlalchemy import select
from sqlalchemy import text

from snapper.application.portfolio.reconciliation_view import build_portfolio_reconciliation_view
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioDriftEpisode
from snapper.data.models import PortfolioReconciliationMethodConfig
from snapper.data.models import PortfolioReconciliationObservation
from snapper.data.models import PortfolioReconciliationState
from snapper.data.models import VenueAccountState
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
_SESSION = "00000000-0000-7000-8000-000000000101"
_WALLET = "00000000-0000-7000-8000-000000000201"
_OTHER_WALLET = "00000000-0000-7000-8000-000000000202"
_ACCOUNT = "00000000-0000-7000-8000-000000000301"
_OTHER_ACCOUNT = "00000000-0000-7000-8000-000000000302"
_OBSERVATION = "00000000-0000-7000-8000-000000000401"
_STATE = "00000000-0000-7000-8000-000000000501"
_CONFIG = "00000000-0000-7000-8000-000000000601"
_EPISODE = "00000000-0000-7000-8000-000000000701"
_OTHER_EPISODE = "00000000-0000-7000-8000-000000000702"


async def _make_repo(tmp_path: Path, name: str) -> SQLAlchemyRepository:
    """Create one isolated full-schema repository."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / name}")
    await repository.create_all()
    return repository


def _account(
    wallet_public_id: str,
    public_id: str,
    exchange: str = "kraken_futures",
) -> VenueAccountState:
    """Build one active account-state anchor for read contexts."""
    return VenueAccountState(
        wallet_public_id=wallet_public_id,
        exchange=exchange,
        mode="live",
        sync_status="error",
        balance_status="error",
        position_status="error",
        valuation_status="native_only",
        balances_json=None,
        open_positions_json=None,
        balance_observed_at=None,
        position_observed_at=None,
        current_attempt_observation_id=1,
        balance_payload_source_observation_id=None,
        position_payload_source_observation_id=None,
        authoritative_until=None,
        error="venue unavailable",
        public_id=public_id,
        session_id=_SESSION,
        sequence_id=1,
        timestamp=_NOW,
        known_to=KNOWN_TO_MAX,
    )


def _observation(
    wallet_public_id: str,
    public_id: str,
    mismatch_count: int,
    episode_public_id: str | None,
) -> PortfolioReconciliationObservation:
    """Build one full mismatched reconciliation observation."""
    return PortfolioReconciliationObservation(
        wallet_public_id=wallet_public_id,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        evaluation_status="mismatched",
        venue_account_state_public_id=_ACCOUNT,
        venue_account_observation_id=11,
        account_authoritative_until=_NOW + timedelta(minutes=5),
        source_watermark_kind="venue_event_id",
        source_watermark=12,
        anchor_public_id=None,
        expected_json="{}",
        actual_json="{}",
        difference_json="{}",
        tolerance_json="{}",
        resulting_full_mismatch_count=mismatch_count,
        drift_episode_public_id=episode_public_id,
        error=None,
        public_id=public_id,
        session_id=_SESSION,
        sequence_id=2,
        timestamp=_NOW,
        known_to=KNOWN_TO_MAX,
    )


def _state(
    observation_id: int,
    mismatch_count: int,
    episode_public_id: str | None,
) -> PortfolioReconciliationState:
    """Build one active state referring to a full mismatch observation."""
    return PortfolioReconciliationState(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        current_evaluation_status="mismatched",
        current_observation_id=observation_id,
        last_full_observation_id=observation_id,
        last_full_outcome="mismatched",
        detail_source_observation_id=observation_id,
        consecutive_full_mismatches=mismatch_count,
        open_drift_episode_public_id=episode_public_id,
        anchor_public_id=None,
        venue_account_state_public_id=_ACCOUNT,
        venue_account_observation_id=11,
        source_watermark_kind="venue_event_id",
        source_watermark=12,
        expected_json="{}",
        actual_json="{}",
        difference_json="{}",
        tolerance_json="{}",
        reconciled_at=_NOW,
        authoritative_until=_NOW + timedelta(minutes=5),
        error=None,
        public_id=_STATE,
        session_id=_SESSION,
        sequence_id=2,
        timestamp=_NOW,
        known_to=KNOWN_TO_MAX,
    )


def _config() -> PortfolioReconciliationMethodConfig:
    """Build the active method classification for the target account."""
    return PortfolioReconciliationMethodConfig(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        method="futures_position",
        public_id=_CONFIG,
        session_id=_SESSION,
        sequence_id=1,
        timestamp=_NOW,
        known_to=KNOWN_TO_MAX,
    )


def _episode(
    observation_id: int,
    public_id: str = _EPISODE,
) -> PortfolioDriftEpisode:
    """Build one active open drift episode owned by the target account."""
    return PortfolioDriftEpisode(
        wallet_public_id=_WALLET,
        exchange="kraken_futures",
        mode="live",
        status="open",
        opened_at=_NOW,
        closed_at=None,
        trigger_observation_id=observation_id,
        last_observation_id=observation_id,
        details_source_observation_id=observation_id,
        latest_full_mismatch_count=3,
        resolution_reason=None,
        closed_by_user_public_id=None,
        closed_by_operator_public_id=None,
        rebase_anchor_public_id=None,
        public_id=public_id,
        session_id=_SESSION,
        sequence_id=2,
        timestamp=_NOW,
        known_to=KNOWN_TO_MAX,
    )


async def _seed_complete_context(repository: SQLAlchemyRepository) -> int:
    """Persist one complete context plus an unrelated account."""
    async with repository.session() as session:
        observation = _observation(_WALLET, _OBSERVATION, 3, _EPISODE)
        session.add_all(
            [
                _account(_WALLET, _ACCOUNT),
                _account(_OTHER_WALLET, _OTHER_ACCOUNT, "kraken"),
                observation,
            ]
        )
        await session.flush()
        observation_id = int(observation.id)
        session.add_all(
            [
                _state(observation_id, 3, _EPISODE),
                _config(),
                _episode(observation_id),
            ]
        )
        await session.commit()
        return observation_id


async def test_read_contexts_are_complete_scoped_and_one_query(
    tmp_path: Path,
) -> None:
    """One set query projects every dependency without an account N plus one."""
    repository = await _make_repo(tmp_path, "complete-read-context.db")
    observation_id = await _seed_complete_context(repository)
    select_statements: list[str] = []

    def count_selects(*args: object) -> None:
        """Capture SQL set reads emitted by the context operation."""
        if len(args) > 2 and isinstance(args[2], str):
            statement = args[2].lstrip()
            if statement.upper().startswith("SELECT"):
                select_statements.append(statement)

    event.listen(repository.engine.sync_engine, "before_cursor_execute", count_selects)
    try:
        contexts = await repository.get_portfolio_reconciliation_read_contexts()
        assert await repository.get_portfolio_reconciliation_read_contexts([]) == []
    finally:
        event.remove(repository.engine.sync_engine, "before_cursor_execute", count_selects)
    assert len(select_statements) == 1
    assert len(contexts) == 2
    assert [row["account_state"]["public_id"] for row in contexts] == [
        _OTHER_ACCOUNT,
        _ACCOUNT,
    ]
    context = next(row for row in contexts if row["account_state"]["public_id"] == _ACCOUNT)
    assert context["account_state"]["public_id"] == _ACCOUNT
    assert context["state"] is not None
    assert context["state"]["public_id"] == _STATE
    assert context["config"] is not None
    assert context["config"]["public_id"] == _CONFIG
    assert [row["id"] for row in context["observations"]] == [observation_id]
    assert context["latest_ordered_observation_id"] == observation_id
    assert context["latest_appended_observation_id"] == observation_id
    assert context["open_drift_episode"] is not None
    assert context["open_drift_episode"] == {
        "wallet_public_id": _WALLET,
        "exchange": "kraken_futures",
        "mode": "live",
        "status": "open",
        "opened_at": _NOW,
        "trigger_observation_id": observation_id,
        "last_observation_id": observation_id,
        "details_source_observation_id": observation_id,
        "latest_full_mismatch_count": 3,
        "public_id": _EPISODE,
    }
    assert context["spot_anchor"] is None
    empty_context = next(
        row for row in contexts if row["account_state"]["public_id"] == _OTHER_ACCOUNT
    )
    assert empty_context["state"] is None
    assert empty_context["observations"] == []
    assert empty_context["config"] is None
    assert empty_context["latest_ordered_observation_id"] is None
    assert empty_context["latest_appended_observation_id"] is None
    assert empty_context["open_drift_episode"] is None
    assert empty_context["spot_anchor"] is None


async def test_read_context_preserves_cross_account_observation_reference(
    tmp_path: Path,
) -> None:
    """A foreign id reference reaches validators with its forged identity intact."""
    repository = await _make_repo(tmp_path, "foreign-read-context.db")
    async with repository.session() as session:
        observation = _observation(_OTHER_WALLET, _OBSERVATION, 1, None)
        session.add_all([_account(_WALLET, _ACCOUNT), observation])
        await session.flush()
        observation_id = int(observation.id)
        session.add_all([_state(observation_id, 1, None), _config()])
        await session.commit()
    context = (await repository.get_portfolio_reconciliation_read_contexts([_WALLET]))[0]
    assert context["latest_ordered_observation_id"] is None
    assert context["latest_appended_observation_id"] is None
    assert len(context["observations"]) == 1
    assert context["observations"][0]["id"] == observation_id
    assert context["observations"][0]["wallet_public_id"] == _OTHER_WALLET
    view = build_portfolio_reconciliation_view(context, _NOW)
    assert view.effective_status == "corrupt"
    assert view.is_authoritative is False
    assert view.expected is None


async def test_read_context_rejects_persisted_watermark_lineage_mismatch(
    tmp_path: Path,
) -> None:
    """A directly forged state watermark reaches the view and fails closed."""
    repository = await _make_repo(tmp_path, "watermark-read-context.db")
    await _seed_complete_context(repository)
    async with repository.session() as session:
        state = (
            await session.execute(
                select(PortfolioReconciliationState).where(
                    PortfolioReconciliationState.wallet_public_id == _WALLET,
                    PortfolioReconciliationState.known_to == KNOWN_TO_MAX,
                )
            )
        ).scalar_one()
        state.source_watermark = 13
        await session.commit()
    context = (await repository.get_portfolio_reconciliation_read_contexts([_WALLET]))[0]
    view = build_portfolio_reconciliation_view(context, _NOW)
    assert view.effective_status == "corrupt"
    assert view.is_authoritative is False
    assert view.expected is None


async def test_read_context_joins_only_the_state_referenced_episode(
    tmp_path: Path,
) -> None:
    """A corrupted second open episode cannot duplicate an account context.

    Given: An isolated database whose open-identity index is bypassed to persist
        two active open episodes for one account.
    When: The batched read context is loaded for that account.
    Then: Exactly one account row contains only the episode referenced by state.
    """
    repository = await _make_repo(tmp_path, "duplicate-open-episode-context.db")
    observation_id = await _seed_complete_context(repository)
    async with repository.session() as session:
        await session.execute(text("DROP INDEX uq_portfolio_drift_episodes_open_identity"))
        session.add(_episode(observation_id, _OTHER_EPISODE))
        await session.commit()

    contexts = await repository.get_portfolio_reconciliation_read_contexts([_WALLET])

    assert len(contexts) == 1
    assert contexts[0]["open_drift_episode"] is not None
    assert contexts[0]["open_drift_episode"]["public_id"] == _EPISODE
