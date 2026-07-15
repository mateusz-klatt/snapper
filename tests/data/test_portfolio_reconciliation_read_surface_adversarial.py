"""Database-backed adversarial tests for reconciliation read truth."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from uuid import uuid7

import pytest
from sqlalchemy import delete

from snapper.application.portfolio.reconciliation_view import build_portfolio_reconciliation_view
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioReconciliationMethodConfig
from snapper.data.models import PortfolioReconciliationObservation
from snapper.data.models import PortfolioReconciliationState
from snapper.data.models import VenueAccountState
from snapper.data.repository import SQLAlchemyRepository

type ReconciliationForgery = Literal[
    "invalid_method",
    "watermark_ahead",
    "cross_account_observation",
]


async def _persist_forged_context(
    repository: SQLAlchemyRepository,
    forgery: ReconciliationForgery,
    now: datetime,
) -> tuple[str, str]:
    """Persist one CHECK-valid state whose cross-row invariants are corrupt."""
    wallet_public_id = str(uuid7())
    observation_wallet_public_id = (
        str(uuid7()) if forgery == "cross_account_observation" else wallet_public_id
    )
    account_public_id = str(uuid7())
    session_id = str(uuid7())
    anchor_public_id = str(uuid7()) if forgery == "invalid_method" else None
    state_method = "spot_execution_replay" if forgery == "invalid_method" else "futures_position"
    state_watermark = 13 if forgery == "watermark_ahead" else 12
    async with repository.session() as session:
        account = VenueAccountState(
            wallet_public_id=wallet_public_id,
            exchange="kraken_futures",
            mode="live",
            sync_status="observed",
            balance_status="observed",
            position_status="observed",
            valuation_status="native_only",
            balances_json="[]",
            open_positions_json="[]",
            balance_observed_at=now,
            position_observed_at=now,
            current_attempt_observation_id=41,
            balance_payload_source_observation_id=41,
            position_payload_source_observation_id=41,
            authoritative_until=now + timedelta(minutes=5),
            error=None,
            public_id=account_public_id,
            session_id=session_id,
            sequence_id=1,
            timestamp=now,
            known_to=KNOWN_TO_MAX,
        )
        observation = PortfolioReconciliationObservation(
            wallet_public_id=observation_wallet_public_id,
            exchange="kraken_futures",
            mode="live",
            method="futures_position",
            evaluation_status="matched",
            venue_account_state_public_id=account_public_id,
            venue_account_observation_id=41,
            account_authoritative_until=now + timedelta(minutes=5),
            source_watermark_kind="venue_event_id",
            source_watermark=12,
            anchor_public_id=anchor_public_id,
            expected_json='{"position":"1"}',
            actual_json='{"position":"1"}',
            difference_json='{"position":"0"}',
            tolerance_json='{"absolute":"0"}',
            resulting_full_mismatch_count=0,
            drift_episode_public_id=None,
            error=None,
            public_id=str(uuid7()),
            session_id=session_id,
            sequence_id=2,
            timestamp=now,
            known_to=KNOWN_TO_MAX,
        )
        session.add_all([account, observation])
        await session.flush()
        observation_id = int(observation.id)
        session.add_all(
            [
                PortfolioReconciliationMethodConfig(
                    wallet_public_id=wallet_public_id,
                    exchange="kraken_futures",
                    mode="live",
                    method=state_method,
                    public_id=str(uuid7()),
                    session_id=session_id,
                    sequence_id=1,
                    timestamp=now - timedelta(minutes=1),
                    known_to=KNOWN_TO_MAX,
                ),
                PortfolioReconciliationState(
                    wallet_public_id=wallet_public_id,
                    exchange="kraken_futures",
                    mode="live",
                    method=state_method,
                    current_evaluation_status="matched",
                    current_observation_id=observation_id,
                    last_full_observation_id=observation_id,
                    last_full_outcome="matched",
                    detail_source_observation_id=observation_id,
                    consecutive_full_mismatches=0,
                    open_drift_episode_public_id=None,
                    anchor_public_id=anchor_public_id,
                    venue_account_state_public_id=account_public_id,
                    venue_account_observation_id=41,
                    source_watermark_kind="venue_event_id",
                    source_watermark=state_watermark,
                    expected_json='{"position":"1"}',
                    actual_json='{"position":"1"}',
                    difference_json='{"position":"0"}',
                    tolerance_json='{"absolute":"0"}',
                    reconciled_at=now,
                    authoritative_until=now + timedelta(minutes=5),
                    error=None,
                    public_id=str(uuid7()),
                    session_id=session_id,
                    sequence_id=2,
                    timestamp=now,
                    known_to=KNOWN_TO_MAX,
                ),
            ]
        )
        await session.commit()
    return wallet_public_id, session_id


async def _delete_forged_context(
    repository: SQLAlchemyRepository,
    session_id: str,
) -> None:
    """Remove every directly persisted row owned by one adversarial fixture."""
    async with repository.session() as session:
        for model in (
            PortfolioReconciliationState,
            PortfolioReconciliationMethodConfig,
            PortfolioReconciliationObservation,
            VenueAccountState,
        ):
            await session.execute(delete(model).where(model.session_id == session_id))
        await session.commit()


@pytest.mark.parametrize(
    "forgery",
    ["invalid_method", "watermark_ahead", "cross_account_observation"],
)
async def test_persisted_forgery_fails_closed_on_configured_database(
    forgery: ReconciliationForgery,
) -> None:
    """Directly persisted cross-row corruption never produces authority.

    The configured test URL makes the same test run on the default SQLite
    harness and the coordinator's disposable PostgreSQL run. Literal NULL or
    unknown methods cannot be inserted portably because both schemas enforce
    NOT NULL and the same method CHECK, so the invalid-method case persists a
    CHECK-valid method that conflicts with its referenced observation.

    Given: A directly persisted state with one forged cross-row invariant.
    When: The configured database context is loaded and projected.
    Then: The strict view clears evidence and denies authority as corrupt.

    Args:
        forgery: Portable persisted corruption variant to exercise.
    """
    now = datetime.now(UTC)
    repository = SQLAlchemyRepository(BootstrapSettingsLoader().db_url)
    persisted_session_id: str | None = None
    try:
        wallet_public_id, persisted_session_id = await _persist_forged_context(
            repository,
            forgery,
            now,
        )

        contexts = await repository.get_portfolio_reconciliation_read_contexts([wallet_public_id])

        assert len(contexts) == 1
        view = build_portfolio_reconciliation_view(contexts[0], now)
        assert view.effective_status == "corrupt"
        assert view.is_authoritative is False
        assert view.expected is None
        assert view.actual is None
        assert view.difference is None
        assert view.tolerance is None
    finally:
        if persisted_session_id is not None:
            await _delete_forged_context(repository, persisted_session_id)
        await repository.engine.dispose()
