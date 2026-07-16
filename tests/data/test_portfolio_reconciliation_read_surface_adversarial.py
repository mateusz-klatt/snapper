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
from snapper.data.models import PortfolioSpotReconciliationAnchor
from snapper.data.models import VenueAccountState
from snapper.data.repository import SQLAlchemyRepository

type ReconciliationForgery = Literal[
    "invalid_method",
    "watermark_ahead",
    "cross_account_observation",
    "causal_mismatch",
]
type SpotAnchorForgery = Literal[
    "missing_anchor",
    "foreign_anchor_identity",
    "wrong_state_watermark_kind",
    "anchor_watermark_ahead",
]
type ReadSurfaceForgery = ReconciliationForgery | SpotAnchorForgery


async def _persist_forged_context(
    repository: SQLAlchemyRepository,
    forgery: ReadSurfaceForgery,
    now: datetime,
) -> tuple[str, str]:
    """Persist one CHECK-valid state whose cross-row invariants are corrupt."""
    anchor_forgery = forgery in (
        "missing_anchor",
        "foreign_anchor_identity",
        "wrong_state_watermark_kind",
        "anchor_watermark_ahead",
    )
    causal_mismatch = forgery == "causal_mismatch"
    wallet_public_id = str(uuid7())
    observation_wallet_public_id = (
        str(uuid7()) if forgery == "cross_account_observation" else wallet_public_id
    )
    account_public_id = str(uuid7())
    session_id = str(uuid7())
    state_method = (
        "unclassified"
        if causal_mismatch
        else (
            "spot_execution_replay"
            if forgery == "invalid_method" or anchor_forgery
            else "futures_position"
        )
    )
    config_method = "futures_position" if causal_mismatch else state_method
    observation_method = "futures_position" if forgery == "invalid_method" else state_method
    anchor_public_id = str(uuid7()) if state_method == "spot_execution_replay" else None
    watermark_kind = (
        None
        if causal_mismatch
        else (
            "venue_event_id"
            if forgery == "wrong_state_watermark_kind" or not anchor_forgery
            else "execution_id"
        )
    )
    state_watermark = None if causal_mismatch else (13 if forgery == "watermark_ahead" else 12)
    evaluation_status = "incomplete" if causal_mismatch else "matched"
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
            method=observation_method,
            evaluation_status=evaluation_status,
            venue_account_state_public_id=None if causal_mismatch else account_public_id,
            venue_account_observation_id=None if causal_mismatch else 41,
            account_authoritative_until=None if causal_mismatch else now + timedelta(minutes=5),
            source_watermark_kind=watermark_kind,
            source_watermark=None if causal_mismatch else 12,
            anchor_public_id=anchor_public_id,
            expected_json=None if causal_mismatch else '{"position":"1"}',
            actual_json=None if causal_mismatch else '{"position":"1"}',
            difference_json=None if causal_mismatch else '{"position":"0"}',
            tolerance_json=None if causal_mismatch else '{"absolute":"0"}',
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
        rows: list[
            PortfolioReconciliationMethodConfig
            | PortfolioReconciliationState
            | PortfolioSpotReconciliationAnchor
        ] = [
            PortfolioReconciliationMethodConfig(
                wallet_public_id=wallet_public_id,
                exchange="kraken_futures",
                mode="live",
                method=config_method,
                classified_after_observation_id=(observation_id + 1 if causal_mismatch else None),
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
                current_evaluation_status=evaluation_status,
                current_observation_id=observation_id,
                last_full_observation_id=None if causal_mismatch else observation_id,
                last_full_outcome=None if causal_mismatch else "matched",
                detail_source_observation_id=None if causal_mismatch else observation_id,
                consecutive_full_mismatches=0,
                open_drift_episode_public_id=None,
                anchor_public_id=anchor_public_id,
                venue_account_state_public_id=None if causal_mismatch else account_public_id,
                venue_account_observation_id=None if causal_mismatch else 41,
                source_watermark_kind=watermark_kind,
                source_watermark=state_watermark,
                expected_json=None if causal_mismatch else '{"position":"1"}',
                actual_json=None if causal_mismatch else '{"position":"1"}',
                difference_json=None if causal_mismatch else '{"position":"0"}',
                tolerance_json=None if causal_mismatch else '{"absolute":"0"}',
                reconciled_at=None if causal_mismatch else now,
                authoritative_until=None if causal_mismatch else now + timedelta(minutes=5),
                error=None,
                public_id=str(uuid7()),
                session_id=session_id,
                sequence_id=2,
                timestamp=now,
                known_to=KNOWN_TO_MAX,
            ),
        ]
        if anchor_forgery and forgery != "missing_anchor":
            first_request_started_at = now - timedelta(seconds=4)
            first_request_completed_at = now - timedelta(seconds=3)
            second_request_started_at = now - timedelta(seconds=2)
            second_request_completed_at = now - timedelta(seconds=1)
            rows.append(
                PortfolioSpotReconciliationAnchor(
                    wallet_public_id=(
                        str(uuid7()) if forgery == "foreign_anchor_identity" else wallet_public_id
                    ),
                    exchange="kraken_futures",
                    mode="live",
                    venue_account_state_public_id=account_public_id,
                    balance_observation_id=41,
                    source_watermark_kind="execution_id",
                    source_watermark=(13 if forgery == "anchor_watermark_ahead" else 11),
                    balances_json='{"USD":"1"}',
                    first_request_started_at=first_request_started_at,
                    first_request_completed_at=first_request_completed_at,
                    second_request_started_at=second_request_started_at,
                    second_request_completed_at=second_request_completed_at,
                    boundary_status="double_read_equal",
                    inventory_status="certified_full",
                    margin_status="cash",
                    provenance="adversarial-read-surface",
                    public_id=anchor_public_id,
                    session_id=session_id,
                    sequence_id=1,
                    timestamp=now,
                    known_to=KNOWN_TO_MAX,
                )
            )
        session.add_all(rows)
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
            PortfolioSpotReconciliationAnchor,
            PortfolioReconciliationObservation,
            VenueAccountState,
        ):
            await session.execute(delete(model).where(model.session_id == session_id))
        await session.commit()


@pytest.mark.parametrize(
    "forgery",
    ["invalid_method", "watermark_ahead", "cross_account_observation", "causal_mismatch"],
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


@pytest.mark.parametrize(
    "forgery",
    [
        "missing_anchor",
        "foreign_anchor_identity",
        "wrong_state_watermark_kind",
        "anchor_watermark_ahead",
    ],
)
async def test_persisted_spot_anchor_forgery_fails_closed_on_configured_database(
    forgery: SpotAnchorForgery,
) -> None:
    """Forged spot-anchor lineage remains corrupt on SQLite and PostgreSQL.

    The anchor table restricts its own watermark kind to ``execution_id`` on
    both backends. The wrong-kind variant therefore forges the state and its
    observation while keeping the referenced anchor schema-valid.

    Given: A full matched spot state with forged referenced-anchor lineage.
    When: The configured database context is loaded and projected.
    Then: The view clears evidence and denies authority as corrupt.

    Args:
        forgery: Portable persisted spot-anchor corruption variant.
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
        context = contexts[0]
        if forgery == "missing_anchor":
            assert context["spot_anchor"] is None
        else:
            assert context["spot_anchor"] is not None
        view = build_portfolio_reconciliation_view(context, now)
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
