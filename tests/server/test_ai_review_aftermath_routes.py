"""REST contract tests for terminal AI-review aftermath reads."""

from collections.abc import AsyncGenerator
from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.auth.dependencies import require_authentication
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.auth.scope_grant_service import get_scope_grant_service
from snapper.data.repository import Repository
from snapper.data.repository_types import AiReviewAftermathRow
from snapper.data.repository_types import AiReviewRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import create_app
from snapper.server.dependencies import get_repository_dependency


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable background services for focused route tests.

    Args:
        _app: FastAPI application supplied by the lifespan protocol.

    Yields:
        Control to the test client without starting external services.
    """
    yield


def _delegate_principal() -> AuthPrincipal:
    """Return a registered AI-delegate principal with review permissions."""
    return AuthPrincipal(
        username="delegate",
        role=UserRole.AI_DELEGATE,
        user_public_id="user-1",
        operator_public_ids=["operator-1"],
        primary_operator_public_id="operator-1",
        delegate_public_id="delegate-1",
    )


def _operator_principal() -> AuthPrincipal:
    """Return a permission-bearing non-delegate principal."""
    return AuthPrincipal(
        username="operator",
        role=UserRole.OPERATOR,
        user_public_id="user-2",
        operator_public_ids=["operator-1"],
        primary_operator_public_id="operator-1",
    )


def _viewer_principal() -> AuthPrincipal:
    """Return a viewer who lacks the AI-review read permission."""
    return AuthPrincipal(
        username="viewer",
        role=UserRole.VIEWER,
        user_public_id="user-3",
        operator_public_ids=["operator-1"],
        primary_operator_public_id="operator-1",
    )


def _review(status: str = "timeout") -> AiReviewRow:
    """Build a complete review row for route policy checks.

    Args:
        status: Lifecycle state returned by the repository mock.

    Returns:
        Fully populated :class:`AiReviewRow` fixture.
    """
    created_at = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
    terminal = status not in {"pending", "fanout_dispatched"}
    return {
        "public_id": "review-1",
        "session_id": "session-1",
        "sequence_id": 1,
        "user_public_id": "strategy-user-1",
        "operator_public_id": "operator-1",
        "wallet_public_id": "wallet-1",
        "instrument_public_id": "instrument-1",
        "strategy_public_id": "strategy-1",
        "selected_delegate_public_id": "delegate-1",
        "responding_delegate_public_id": None,
        "resolution_mode": "timeout_no_response" if terminal else None,
        "status": status,
        "signal_envelope": {"side": "buy", "thesis": "breakout"},
        "signal_snapshot_hash": "a" * 64,
        "instrument_metadata": {"spread_bps": 8.0},
        "deadline": created_at + timedelta(seconds=5),
        "fanout_after": created_at + timedelta(seconds=2),
        "decision": None,
        "rationale": None,
        "dispatch_version": 1,
        "counter_decremented_at": created_at + timedelta(seconds=6) if terminal else None,
        "created_at": created_at,
        "updated_at": created_at + timedelta(seconds=6) if terminal else created_at,
        "resolved_at": created_at + timedelta(seconds=6) if terminal else None,
    }


def _aftermath(review: AiReviewRow) -> AiReviewAftermathRow:
    """Build an empty-activity aftermath around a review row.

    Args:
        review: Terminal review carried by the projection.

    Returns:
        Typed projection with no orders, fills, cycles, or position.
    """
    return {
        "review": review,
        "window_started_at": review["created_at"],
        "as_of": review["created_at"] + timedelta(minutes=1),
        "orders": [],
        "executions": [],
        "position_cycle_transitions": [],
        "current_positions": [],
    }


def _repository_mock(
    *,
    review: AiReviewRow | None,
    scope_ok: bool = True,
    aftermath: AiReviewAftermathRow | None = None,
) -> AsyncMock:
    """Configure an asynchronous repository double for the route.

    Args:
        review: Row returned by the initial review lookup.
        scope_ok: Delegate grant-check result.
        aftermath: Aggregate projection returned after authorization.

    Returns:
        Async mock exposing the three read methods used by the route.
    """
    repo = AsyncMock(spec=Repository)
    repo.get_ai_review = AsyncMock(return_value=review)
    repo.has_grant_for_delegate = AsyncMock(return_value=scope_ok)
    repo.get_ai_review_aftermath = AsyncMock(return_value=aftermath)
    get_scope_grant_service().repository = cast(Repository, repo)
    return repo


@pytest.fixture(autouse=True)
def _clear_scope_grant_service() -> Generator[None]:
    """Reset the scope-policy singleton around each route case."""
    ScopeGrantService.clear_instance()
    yield
    ScopeGrantService.clear_instance()


def _client(repo: AsyncMock, principal: AuthPrincipal) -> TestClient:
    """Create a focused application client with auth and repository overrides.

    Args:
        repo: Repository double used by the route.
        principal: Authenticated caller returned by the auth dependency.

    Returns:
        Synchronous FastAPI test client.
    """
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    app.state.rest_tracker = SequenceTracker()

    def _principal() -> AuthPrincipal:
        """Return the case-specific authenticated principal."""
        return principal

    app.dependency_overrides[require_authentication] = _principal
    app.dependency_overrides[get_repository_dependency] = lambda: cast(Repository, repo)
    return TestClient(app)


class TestAiReviewAftermathRoute:
    """Authorization, lifecycle, and response behavior for the REST read."""

    def test_terminal_review_returns_empty_aftermath_without_mutation(self) -> None:
        """Authorized terminal review returns a Pydantic-typed empty projection.

        Given a registered delegate with an active review-scope grant,
        When the terminal review aftermath route is requested,
        Then the full review and empty activity and position lists return as 200.
        """
        review = _review()
        repo = _repository_mock(review=review, aftermath=_aftermath(review))
        response = _client(repo, _delegate_principal()).get("/api/ai-reviews/review-1/aftermath")
        assert response.status_code == 200
        body = response.json()
        assert body["review"]["public_id"] == "review-1"
        assert body["review"]["status"] == "timeout"
        assert body["orders"] == []
        assert body["executions"] == []
        assert body["position_cycle_transitions"] == []
        assert body["current_positions"] == []
        scope_as_of = repo.has_grant_for_delegate.await_args.kwargs["as_of"]
        projection_as_of = repo.get_ai_review_aftermath.await_args.kwargs["as_of"]
        assert scope_as_of == projection_as_of

    def test_unknown_review_returns_404_without_scope_read(self) -> None:
        """Unknown review is anti-enumerated as ``review_not_found``.

        Given no review row for the requested public identifier,
        When the aftermath route is requested,
        Then it returns 404 before scope or aggregate reads.
        """
        repo = _repository_mock(review=None)
        response = _client(repo, _delegate_principal()).get("/api/ai-reviews/missing/aftermath")
        assert response.status_code == 404
        assert response.json()["detail"]["error_code"] == "review_not_found"
        repo.has_grant_for_delegate.assert_not_awaited()
        repo.get_ai_review_aftermath.assert_not_awaited()

    def test_out_of_scope_review_is_hidden_as_404(self) -> None:
        """Revoked delegate grant hides an otherwise existing review.

        Given an existing terminal review outside the delegate's active grant,
        When the aftermath route is requested,
        Then it returns the same 404 used for an unknown identifier.
        """
        review = _review()
        repo = _repository_mock(review=review, scope_ok=False)
        response = _client(repo, _delegate_principal()).get("/api/ai-reviews/review-1/aftermath")
        assert response.status_code == 404
        assert response.json()["detail"]["error_code"] == "review_not_found"
        repo.get_ai_review_aftermath.assert_not_awaited()

    def test_pending_review_returns_409_without_projection(self) -> None:
        """Non-terminal review returns an explicit lifecycle conflict.

        Given an in-scope review that is still pending,
        When its aftermath is requested,
        Then the route returns ``review_not_terminal`` and reads no activity.
        """
        repo = _repository_mock(review=_review("pending"))
        response = _client(repo, _delegate_principal()).get("/api/ai-reviews/review-1/aftermath")
        assert response.status_code == 409
        detail = response.json()["detail"]
        assert detail["error_code"] == "review_not_terminal"
        assert detail["details"]["status"] == "pending"
        repo.get_ai_review_aftermath.assert_not_awaited()

    def test_non_delegate_returns_422_before_repository_reads(self) -> None:
        """Permission-bearing operator cannot use the delegate projection.

        Given an operator who holds ``READ_SIGNALS`` but has no delegate identity,
        When the aftermath route is requested,
        Then it returns ``not_a_delegate`` without touching the repository.
        """
        review = _review()
        repo = _repository_mock(review=review, aftermath=_aftermath(review))
        response = _client(repo, _operator_principal()).get("/api/ai-reviews/review-1/aftermath")
        assert response.status_code == 422
        assert response.json()["detail"]["error_code"] == "not_a_delegate"
        repo.get_ai_review.assert_not_awaited()

    def test_viewer_without_read_signals_returns_403(self) -> None:
        """Role permission gate rejects a viewer before route execution.

        Given a viewer who has position access but lacks ``READ_SIGNALS``,
        When the aftermath route is requested,
        Then FastAPI returns 403 and no review row is loaded.
        """
        review = _review()
        repo = _repository_mock(review=review, aftermath=_aftermath(review))
        response = _client(repo, _viewer_principal()).get("/api/ai-reviews/review-1/aftermath")
        assert response.status_code == 403
        repo.get_ai_review.assert_not_awaited()

    def test_projection_disappearance_returns_404(self) -> None:
        """Missing aggregate after authorization fails closed as not found.

        Given a terminal in-scope review that vanishes before aggregate loading,
        When the projection lookup returns ``None``,
        Then the route returns 404 rather than fabricating an empty response.
        """
        repo = _repository_mock(review=_review(), aftermath=None)
        response = _client(repo, _delegate_principal()).get("/api/ai-reviews/review-1/aftermath")
        assert response.status_code == 404
        assert response.json()["detail"]["error_code"] == "review_not_found"
