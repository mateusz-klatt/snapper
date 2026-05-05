"""Tests for the ``ai-reviews`` REST endpoints.

Covers the HTTP-status surface of the two routes added by
:mod:`snapper.server.ai_review_routes`:

- ``POST /api/ai-reviews/{review_public_id}/decision`` — structured
  envelope wrapped in HTTP statuses (200 / 404 / 403 / 409 / 410 /
  422 / 503).
- ``GET /api/ai-reviews/pending`` — list pending reviews keyed by the
  caller's ``AuthPrincipal.delegate_public_id``.

The :class:`AiReviewService` is mocked at the singleton level so the
tests focus on the route's HTTP-surface contract, not the underlying
state machine (covered in the service-level test suite).
"""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import cast
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.application.ai_review.service import AiReviewDecisionResult
from snapper.application.ai_review.service import AiReviewService
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.core.types import AiReviewResolutionModeEnum
from snapper.core.types import AiReviewStatusEnum
from snapper.data.repository_types import PendingReviewSummary
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable the FastAPI lifespan so tests don't spin up ZMQ + DB."""
    yield


def _delegate_principal() -> AuthPrincipal:
    """AI_DELEGATE principal with ``delegate_public_id`` populated."""
    return AuthPrincipal(
        username="delegate-1",
        role=UserRole.AI_DELEGATE,
        user_public_id="user-1",
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
        delegate_public_id="del-1",
    )


def _operator_principal() -> AuthPrincipal:
    """OPERATOR principal — has CREATE_ORDERS + READ_SIGNALS but no delegate id."""
    return AuthPrincipal(
        username="operator-1",
        role=UserRole.OPERATOR,
        user_public_id="op-user-1",
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )


def _viewer_principal() -> AuthPrincipal:
    """VIEWER principal — lacks CREATE_ORDERS so decision route 403s."""
    return AuthPrincipal(
        username="viewer-1",
        role=UserRole.VIEWER,
        user_public_id="viewer-user-1",
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )


def _create_client(
    *,
    repo: Any,
    principal: AuthPrincipal,
) -> TestClient:
    """Build a TestClient with auth + repo overridden, lifespan no-oped."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    app.state.rest_tracker = SequenceTracker()

    def _skip_csrf() -> None:
        return None

    def _principal() -> AuthPrincipal:
        return principal

    app.dependency_overrides[validate_csrf_token] = _skip_csrf
    app.dependency_overrides[require_authentication] = _principal
    app.dependency_overrides[get_repository_dependency] = lambda: repo
    return TestClient(app)


@pytest.fixture(autouse=True)
def _clear_singletons() -> Any:
    """Reset AiReviewService + ScopeGrantService between tests."""
    AiReviewService.clear_instance()
    ScopeGrantService.clear_instance()
    yield
    AiReviewService.clear_instance()
    ScopeGrantService.clear_instance()


def _stub_submit_decision(
    monkeypatch: pytest.MonkeyPatch, result: AiReviewDecisionResult
) -> AsyncMock:
    """Stub the AiReviewService singleton's ``submit_decision``."""
    svc = AiReviewService.get_instance()
    stub = AsyncMock(return_value=result)
    monkeypatch.setattr(svc, "submit_decision", stub)
    return stub


def _decision_envelope(
    *,
    decision: str,
    rationale: str | None = None,
    public_id: str = "req-decision-1",
    sequence_id: int = 1,
) -> dict[str, Any]:
    """Return a canonical ``AiReviewDecisionCommand`` envelope.

    Mirrors the ``CreateOrderCommand`` shape used by every other
    mutating REST endpoint: provenance fields on the envelope,
    domain payload (decision + rationale) under ``payload``.
    """
    payload: dict[str, Any] = {"decision": decision}
    if rationale is not None:
        payload["rationale"] = rationale
    return {
        "type": "ai_review_decision_command",
        "session_id": "s1",
        "sequence_id": sequence_id,
        "public_id": public_id,
        "timestamp": datetime(2026, 4, 26, 12, 0, 0, tzinfo=UTC).isoformat(),
        "payload": payload,
    }


class TestSubmitDecisionRoute:
    """``POST /api/ai-reviews/{id}/decision`` HTTP-status mapping."""

    def test_happy_path_approve_returns_200_envelope(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """First valid approve -> 200 with success=True envelope.

        Given a stubbed service returning the happy-path envelope,
        When the route is POSTed with decision='approve',
        Then HTTP 200 + JSON body carries success=True, error_code=None,
        and details with status/resolution_mode/dispatch_version
        forwarded from the service result.
        """
        _stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code=None,
                message="Decision recorded.",
                status=AiReviewStatusEnum.RESOLVED_APPROVED,
                resolution_mode=AiReviewResolutionModeEnum.PICK_ONE_PRIMARY,
                dispatch_version=0,
                details={"previous_status": "pending"},
            ),
        )
        client = _create_client(repo=AsyncMock(), principal=_delegate_principal())
        response = client.post(
            "/api/ai-reviews/rev-1/decision",
            json=_decision_envelope(decision="approve", rationale="LGTM"),
        )
        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["error_code"] is None
        assert body["details"]["status"] == "resolved_approved"
        assert body["details"]["resolution_mode"] == "pick_one_primary"
        assert body["details"]["dispatch_version"] == 0

    def test_idempotent_retry_returns_200_with_classifier(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``decision_already_recorded`` -> 200 with non-null error_code.

        Given the service returns the idempotent-retry envelope,
        When the route wraps it,
        Then HTTP 200 (not 409) and success=True so bridges treat the
        retry as a no-op rather than a hard error.
        """
        _stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code="decision_already_recorded",
                message="Decision already recorded; idempotent retry.",
                status=AiReviewStatusEnum.RESOLVED_APPROVED,
                resolution_mode=AiReviewResolutionModeEnum.PICK_ONE_PRIMARY,
                dispatch_version=0,
                details={"decision": "approve"},
            ),
        )
        client = _create_client(repo=AsyncMock(), principal=_delegate_principal())
        response = client.post(
            "/api/ai-reviews/rev-1/decision",
            json=_decision_envelope(decision="approve"),
        )
        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["error_code"] == "decision_already_recorded"

    @pytest.mark.parametrize(
        ("error_code", "expected_status"),
        [
            ("review_not_found", 404),
            ("not_authorized", 403),
            ("review_already_resolved_by_peer", 409),
            ("review_id_expired", 410),
            ("review_state_race", 503),
        ],
    )
    def test_hard_error_codes_map_to_http_statuses(
        self,
        monkeypatch: pytest.MonkeyPatch,
        error_code: str,
        expected_status: int,
    ) -> None:
        """Each hard-error classifier yields its HTTP code.

        Given the service returns one of the hard-error codes,
        When the route translates the envelope,
        Then the matching HTTP status is raised and the body's
        ``detail`` carries the same envelope shape MCP clients see.
        """
        _stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code=error_code,
                message=f"sentinel: {error_code}",
                status=None,
                resolution_mode=None,
                dispatch_version=None,
                details={},
            ),
        )
        client = _create_client(repo=AsyncMock(), principal=_delegate_principal())
        response = client.post(
            "/api/ai-reviews/rev-1/decision",
            json=_decision_envelope(decision="reject"),
        )
        assert response.status_code == expected_status
        envelope = response.json()["detail"]
        assert envelope["success"] is False
        assert envelope["error_code"] == error_code

    def test_invalid_decision_short_circuits_with_422(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unknown decision string -> 422 WITHOUT calling submit_decision.

        Given a POST body with decision='maybe',
        When the route validates against AiReviewDecisionEnum,
        Then 422 is raised and submit_decision is never awaited; the
        envelope carries ``error_code='invalid_decision'`` AND the
        offending value on details (sanitised on the way out).
        """
        stub = _stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code=None,
                message="should never be called",
                status=AiReviewStatusEnum.RESOLVED_APPROVED,
                resolution_mode=AiReviewResolutionModeEnum.PICK_ONE_PRIMARY,
                dispatch_version=0,
                details={},
            ),
        )
        client = _create_client(repo=AsyncMock(), principal=_delegate_principal())
        response = client.post(
            "/api/ai-reviews/rev-1/decision",
            json=_decision_envelope(decision="maybe"),
        )
        assert response.status_code == 422
        envelope = response.json()["detail"]
        assert envelope["success"] is False
        assert envelope["error_code"] == "invalid_decision"
        stub.assert_not_called()

    def test_flat_body_without_envelope_is_rejected(self) -> None:
        """Legacy flat ``{decision, rationale}`` body -> 422.

        Locks in the envelope contract: every mutating REST endpoint
        (orders, brackets, trailing stops, backtests, AI review
        decisions) MUST receive a ``PayloadRequest`` envelope with
        provenance fields and a nested ``payload``. A flat body lacks
        the discriminator ``type`` and the ``payload`` wrapper, so
        :class:`AiReviewDecisionCommand` validation rejects it before
        the route body runs.
        """
        client = _create_client(repo=AsyncMock(), principal=_delegate_principal())
        response = client.post(
            "/api/ai-reviews/rev-1/decision",
            json={"decision": "approve", "rationale": "LGTM"},
        )
        assert response.status_code == 422

    def test_permission_denied_for_role_without_create_orders(self) -> None:
        """VIEWER role -> 403 from require_permission(CREATE_ORDERS).

        Given a VIEWER principal (lacks CREATE_ORDERS),
        When the route is invoked,
        Then require_permission raises 403 BEFORE the route body runs.
        """
        client = _create_client(repo=AsyncMock(), principal=_viewer_principal())
        response = client.post(
            "/api/ai-reviews/rev-1/decision",
            json=_decision_envelope(decision="approve"),
        )
        assert response.status_code == 403


class TestListPendingRoute:
    """``GET /api/ai-reviews/pending`` HTTP-surface coverage."""

    def _make_summary(
        self,
        *,
        public_id: str,
        fanout_after: datetime,
        wallet_public_id: str = "wal-1",
        instrument: str | None = None,
        signal_envelope: dict[str, object] | None = None,
    ) -> PendingReviewSummary:
        return cast(
            PendingReviewSummary,
            {
                "public_id": public_id,
                "selected_delegate_public_id": "del-1",
                "wallet_public_id": wallet_public_id,
                "dispatch_version": 0,
                "status": "pending",
                "deadline": fanout_after + timedelta(seconds=60),
                "fanout_after": fanout_after,
                "instrument": instrument,
                "signal_envelope": signal_envelope,
            },
        )

    def test_returns_pending_reviews_for_delegate(self) -> None:
        """Delegate principal -> 200 with serialised pending rows.

        Given a repo returning two pending reviews for the delegate,
        When GET /api/ai-reviews/pending is called,
        Then HTTP 200 + body items[] carries both rows with the
        repository's PendingReviewSummary shape mapped 1:1 to the
        REST schema.
        """
        now = datetime(2026, 4, 26, 12, 0, 0, tzinfo=UTC)
        repo = AsyncMock()
        repo.list_pending_reviews_for_delegate = AsyncMock(
            return_value=[
                self._make_summary(public_id="rev-a", fanout_after=now),
                self._make_summary(public_id="rev-b", fanout_after=now + timedelta(seconds=1)),
            ]
        )
        client = _create_client(repo=repo, principal=_delegate_principal())
        response = client.get("/api/ai-reviews/pending")
        assert response.status_code == 200
        body = response.json()
        assert body["count"] == 2
        assert body["items"][0]["review_public_id"] == "rev-a"
        assert body["items"][1]["review_public_id"] == "rev-b"
        repo.list_pending_reviews_for_delegate.assert_awaited_once()

    def test_returns_empty_list_when_no_pending(self) -> None:
        """No matching rows -> 200 + empty items array.

        Given the repo returns an empty list,
        When GET /api/ai-reviews/pending is called,
        Then the response is 200 with count=0 and items=[].
        """
        repo = AsyncMock()
        repo.list_pending_reviews_for_delegate = AsyncMock(return_value=[])
        client = _create_client(repo=repo, principal=_delegate_principal())
        response = client.get("/api/ai-reviews/pending")
        assert response.status_code == 200
        body = response.json()
        assert body["count"] == 0
        assert body["items"] == []

    def test_non_delegate_principal_returns_422(self) -> None:
        """OPERATOR has READ_SIGNALS but no delegate_public_id -> 422.

        Given an OPERATOR principal (delegate_public_id is None),
        When GET /api/ai-reviews/pending is called,
        Then the route raises 422 with error_code='not_a_delegate'
        because the endpoint is keyed by the delegate identity.
        """
        client = _create_client(repo=AsyncMock(), principal=_operator_principal())
        response = client.get("/api/ai-reviews/pending")
        assert response.status_code == 422
        envelope = response.json()["detail"]
        assert envelope["error_code"] == "not_a_delegate"

    def test_limit_query_param_clamped(self) -> None:
        """``limit`` outside [1, 500] is rejected by FastAPI validation.

        Given a query string with limit=0,
        When GET /api/ai-reviews/pending is called,
        Then FastAPI's validator returns 422 (not a route-body error).
        """
        client = _create_client(repo=AsyncMock(), principal=_delegate_principal())
        response = client.get("/api/ai-reviews/pending?limit=0")
        assert response.status_code == 422

    def test_wallet_public_id_filter_threaded_through_to_repo(self) -> None:
        """``?wallet_public_id=`` query param reaches the repo predicate.

        The bridge passes wallet_public_id to scope the catch-up
        snapshot. The route MUST forward the filter so the repo's
        WHERE clause adds ``ai_reviews.wallet_public_id ==
        wallet_public_id``.

        Given a repo whose mock records the kwargs it received,
        When GET /api/ai-reviews/pending?wallet_public_id=wal-9 runs,
        Then the repo call carries ``wallet_public_id='wal-9'``.
        """
        repo = AsyncMock()
        repo.list_pending_reviews_for_delegate = AsyncMock(return_value=[])
        client = _create_client(repo=repo, principal=_delegate_principal())
        response = client.get("/api/ai-reviews/pending?wallet_public_id=wal-9")
        assert response.status_code == 200
        repo.list_pending_reviews_for_delegate.assert_awaited_once()
        kwargs = repo.list_pending_reviews_for_delegate.await_args.kwargs
        assert kwargs["wallet_public_id"] == "wal-9"
        assert kwargs["selected_delegate_public_id"] == "del-1"
