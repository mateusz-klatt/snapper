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

import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.application.ai_review.service import AiReviewDecisionResult
from snapper.application.ai_review.service import AiReviewService
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.auth.tokens import TokenManager
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
    """OPERATOR principal — holds CREATE_ORDERS + READ_SIGNALS, never the decision write."""
    return AuthPrincipal(
        username="operator-1",
        role=UserRole.OPERATOR,
        user_public_id="op-user-1",
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )


def _viewer_principal() -> AuthPrincipal:
    """VIEWER principal — lacks every decision capability so the route 403s."""
    return AuthPrincipal(
        username="viewer-1",
        role=UserRole.VIEWER,
        user_public_id="viewer-user-1",
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )


def _reviewer_principal() -> AuthPrincipal:
    """AI_REVIEWER principal holding the decision permission without delegate identity."""
    return AuthPrincipal(
        username="reviewer-1",
        role=UserRole.AI_REVIEWER,
        user_public_id="reviewer-user-1",
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )


def _legacy_v1_delegate_principal() -> AuthPrincipal:
    """AI_DELEGATE principal on a historical scope-version-one token grant.

    Carries only ``create:orders`` because the dedicated decision permission
    did not exist when the token was minted. The shared capability projector
    admits it through its narrow compatibility branch, which is what keeps the
    REST fallback usable for the same legacy tokens MCP still admits.
    """
    return AuthPrincipal(
        username="legacy-delegate-1",
        role=UserRole.AI_DELEGATE,
        user_public_id="legacy-user-1",
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
        delegate_public_id="legacy-del-1",
        permissions=[Permission.CREATE_ORDERS.value],
        permission_scope_version=1,
    )


def _principal_from_pre_versioning_jwt(
    *,
    username: str,
    role: UserRole,
    user_public_id: str,
    permissions: list[str],
    delegate_public_id: str | None = None,
) -> AuthPrincipal:
    """Build a principal from a signed JWT that genuinely OMITS the scope claim.

    The distinction matters: a pre-versioning token does not carry
    ``permission_scope_version`` set to null, it carries no such key at all.
    Encoding a payload without the key and decoding it through
    :meth:`TokenManager.verify_token` exercises the real claim-extraction path
    — ``jwt.decode`` then :meth:`TokenClaims.model_validate_json` — so the
    ``None`` reaching the projector is produced by the schema default for an
    absent claim rather than assigned by the test.

    The principal is then assembled exactly as :func:`get_current_user`
    assembles it from decoded claims. The full request-path chain is not used
    because it runs :meth:`TokenManager.verify_token_with_db`, which needs a
    live ``user_active_tokens`` inventory; these route tests override
    ``require_authentication`` and drive an ``AsyncMock`` repository, so
    decoding is the most faithful mechanism reachable in this harness.

    Args:
        username: Username claim for the minted token.
        role: Role claim for the minted token.
        user_public_id: Stable user identifier claim.
        permissions: Explicit legacy permission strings carried by the token.
        delegate_public_id: Delegate lifecycle identity resolved by the
            repository lookup inside :func:`get_current_user`.

    Returns:
        The principal a pre-versioning token of this shape produces.
    """
    token_manager = TokenManager()
    now = datetime.now(UTC)
    payload: dict[str, str | int | list[str]] = {
        "sub": user_public_id,
        "username": username,
        "role": role.value,
        "permissions": permissions,
        "sid": f"{username}-session",
        "exp": int((now + timedelta(hours=1)).timestamp()),
        "iat": int(now.timestamp()),
        "jti": f"{username}-jti",
        "user_public_id": user_public_id,
        "operator_public_ids": ["op-1"],
        "primary_operator_public_id": "op-1",
    }
    assert "permission_scope_version" not in payload
    token = jwt.encode(
        payload,
        token_manager.settings.auth_secret_key,
        algorithm=token_manager.settings.auth_algorithm,
    )
    claims = token_manager.verify_token(token)
    assert claims is not None
    assert claims.permission_scope_version is None
    return AuthPrincipal(
        username=claims.username,
        role=claims.role,
        user_public_id=claims.user_public_id,
        operator_public_ids=claims.operator_public_ids,
        primary_operator_public_id=claims.primary_operator_public_id,
        active_wallet_public_id=claims.active_wallet_public_id,
        permissions=claims.permissions,
        permission_scope_version=claims.permission_scope_version,
        delegate_public_id=delegate_public_id,
    )


def _pre_versioning_delegate_principal() -> AuthPrincipal:
    """AI_DELEGATE principal on the exact pre-versioning production token shape."""
    return _principal_from_pre_versioning_jwt(
        username="pre-versioning-delegate",
        role=UserRole.AI_DELEGATE,
        user_public_id="pre-versioning-user-1",
        permissions=[
            Permission.CANCEL_ORDERS.value,
            Permission.CREATE_ORDERS.value,
            Permission.MANAGE_POSITIONS.value,
            Permission.READ_BACKTESTS.value,
            Permission.READ_MARKET_DATA.value,
            Permission.READ_ORDERS.value,
            Permission.READ_POSITIONS.value,
            Permission.READ_SIGNALS.value,
            Permission.READ_STRATEGIES.value,
            Permission.READ_SYSTEM_STATUS.value,
        ],
        delegate_public_id="pre-versioning-del-1",
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

    def test_permission_denied_for_role_without_decision_permission(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """VIEWER role -> 403 from require_ai_review_decision_access.

        Given a VIEWER principal (projects no decision capability),
        When the route is invoked,
        Then the dependency raises 403 BEFORE the route body runs.
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
        client = _create_client(repo=AsyncMock(), principal=_viewer_principal())
        response = client.post(
            "/api/ai-reviews/rev-1/decision",
            json=_decision_envelope(decision="approve"),
        )
        assert response.status_code == 403
        stub.assert_not_called()

    def test_order_creator_without_decision_permission_is_denied_at_the_route_gate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An order-creating principal without the decision write never reaches the service.

        Given: An OPERATOR principal holding create:orders but not
            submit:ai_review_decision — the shape the historical CREATE_ORDERS
            gate admitted, leaving the rejection to the service's
            delegate-identity check.
        When: It submits an approval through the REST decision route.
        Then: The route dependency returns HTTP 403 and submit_decision is
            never awaited, pinning the denial at the route layer.
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
        client = _create_client(repo=AsyncMock(), principal=_operator_principal())

        response = client.post(
            "/api/ai-reviews/rev-operator/decision",
            json=_decision_envelope(decision="approve"),
        )

        assert response.status_code == 403
        assert response.json()["detail"] == (
            f"Permission '{Permission.SUBMIT_AI_REVIEW_DECISION.value}' required"
        )
        stub.assert_not_called()

    def test_legacy_v1_delegate_token_keeps_rest_decision_access(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A historical v1 delegate token still clears the REST decision gate.

        Given: An AI_DELEGATE principal on a permission-scope-version-one grant
            carrying only create:orders, because the dedicated decision
            permission postdates the token.
        When: It submits an approval through the REST decision route.
        Then: The shared capability projector admits it and the service records
            the decision, mirroring the MCP call gate exactly — the REST route
            is the bridge's HTTP fallback, so the same legacy token must work
            on both transports.
        """
        stub = _stub_submit_decision(
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
        client = _create_client(repo=AsyncMock(), principal=_legacy_v1_delegate_principal())

        response = client.post(
            "/api/ai-reviews/rev-legacy/decision",
            json=_decision_envelope(decision="approve"),
        )

        assert response.status_code == 200
        assert response.json()["success"] is True
        stub.assert_awaited_once()

    def test_pre_versioning_delegate_token_keeps_rest_decision_access(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The production pre-versioning delegate token clears the REST gate.

        Given: An AI_DELEGATE principal decoded from a signed JWT that OMITS
            the permission-scope-version claim entirely — the shape of the
            production delegate token, which predates scope versioning — whose
            explicit legacy permission list carries create:orders but not the
            dedicated decision permission.
        When: It submits an approval through the REST decision route.
        Then: The shared projector's legacy window admits it and the service
            records the decision, so the absent-version compatibility that
            motivated the projector widening is pinned on this transport and
            not only at the projector's own unit boundary.
        """
        stub = _stub_submit_decision(
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
        principal = _pre_versioning_delegate_principal()
        client = _create_client(repo=AsyncMock(), principal=principal)

        response = client.post(
            "/api/ai-reviews/rev-pre-versioning/decision",
            json=_decision_envelope(decision="approve"),
        )

        assert principal.permission_scope_version is None
        assert Permission.SUBMIT_AI_REVIEW_DECISION.value not in (principal.permissions or [])
        assert response.status_code == 200
        assert response.json()["success"] is True
        stub.assert_awaited_once()

    @pytest.mark.parametrize(
        ("role", "username", "user_public_id"),
        [
            pytest.param(UserRole.OPERATOR, "pre-versioning-operator", "pv-op-1", id="operator"),
            pytest.param(UserRole.ADMIN, "pre-versioning-admin", "pv-admin-1", id="admin"),
        ],
    )
    def test_pre_versioning_create_only_token_is_denied_for_non_legacy_roles(
        self,
        monkeypatch: pytest.MonkeyPatch,
        role: UserRole,
        username: str,
        user_public_id: str,
    ) -> None:
        """An absent version claim does not open the gate for every create-only token.

        Given: A create-only principal decoded from a signed JWT with no
            permission-scope-version claim, on a role outside the legacy
            window — OPERATOR, whose ceiling never grants the decision
            permission, and ADMIN, which is excluded by the branch's
            no-user-administration conjunct despite holding it.
        When: Each submits an approval through the REST decision route.
        Then: Both are refused at the route gate and the service is never
            awaited, bounding the absent-version widening to exactly the
            delegate and reviewer ceilings it was introduced for.
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
        principal = _principal_from_pre_versioning_jwt(
            username=username,
            role=role,
            user_public_id=user_public_id,
            permissions=[Permission.CREATE_ORDERS.value],
        )
        client = _create_client(repo=AsyncMock(), principal=principal)

        response = client.post(
            f"/api/ai-reviews/rev-{username}/decision",
            json=_decision_envelope(decision="approve"),
        )

        assert principal.permission_scope_version is None
        assert response.status_code == 403
        assert response.json()["detail"] == (
            f"Permission '{Permission.SUBMIT_AI_REVIEW_DECISION.value}' required"
        )
        stub.assert_not_called()

    def test_ai_reviewer_passes_the_route_gate_and_is_denied_by_the_service(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A decision-capable principal without delegate identity 403s one layer deeper.

        Given: An AI_REVIEWER principal holding submit:ai_review_decision but
            no delegate lifecycle identity.
        When: It submits an approval through the REST decision route.
        Then: The route permission gate admits it, the service is awaited, and
            the service's ``not_authorized`` envelope maps to HTTP 403 —
            proving the deeper identity check still owns that denial.
        """
        stub = _stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code="not_authorized",
                message="Caller is not registered as an AI delegate.",
                status=None,
                resolution_mode=None,
                dispatch_version=None,
                details={},
            ),
        )
        client = _create_client(repo=AsyncMock(), principal=_reviewer_principal())

        response = client.post(
            "/api/ai-reviews/rev-reviewer/decision",
            json=_decision_envelope(decision="approve"),
        )

        assert response.status_code == 403
        assert response.json()["detail"]["error_code"] == "not_authorized"
        stub.assert_awaited_once()


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


def _admin_principal() -> AuthPrincipal:
    """ADMIN principal — above the OPERATOR gate on ``GET /api/ai-reviews``."""
    return AuthPrincipal(
        username="admin-1",
        role=UserRole.ADMIN,
        user_public_id="admin-user-1",
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )


def _admin_review_row(**overrides: Any) -> dict[str, Any]:
    """Build a full ``AiReviewRow``-shaped dict for the list route mock."""
    now = datetime(2026, 7, 8, 12, 0, 0, tzinfo=UTC)
    base: dict[str, Any] = {
        "public_id": "rev-1",
        "session_id": "sess-1",
        "sequence_id": 1,
        "user_public_id": "user-1",
        "operator_public_id": "op-1",
        "wallet_public_id": "wal-1",
        "instrument_public_id": "inst-1",
        "strategy_public_id": "strat-1",
        "selected_delegate_public_id": "del-1",
        "responding_delegate_public_id": "del-1",
        "resolution_mode": "pick_one_primary",
        "status": "resolved_approved",
        "signal_envelope": {"side": "buy", "thesis": "t"},
        "signal_snapshot_hash": "h",
        "instrument_metadata": {},
        "deadline": now + timedelta(seconds=60),
        "fanout_after": now + timedelta(seconds=30),
        "decision": "approve",
        "rationale": "looks good",
        "dispatch_version": 0,
        "counter_decremented_at": None,
        "created_at": now,
        "updated_at": now,
        "resolved_at": now,
    }
    base.update(overrides)
    return base


class TestListAiReviewsRoute:
    """``GET /api/ai-reviews`` permission-gated read-only observability surface."""

    def test_operator_sees_review_with_decision(self) -> None:
        """An operator gets the terminal decision + rationale + envelope."""
        repo = AsyncMock()
        repo.list_ai_reviews = AsyncMock(return_value=[_admin_review_row()])
        client = _create_client(repo=repo, principal=_operator_principal())
        response = client.get("/api/ai-reviews")
        assert response.status_code == 200
        body = response.json()
        assert body["count"] == 1
        item = body["items"][0]
        assert item["review_public_id"] == "rev-1"
        assert item["decision"] == "approve"
        assert item["rationale"] == "looks good"
        assert item["status"] == "resolved_approved"
        assert item["responding_delegate_public_id"] == "del-1"
        assert item["signal_envelope"] == {"side": "buy", "thesis": "t"}
        kwargs = repo.list_ai_reviews.await_args.kwargs
        assert kwargs["operator_public_ids"] == ["op-1"]

    def test_operator_is_scoped_to_its_operator_memberships(self) -> None:
        """A non-admin operator only sees its own operators' reviews (no cross-tenant read)."""
        repo = AsyncMock()
        repo.list_ai_reviews = AsyncMock(return_value=[])
        client = _create_client(repo=repo, principal=_operator_principal())
        response = client.get("/api/ai-reviews")
        assert response.status_code == 200
        kwargs = repo.list_ai_reviews.await_args.kwargs
        assert kwargs["operator_public_ids"] == ["op-1"]

    def test_admin_is_allowed_and_unscoped(self) -> None:
        """ADMIN is admitted and sees every operator's rows (no operator scoping)."""
        repo = AsyncMock()
        repo.list_ai_reviews = AsyncMock(return_value=[])
        client = _create_client(repo=repo, principal=_admin_principal())
        response = client.get("/api/ai-reviews")
        assert response.status_code == 200
        assert response.json()["count"] == 0
        kwargs = repo.list_ai_reviews.await_args.kwargs
        assert kwargs["operator_public_ids"] is None

    def test_viewer_is_allowed_with_membership_scope(self) -> None:
        """A read-only operator sees reviews inside its operator memberships.

        Given: A VIEWER principal with one operator membership,
        When: The principal reads the AI-review audit list,
        Then: The route returns 200 and forwards that membership as its scope.
        """
        repo = AsyncMock()
        repo.list_ai_reviews = AsyncMock(return_value=[])
        client = _create_client(repo=repo, principal=_viewer_principal())
        response = client.get("/api/ai-reviews")
        assert response.status_code == 200
        kwargs = repo.list_ai_reviews.await_args.kwargs
        assert kwargs["operator_public_ids"] == ["op-1"]

    def test_delegate_is_forbidden(self) -> None:
        """A principal without READ_AI_REVIEWS cannot read other books' reviews."""
        repo = AsyncMock()
        repo.list_ai_reviews = AsyncMock(return_value=[])
        client = _create_client(repo=repo, principal=_delegate_principal())
        response = client.get("/api/ai-reviews")
        assert response.status_code == 403
        repo.list_ai_reviews.assert_not_awaited()

    def test_query_filters_threaded_to_repo(self) -> None:
        """``status`` / ``wallet_public_id`` / ``strategy_public_id`` / ``limit`` forward."""
        repo = AsyncMock()
        repo.list_ai_reviews = AsyncMock(return_value=[])
        client = _create_client(repo=repo, principal=_operator_principal())
        response = client.get(
            "/api/ai-reviews?status=resolved_approved&wallet_public_id=wal-9"
            "&strategy_public_id=strat-9&limit=25"
        )
        assert response.status_code == 200
        repo.list_ai_reviews.assert_awaited_once()
        kwargs = repo.list_ai_reviews.await_args.kwargs
        assert kwargs["status"] == "resolved_approved"
        assert kwargs["wallet_public_id"] == "wal-9"
        assert kwargs["strategy_public_id"] == "strat-9"
        assert kwargs["limit"] == 25
        assert kwargs["operator_public_ids"] == ["op-1"]

    def test_limit_out_of_range_is_422(self) -> None:
        """``limit=0`` violates the ``ge=1`` bound -> 422 before the repo call."""
        repo = AsyncMock()
        repo.list_ai_reviews = AsyncMock(return_value=[])
        client = _create_client(repo=repo, principal=_operator_principal())
        response = client.get("/api/ai-reviews?limit=0")
        assert response.status_code == 422
