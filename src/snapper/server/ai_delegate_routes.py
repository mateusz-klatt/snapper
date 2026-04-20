"""AI delegate CRUD routes (plan §4 Day 4b).

Mounted at ``/api/ai-delegates``. Only operators (OPERATOR /
ADMIN) can create + manage delegates; the plan §3 role hierarchy
gates access via ``require_role``. Delegates themselves are
AI_DELEGATE users and cannot manage other delegates — the
``role_hierarchy`` dict in ``require_role`` puts AI_DELEGATE
below VIEWER.

Routes:

    - ``POST /api/ai-delegates`` — atomic create returning a
      one-shot access+refresh pair (plan §4 Day 4 item 2).
    - ``GET /api/ai-delegates`` — list the caller's delegates.
    - ``GET /api/ai-delegates/{id}`` — single-delegate detail.
    - ``PATCH /api/ai-delegates/{id}`` — SCD2-close+insert new
      caps. Username/label are immutable post-mint (would
      invalidate live tokens without rotation).
    - ``POST /api/ai-delegates/{id}/deactivate`` — reuse
      ``UserService.deactivate_user`` so the shared kill-switch
      bus event + token revocation logic kicks in.
"""

from datetime import UTC
from datetime import datetime
from typing import Annotated
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from fastapi.responses import JSONResponse

from snapper.api.schemas.ai_delegates import DelegateCapsUpdateRequest
from snapper.api.schemas.ai_delegates import DelegateCreatedResponse
from snapper.api.schemas.ai_delegates import DelegateCreateRequest
from snapper.api.schemas.ai_delegates import DelegateDeactivateRequest
from snapper.api.schemas.ai_delegates import DelegateListResponse
from snapper.api.schemas.ai_delegates import DelegateResponse
from snapper.application.ai_delegates.service import DelegateLabelConflictError
from snapper.application.ai_delegates.service import DelegateNotFoundError
from snapper.application.ai_delegates.service import DelegateOperatorBindingError
from snapper.application.ai_delegates.service import DelegateProliferationError
from snapper.application.ai_delegates.service import DelegateService
from snapper.application.ai_delegates.service import InvalidOwnerPrincipalError
from snapper.auth.dependencies import require_role
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import get_token_manager
from snapper.auth.user_service import get_user_service
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema
from snapper.server.json_body import optional_json_body

router = APIRouter(prefix="/ai-delegates", tags=["ai-delegates"])

_REST_STREAM = "rest.control"
_DELEGATE_NOT_FOUND = "Delegate not found"
_INVALID_PRINCIPAL = (
    "AI delegate management requires an authenticated principal with a "
    "populated user_public_id. Re-login to obtain a current token."
)
_AI_INTEGRATION_FLAG_KEY = "ai_integration_enabled"


class AiIntegrationDisabledError(Exception):
    """Raised by :func:`require_ai_integration_enabled` when the flag is off.

    Caught by :func:`ai_integration_disabled_handler` (registered on
    the FastAPI app in :mod:`snapper.server.app`) and translated
    into a 503 :class:`JSONResponse` whose body matches the MCP
    sub-app's :class:`FeatureFlagMiddleware` envelope EXACTLY
    (``{"error_code": "feature_disabled", "detail": "..."}``). Day
    5d-B1 Rfollowup: gpt-5.4 review caught that the previous
    ``HTTPException`` path emitted FastAPI's default
    ``{"detail": ...}`` wrapper, breaking plan §3.12 parity.
    """


def require_ai_integration_enabled(request: Request) -> None:
    """Reject the request when the AI integration feature flag is off.

    Day 5c review MAJOR closure: plan §3.12 requires
    ``/api/ai-delegates/*`` to share the same feature gate as
    ``/api/mcp``. Without this dependency, operators could mint
    delegates + tokens while the feature is disabled, leaking a
    management surface that the rest of Phase A refuses to serve.

    Args:
        request: Active FastAPI request; settings service is pulled
            from ``app.state`` so toggling the DB setting takes
            effect without a restart.

    Raises:
        AiIntegrationDisabledError: when the flag is off or the
            settings service has not been initialised yet
            (fail-closed during startup). Translated to a 503
            JSONResponse with the MCP-compatible
            ``{"error_code": "feature_disabled"}`` envelope by the
            app-level exception handler.
    """
    settings_service = getattr(request.app.state, "settings_service", None)
    enabled = bool(
        settings_service.get_setting(_AI_INTEGRATION_FLAG_KEY, default=False)
        if settings_service is not None
        else False
    )
    if enabled:
        return
    raise AiIntegrationDisabledError(
        "AI integration is disabled. Enable the "
        f"'{_AI_INTEGRATION_FLAG_KEY}' setting to activate the delegate surface."
    )


def ai_integration_disabled_handler(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    """Translate :class:`AiIntegrationDisabledError` to MCP-parity 503.

    The body shape is identical to
    :class:`~snapper.mcp.server.FeatureFlagMiddleware`'s 503 so a
    frontend / CLI branching on ``error_code`` sees one payload
    regardless of which surface returned the 503 (plan §3.12).

    Args:
        _request: The inbound FastAPI request (unused — the
            envelope is constant across call sites).
        exc: The caught exception. Annotated as :class:`Exception`
            to match Starlette's exception-handler signature
            (Starlette calls through a shared dispatcher that
            types every handler as ``Callable[[Request, Exception],
            Response]``). In practice the app-level registration
            only routes :class:`AiIntegrationDisabledError` here.

    Returns:
        A 503 :class:`JSONResponse` with the vendor-neutral
        ``error_code`` envelope.
    """
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "error_code": "feature_disabled",
            "detail": str(exc),
        },
    )


def _build_service(repository: Repository) -> DelegateService:
    """Tiny factory so tests can monkey-patch without touching the route."""
    return DelegateService(repository=repository, token_manager=get_token_manager())


@router.post("", openapi_extra=openapi_schema(DelegateCreateRequest))
async def create_delegate(
    request: Request,
    body: Annotated[DelegateCreateRequest, Depends(json_body(DelegateCreateRequest))],
    owner: Annotated[AuthPrincipal, Depends(require_role(UserRole.OPERATOR))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    _flag: Annotated[None, Depends(require_ai_integration_enabled)] = None,
) -> DelegateCreatedResponse:
    """Atomically create a new AI delegate + mint its token pair.

    The response carries the access + refresh JWT pair ONCE. The
    operator must copy the tokens into their MCP client config
    within the HTTP session; Snapper will not re-serve them on
    the list or detail endpoints.

    Args:
        request: FastAPI request (for the REST tracker).
        body: :class:`DelegateCreateRequest` with label + optional
            caps.
        owner: Authenticated operator (OPERATOR or ADMIN via
            ``require_role``).
        repo: Repository dep — the service opens a single
            transactional scope underneath.
        _csrf: CSRF validation dep. Required because create is a
            cookie-flow-admitting endpoint; pure-Bearer clients
            bypass via the middleware rule in plan §3.7.

    Returns:
        201-shaped :class:`DelegateCreatedResponse`.

    Raises:
        HTTPException: 422 when the caller-supplied
            ``operator_public_id`` is outside the caller's claim
            set OR no selection was given and the caller has no
            primary operator. 409 when a unique username can't be
            derived from the label (pathological input) OR the
            owner already holds the per-owner delegate cap.
    """
    service = _build_service(repo)
    try:
        payload = await service.create_delegate(owner=owner, body=body.payload)
    except InvalidOwnerPrincipalError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=_INVALID_PRINCIPAL
        ) from exc
    except DelegateOperatorBindingError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc
    except DelegateProliferationError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    except DelegateLabelConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    tracker: SequenceTracker = request.app.state.rest_tracker
    return DelegateCreatedResponse(
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        payload=payload,
    )


@router.get("")
async def list_delegates(
    request: Request,
    owner: Annotated[AuthPrincipal, Depends(require_role(UserRole.OPERATOR))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    _flag: Annotated[None, Depends(require_ai_integration_enabled)] = None,
) -> DelegateListResponse:
    """Return every SCD2-active delegate the caller owns.

    Deactivated delegates drop out of the list; the frontend
    list view tracks the active set.
    """
    service = _build_service(repo)
    try:
        delegates = await service.list_delegates(owner_public_id=owner.user_public_id)
    except InvalidOwnerPrincipalError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=_INVALID_PRINCIPAL
        ) from exc
    tracker: SequenceTracker = request.app.state.rest_tracker
    return DelegateListResponse(
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        payload=delegates,
        count=len(delegates),
    )


@router.get("/{delegate_public_id}", responses={404: {"description": "Delegate not found"}})
async def get_delegate(
    request: Request,
    delegate_public_id: str,
    owner: Annotated[AuthPrincipal, Depends(require_role(UserRole.OPERATOR))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    _flag: Annotated[None, Depends(require_ai_integration_enabled)] = None,
) -> DelegateResponse:
    """Fetch a single delegate owned by the caller.

    Returns 404 both for "no such delegate" AND "not owned by
    you" so cross-tenant existence isn't leaked via error codes.
    """
    service = _build_service(repo)
    try:
        delegate = await service.get_delegate(
            public_id=delegate_public_id,
            owner_public_id=owner.user_public_id,
        )
    except InvalidOwnerPrincipalError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=_INVALID_PRINCIPAL
        ) from exc
    except DelegateNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=_DELEGATE_NOT_FOUND
        ) from exc
    tracker: SequenceTracker = request.app.state.rest_tracker
    return DelegateResponse(
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        payload=delegate,
    )


@router.patch(
    "/{delegate_public_id}",
    openapi_extra=openapi_schema(DelegateCapsUpdateRequest),
    responses={404: {"description": _DELEGATE_NOT_FOUND}},
)
async def update_delegate_caps(
    request: Request,
    delegate_public_id: str,
    body: Annotated[DelegateCapsUpdateRequest, Depends(json_body(DelegateCapsUpdateRequest))],
    owner: Annotated[AuthPrincipal, Depends(require_role(UserRole.OPERATOR))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    _flag: Annotated[None, Depends(require_ai_integration_enabled)] = None,
) -> DelegateResponse:
    """SCD2 close+insert new caps for a delegate the caller owns."""
    service = _build_service(repo)
    try:
        delegate = await service.update_caps(
            public_id=delegate_public_id,
            owner_public_id=owner.user_public_id,
            body=body.payload,
        )
    except InvalidOwnerPrincipalError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=_INVALID_PRINCIPAL
        ) from exc
    except DelegateNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=_DELEGATE_NOT_FOUND
        ) from exc
    tracker: SequenceTracker = request.app.state.rest_tracker
    return DelegateResponse(
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        payload=delegate,
    )


@router.post(
    "/{delegate_public_id}/deactivate",
    openapi_extra=openapi_schema(DelegateDeactivateRequest, required=False),
    responses={404: {"description": _DELEGATE_NOT_FOUND}},
)
async def deactivate_delegate(
    request: Request,
    delegate_public_id: str,
    body: Annotated[
        DelegateDeactivateRequest | None,
        Depends(optional_json_body(DelegateDeactivateRequest)),
    ],
    owner: Annotated[AuthPrincipal, Depends(require_role(UserRole.OPERATOR))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    _flag: Annotated[None, Depends(require_ai_integration_enabled)] = None,
) -> DelegateResponse:
    """Deactivate a delegate via the shared kill-switch flow.

    The heavy lifting (SCD2-close ``is_active=True`` → insert
    ``is_active=False``, revoke ``user_active_tokens``, publish
    ``admin.user_deactivated``) lives in
    :meth:`UserService.deactivate_user` — so the same bus-event
    fanout + verify-cache eviction that the admin deactivation
    path uses applies here verbatim. This route only enforces
    "caller owns this delegate" and converts the service's
    bool return into the HTTP shape.
    """
    service = _build_service(repo)
    try:
        current = await service.get_delegate(
            public_id=delegate_public_id,
            owner_public_id=owner.user_public_id,
        )
    except InvalidOwnerPrincipalError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=_INVALID_PRINCIPAL
        ) from exc
    except DelegateNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=_DELEGATE_NOT_FOUND
        ) from exc
    reason = body.payload.reason if body is not None else None
    user_service = get_user_service()
    success = await user_service.deactivate_user(user_public_id=delegate_public_id, reason=reason)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=_DELEGATE_NOT_FOUND)
    tracker: SequenceTracker = request.app.state.rest_tracker
    return DelegateResponse(
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        payload=current.model_copy(update={"is_active": False}),
    )


__all__ = ["router"]
