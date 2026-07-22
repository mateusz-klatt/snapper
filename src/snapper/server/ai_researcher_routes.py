"""AI researcher provisioning route.

The endpoint gives operators a dedicated path for minting research-only
automation principals. Researcher accounts remain separate from AI
delegate lifecycle and consult admission state.
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

from snapper.api.schemas.ai_researchers import ResearcherCreatedResponse
from snapper.api.schemas.ai_researchers import ResearcherCreateRequest
from snapper.application.ai_researchers.service import InvalidResearcherOwnerPrincipalError
from snapper.application.ai_researchers.service import ResearcherProliferationError
from snapper.application.ai_researchers.service import ResearcherService
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import PermissionScopeError
from snapper.auth.tokens import get_token_manager
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.ai_delegate_routes import require_ai_integration_enabled
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

router = APIRouter(prefix="/ai-researchers", tags=["ai-researchers"])

_REST_STREAM = "rest.control"
_INVALID_PRINCIPAL = (
    "AI researcher provisioning requires an authenticated principal with a "
    "populated user_public_id. Re-login to obtain a current token."
)


def _build_service(repository: Repository) -> ResearcherService:
    """Build the researcher service for one route invocation.

    Args:
        repository: Request-scoped repository dependency.

    Returns:
        Researcher service using the shared token manager.
    """
    return ResearcherService(repository=repository, token_manager=get_token_manager())


@router.post("", openapi_extra=openapi_schema(ResearcherCreateRequest))
async def create_researcher(
    request: Request,
    body: Annotated[ResearcherCreateRequest, Depends(json_body(ResearcherCreateRequest))],
    owner: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.MANAGE_AI_INTEGRATION)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    _flag: Annotated[None, Depends(require_ai_integration_enabled)] = None,
) -> ResearcherCreatedResponse:
    """Provision a research-only principal and return its token once.

    Args:
        request: FastAPI request carrying the REST provenance tracker.
        body: Validated researcher creation request envelope.
        owner: Authenticated operator or administrator creating the principal.
        repo: Repository dependency used for the atomic insert.
        _csrf: CSRF validation dependency.
        _flag: AI integration feature-gate dependency.

    Returns:
        Standard response envelope carrying the researcher and access token.

    Raises:
        HTTPException: If the owner identity is invalid, the requested token
            scope exceeds the researcher role, or the owner reached the
            researcher cap.
    """
    service = _build_service(repo)
    try:
        payload = await service.create_researcher(owner=owner, body=body.payload)
    except InvalidResearcherOwnerPrincipalError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=_INVALID_PRINCIPAL,
        ) from exc
    except PermissionScopeError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc
    except ResearcherProliferationError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    tracker: SequenceTracker = request.app.state.rest_tracker
    return ResearcherCreatedResponse(
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        payload=payload,
    )
