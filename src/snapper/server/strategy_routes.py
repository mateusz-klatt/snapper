"""REST API routes for strategy read-only status.

Provides a lightweight endpoint for viewing configured strategies
without requiring full process management permissions. This allows
viewers with READ_STRATEGIES permission to see strategy status.

Endpoints:
    - ``GET /strategies`` - List configured strategy processes.

The endpoint requires only READ_STRATEGIES permission (viewer+).
"""

import datetime as dt
from typing import Annotated
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request

from snapper.api.schemas.process import StrategyListResponse
from snapper.api.schemas.process import StrategyProcess
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.types import ProcessRoleEnum
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.strategies.factory import StrategyFactory

router = APIRouter(prefix="/strategies", tags=["strategies"])

_REST_STREAM = "rest.strategies"


def _resolve_strategy_class(tags: tuple[str, ...]) -> str | None:
    """Recover the exact StrategyFactory key from a strategy process's tags.

    Strategy processes are registered with tags
    ``("strategy", strategy_class.lower())`` (see ``create_strategy_process``),
    so the original registry key — the value the backtest create form needs to
    pre-select its strategy dropdown — is recovered by a case-insensitive match
    of the tags against the registry keys.

    Args:
        tags: The process config tags.

    Returns:
        The exact registered strategy_class key, or None when no tag matches.
    """
    by_lower = {key.casefold(): key for key in StrategyFactory.STRATEGY_CLASSES}
    for tag in tags:
        match = by_lower.get(tag.casefold())
        if match is not None:
            return match

    return None


@router.get("")
async def list_strategies(
    request: Request,
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_STRATEGIES))],
) -> StrategyListResponse:
    """List configured strategy processes with lightweight status.

    Returns only strategy-role processes with minimal fields
    (name, running, enabled, mode) for read-only views.

    Args:
        request: FastAPI request containing app state.
        _user: Authenticated user with READ_STRATEGIES permission.

    Returns:
        Strategy list with running status.
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    ts = dt.datetime.now(dt.UTC)
    factory: ProcessLauncherService = request.app.state.process_factory
    configs = await factory.get_process_configs()
    strategies = [
        StrategyProcess(
            name=config.name,
            running=config.name in factory.started_processes,
            enabled=config.enabled,
            mode=config.mode,
            strategy_class=_resolve_strategy_class(config.tags),
            session_id=sid,
            sequence_id=tracker.next_sequence(_REST_STREAM),
            public_id=str(uuid7()),
            timestamp=ts,
        )
        for config in configs
        if config.role is ProcessRoleEnum.STRATEGY
    ]
    return StrategyListResponse(
        payload=strategies,
        count=len(strategies),
        session_id=sid,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=str(uuid7()),
        timestamp=ts,
    )
