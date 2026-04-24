"""AI delegate schemas for the REST API.

This module defines request/response schemas for the
``/api/ai-delegates`` CRUD surface. Delegates are
class:`~snapper.auth.domain.roles.UserRole.AI_DELEGATE` users an
operator creates so an MCP-compatible client can authenticate to
Snapper with a scoped bearer token pair instead of the operator's
primary credentials.
Envelopes follow the standard Snapper pattern
(:class:`~snapper.api.schemas.base.PayloadRequest` /
class:`~snapper.api.schemas.base.PayloadResponse`) so provenance
fields land on the envelope and the domain body carries only the
caller's intent.
"""

from datetime import datetime
from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.core.json_types import JsonObject


class DelegateCapsBody(StrictBody):
    """Per-delegate trading safety caps.

    Every field is optional; a ``None`` cap means "inherit the
    Snapper-wide default" per the fallback policy.
    The underlying :class:`~snapper.data.models.UserTradingCaps`
    row is always written at create time (even for all-``None``
    caps) so the ``TradingCapsEnforcer.guard`` surface has
    a row to read + cap history is SCD2-preserved.

    Attributes:
        max_order_quantity_per_instrument: JSON ``{instrument: qty}``
            OR a scalar — passed through unchanged to the
            enforcer.
        max_open_orders: in-flight command cap.
        max_daily_notional_usd: rolling 24h USD-notional cap.
        max_cancels_per_minute: sliding-window cancel cap.
    """

    max_order_quantity_per_instrument: JsonObject | None = Field(
        None, description="JSON dict {instrument_public_id: qty} or null for unbounded"
    )
    max_open_orders: int | None = Field(
        None, ge=0, description="In-flight command cap (null = unbounded)"
    )
    max_daily_notional_usd: float | None = Field(
        None, ge=0.0, description="Rolling 24h USD notional cap (null = unbounded)"
    )
    max_cancels_per_minute: int | None = Field(
        None, ge=0, description="Sliding 60s cancel cap (null = unbounded)"
    )


class DelegateCreateBody(StrictBody):
    """Operator-supplied fields for creating a new AI delegate.

    The operator chooses a human-readable ``label`` that becomes
    the delegate's username (prefixed with ``ai-``). Caps are
    optional — every cap defaulting to the Snapper-wide fallback
     The operator also picks WHICH of their
    authenticated operators the delegate inherits membership
    on — the minted delegate's wallet-scope set equals the
    chosen operator's scope grants. The
    selection MUST sit inside the caller's authenticated operator
    set; omit to default to the caller's
    ``primary_operator_public_id``.

    Attributes:
        label: Non-empty human-readable tag. Normalised to
            ``ai-<label>`` as the username so the delegate is
            listable in the standard user table without an
            auxiliary display-name column.
        caps: Optional per-delegate trading safety caps.
        operator_public_id: Operator the delegate is bound to
            must be in the caller's claim set. ``None`` defers
            to the caller's primary operator so simple callers
            don't need to know their membership set.
        long_lived: When ``True`` the delegate receives a single
            long-lived access token (no refresh token), suitable
            for local MCP clients where the operator prefers a
            paste-once token over the rotating 15-min access +
            7-day refresh pair. Defaults to ``False`` so existing
            integrations see no behaviour change. Kill switch
            (``POST /api/ai-delegates/{id}/deactivate``) works
            identically on long-lived tokens.
    """

    label: str = Field(..., min_length=1, max_length=48, description="Delegate label")
    caps: DelegateCapsBody = Field(
        default_factory=DelegateCapsBody, description="Per-delegate trading caps"
    )
    operator_public_id: str | None = Field(
        None,
        description=(
            "Operator the delegate is bound to — must be in the caller's claim set. "
            "Null defers to the caller's primary operator."
        ),
    )
    long_lived: bool = Field(
        False,
        description=(
            "When true, issue a non-rotating long-lived access token (no refresh). "
            "Defaults to false — existing rotating behaviour."
        ),
    )


class DelegateCreateRequest(PayloadRequest[Literal["delegate_create_request"], DelegateCreateBody]):
    """Create-delegate request envelope."""

    type: Literal["delegate_create_request"] = "delegate_create_request"


class DelegateRead(StrictBody):
    """Public projection of a delegate used by list/detail reads.

    The ``access_token`` + ``refresh_token`` fields are NEVER set
    on list/detail responses — only the POST-create response
    includes them (once, in :class:`DelegateCreatedPayload`).
    Once issued, the tokens live only in the client (env var /
    keychain); Snapper never re-serves them.

    Attributes:
        public_id: Delegate user's UUID7 public identifier.
        username: Delegate's username (``ai-<label>`` shape).
        label: Human-readable label the operator supplied.
        created_by_user_public_id: Owner operator's public_id.
        created_at: Bus-time when the delegate was minted.
        is_active: Flipped to ``False`` on
            ``POST /api/ai-delegates/{id}/deactivate``.
        caps: Current trading caps (always populated).
        token_kind: ``"rotating"`` (default — 15-min access + 7-day
            refresh pair) or ``"long_lived"`` (single access token
            with ~10y expiry, no refresh token issued). Computed
            from the live ``user_active_tokens`` inventory: a
            delegate with any active refresh row is ``rotating``;
            otherwise ``long_lived``. Frontend renders a "PAT"
            badge when ``long_lived``.
    """

    public_id: str
    username: str
    label: str
    created_by_user_public_id: str
    created_at: datetime
    is_active: bool
    caps: DelegateCapsBody
    token_kind: Literal["rotating", "long_lived"]


class DelegateCreatedPayload(StrictBody):
    """Create-delegate response body — the ONLY place tokens surface.

    Returned from ``POST /api/ai-delegates``. The operator must
    copy the tokens out of the response within their session; the
    list + detail endpoints deliberately do not re-serve them.

    Attributes:
        delegate: The newly-minted :class:`DelegateRead`
            projection.
        access_token: Freshly-minted JWT — operator copies into
            the MCP client config.
        refresh_token: Freshly-minted refresh JWT, OR ``None`` when
            the caller opted into ``long_lived`` mode. Clients must
            treat ``None`` as "no rotation expected" and stop
            calling ``POST /api/auth/refresh``.
        expires_in: Access-token lifetime in seconds. For rotating
            delegates this matches the standard short TTL; for
            long-lived delegates this is the ~10-year window
            (approximately ``3650 * 86400`` seconds). Mirrors the
            :class:`~snapper.auth.schemas.tokens.TokenPair` shape so
            CLI clients that also handle login responses can share
            deserialisation code.
        token_kind: ``"rotating"`` (default) or ``"long_lived"``.
            Mirrors the freshly-minted delegate's
            :attr:`DelegateRead.token_kind`; surfaced here so the
            frontend can branch config-snippet generation without a
            follow-up GET.
    """

    delegate: DelegateRead
    access_token: str
    refresh_token: str | None
    expires_in: int
    token_kind: Literal["rotating", "long_lived"]


class DelegateResponse(PayloadResponse[Literal["delegate_response"], DelegateRead]):
    """Single delegate read response envelope."""

    type: Literal["delegate_response"] = "delegate_response"


class DelegateCreatedResponse(
    PayloadResponse[Literal["delegate_created_response"], DelegateCreatedPayload]
):
    """POST-create response envelope — one-shot token surface."""

    type: Literal["delegate_created_response"] = "delegate_created_response"


class DelegateListResponse(PayloadListResponse[Literal["delegate_list"], DelegateRead]):
    """List-delegates response envelope."""

    type: Literal["delegate_list"] = "delegate_list"


class DelegateCapsUpdateBody(StrictBody):
    """Body for ``PATCH /api/ai-delegates/{id}``.

    Only the caps fields are mutable post-create. ``label`` and
    ``username`` are immutable after mint (changing them would
    invalidate live tokens without an atomic rotation).

    Attributes:
        caps: Replacement caps — every field replaces the
            corresponding existing cap. Missing fields are treated
            as ``None`` (unbounded).
    """

    caps: DelegateCapsBody


class DelegateCapsUpdateRequest(
    PayloadRequest[Literal["delegate_caps_update_request"], DelegateCapsUpdateBody]
):
    """Update-delegate-caps request envelope."""

    type: Literal["delegate_caps_update_request"] = "delegate_caps_update_request"


class DelegateDeactivateBody(StrictBody):
    """Body for ``POST /api/ai-delegates/{id}/deactivate``.

    Attributes:
        reason: Optional free-text reason recorded in audit logs
            + propagated through ``admin.user_deactivated`` so
            the WS listener's close-reason carries context.
    """

    reason: str | None = Field(None, max_length=120, description="Optional audit reason")


class DelegateDeactivateRequest(
    PayloadRequest[Literal["delegate_deactivate_request"], DelegateDeactivateBody]
):
    """Deactivate-delegate request envelope."""

    type: Literal["delegate_deactivate_request"] = "delegate_deactivate_request"


__all__ = [
    "DelegateCapsBody",
    "DelegateCapsUpdateBody",
    "DelegateCapsUpdateRequest",
    "DelegateCreateBody",
    "DelegateCreateRequest",
    "DelegateCreatedPayload",
    "DelegateCreatedResponse",
    "DelegateDeactivateBody",
    "DelegateDeactivateRequest",
    "DelegateListResponse",
    "DelegateRead",
    "DelegateResponse",
]
