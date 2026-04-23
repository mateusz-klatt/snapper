"""Pre-insert DTO for trade-command submissions.

class:`TradeCommandSubmission` is the value object every insert
site constructs BEFORE acquiring the cap-enforcer guard. It is
intentionally **distinct** from :class:`TradeCommandRow` (the
post-insert DB-projection TypedDict): this one carries the request
context that the enforcer needs for cap evaluation, without the
DB-assigned fields (``public_id``, ``created_at``, ``session_id``
``sequence_id``, ``timestamp``) which are populated at insert
time.
The DTO is frozen so an insert pipeline can pass it by reference
without worrying about mutations between cap check and actual
insert.
"""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class TradeCommandSubmission:
    """Pre-insert DTO consumed by :class:`TradingCapsEnforcer.guard`.

    Attributes:
        user_public_id: UUID of the submitting user. May be ``None``
            for service-principal submissions (e.g., strategy hot
            path) where no human/AI user is the actor — caps are
            skipped in that branch by the enforcer.
        operator_public_id: UUID of the owning operator (trading
            desk in user-facing copy). ``None`` for principal-less
            submissions.
        wallet_public_id: UUID of the wallet the command targets.
            ``None`` for submissions that do not carry wallet
            context (legacy paths — enforcer treats as unscoped).
        instrument_public_id: UUID of the instrument being traded.
            Required for per-instrument cap enforcement.
        command_type: ``"submit"`` | ``"cancel"`` | ``"replace"``.
            Branches cap evaluation: submit → qty + open-orders +
            notional; cancel → cancels-per-minute only; replace →
            treated like submit.
        side: ``"buy"`` | ``"sell"`` — required when
            ``command_type == "submit"``.
        order_type: e.g. ``"market"`` | ``"limit"`` — carried for
            logging + downstream row construction.
        quantity: Submit quantity as Decimal for precision-safe
            multiplication with the USD price oracle. ``None``
            for cancels.
        price: Limit price for limit orders; ``None`` for market
            orders or cancels.
        source_surface: One of ``"mcp" | "rest" | "strategy" |
            "ws"``. Threaded into the inserted row.
        idempotency_key: Optional caller-supplied dedupe key.
            Required on MCP ``submit_manual_order``.
    """

    user_public_id: str | None
    operator_public_id: str | None
    wallet_public_id: str | None
    instrument_public_id: str | None
    command_type: str
    side: str | None
    order_type: str | None
    quantity: Decimal | None
    price: Decimal | None
    source_surface: str
    idempotency_key: str | None
