"""Verify existing durable intent and queued frame material before real creation."""

from snapper.application.trade.command_request import order_request_from_command
from snapper.core.command_material import TradeCommandMaterialUnavailableError
from snapper.data.repository import Repository
from snapper.messaging.schemas.data import OrderRequestData

_REQUEST_MATERIAL_FIELDS = (
    "strategy_id",
    "exchange",
    "instrument",
    "mode",
    "side",
    "order_type",
    "quantity",
    "price",
    "stop_price",
    "client_order_id",
    "leverage",
    "reduce_only",
    "post_only",
    "wallet_public_id",
    "operator_public_id",
    "user_public_id",
    "origin",
    "replay_window_start",
    "replay_window_end",
)


async def require_consistent_execution_command(
    repository: Repository,
    order: OrderRequestData,
    actual_wallet_public_id: str,
    actual_exchange: str,
) -> None:
    """Refuse inconsistent historical material or a mismatching queued dispatch frame.

    The getter remains available to adoption and cancellation. This assertion is
    only a CREATE boundary; it never turns conflicting history into absence or
    a venue rejection. Request timestamps and bus provenance are excluded because
    direct dispatch and outbox recovery legitimately stamp them differently.

    Args:
        repository: Actual execution repository containing durable intent history.
        order: Queued frame about to be translated into an execution request.
        actual_wallet_public_id: Executor-owned account identity.
        actual_exchange: Actual executor venue rather than a caller assertion.

    Raises:
        TradeCommandMaterialUnavailableError: If proof or frame identity is unavailable.
    """
    command_id: str | None = None
    try:
        command = await repository.get_active_create_command_by_client_order_id(
            order.client_order_id, actual_exchange
        )
        if command is None:
            raise TradeCommandMaterialUnavailableError(None, "durable_create_command_missing")
        command_id = command["public_id"]
        expected_mode = "paper" if actual_exchange == "paper" else "live"
        if (
            not actual_wallet_public_id
            or command["wallet_public_id"] != actual_wallet_public_id
            or order.wallet_public_id != actual_wallet_public_id
            or command["exchange"] != actual_exchange
            or order.exchange != actual_exchange
            or command["mode"] != expected_mode
            or order.mode != expected_mode
        ):
            raise TradeCommandMaterialUnavailableError(command_id, "execution_scope_mismatch")
        proven = await repository.require_consistent_trade_command_material(command_id)
        if proven["public_id"] != command_id:
            raise TradeCommandMaterialUnavailableError(
                command_id, "material_proof_command_identity_mismatch"
            )
        expected = order_request_from_command(proven)
        material_fields = _REQUEST_MATERIAL_FIELDS + (
            ("strategy_tag",) if expected_mode == "paper" else ()
        )
        changed = tuple(
            field for field in material_fields if getattr(order, field) != getattr(expected, field)
        )
        if changed:
            raise TradeCommandMaterialUnavailableError(
                command_id, f"queued_request_material_mismatch: {', '.join(changed)}"
            )
    except TradeCommandMaterialUnavailableError:
        raise
    except Exception as exc:
        raise TradeCommandMaterialUnavailableError(
            command_id, "durable_request_proof_unavailable"
        ) from exc
