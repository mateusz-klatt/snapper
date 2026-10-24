"""Expose exact order-instrument identity and catalog sizing to MCP callers."""

from collections.abc import Callable
from datetime import UTC
from datetime import datetime

from mcp.server import MCPServer
from mcp.types import CallToolResult

from snapper.application.trade.execution_modifiers import LIMIT_POST_ONLY_TYPES
from snapper.application.trade.execution_modifiers import honoured_flags
from snapper.application.trade.execution_products import product_execution_refusal
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import has_effective_permission
from snapper.auth.schemas.tokens import TokenClaims
from snapper.core.json_types import JsonObject
from snapper.data.repository import Repository
from snapper.data.repository import is_effective_unit_certified
from snapper.data.repository_types import InstrumentSpecRow
from snapper.infrastructure.symbols.functions import is_tradeable
from snapper.mcp.error_envelope import to_call_tool_result


def _sizing_spec(spec: InstrumentSpecRow | None, as_of: datetime) -> JsonObject | None:
    """Serialize sizing evidence without treating missing data as certification.

    Args:
        spec: The active catalog specification, if one exists.
        as_of: Time at which the freshness of unit evidence is evaluated.

    Returns:
        JSON-safe sizing metadata, or null when no specification exists.
    """
    if spec is None:
        return None
    contract_size = spec["contract_size"]
    observed_at = spec["spec_observed_at"]
    expiry_at = spec["expiry_at"]
    return {
        "instrument_kind": spec["instrument_kind"],
        "quantity_unit": spec["quantity_unit"],
        "native_order_quantity_unit": spec.get("native_order_quantity_unit"),
        "contract_size": str(contract_size) if contract_size is not None else None,
        "quantity_increment": spec["lot_size"],
        "minimum_quantity": spec["min_order_size"],
        "maximum_quantity": spec["max_order_size"],
        "quantity_decimals": spec["qty_decimals"],
        "price_increment": spec["tick_size"],
        "status": spec["status"],
        "expiry_at": expiry_at.isoformat() if expiry_at is not None else None,
        "unit_certified": is_effective_unit_certified(spec, as_of),
        "contract_payoff_model": spec.get("contract_payoff_model"),
        "contract_value_currency": spec.get("contract_value_currency"),
        "settlement_currency": spec.get("settlement_currency"),
        "spec_source": spec["spec_source"],
        "spec_version": spec["spec_version"],
        "spec_observed_at": observed_at.isoformat() if observed_at is not None else None,
    }


async def describe_order_instrument(
    repo: Repository, exchange: str, instrument: str
) -> CallToolResult:
    """Resolve a single market and disclose its catalog execution constraints.

    Args:
        repo: Repository used for a temporal identity and specification snapshot.
        exchange: Exact venue identifier, without paper remapping.
        instrument: Exact native symbol from the venue's instrument listing.

    Returns:
        Instrument UUID, sizing evidence and client-level order instructions.
        Account eligibility and live trading permission are explicitly unchecked.
    """
    as_of = datetime.now(UTC)
    public_id = await repo.get_instrument_public_id_by_symbol(instrument, exchange, as_of)
    if public_id is None:
        return to_call_tool_result(
            success=False,
            error_code="unknown_instrument",
            message="No active instrument resolves for this symbol and venue.",
            details={"symbol": instrument, "exchange": exchange},
        )
    spec = await repo.get_instrument_spec(public_id, as_of)
    capabilities = await repo.get_instrument_capabilities(
        as_of, exchange=exchange, instrument_public_id=public_id
    )
    constraints: JsonObject | None = None
    if len(capabilities) == 1:
        capability = capabilities[0]
        constraints = {
            "supported_order_types": list(capability["supported_order_types"]),
            "minimum_notional": capability["min_notional"],
            "maximum_order_size": capability["max_order_size"],
            "supports_post_only": capability["supports_post_only"],
            "supports_reduce_only": capability["supports_reduce_only"],
            "supports_leverage": capability["supports_leverage"],
        }
    refusal = product_execution_refusal(instrument, exchange)
    return to_call_tool_result(
        success=True,
        error_code=None,
        message="Catalog metadata only; account eligibility and live activation are not checked.",
        details={
            "exchange": exchange,
            "symbol": instrument,
            "instrument_public_id": public_id,
            "cached_can_trade": is_tradeable(instrument, exchange),
            "application_execution_refusal": (
                {
                    "error_code": refusal.error_code,
                    "message": refusal.message,
                    "details": refusal.details,
                }
                if refusal is not None
                else None
            ),
            "capability_source": "symbol_mapper_cache",
            "sizing": _sizing_spec(spec, as_of),
            "order_constraints": constraints,
            "order_constraints_resolved": len(capabilities) == 1,
            "client_supported_modifiers": [*sorted(honoured_flags(exchange))],
            "client_post_only_order_types": [*sorted(LIMIT_POST_ONLY_TYPES.get(exchange, ()))],
            "manual_entry_time_in_force": ["GTC"],
            "account_eligibility": "not_checked",
        },
    )


def register_order_instrument_tools(
    server: MCPServer,
    repository_getter: Callable[[], Repository | None],
    claims_getter: Callable[[], TokenClaims],
) -> None:
    """Register permission-checked instrument discovery independently of writes.

    Args:
        server: The MCP server hosting Snapper's authenticated tools.
        repository_getter: Active repository getter, possibly not initialized.
        claims_getter: Authenticated claims for the current call.
    """

    @server.tool()
    async def get_order_instrument(exchange: str, instrument: str) -> CallToolResult:
        """Get the UUID and sizing rules for an exact order-entry instrument.

        Use list_instruments to select the exact native symbol first. Quantity
        follows native_order_quantity_unit and its increment; quantity_unit is
        the internal position representation. The contract multiplier is
        separate from quantity increment. Missing or stale metadata is disclosed,
        never filled with guessed units. This read does not check account access,
        funds, risk caps or live activation and does not place an order.

        Args:
            exchange: Canonical execution or market-data venue identifier.
            instrument: Exact native symbol, including quote and expiry suffixes.

        Returns:
            Canonical envelope containing identity, sizing and order constraints.
        """
        claims = claims_getter()
        if not has_effective_permission(
            role=claims.role,
            token_permissions=claims.permissions,
            permission_scope_version=claims.permission_scope_version,
            permission=Permission.READ_MARKET_DATA,
        ):
            return to_call_tool_result(
                success=False,
                error_code="permission_denied",
                message="READ_MARKET_DATA permission is required.",
            )
        repo = repository_getter()
        if repo is None:
            return to_call_tool_result(
                success=False,
                error_code="service_unavailable",
                message="The instrument repository is not initialized.",
            )
        return await describe_order_instrument(repo, exchange, instrument)
