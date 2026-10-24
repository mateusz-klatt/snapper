"""Instrument discovery returns order identity and sizing without implying activation."""

import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from mcp.server import MCPServer
from mcp.types import CallToolResult
from mcp.types import TextContent

from snapper.auth.schemas.tokens import TokenClaims
from snapper.core.json_types import JsonObject
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentSpecRow
from snapper.mcp.order_instruments import _sizing_spec
from snapper.mcp.order_instruments import describe_order_instrument
from snapper.mcp.order_instruments import register_order_instrument_tools
from tests.mcp.raw_dispatch import call_raw_tool
from tests.mcp.test_tools import _make_claims


def _decode(result: CallToolResult) -> JsonObject:
    """Read the canonical MCP envelope used by these discovery tests.

    Args:
        result: The direct tool result containing one JSON text block.

    Returns:
        The parsed JSON envelope.
    """
    content = result.content[0]
    assert isinstance(content, TextContent)
    return cast(JsonObject, json.loads(content.text))


def _spec(as_of: datetime) -> InstrumentSpecRow:
    """Build complete derivative sizing evidence at a controlled observation time.

    Args:
        as_of: Observation time for certification freshness.

    Returns:
        An instrument specification independent of live credentials.
    """
    return {
        "instrument_public_id": "inst-1",
        "tick_size": 0.5,
        "lot_size": 0.0001,
        "min_order_size": 0.0001,
        "max_order_size": None,
        "cost_decimals": None,
        "qty_decimals": 4,
        "margin_initial": 0.02,
        "position_limit_long": None,
        "position_limit_short": None,
        "status": "active",
        "contract_size": Decimal("1.000000000000000001"),
        "quantity_unit": "contract_count",
        "native_order_quantity_unit": "contract_count",
        "spec_source": "test_catalog",
        "spec_version": "test-v2",
        "spec_observed_at": as_of,
        "unit_certified": True,
        "expiry_at": as_of + timedelta(days=1),
        "instrument_kind": "future",
        "funding_type": None,
        "funding_frequency_hours": None,
        "rollover_rate_long": None,
        "rollover_rate_short": None,
        "max_funding_rate": None,
    }


def test_sizing_is_explicit_and_preserves_decimal_multiplier() -> None:
    """Separate quantity increment, multiplier and certification freshness.

    Given: a contract multiplier with more precision than a float can retain.
    When: sizing metadata is projected for an MCP caller.
    Then: the multiplier remains exact and stale evidence is not certified.
    """
    as_of = datetime.now(UTC)
    spec = _spec(as_of)
    sizing = _sizing_spec(spec, as_of)
    assert sizing is not None
    assert sizing["contract_size"] == "1.000000000000000001"
    assert sizing["quantity_increment"] == 0.0001
    assert sizing["quantity_unit"] == "contract_count"
    assert sizing["native_order_quantity_unit"] == "contract_count"
    assert sizing["unit_certified"] is True
    expiry_at = spec["expiry_at"]
    assert expiry_at is not None
    assert sizing["expiry_at"] == expiry_at.isoformat()
    stale = _sizing_spec(spec, as_of + timedelta(hours=12))
    assert stale is not None
    assert stale["unit_certified"] is False


def test_incomplete_sizing_stays_unknown() -> None:
    """Preserve unknown metadata rather than inventing unit or expiry values.

    Given: an unresolved specification or nullable sizing evidence.
    When: sizing is projected.
    Then: absent fields remain null and unit certification remains false.
    """
    as_of = datetime.now(UTC)
    assert _sizing_spec(None, as_of) is None
    spec = _spec(as_of)
    spec["contract_size"] = None
    spec["spec_observed_at"] = None
    spec["expiry_at"] = None
    sizing = _sizing_spec(spec, as_of)
    assert sizing is not None
    assert sizing["contract_size"] is None
    assert sizing["expiry_at"] is None
    assert sizing["spec_observed_at"] is None
    assert sizing["unit_certified"] is False


@pytest.mark.asyncio
async def test_unknown_instrument_does_not_read_specs() -> None:
    """Refuse a missing venue identity before fetching any sizing metadata.

    Given: no active instrument resolves for the requested symbol and venue.
    When: a caller requests its order-entry contract.
    Then: the tool returns unknown_instrument and performs no specification read.
    """
    repo = AsyncMock(spec=Repository)
    repo.get_instrument_public_id_by_symbol.return_value = None
    result = _decode(await describe_order_instrument(cast(Repository, repo), "kraken", "NOPE"))
    assert result["error_code"] == "unknown_instrument"
    repo.get_instrument_spec.assert_not_awaited()
    repo.get_instrument_capabilities.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("capability_count", [0, 1, 2])
async def test_discovery_discloses_ambiguity_and_unchecked_account(capability_count: int) -> None:
    """Never turn catalog lookup into an account or risk approval.

    Given: a known futures instrument and zero, one or conflicting feature rows.
    When: its execution metadata is requested.
    Then: only one unambiguous row resolves constraints, and account access stays unchecked.
    """
    repo = AsyncMock(spec=Repository)
    repo.get_instrument_public_id_by_symbol.return_value = "inst-1"
    repo.get_instrument_spec.return_value = _spec(datetime.now(UTC))
    repo.get_instrument_capabilities.return_value = [
        {
            "supported_order_types": ["market", "limit"],
            "min_notional": 0.5,
            "max_order_size": None,
            "supports_post_only": True,
            "supports_reduce_only": True,
            "supports_leverage": False,
        }
    ] * capability_count
    result = _decode(
        await describe_order_instrument(cast(Repository, repo), "kraken_futures", "BTC-USD-PERP")
    )
    assert result["success"] is True
    details = result["details"]
    assert isinstance(details, dict)
    assert details["instrument_public_id"] == "inst-1"
    assert details["cached_can_trade"] is False
    assert details["capability_source"] == "symbol_mapper_cache"
    assert details["account_eligibility"] == "not_checked"
    refusal = details["application_execution_refusal"]
    assert isinstance(refusal, dict)
    assert refusal["error_code"] == "derivative_accounting_unavailable"
    assert details["order_constraints_resolved"] is (capability_count == 1)
    assert (details["order_constraints"] is None) is (capability_count != 1)
    assert details["client_supported_modifiers"] == ["post_only", "reduce_only"]
    assert details["manual_entry_time_in_force"] == ["GTC"]


@pytest.mark.asyncio
@pytest.mark.parametrize("permitted", [True, False])
async def test_tool_permission_precedes_repository_access(permitted: bool) -> None:
    """Refuse insufficient permission before evaluating service availability.

    Given: an uninitialized repository and either read permission or an empty grant.
    When: the registered tool is called through MCP dispatch.
    Then: permission denial or service_unavailable is returned without data reads.
    """
    server: MCPServer[None] = MCPServer("instrument-test")
    repository_getter = MagicMock(return_value=None)
    claims: TokenClaims = _make_claims(permissions=None if permitted else [])
    register_order_instrument_tools(server, repository_getter, lambda: claims)
    result = await call_raw_tool(
        server, "get_order_instrument", {"exchange": "kraken", "instrument": "BTC-USD"}
    )
    assert isinstance(result, CallToolResult)
    envelope = _decode(result)
    assert envelope["error_code"] == ("service_unavailable" if permitted else "permission_denied")
    assert repository_getter.call_count == int(permitted)


@pytest.mark.asyncio
async def test_registered_discovery_uses_exact_native_identity() -> None:
    """Use the caller's full venue symbol instead of guessing an underlying ticker.

    Given: an initialized repository with a tokenized EUR instrument.
    When: the read tool is dispatched with the full native market identity.
    Then: the exact symbol and venue resolve the returned instrument UUID.
    """
    server: MCPServer[None] = MCPServer("instrument-test")
    repo = AsyncMock(spec=Repository)
    repo.get_instrument_public_id_by_symbol.return_value = "inst-eur"
    repo.get_instrument_spec.return_value = None
    repo.get_instrument_capabilities.return_value = []
    register_order_instrument_tools(server, lambda: cast(Repository, repo), _make_claims)
    result = await call_raw_tool(
        server, "get_order_instrument", {"exchange": "kraken", "instrument": "AAPLX-EUR"}
    )
    assert isinstance(result, CallToolResult)
    envelope = _decode(result)
    details = envelope["details"]
    assert isinstance(details, dict)
    assert details["instrument_public_id"] == "inst-eur"
    assert details["sizing"] is None
    args = repo.get_instrument_public_id_by_symbol.await_args
    assert args is not None
    assert args.args[:2] == ("AAPLX-EUR", "kraken")
