"""Reject market-data-only manual intents at the MCP admission boundary."""

from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from mcp.types import CallToolResult

import snapper.application.trade.execution_products as product_policy
import snapper.mcp.tools as mcp_tools
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentValuationMetadataRow
from snapper.mcp.tools import _ManualOrderInput
from snapper.mcp.tools import _prepare_manual_order
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.mcp.raw_dispatch import call_raw_tool
from tests.mcp.test_tools import _build_server
from tests.mcp.test_tools import _decode_envelope
from tests.mcp.test_tools import _make_claims


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange", ["kraken_futures", "kraken_equities", "unknown_venue"])
async def test_nontradable_manual_order_creates_no_intent(exchange: str) -> None:
    """Reject catalog denial independently of wallet and identity admission.

    Given: an accessible wallet and a resolved instrument with no execution capability.
    When: an MCP manual order is prepared for the market-data-only venue.
    Then: the caller receives a capability refusal before any order intent is persisted.
    """
    repo = AsyncMock(spec=Repository)
    wallet = {"public_id": "wallet-1", "is_paper": False}
    repo.list_accessible_wallets_for_operators.return_value = [wallet]
    repo.list_active_wallets.return_value = [wallet]
    repo.list_active_wallet_credentials.return_value = [
        {"wallet_public_id": "wallet-1", "exchange": exchange}
    ]
    repo.get_instrument_public_id_by_symbol.return_value = "inst-1"
    enforcer = MagicMock(spec=TradingCapsEnforcer)
    order = _ManualOrderInput(
        exchange=exchange,
        instrument="BTC-USD",
        instrument_public_id="inst-1",
        side="buy",
        order_type="market",
        quantity=1,
        wallet_public_id="wallet-1",
        idempotency_key="untradable-market",
        price=None,
        stop_price=None,
        operator_public_id=None,
        ai_review_public_id=None,
        leverage=None,
        post_only=False,
        reduce_only=False,
    )

    result = await _prepare_manual_order(
        repository_getter=lambda: cast(Repository, repo),
        caps_enforcer_getter=lambda: cast(TradingCapsEnforcer, enforcer),
        claims_getter=_make_claims,
        order=order,
        tracker=SequenceTracker(),
    )

    assert isinstance(result, CallToolResult)
    envelope = _decode_envelope(result)
    assert envelope["success"] is False
    assert envelope["error_code"] == "instrument_market_data_only"
    assert envelope["details"]["symbol"] == "BTC-USD"
    assert envelope["details"]["exchange"] == exchange
    repo.get_instrument_public_id_by_symbol.assert_awaited_once()
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()
    enforcer.guard.assert_not_called()


def _accounting_order(exchange: str, instrument: str, order_type: str) -> _ManualOrderInput:
    """Build an MCP request spanning optional limit and stop-price conversions."""
    return _ManualOrderInput(
        exchange=exchange,
        instrument=instrument,
        instrument_public_id="inst-1",
        side="buy",
        order_type=order_type,
        quantity=1,
        wallet_public_id="wallet-1",
        idempotency_key="product-accounting-test",
        price=150.0 if order_type in ("limit", "stop_limit") else None,
        stop_price=140.0 if order_type in ("stop", "stop_limit") else None,
        operator_public_id=None,
        ai_review_public_id=None,
        leverage=None,
        post_only=False,
        reduce_only=False,
    )


def _accounting_repository(exchange: str) -> AsyncMock:
    """Resolve an exact ordinary Kraken identity or its authoritative paper mirror.

    Args:
        exchange: Selected actual execution venue for the fixture wallet.

    Returns:
        Explicit repository boundary results without database or account access.
    """
    repo = AsyncMock(spec=Repository)
    wallet = {"public_id": "wallet-1", "is_paper": exchange == "paper"}
    repo.list_accessible_wallets_for_operators.return_value = [wallet]
    repo.list_active_wallets.return_value = [wallet]
    repo.list_active_wallet_credentials.return_value = [
        {"wallet_public_id": "wallet-1", "exchange": exchange}
    ]
    repo.get_instrument_public_id_by_symbol.return_value = "inst-1"
    if exchange == "paper":
        repo.resolve_source_instrument_public_id.return_value = {
            "valuation_public_id": "source-inst-1",
            "is_paper": True,
            "mapped": True,
        }
        metadata: InstrumentValuationMetadataRow = {
            "exchange": "kraken",
            "base": "BTC",
            "quote": "USD",
            "spec": None,
        }
        repo.get_active_instrument_valuation_metadata.return_value = metadata
    return repo


@pytest.mark.parametrize(
    ("exchange", "instrument", "code"),
    [
        ("kraken", "AAPL", "tokenized_unit_accounting_unavailable"),
        ("kraken_futures", "BTC-USD-PERP", "derivative_accounting_unavailable"),
    ],
)
@pytest.mark.parametrize("order_type", ["market", "limit", "stop", "stop_limit"])
async def test_stale_mcp_capability_cannot_create_unsupported_product_intent(
    monkeypatch: pytest.MonkeyPatch,
    exchange: str,
    instrument: str,
    code: str,
    order_type: str,
) -> None:
    """MCP applies product accounting support even when old catalog rows permit trading.

    Given: An exact identity, accessible wallet, and stale can_trade=True,
    When: MCP prepares a tokenized or derivative manual order,
    Then: The accounting refusal precedes every caps call and intent write.
    """
    repo = _accounting_repository(exchange)
    enforcer = MagicMock(spec=TradingCapsEnforcer)
    monkeypatch.setattr(mcp_tools, "is_tradeable", lambda _symbol, _exchange: True)
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "tokenized_asset")
    result = await _prepare_manual_order(
        repository_getter=lambda: cast(Repository, repo),
        caps_enforcer_getter=lambda: cast(TradingCapsEnforcer, enforcer),
        claims_getter=_make_claims,
        order=_accounting_order(exchange, instrument, order_type),
        tracker=SequenceTracker(),
    )
    assert isinstance(result, CallToolResult)
    envelope = _decode_envelope(result)
    assert envelope["success"] is False
    assert envelope["error_code"] == code
    assert envelope["details"]["instrument"] == instrument
    assert envelope["details"]["exchange"] == exchange
    repo.get_instrument_public_id_by_symbol.assert_awaited_once()
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()
    enforcer.guard.assert_not_called()


@pytest.mark.parametrize(("exchange", "instrument"), [("kraken", "BTC-USD"), ("paper", "BTC-USD")])
async def test_mcp_ordinary_spot_and_paper_keep_their_existing_preparation(
    monkeypatch: pytest.MonkeyPatch, exchange: str, instrument: str
) -> None:
    """Supported execution paths still prepare an exact native identity and selected venue.

    Given: An ordinary Kraken currency product or its authoritative paper mirror,
    When: MCP applies its existing identity and manual-order admission checks,
    Then: Preparation succeeds with the selected venue and no premature persistence.
    """
    repo = _accounting_repository(exchange)
    enforcer = MagicMock(spec=TradingCapsEnforcer)
    monkeypatch.setattr(mcp_tools, "is_tradeable", lambda _symbol, _exchange: True)
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    result = await _prepare_manual_order(
        repository_getter=lambda: cast(Repository, repo),
        caps_enforcer_getter=lambda: cast(TradingCapsEnforcer, enforcer),
        claims_getter=_make_claims,
        order=_accounting_order(exchange, instrument, "limit"),
        tracker=SequenceTracker(),
    )
    assert not isinstance(result, CallToolResult)
    assert result.exchange == exchange
    assert result.instrument == instrument
    assert result.plan_row["instrument_public_id"] == "inst-1"
    if exchange == "paper":
        repo.resolve_source_instrument_public_id.assert_awaited_once_with("inst-1")
        repo.get_active_instrument_valuation_metadata.assert_awaited_once_with("source-inst-1")
    else:
        repo.resolve_source_instrument_public_id.assert_not_awaited()
        repo.get_active_instrument_valuation_metadata.assert_not_awaited()
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()


@pytest.mark.parametrize(
    ("profile", "source_exchange", "instrument", "code"),
    [
        ("future", "kraken_futures", "BTC-USD-PERP", "derivative_accounting_unavailable"),
        ("token", "kraken", "AAPL", "tokenized_unit_accounting_unavailable"),
        ("unmapped", "kraken", "BTC-USD", "paper_source_identity_unavailable"),
        ("missing_profile", "polygon", "AAPL", "paper_source_identity_unavailable"),
    ],
)
async def test_direct_paper_mcp_tool_refuses_unsupported_or_unproven_source_profiles(
    monkeypatch: pytest.MonkeyPatch,
    profile: str,
    source_exchange: str,
    instrument: str,
    code: str,
) -> None:
    """Run the registered MCP tool through authoritative paper-source admission.

    Given: A directly selected paper UUID whose source is derivative, token or unproven.
    When: submit_manual_order dispatches through the real MCP tool manager.
    Then: The shared source barrier refuses before caps and every intent write.
    """
    repo = _accounting_repository("paper")
    repo.resolve_source_instrument_public_id.return_value = {
        "valuation_public_id": "source-inst-1" if profile != "unmapped" else "inst-1",
        "is_paper": True,
        "mapped": profile != "unmapped",
    }
    metadata: InstrumentValuationMetadataRow = {
        "exchange": source_exchange,
        "base": "AAPL" if instrument == "AAPL" else "BTC",
        "quote": "USD",
        "spec": None,
    }
    repo.get_active_instrument_valuation_metadata.return_value = metadata
    enforcer = MagicMock(spec=TradingCapsEnforcer)
    monkeypatch.setattr(mcp_tools, "is_tradeable", lambda _symbol, _exchange: True)
    monkeypatch.setattr(
        product_policy,
        "native_to_kraken_asset_class",
        lambda _: "tokenized_asset" if profile == "token" else "currency",
    )
    server = _build_server(repository=repo, caps_enforcer=enforcer)
    result = await call_raw_tool(
        server,
        "submit_manual_order",
        {
            "exchange": "paper",
            "instrument": instrument,
            "instrument_public_id": "inst-1",
            "side": "buy",
            "order_type": "limit",
            "quantity": 1.0,
            "price": 150.0,
            "wallet_public_id": "wallet-1",
            "idempotency_key": "paper-source-profile-refusal",
        },
    )
    assert isinstance(result, CallToolResult)
    envelope = _decode_envelope(result)
    assert envelope["success"] is False
    assert envelope["error_code"] == code
    repo.get_instrument_public_id_by_symbol.assert_awaited_once()
    repo.resolve_source_instrument_public_id.assert_awaited_once_with("inst-1")
    if profile == "unmapped":
        repo.get_active_instrument_valuation_metadata.assert_not_awaited()
    else:
        repo.get_active_instrument_valuation_metadata.assert_awaited_once_with("source-inst-1")
    enforcer.guard.assert_not_called()
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()
