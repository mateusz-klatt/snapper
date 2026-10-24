"""REST admission rejects unsupported product accounting before creating intent."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import cast
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import snapper.application.trade.execution_products as product_policy
import snapper.server._capability_guard as capability_guard
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.core.json_types import JsonObject
from snapper.data.repository_types import InstrumentSpecRow
from snapper.server.dependencies import get_caps_enforcer_dependency
from tests.server.test_order_routes import _admit_caps_enforcer
from tests.server.test_order_routes import _arm_execution_venue
from tests.server.test_order_routes import _create_client
from tests.server.test_order_routes import _create_order_repo
from tests.server.test_order_routes import _ts


@contextmanager
def _client(repo: object, enforcer: TradingCapsEnforcer) -> Iterator[TestClient]:
    """Use the existing endpoint-only fixture without starting its disabled lifespan."""
    client = _create_client(repo)
    cast(FastAPI, client.app).dependency_overrides[get_caps_enforcer_dependency] = lambda: enforcer
    try:
        yield client
    finally:
        client.close()


def _body(exchange: str, instrument: str, mode: str, order_type: str) -> JsonObject:
    """Build a real command envelope spanning nullable limit and trigger prices."""
    payload: JsonObject = {
        "instrument": instrument,
        "instrument_public_id": "inst-1",
        "exchange": exchange,
        "mode": mode,
        "side": "buy",
        "order_type": order_type,
        "quantity": 0.5,
        "wallet_public_id": "wallet-1",
    }
    if order_type in ("limit", "stop_limit"):
        payload["price"] = 150.0
    if order_type in ("stop", "stop_limit"):
        payload["stop_price"] = 140.0
    return {
        "type": "create_order_command",
        "session_id": "product-policy-test",
        "sequence_id": 1,
        "public_id": "product-policy-command",
        "timestamp": _ts().isoformat(),
        "payload": payload,
    }


def _cash_share_spec() -> InstrumentSpecRow:
    """Provide explicit fixture cash-share profile evidence for the exact source UUID.

    Returns:
        A complete catalog row whose spot kind is assumed proven by the fixture.
    """
    return {
        "instrument_public_id": "inst-1",
        "tick_size": None,
        "lot_size": None,
        "min_order_size": None,
        "max_order_size": None,
        "cost_decimals": None,
        "qty_decimals": None,
        "margin_initial": None,
        "position_limit_long": None,
        "position_limit_short": None,
        "status": None,
        "contract_size": None,
        "quantity_unit": "base_asset",
        "native_order_quantity_unit": "base_asset",
        "contract_payoff_model": None,
        "contract_value_currency": None,
        "settlement_currency": None,
        "spec_source": "verified-fixture-cash-profile",
        "spec_version": "fixture",
        "spec_observed_at": _ts(),
        "unit_certified": False,
        "expiry_at": None,
        "instrument_kind": "spot",
        "funding_type": None,
        "funding_frequency_hours": None,
        "rollover_rate_long": None,
        "rollover_rate_short": None,
        "max_funding_rate": None,
    }


@pytest.mark.parametrize(
    ("exchange", "instrument", "code"),
    [
        ("kraken", "AAPL", "tokenized_unit_accounting_unavailable"),
        ("kraken_futures", "BTC-USD-PERP", "derivative_accounting_unavailable"),
    ],
)
@pytest.mark.parametrize("order_type", ["market", "limit", "stop", "stop_limit"])
def test_stale_capability_cannot_create_unsupported_product_intent(
    monkeypatch: pytest.MonkeyPatch,
    exchange: str,
    instrument: str,
    code: str,
    order_type: str,
) -> None:
    """The real REST route applies product support after identity and wallet admission.

    Given: An exact instrument identity and stale can_trade=True on a credentialed wallet,
    When: A tokenized or derivative command reaches manual-order admission,
    Then: Its explicit accounting refusal precedes caps and every intent write.
    """
    repo = _create_order_repo()
    _arm_execution_venue(repo, credential_exchanges=(exchange,))
    monkeypatch.setattr(capability_guard, "is_tradeable", lambda _symbol, _exchange: True)
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "tokenized_asset")
    enforcer = MagicMock(spec=TradingCapsEnforcer)
    with _client(repo, enforcer) as client:
        response = client.post("/api/orders", json=_body(exchange, instrument, "live", order_type))
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["error_code"] == code
    assert detail["instrument"] == instrument
    assert detail["exchange"] == exchange
    assert "Snapper cannot send" in detail["reason"]
    repo.get_instrument_public_id_by_symbol.assert_awaited_once()
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()
    repo.update_execution_plan_status.assert_not_awaited()
    enforcer.guard.assert_not_called()


@pytest.mark.parametrize(
    ("instrument", "mode", "effective_exchange"),
    [("BTC-USD", "live", "kraken"), ("BTC-USD", "paper", "paper")],
)
def test_ordinary_spot_and_paper_requests_keep_the_existing_admission_path(
    monkeypatch: pytest.MonkeyPatch, instrument: str, mode: str, effective_exchange: str
) -> None:
    """Product accounting policy leaves ordinary Spot and selected paper execution available.

    Given: A positively classified currency Spot product selected for a live or paper wallet,
    When: The real REST route admits a limit order,
    Then: Existing caps run and both plan and command use the selected execution venue.
    """
    repo = _create_order_repo()
    repo.get_active_instrument_valuation_metadata.return_value = {
        "exchange": "kraken",
        "base": "BTC",
        "quote": "USD",
        "spec": None,
    }
    _arm_execution_venue(repo, is_paper=mode == "paper", credential_exchanges=(effective_exchange,))
    monkeypatch.setattr(capability_guard, "is_tradeable", lambda _symbol, _exchange: True)
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    enforcer = _admit_caps_enforcer()
    with _client(repo, enforcer) as client:
        response = client.post("/api/orders", json=_body("kraken", instrument, mode, "limit"))
    assert response.status_code == 200
    enforcer.guard.assert_called_once()
    repo.insert_execution_plan.assert_awaited_once()
    repo.insert_trade_command.assert_awaited_once()
    assert repo.insert_execution_plan.call_args.args[0]["exchange"] == effective_exchange
    assert repo.insert_trade_command.call_args.args[0]["exchange"] == effective_exchange


@pytest.mark.parametrize(
    ("source_exchange", "instrument", "code"),
    [
        ("kraken", "AAPL", "tokenized_unit_accounting_unavailable"),
        ("kraken_futures", "BTC-USD-PERP", "derivative_accounting_unavailable"),
    ],
)
def test_stale_source_capability_cannot_remap_unsupported_products_into_cash_paper(
    monkeypatch: pytest.MonkeyPatch,
    source_exchange: str,
    instrument: str,
    code: str,
) -> None:
    """Preserve the authoritative source product barrier across paper remapping.

    Given: a resolved source UUID, stale can_trade=True and credentialed paper wallet.
    When: a token or future limit order selects paper execution in the real REST route.
    Then: source accounting support refuses before caps, plan or command persistence.
    """
    repo = _create_order_repo()
    _arm_execution_venue(repo, is_paper=True, credential_exchanges=("paper",))
    monkeypatch.setattr(capability_guard, "is_tradeable", lambda _symbol, _exchange: True)
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "tokenized_asset")
    enforcer = MagicMock(spec=TradingCapsEnforcer)
    with _client(repo, enforcer) as client:
        response = client.post(
            "/api/orders", json=_body(source_exchange, instrument, "paper", "limit")
        )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["error_code"] == code
    assert detail["exchange"] == source_exchange
    repo.get_instrument_public_id_by_symbol.assert_awaited_once()
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()
    enforcer.guard.assert_not_called()


def test_polygon_cash_share_paper_does_not_inherit_a_colliding_kraken_token_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Apply product classification only to the authoritative source venue.

    Given: a Polygon cash-share UUID and an identically named Kraken token alias.
    When: that cash share selects paper execution in the real REST route.
    Then: caps and persistence proceed without consulting the foreign Kraken class.
    """
    repo = _create_order_repo()
    repo.get_active_instrument_valuation_metadata.return_value = {
        "exchange": "polygon",
        "base": "AAPL",
        "quote": "USD",
        "spec": _cash_share_spec(),
    }
    _arm_execution_venue(repo, is_paper=True, credential_exchanges=("paper",))
    monkeypatch.setattr(capability_guard, "is_tradeable", lambda _symbol, _exchange: True)
    lookup = MagicMock(side_effect=AssertionError("Polygon cash shares are not Kraken tokens"))
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lookup)
    enforcer = _admit_caps_enforcer()
    with _client(repo, enforcer) as client:
        response = client.post("/api/orders", json=_body("polygon", "AAPL", "paper", "limit"))
    assert response.status_code == 200
    lookup.assert_not_called()
    assert repo.insert_execution_plan.call_args.args[0]["instrument_public_id"] == "inst-1"
    assert repo.insert_execution_plan.call_args.args[0]["params"]["source_exchange"] == "polygon"
    assert repo.insert_trade_command.call_args.args[0]["exchange"] == "paper"
