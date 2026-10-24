"""Actual issued product HTTP source checks at signer, response and selected-session boundaries."""

from pathlib import Path
from typing import Any

import pytest
from kraken.futures import User
from pydantic import ValidationError
from requests import PreparedRequest
from requests import Response
from requests import Session

import snapper.application.trade.futures_product_read as producer
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    FuturesProductTransportError,
)
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    FuturesTradingInstrumentsWireReceipt,
)
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    GuardedFuturesProductReceipt,
)
from snapper.infrastructure.exchanges.futures_trading_instruments_wire import (
    require_guarded_product_receipt,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from tests.helpers.futures_product_read import PRODUCT_PATH
from tests.helpers.futures_product_read import futures_product_scenario
from tests.helpers.native_account_authority import native_authority_uuid
from tests.infrastructure.exchanges.test_futures_product_wire import _actual_source


def _observe_actual_refusals(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Observe only fixed transport errors while preserving the actual original signing and read path."""
    actual_read = producer.read_futures_trading_instruments_wire
    refusals: list[str] = []

    def read_actual(
        user: User,
        selected: ExecutionClientSelection,
        token: PreparedKeyToken,
        attempt_public_id: str,
    ) -> GuardedFuturesProductReceipt:
        """Keep genuine issued-key ownership and original endpoint dispatch unchanged."""
        try:
            return actual_read(user, selected, token, attempt_public_id)
        except FuturesProductTransportError as error:
            refusals.append(str(error))
            raise

    monkeypatch.setattr(producer, "read_futures_trading_instruments_wire", read_actual)
    return refusals


@pytest.mark.parametrize(
    ("index", "replacement"),
    (
        (0, "POST"),
        (1, "https://different.invalid/derivatives/api/v3/trading/instruments"),
        (3, "unexpected=body"),
        (4, "unexpected=query"),
    ),
)
async def test_actual_product_signer_refuses_changed_method_url_body_or_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, index: int, replacement: str
) -> None:
    """Refuse a widened SDK request before its final prepared request reaches physical transport.

    Given: Real same-key account preparation and a separately committed product request.
    When: Genuine SDK signing returns one altered method, URL, body or query for that product GET.
    Then: Actual capture records a fixed failure and no altered product request reaches the adapter.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        user = native.client._user_client
        assert user is not None
        actual_prepare = user._prepare_request
        refusals = _observe_actual_refusals(monkeypatch)

        def change_signed_product(**kwargs: Any) -> tuple[object, ...]:
            """Change one SDK boundary field after genuine signing, leaving key and account reads intact."""
            values = list(actual_prepare(**kwargs))
            if kwargs.get("uri") == PRODUCT_PATH:
                values[index] = replacement
            return tuple(values)

        monkeypatch.setattr(user, "_prepare_request", change_signed_product)
        outcome = await producer.read_native_futures_products(
            native.repository, native.client, native.tracker
        )
        assert outcome.payload.result.kind == "failed"
        assert refusals == ["futures_product_prepared_request_mismatch"]
        assert len(scenario.adapter.calls) == 2
        assert not scenario.adapter.product_calls
        assert all(response.raw.closed for response in scenario.adapter.responses)


@pytest.mark.parametrize("fault", ("response_url", "response_history", "selected_session"))
async def test_actual_product_response_rebinds_route_and_original_sdk_session_before_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Do not accept original body bytes through a changed response route or SDK-session pointer.

    Given: A genuine issued key, committed product attempt and signed local HTTP response.
    When: The actual Session returns changed route metadata or changes the selected SDK Session.
    Then: The first actual capture retains only a fixed failure and closes every original response.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        native = scenario.native
        user = native.client._user_client
        assert user is not None
        selected_session = getattr(user, "_FuturesClient__session", None)
        assert isinstance(selected_session, Session)
        actual_send = selected_session.send
        replacement = Session()
        refusals = _observe_actual_refusals(monkeypatch)

        def alter_actual_response(request: PreparedRequest, **kwargs: object) -> Response:
            """Return a real signed local response before changing only its external provenance boundary."""
            response = actual_send(request, **kwargs)
            if (request.url or "").endswith(PRODUCT_PATH):
                if fault == "response_url":
                    response.url = "https://different.invalid/product-response"
                elif fault == "response_history":
                    response.history = [Response()]
                else:
                    monkeypatch.setattr(user, "_FuturesClient__session", replacement)
            return response

        monkeypatch.setattr(selected_session, "send", alter_actual_response)
        try:
            outcome = await producer.read_native_futures_products(
                native.repository, native.client, native.tracker
            )
            assert outcome.payload.result.kind == "failed"
            expected = (
                "futures_product_actual_session_changed"
                if fault == "selected_session"
                else "futures_product_response_route_mismatch"
            )
            assert refusals == [expected]
            assert len(scenario.adapter.product_calls) == 1
            assert scenario.adapter.visible_product_sources == [(1, 1, 1)]
            assert all(response.raw.closed for response in scenario.adapter.responses)
        finally:
            monkeypatch.setattr(user, "_FuturesClient__session", selected_session)
            replacement.close()


@pytest.mark.parametrize("fault", ("selection", "before_proof", "before_request"))
async def test_actual_product_public_source_shape_refuses_changed_selection_or_knowledge_clocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Public source validation preserves the selection and local knowledge of an actually issued read.

    Given: Genuine key, accounts and product HTTP have produced one bound original outcome.
    When: A returned public model is reconstructed with changed selection or impossible source clocks.
    Then: Shape validation refuses it and the actually issued source and persisted outcome remain original.
    """
    async with futures_product_scenario(tmp_path, monkeypatch) as scenario:
        outcome, actual = await _actual_source(scenario, monkeypatch)
        original = require_guarded_product_receipt(actual)
        changed = original.model_dump(mode="python")
        if fault == "selection":
            changed["client_selection"] = ExecutionClientSelection.model_validate(
                original.client_selection.model_dump(mode="python")
                | {"wallet_public_id": native_authority_uuid(987)}
            )
            reason = "selected_key_mismatch"
        else:
            earlier = original.request_started_at.replace(year=2025)
            changed["request_started_at" if fault == "before_proof" else "observed_at"] = earlier
            reason = "actual_source_clock_mismatch"
        with pytest.raises(ValidationError, match=reason):
            FuturesTradingInstrumentsWireReceipt.model_validate(changed)
        assert require_guarded_product_receipt(actual) == original
        assert outcome.payload.result.kind == "captured"
        assert original.entity_bytes == scenario.adapter._original_body()
        assert len(scenario.adapter.product_calls) == 1
