"""Actual installed private SDK callback must not persist auth-bearing financial-shaped frames.

This is an unrun source counterexample candidate. The existing accounts HTTP
privacy classifier is a separate path. No production change or measured leak
is claimed by this file, and no authenticated external operation is performed.
"""

import asyncio
import base64
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

import tests.infrastructure.exchanges.implementations.test_authenticated_private_execution_lifecycle as fixture
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import PrivateSourceOwnershipReceipt
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)


@pytest.mark.parametrize("auth_name", ("Authent", "nonce", "privatekey", "apisecret"))
async def test_actual_private_callback_refuses_auth_fields_before_any_raw_or_source_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, auth_name: str
) -> None:
    """Require the actual installed receive path to refuse auth material before durable financial capture.

    Given: Real signed local key/accounts preparation and genuine physical private socket authentication.
    When: The local provider sends a financial-shaped frame containing a known authentication field.
    Then: The unchanged SDK callback completes without storing its original bytes or any receipt digest.
    """
    marker = "SYNTHETIC_WS_AUTH_FIELD_NEVER_RETAINED"
    original = fixture._FILLS
    unsafe = original.replace(
        '{"feed":"fills",',
        '{"feed":"fills",' + json.dumps(auth_name) + ":" + json.dumps(marker) + ",",
        1,
    )
    assert unsafe != original
    completed = asyncio.Event()
    actual_callback = KrakenFuturesExchangeClient._on_execution_message

    async def observe_actual_completion(
        client: KrakenFuturesExchangeClient, message: Mapping[str, Any]
    ) -> None:
        """Await the unchanged application callback after the real SDK decoder, never invoke it manually."""
        await actual_callback(client, message)
        financial = (
            isinstance(message, PrivateWireMessage) and message.wire_bytes == unsafe.encode()
        )
        sanitized = (
            message.get("event") == "error"
            and message.get("message") == "private_wire_decode_refused"
        )
        if financial or sanitized:
            actual = client._private_ws_client
            assert actual is not None
            actual._conn.keep_alive = False
            completed.set()

    monkeypatch.setattr(fixture, "_FILLS", unsafe)
    monkeypatch.setattr(
        KrakenFuturesExchangeClient, "_on_execution_message", observe_actual_completion
    )
    async with fixture._installed(tmp_path, monkeypatch, "valid") as runtime:
        await runtime.provider.ws.subscribe(feed="fills")
        await completed.wait()
        assert any(socket.reads >= 3 for socket in runtime.provider.sockets)
        async with runtime.source.repository.session() as session:
            rows = (await session.scalars(select(AccountingRawReceipt))).all()
            sidecars = (await session.scalars(select(PrivateSourceOwnershipReceipt))).all()
        unsafe_ids: list[str] = []
        for row in rows:
            raw = RawAccountingObservation.model_validate_json(row.canonical_payload)
            material = (
                base64.b64decode(raw.material, validate=True)
                if raw.representation == "wire_bytes_base64"
                else raw.material.encode()
            )
            if marker.encode() in material or material == unsafe.encode():
                unsafe_ids.append(row.public_id)
        assert unsafe_ids == []
        assert runtime.captures.raw_ids == []
        assert sidecars == []
