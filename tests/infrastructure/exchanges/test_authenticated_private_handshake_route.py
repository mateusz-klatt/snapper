"""Actual connected SDK route must retain and recheck its original physical handshake."""

from pathlib import Path

import pytest

from snapper.infrastructure.exchanges.owned_private_socket import PrivateSocketBoundaryError
from tests.helpers.native_account_authority import native_authority_scenario
from tests.infrastructure.exchanges.test_authenticated_private_socket import _scenario


@pytest.mark.parametrize("fault", ("host", "path"))
async def test_actual_connected_handshake_change_refuses_auth_before_physical_provider_send(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """Recheck actual connected Host and path rather than trusting a pre-connect origin label.

    Given: Real signed key/accounts preparation and genuine SDK connection with its original handshake.
    When: The actual closed provider's Host or path changes after connection but before authentication.
    Then: Current selection disappears and guarded authentication refuses before any provider send.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(source, monkeypatch, "valid") as value,
    ):
        await value.start()
        physical = value.sockets[0]
        actual_proxy = value.ws._conn.socket
        assert getattr(actual_proxy, "request", None) is physical.request
        assert source.client.execution_client_selection() == source.selection
        assert physical.sent_events == []
        if fault == "host":
            physical.request.headers["Host"] = "different.invalid"
        else:
            physical.request.path = "/different-physical-path"
        assert source.client.execution_client_selection() is None
        with pytest.raises(PrivateSocketBoundaryError, match="private_socket_send_refused"):
            await value.ws.subscribe(feed="fills")
        assert physical.sent_events == []
        assert value.tickets == []
