"""Exercise actual SDK signing refusal before any key-info HTTP request is sent."""

import pytest

from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    NativeKeyIdentityTransportError,
)
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _Adapter
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _assert_safe_error
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _client
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _read


def test_actual_sdk_malformed_signing_secret_has_a_closed_safe_failure_boundary() -> None:
    """A real SDK signer failure cannot bypass the safe selected-key refusal contract.

    Given: The actual SDK User with a synthetic malformed base64 signing secret,
    When: Its original signing implementation prepares the fixed key-info request,
    Then: A fixed safe transport refusal has no original exception context and no HTTP call occurred.
    """
    adapter = _Adapter()
    with _client(adapter) as user:
        user._secret = "x"
        with pytest.raises(
            NativeKeyIdentityTransportError, match="native_key_check_transport_failed"
        ) as caught:
            _read(user)
    assert not adapter.calls and not adapter.responses and not adapter.streams
    _assert_safe_error(caught.value)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
