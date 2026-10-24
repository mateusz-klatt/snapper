"""Observe sensitive original exception context at the actual new Futures account-link entrypoint.

These genuine local HTTP candidates have not executed. They preserve the
existing public error classes/reasons and require source closure before the
separate error-context assertion, without changing production transport.
"""

import pytest
from kraken.futures import User

from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    NativeKeyIdentityTransportError,
)
from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    read_futures_prepared_account_link,
)
from snapper.infrastructure.exchanges.prepared_futures_account_link import (
    PreparedFuturesAccountLink,
)
from snapper.infrastructure.network.egress_context import egress_identity
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _PRIVATE_MARKER
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _Adapter
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _assert_closed
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _assert_safe_error
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _body
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _client
from tests.infrastructure.exchanges.test_kraken_futures_key_identity import _selection


def _link(user: User) -> PreparedFuturesAccountLink:
    """Use the actual new public entrypoint and unchanged signed SDK without a private issuer."""
    with egress_identity(
        exchange="kraken_futures",
        traffic_class="private",
        owner="test",
        operation="futures_link_error_context",
    ):
        return read_futures_prepared_account_link(user, _selection())


@pytest.mark.parametrize("failure", ("malformed_json", "malformed_iiban"))
def test_actual_futures_account_link_refusal_drops_sensitive_original_exception_context(
    failure: str,
) -> None:
    """A fixed public refusal cannot retain its original private key-check document or invalid field.

    Given: Actual signed SDK HTTP with malformed private JSON or valid key/UUID and invalid IIBAN,
    When: The explicit new Futures account-link operation refuses the original response,
    Then: Its fixed reason and closed source remain correct and no sensitive exception context escapes.
    """
    if failure == "malformed_json":
        body = _body(iiban="AA00 TEST ACCT 0001")[:-1] + b',"broken":'
        reason = "native_key_check_json_invalid"
    else:
        body = _body(iiban=_PRIVATE_MARKER)
        reason = "native_key_check_internal_account_unavailable"
    adapter = _Adapter(body)
    with _client(adapter) as user, pytest.raises(NativeKeyIdentityTransportError) as caught:
        _link(user)
    assert type(caught.value) is NativeKeyIdentityTransportError
    assert str(caught.value) == reason
    assert len(adapter.calls) == 1
    _assert_safe_error(caught.value)
    _assert_closed(adapter)
    assert caught.value.__context__ is None
