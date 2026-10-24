"""Classify fixed-endpoint state bodies before raw persistence without interpreting account amounts."""

import json

import pytest

import snapper.infrastructure.exchanges.futures_native_state_privacy as privacy
from snapper.infrastructure.exchanges.futures_native_state_privacy import (
    native_accounts_body_is_classified,
)


def test_original_successful_financial_envelope_preserves_exact_decimal_bytes() -> None:
    """Privacy classification neither parses financial amounts nor rewrites original entity bytes.

    Given: A documented successful accounts object containing a long decimal lexeme,
    When: The bounded privacy classifier observes the original source,
    Then: It permits later strict financial interpretation and leaves every byte unchanged.
    """
    body = b'{"result":"success","accounts":{"flex":{"currencies":{"USD":{"quantity":1.00000000000000000000001}}}}}'
    original = bytes(body)
    assert native_accounts_body_is_classified(
        b'{"result":"success","accounts":{},"safe":[true,null,1,1.5]}'
    )
    assert native_accounts_body_is_classified(body)
    assert body == original


@pytest.mark.parametrize(
    "body",
    (
        b"not JSON",
        b"\xff",
        b"[]",
        b"null",
        b'{"result":"success","accounts":{},"bad":NaN}',
        b'{"result":"success","accounts":{},"bad":Infinity}',
        b'{"result":"success","accounts":{},"bad":-Infinity}',
        b'{"result":"error","accounts":{}}',
        b'{"result":"success","accounts":[]}',
        b'{"result":"success","accounts":{},"nested":{"aPiKeY":"private"}}',
        b'{"result":"success","accounts":{},"nested":{"Authent":"SYNTHETIC_OTHER_AUTHENT"}}',
        b'{"result":"success","accounts":{},"nested":[{"signature":"private"}]}',
        b'{"result":"success","accounts":{},"safe":{" Authorization ":"private"}}',
        b'{"result":"success","accounts":{},"safe":{"apiKey":"private"},"safe":{}}',
        b'{"result":"success","accounts":{},"safe":{"event":" SUBSCRIBED "}}',
        b'{"result":"success","accounts":{},"safe":{"method":"subscribe"}}',
        b'{"result":"success","accounts":{},"safe":{"python-kraken-sdk":"private"}}',
    ),
)
def test_unknown_or_all_depth_auth_control_body_is_refused(body: bytes) -> None:
    """An authenticated endpoint name alone cannot classify its returned content as financial state.

    Given: An unknown envelope, malformed body or original nested auth/control field,
    When: Classification examines all original pairs before duplicate overwrite,
    Then: It refuses the entire source before raw encoding or a retained content digest.
    """
    assert not native_accounts_body_is_classified(body)


@pytest.mark.parametrize(
    "body",
    (
        b'{"result":"success","accounts":{},"unknown":"SYNTHETIC_KEY"}',
        b'{"result":"success","accounts":{},"unknown":"SYNTHETIC_\\u004bEY"}',
        b'{"result":"success","accounts":{},"unknown":"SYNTHETIC_KEY","unknown":"overwritten"}',
        b'{"result":"success","accounts":{},"SYNTHETIC_KEY":{"quantity":1}}',
    ),
)
def test_known_actual_auth_echo_cannot_hide_under_arbitrary_or_overwritten_fields(
    body: bytes,
) -> None:
    """Known local signing material remains transient even when a provider uses an unknown field name.

    Given: Original JSON with an actual synthetic auth value in a value or field name,
    When: The closed operation supplies that exact value only for an in-memory privacy check,
    Then: Escaping or duplicate overwrite cannot make the source eligible for persistence.
    """
    assert not native_accounts_body_is_classified(body, ("SYNTHETIC_KEY",))


@pytest.mark.parametrize("bound", ("bytes", "fields", "nodes", "depth"))
def test_explicit_resource_bounds_refuse_source_without_guessing_a_safe_partial_body(
    monkeypatch: pytest.MonkeyPatch, bound: str
) -> None:
    """Privacy checks remain bounded and never retain a truncated or partially inspected entity.

    Given: A genuine success-shaped source exceeding one explicitly selected inspection bound,
    When: The complete privacy classifier evaluates it,
    Then: The entire source is refused rather than treating an unchecked remainder as harmless.
    """
    body = json.dumps(
        {"result": "success", "accounts": {"flex": {"currencies": {"USD": 1}}}}
    ).encode()
    limits = {
        "bytes": "_MAX_BODY_BYTES",
        "fields": "_MAX_FIELDS",
        "nodes": "_MAX_NODES",
        "depth": "_MAX_DEPTH",
    }
    monkeypatch.setattr(privacy, limits[bound], 1)
    assert not native_accounts_body_is_classified(body)


def test_runtime_parser_recursion_refusal_is_fixed_and_does_not_publish_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A parser recursion refusal is treated as unclassified source without reproducing its error text.

    Given: An actual parser boundary raising RecursionError containing synthetic sensitive text,
    When: The privacy classifier handles that failure,
    Then: Its public result is only false and neither exception nor body leaves the boundary.
    """

    def refuse(*args: object, **kwargs: object) -> object:
        """Represent the runtime parser's platform-dependent recursion refusal at its exact boundary."""
        raise RecursionError("SYNTHETIC_PRIVATE_BODY")

    monkeypatch.setattr(privacy.json, "loads", refuse)
    assert not native_accounts_body_is_classified(b"SYNTHETIC_PRIVATE_BODY")
