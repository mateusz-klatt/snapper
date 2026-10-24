"""Privacy classification of original product responses before any source persistence or digest."""

import pytest

from snapper.application.trade.futures_product_source import FuturesProductUnsafeSourceError
from snapper.application.trade.futures_product_source import parse_safe_futures_product_source
from snapper.application.trade.futures_trading_instruments import FuturesProductSourceError
from tests.application.trade.test_futures_trading_instruments import _row
from tests.application.trade.test_futures_trading_instruments import _wire


def test_safe_unknown_metadata_and_authentication_words_in_values_remain_original_data() -> None:
    """Keep safe unrecognized provider information without conflating field names and string values.

    Given: A successful inventory contains nested unknown metadata and an instrument named TOKEN.
    When: The complete original entity passes the product privacy and syntax boundary.
    Then: The inventory remains usable as an observation without normalization or an order permit.
    """
    entity = _wire(
        [_row({"symbol": "PF_TOKENUSD"})],
        {"extra": [{"description": "apiKey", "flags": [1, None, False]}, {}]},
    )
    snapshot = parse_safe_futures_product_source(entity)
    assert snapshot.instruments[0].wire_symbol == "PF_TOKENUSD"
    assert snapshot.parser_policy == "kraken_futures_trading_instruments_v1"


@pytest.mark.parametrize("field", ["APIKEY", "aPi_SeCrEt", "AuthEnt", "Nonce", "access_token"])
@pytest.mark.parametrize("nested", [False, True])
def test_authentication_field_names_at_any_depth_refuse_source_retention(
    field: str, nested: bool
) -> None:
    """Refuse sensitive success-shaped source bytes before any raw recording or digest can occur.

    Given: A complete product list contains a top-level or array-nested authentication field.
    When: The original entity is classified for durable success capture.
    Then: It is refused with a fixed reason that excludes the synthetic secret value.
    """
    sensitive = {field: "synthetic-product-secret"}
    extra = {"extra": [{"inner": sensitive}]} if nested else sensitive
    with pytest.raises(FuturesProductUnsafeSourceError) as error:
        parse_safe_futures_product_source(_wire(changes=extra))
    assert "synthetic-product-secret" not in str(error.value)


@pytest.mark.parametrize("extra", [{"event": "challenge"}, {"method": "subscribe"}])
def test_control_envelopes_cannot_be_retained_as_successful_product_sources(
    extra: dict[str, object],
) -> None:
    """Keep control responses outside the durable product-source domain.

    Given: A successful-looking inventory also declares a recognized control operation.
    When: The producer classifies its original entity before persistence.
    Then: It refuses the control envelope despite the otherwise complete instrument list.
    """
    with pytest.raises(FuturesProductUnsafeSourceError, match="control_source_refused"):
        parse_safe_futures_product_source(_wire(changes=extra))


@pytest.mark.parametrize(
    "entity",
    [
        b"{",
        b"[]",
        b"\xff",
        b'{"extra":{"apiKey":"synthetic-product-secret","apiKey":null}}',
        b'{"extra":NaN}',
    ],
)
def test_invalid_or_duplicate_json_is_not_retained_before_privacy_inspection(entity: bytes) -> None:
    """Refuse ambiguous input without collapsing away authentication-bearing duplicate pairs.

    Given: Original source bytes are malformed, nonobject, non-UTF8, duplicate or nonfinite JSON.
    When: The product privacy boundary decodes the original entity.
    Then: Fixed invalid-source metadata replaces source retention without echoing its content.
    """
    with pytest.raises(FuturesProductSourceError, match="original_json_invalid") as error:
        parse_safe_futures_product_source(entity)
    assert "synthetic-product-secret" not in str(error.value)


def test_oversized_original_body_is_rejected_before_decoding_or_hashing() -> None:
    """Keep the actual entity byte bound independent of decoded structure or base64 padding.

    Given: Original bytes exceed the established eight-mebibyte source limit.
    When: The producer classifies those bytes for possible retention.
    Then: The fixed source boundary refuses before constructing any product observation.
    """
    with pytest.raises(FuturesProductSourceError, match="original_entity_required"):
        parse_safe_futures_product_source(b" " * (8 * 1024 * 1024 + 1))


def test_safe_syntax_without_a_successful_inventory_cannot_be_retained_as_a_source() -> None:
    """Keep source completeness separate from the absence of detected authentication fields.

    Given: Strict safe JSON declares failure instead of a successful product inventory.
    When: Privacy classification proceeds to the actual product parser.
    Then: It refuses the missing success evidence rather than retaining a success-shaped record.
    """
    with pytest.raises(FuturesProductSourceError, match="success_unproven"):
        parse_safe_futures_product_source(_wire(changes={"result": "error"}))
