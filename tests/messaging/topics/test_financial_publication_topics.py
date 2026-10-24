"""Exercise the explicit immutable accounting routing domain without catalog shortcuts."""

import pytest

from snapper.messaging.topics.financial_publication import validate_financial_publication_prefix
from snapper.messaging.topics.validation import validate_subscription_pattern
from snapper.messaging.topics.validation import validate_topic

_WALLET = "00000000-0000-5000-8000-000000000001"
_INSTRUMENT = "00000000-0000-1000-8000-000000000002"


@pytest.mark.parametrize("family", ["contract_position", "native_position.v2"])
@pytest.mark.parametrize(
    ("exchange", "environment"),
    [
        ("kraken_futures", "production"),
        ("kraken_futures", "demo"),
        ("paper", "paper"),
        ("original.provider", "production"),
        ("original..provider", "production"),
    ],
)
def test_original_accounting_scopes_accept_all_canonical_uuid_versions(
    family: str, exchange: str, environment: str
) -> None:
    """Preserve the actual open exchange and evidence UUID domains.

    Given: An exact supported family and canonical source scope with UUID5 and UUID1 references.
    When: The actual topic validator checks the complete destination.
    Then: The original exchange is right-parsed and no present catalog or UUID7 restriction is imposed.
    """
    topic = f"accounting.{family}.{exchange}.{environment}.{_WALLET}.{_INSTRUMENT}"
    assert validate_topic(topic) == (True, "")


@pytest.mark.parametrize(
    "topic",
    [
        "accounting",
        "accounting.",
        "accounting.other.kraken_futures.production",
        f"accounting.native_position.v1.kraken_futures.production.{_WALLET}.{_INSTRUMENT}",
        f"accounting.native_position.v3.kraken_futures.production.{_WALLET}.{_INSTRUMENT}",
        "accounting.contract_position",
        f"accounting.contract_position.kraken_futures.production.{_WALLET}",
        f"accounting.contract_position..production.{_WALLET}.{_INSTRUMENT}",
        f"accounting.contract_position.Kraken_futures.production.{_WALLET}.{_INSTRUMENT}",
        f"accounting.contract_position. kraken_futures.production.{_WALLET}.{_INSTRUMENT}",
        f"accounting.contract_position.kraken_futures.staging.{_WALLET}.{_INSTRUMENT}",
        f"accounting.contract_position.paper.production.{_WALLET}.{_INSTRUMENT}",
        f"accounting.contract_position.paper.demo.{_WALLET}.{_INSTRUMENT}",
        f"accounting.contract_position.kraken_futures.paper.{_WALLET}.{_INSTRUMENT}",
        f"accounting.contract_position.kraken_futures.production.invalid.{_INSTRUMENT}",
        f"accounting.contract_position.kraken_futures.production.{_WALLET}.invalid",
        f"accounting.contract_position.kraken_futures.production.{_WALLET.replace('-', '')}.{_INSTRUMENT}",
        f"accounting.contract_position.kraken_futures.production.{_WALLET}.{_INSTRUMENT}.",
        f"accounting.contract_position.kraken_futures.production.{_WALLET}.{_INSTRUMENT}.extra",
        (
            "accounting.contract_position.kraken_futures.production.AAAAAAAA-AAAA-5AAA-8AAA-AAAAAAAAAAAA."
            f"{_INSTRUMENT}"
        ),
    ],
)
def test_incomplete_or_noncanonical_accounting_destinations_are_refused(topic: str) -> None:
    """Keep malformed routing copies outside the actual validated publisher.

    Given: A malformed family, source namespace, environment relation or UUID spelling.
    When: The actual topic validator checks the destination.
    Then: It returns a nonempty structural refusal instead of consulting another topic family.
    """
    valid, reason = validate_topic(topic)
    assert not valid and reason


@pytest.mark.parametrize(
    "pattern",
    [
        "accounting.",
        "accounting.contract_position.",
        "accounting.native_position.",
        "accounting.native_position.v2.",
        "accounting.contract_position.kraken_futures.",
        "accounting.native_position.v2.kraken_futures.demo.",
        "accounting.contract_position.paper.paper.",
        "accounting.contract_position.original.provider.production.",
        f"accounting.contract_position.kraken_futures.production.{_WALLET}.",
        f"accounting.native_position.v2.original.provider.demo.{_WALLET}.",
        f"accounting.native_position.v2.kraken_futures.production.{_WALLET}.{_INSTRUMENT}.",
        f"accounting.native_position.v2.kraken_futures.production.{_WALLET}.{_INSTRUMENT}",
    ],
)
def test_explicit_internal_accounting_subscriptions_preserve_their_version(pattern: str) -> None:
    """Accept supported source prefixes through the existing subscriber validator.

    Given: An explicit family root or valid exchange, environment, wallet or complete destination.
    When: The actual subscription validator checks the original pattern.
    Then: It preserves canonical IDs and the exact supported family without browser registration.
    """
    assert validate_subscription_pattern(pattern) == (True, "")


@pytest.mark.parametrize(
    "pattern",
    [
        "accounting..",
        "accounting.other.",
        "accounting.native_position.v3.",
        "accounting.native_position.v2..",
        "accounting.contract_position.Kraken_futures.",
        "accounting.contract_position.Kraken_futures.production.",
        "accounting.contract_position.kraken_futures.staging.",
        "accounting.contract_position.paper.production.",
        "accounting.contract_position.kraken_futures.paper.",
        "accounting.contract_position.kraken_futures.production.invalid.",
        f"accounting.contract_position.Kraken_futures.production.{_WALLET}.",
        f"accounting.native_position.v2.kraken_futures.production.{_WALLET}.invalid.",
        "accounting.*.",
    ],
)
def test_unsupported_accounting_subscription_prefixes_are_refused(pattern: str) -> None:
    """Reject unsupported or contradictory internal subscription namespaces.

    Given: A malformed prefix or an unsupported family version.
    When: The actual subscriber validator checks the pattern.
    Then: It returns a nonempty refusal before any subscription is installed.
    """
    valid, reason = validate_subscription_pattern(pattern)
    assert not valid and reason


def test_accounting_prefix_helper_requires_an_actual_prefix_terminator() -> None:
    """Keep the dedicated helper contract distinct from complete destination validation.

    Given: A family name without the required trailing prefix delimiter.
    When: The accounting prefix helper is invoked directly.
    Then: It refuses the incomplete prefix rather than treating it as a complete topic.
    """
    assert validate_financial_publication_prefix("accounting.contract_position")[0] is False


@pytest.mark.parametrize(
    "pattern",
    (
        "accounting..",
        "accounting.native_position.v2..",
        "accounting.contract_position..kraken_futures.",
    ),
)
def test_dedicated_accounting_prefix_boundary_rejects_empty_segments(pattern: str) -> None:
    """Refuse empty routing segments even without the generic subscriber prevalidation.

    Given: A terminated accounting prefix contains an empty family or scope segment.
    When: An internal caller invokes the dedicated public prefix helper directly.
    Then: It receives the explicit structural refusal before any subscription can be installed.
    """
    assert validate_financial_publication_prefix(pattern) == (
        False,
        "Accounting subscription segments cannot be empty",
    )
