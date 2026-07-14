"""Tests for exact concrete-adapter reconciliation policy metadata."""

from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.paper import PaperExchangeClient
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.exchanges.reconciliation_policy import account_mode_for_exchange
from snapper.infrastructure.exchanges.reconciliation_policy import get_reconciliation_method_policy
from snapper.infrastructure.exchanges.reconciliation_policy import (
    get_registered_reconciliation_adapter,
)
from snapper.infrastructure.exchanges.reconciliation_policy import is_reconciliation_method_allowed


def test_registered_policies_are_keyed_by_concrete_adapter_class() -> None:
    """Every known identity resolves to its exact concrete client class.

    Given: Exact registrations for every reviewed and explicitly empty adapter.
    When: Each canonical exchange identity is resolved.
    Then: The registration names the expected concrete client class.
    """
    expected = {
        "kraken": KrakenExchangeClient,
        "kraken_futures": KrakenFuturesExchangeClient,
        "walutomat": WalutomatExchangeClient,
        "paper": PaperExchangeClient,
        "kraken_equities": KrakenEquitiesExchangeClient,
        "polygon": PolygonExchangeClient,
    }

    for exchange, adapter_class in expected.items():
        registration = get_registered_reconciliation_adapter(exchange)
        assert registration is not None
        assert registration.adapter_class is adapter_class


def test_reviewed_adapter_allowed_sets_and_defaults_are_exact() -> None:
    """Reviewed adapters expose only the classification established by review.

    Given: Reviewed Kraken Spot, Kraken Futures, and Walutomat adapters.
    When: Their reconciliation policies are loaded.
    Then: Each allowed set and structural default exactly matches the review.
    """
    kraken = get_reconciliation_method_policy("kraken")
    futures = get_reconciliation_method_policy("kraken_futures")
    walutomat = get_reconciliation_method_policy("walutomat")

    assert kraken.structural_default is None
    assert kraken.allowed == frozenset({"spot_execution_replay", "margin_ledger_replay"})
    assert futures.structural_default == "futures_position"
    assert futures.allowed == frozenset({"futures_position"})
    assert walutomat.structural_default == "spot_execution_replay"
    assert walutomat.allowed == frozenset({"spot_execution_replay"})


def test_unreviewed_paper_market_data_and_unknown_adapters_allow_no_real_method() -> None:
    """Every unproven or non-live adapter has an empty allowed set.

    Given: Paper, market-data-only, and unknown adapter identities.
    When: Their reconciliation policies are resolved.
    Then: No real method or structural default is permitted.
    """
    for exchange in ("paper", "kraken_equities", "polygon", "kraken_futures_preview"):
        policy = get_reconciliation_method_policy(exchange)
        assert policy.structural_default is None
        assert policy.allowed == frozenset()

    assert get_registered_reconciliation_adapter("kraken_futures_preview") is None


def test_allowed_helper_never_uses_exchange_name_similarity() -> None:
    """A futures-looking unknown name cannot inherit the futures policy.

    Given: One registered futures identity and similar or unrelated identities.
    When: The futures method is checked against each identity.
    Then: Only the exact registered futures adapter is allowed.
    """
    assert is_reconciliation_method_allowed("kraken_futures", "futures_position")
    assert not is_reconciliation_method_allowed("kraken_futures_preview", "futures_position")
    assert not is_reconciliation_method_allowed("kraken", "futures_position")


def test_account_mode_matches_executor_paper_venue_discrimination() -> None:
    """Only the exact paper venue selects paper account mode.

    Given: Exact paper, differently cased paper, and live exchange identities.
    When: Account mode is derived for each exchange.
    Then: Only the exact paper venue yields paper mode.
    """
    assert account_mode_for_exchange("paper") == "paper"
    assert account_mode_for_exchange("Paper") == "live"
    assert account_mode_for_exchange("kraken") == "live"
