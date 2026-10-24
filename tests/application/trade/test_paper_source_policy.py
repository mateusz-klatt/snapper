"""Paper admission preserves source product accounting without symbol heuristics."""

from datetime import UTC
from datetime import datetime
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

import snapper.application.trade.execution_products as product_policy
from snapper.application.trade.order_terms import ManualOrderTerms
from snapper.application.trade.order_terms import manual_order_execution_refusal
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import InstrumentValuationMetadataRow
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE
from snapper.infrastructure.exchanges.tokenized_spec_provenance import TOKENIZED_SPEC_SOURCE

_NOW = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)


def _terms(source: str | None = "paper") -> ManualOrderTerms:
    """Provide an exact already-resolved paper identity.

    Args:
        source: Authoritative original venue retained by the transport.

    Returns:
        A finite limit order bound to one exact instrument UUID.
    """
    return ManualOrderTerms(
        exchange="paper",
        instrument="SHARED-NAME",
        instrument_public_id=(
            "exact-paper-uuid" if source in {None, "paper"} else "exact-source-uuid"
        ),
        order_type="limit",
        quantity=Decimal(1),
        price=Decimal(100),
        stop_price=None,
        leverage=None,
        reduce_only=False,
        source_exchange=source,
    )


def _spec() -> InstrumentSpecRow:
    """Provide one exact ordinary cash specification without foreign token evidence.

    Returns:
        A fully typed catalog row for the requested paper UUID.
    """
    return {
        "instrument_public_id": "exact-source-uuid",
        "tick_size": 0.01,
        "lot_size": 1.0,
        "min_order_size": 1.0,
        "max_order_size": None,
        "cost_decimals": 2,
        "qty_decimals": 0,
        "margin_initial": None,
        "position_limit_long": None,
        "position_limit_short": None,
        "status": "online",
        "contract_size": None,
        "quantity_unit": "base_asset",
        "native_order_quantity_unit": "base_asset",
        "contract_payoff_model": None,
        "contract_value_currency": None,
        "settlement_currency": None,
        "spec_source": "fixture:cash-profile-attestation",
        "spec_version": "fixture",
        "spec_observed_at": _NOW,
        "unit_certified": False,
        "expiry_at": None,
        "instrument_kind": "spot",
        "funding_type": None,
        "funding_frequency_hours": None,
        "rollover_rate_long": None,
        "rollover_rate_short": None,
        "max_funding_rate": None,
    }


def _repo(spec: InstrumentSpecRow | None, exchange: str = "polygon") -> MagicMock:
    """Provide the actual source resolver and atomic current source metadata boundary.

    Args:
        spec: Exact current source specification or honest missing metadata.
        exchange: Actual source venue returned by its exact UUID lookup.

    Returns:
        A repository double with explicit source identity and metadata results.
    """
    metadata: InstrumentValuationMetadataRow = {
        "exchange": exchange,
        "base": "CASH-SHARE",
        "quote": "USD",
        "spec": spec,
    }
    repo = MagicMock(spec=Repository)
    repo.resolve_source_instrument_public_id = AsyncMock(
        return_value={"valuation_public_id": "exact-source-uuid", "is_paper": True, "mapped": True}
    )
    repo.get_active_instrument_valuation_metadata = AsyncMock(return_value=metadata)
    return repo


@pytest.mark.parametrize(
    "changes",
    [
        {"quantity_unit": "contract_count"},
        {"instrument_kind": "future_perpetual"},
        {"contract_payoff_model": "inverse"},
        {"contract_value_currency": "USD"},
        {"settlement_currency": "BTC"},
        {"native_order_quantity_unit": "contract_count"},
        {"contract_size": Decimal(1)},
        {"spec_source": FUTURES_SPEC_SOURCE},
    ],
)
@pytest.mark.asyncio
async def test_each_explicit_derivative_signal_on_exact_paper_uuid_refuses(
    changes: dict[str, object],
) -> None:
    """Refuse an explicitly unsupported contract profile on the requested paper UUID.

    Given: one independent derivative signal in that UUID's catalog row.
    When: shared manual admission checks cash-only paper execution.
    Then: derivative accounting refusal precedes every intent write.
    """
    repo = _repo(cast(InstrumentSpecRow, {**_spec(), **changes}))
    refusal = await manual_order_execution_refusal(repo, _terms(), _NOW)
    assert refusal is not None
    assert refusal.error_code == "derivative_accounting_unavailable"
    assert refusal.details["exchange"] == "paper"
    repo.resolve_source_instrument_public_id.assert_awaited_once_with("exact-paper-uuid")
    repo.get_active_instrument_valuation_metadata.assert_awaited_once_with("exact-source-uuid")


@pytest.mark.asyncio
async def test_exact_direct_token_catalog_provenance_refuses_paper_without_suffix_guessing() -> (
    None
):
    """Use direct token specification provenance for a known unsupported paper profile.

    Given: the exact paper UUID retains token REST catalog provenance.
    When: its native spelling supplies no reliable product inference.
    Then: the token unit and rebase accounting barrier remains explicit.
    """
    spec = cast(InstrumentSpecRow, {**_spec(), "spec_source": TOKENIZED_SPEC_SOURCE})
    refusal = await manual_order_execution_refusal(_repo(spec), _terms(None), _NOW)
    assert refusal is not None
    assert refusal.error_code == "tokenized_unit_accounting_unavailable"


@pytest.mark.asyncio
async def test_ordinary_cash_paper_catalog_never_consults_a_foreign_kraken_token_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve ordinary cash-share paper execution under a colliding native name.

    Given: an exact Polygon cash-share specification and a foreign Kraken token alias.
    When: direct paper admission classifies its own UUID.
    Then: no foreign venue lookup creates a false token barrier.
    """
    lookup = MagicMock(side_effect=AssertionError("Foreign symbol classification is not evidence"))
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lookup)
    assert await manual_order_execution_refusal(_repo(_spec()), _terms(), _NOW) is None
    lookup.assert_not_called()


@pytest.mark.asyncio
async def test_foreign_uuid_specification_cannot_classify_requested_paper_instrument() -> None:
    """Refuse a catalog response that contradicts the exact requested identity.

    Given: a returned specification belongs to another UUID.
    When: paper admission compares its explicit identity.
    Then: it refuses instead of using another product's economic profile.
    """
    spec = cast(InstrumentSpecRow, {**_spec(), "instrument_public_id": "foreign-uuid"})
    refusal = await manual_order_execution_refusal(_repo(spec), _terms(), _NOW)
    assert refusal is not None
    assert refusal.error_code == "paper_source_identity_unavailable"


@pytest.mark.asyncio
async def test_missing_direct_paper_source_profile_refuses_instead_of_granting_permission() -> None:
    """Keep absent source accounting evidence distinct from a supported cash profile.

    Given: A mapped Polygon source UUID with no exact cash specification.
    When: Shared paper admission evaluates that unsupported unknown profile.
    Then: Creation refuses instead of treating missing metadata as permission.
    """
    repo = _repo(None)
    refusal = await manual_order_execution_refusal(repo, _terms(), _NOW)
    assert refusal is not None
    assert refusal.error_code == "paper_source_identity_unavailable"
    repo.resolve_source_instrument_public_id.assert_awaited_once_with("exact-paper-uuid")
    repo.get_active_instrument_valuation_metadata.assert_awaited_once_with("exact-source-uuid")


@pytest.mark.parametrize(
    "resolution",
    [
        {"valuation_public_id": "exact-paper-uuid", "is_paper": True, "mapped": False},
        {"valuation_public_id": "exact-source-uuid", "is_paper": False, "mapped": True},
        {"valuation_public_id": "exact-paper-uuid", "is_paper": True, "mapped": True},
    ],
)
@pytest.mark.asyncio
async def test_unproven_or_self_referential_source_mapping_refuses_before_catalog_read(
    resolution: dict[str, object],
) -> None:
    """Require an affirmative stored paper-to-source identity mapping.

    Given: An unmapped, contradictory or self-referential source resolution.
    When: Direct paper creation evaluates its source identity.
    Then: It refuses without selecting a source from the native symbol.
    """
    repo = _repo(_spec())
    repo.resolve_source_instrument_public_id.return_value = resolution
    refusal = await manual_order_execution_refusal(repo, _terms(), _NOW)
    assert refusal is not None
    assert refusal.error_code == "paper_source_identity_unavailable"
    repo.get_active_instrument_valuation_metadata.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_source_uuid_metadata_refuses_after_affirmative_mapping() -> None:
    """Keep a stale mapping from certifying an absent source instrument.

    Given: A stored source mapping whose exact source UUID no longer resolves.
    When: Direct paper admission reads that source identity.
    Then: Creation refuses before any cash simulation intent exists.
    """
    repo = _repo(_spec())
    repo.get_active_instrument_valuation_metadata.return_value = None
    refusal = await manual_order_execution_refusal(repo, _terms(), _NOW)
    assert refusal is not None
    assert refusal.error_code == "paper_source_identity_unavailable"


@pytest.mark.parametrize("actual_exchange", ["paper", "polygon"])
@pytest.mark.asyncio
async def test_source_uuid_cannot_silently_resolve_to_another_venue(
    monkeypatch: pytest.MonkeyPatch, actual_exchange: str
) -> None:
    """Compare an explicit source venue with the exact UUID's atomic identity.

    Given: A positively classified Kraken currency whose UUID resolves to another venue.
    When: Paper admission verifies that exact source identity.
    Then: It refuses the conflicting catalog response.
    """
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    repo = _repo(_spec(), actual_exchange)
    refusal = await manual_order_execution_refusal(repo, _terms("kraken"), _NOW)
    assert refusal is not None
    assert refusal.error_code == "paper_source_identity_unavailable"
    repo.resolve_source_instrument_public_id.assert_not_awaited()
    repo.get_active_instrument_valuation_metadata.assert_awaited_once_with("exact-source-uuid")


@pytest.mark.asyncio
async def test_positive_kraken_currency_class_retains_spot_paper_with_missing_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve the existing explicitly classified Kraken currency cash profile.

    Given: A source UUID whose atomic metadata proves Kraken and class lookup proves currency.
    When: Its optional specification is absent and no contradictory contract evidence exists.
    Then: Spot paper remains available without guessing from its native spelling.
    """
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    repo = _repo(None, "kraken")
    assert await manual_order_execution_refusal(repo, _terms("kraken"), _NOW) is None
    repo.resolve_source_instrument_public_id.assert_not_awaited()
    repo.get_active_instrument_valuation_metadata.assert_awaited_once_with("exact-source-uuid")


@pytest.mark.asyncio
async def test_currency_class_never_overrides_contradictory_source_contract_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inspect exact source specifications even when the route names a known Spot venue.

    Given: A stale positive currency class but contract-count metadata on the source UUID.
    When: The order remaps from Kraken to paper.
    Then: The contradictory contract evidence refuses the cash-only model.
    """
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    spec = cast(InstrumentSpecRow, {**_spec(), "quantity_unit": "contract_count"})
    refusal = await manual_order_execution_refusal(_repo(spec, "kraken"), _terms("kraken"), _NOW)
    assert refusal is not None
    assert refusal.error_code == "derivative_accounting_unavailable"


@pytest.mark.parametrize("source", ["kraken", "paper"])
@pytest.mark.asyncio
async def test_unknown_kraken_class_does_not_default_to_a_currency_profile(
    monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    """Keep an unknown class from becoming a supported paper cash instrument.

    Given: An exact Kraken UUID whose native class lookup is unknown.
    When: Shared paper admission checks an explicit source or resolves a paper mirror.
    Then: It refuses rather than treating the lack of token classification as currency proof.
    """
    lookup = MagicMock(side_effect=ValueError("Unknown exact Kraken catalog class"))
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lookup)
    repo = _repo(None, "kraken")
    refusal = await manual_order_execution_refusal(repo, _terms(source), _NOW)
    assert refusal is not None
    assert refusal.error_code == "paper_source_identity_unavailable"
    if source == "paper":
        repo.resolve_source_instrument_public_id.assert_awaited_once_with("exact-paper-uuid")
        repo.get_active_instrument_valuation_metadata.assert_awaited_once_with("exact-source-uuid")
    else:
        repo.resolve_source_instrument_public_id.assert_not_awaited()
        repo.get_active_instrument_valuation_metadata.assert_not_awaited()


@pytest.mark.parametrize("kind", ["spot", "etf"])
@pytest.mark.asyncio
async def test_polygon_cash_paper_requires_an_explicit_compatible_source_kind(kind: str) -> None:
    """Retain a Polygon cash share or ETF only with explicit source profile evidence.

    Given: An exact Polygon UUID and an attested spot or ETF kind with no contract tuple.
    When: Its order selects the cash-only paper simulator.
    Then: Admission succeeds without treating a legacy equity label as proof.
    """
    spec = cast(InstrumentSpecRow, {**_spec(), "instrument_kind": kind})
    assert await manual_order_execution_refusal(_repo(spec), _terms("polygon"), _NOW) is None


@pytest.mark.asyncio
async def test_polygon_unattested_profile_and_fcm_contract_profile_do_not_default_to_cash() -> None:
    """Keep missing cash evidence and known FCM contracts outside paper cash accounting.

    Given: An unattested Polygon source or a Kraken FCM future source.
    When: Each exact source UUID selects paper execution.
    Then: Unknown Polygon profile and known FCM accounting receive explicit refusals.
    """
    unknown = cast(InstrumentSpecRow, {**_spec(), "instrument_kind": None})
    refusal = await manual_order_execution_refusal(_repo(unknown), _terms("polygon"), _NOW)
    assert refusal is not None
    assert refusal.error_code == "paper_source_identity_unavailable"
    future = cast(InstrumentSpecRow, {**_spec(), "instrument_kind": "future"})
    refusal = await manual_order_execution_refusal(
        _repo(future, "kraken_equities"), _terms("kraken_equities"), _NOW
    )
    assert refusal is not None
    assert refusal.error_code == "derivative_accounting_unavailable"


@pytest.mark.parametrize(
    "exchange,base,quote",
    [
        ("unknown_venue", "SHARE", "USD"),
        ("kraken_equities", "SHARE", "USD"),
        ("polygon", "", "USD"),
        ("polygon", "SHARE", " "),
        ("polygon", "USD", "USD"),
        ("polygon", " SHARE", "USD"),
        ("polygon", "SHARE", "USD "),
    ],
)
@pytest.mark.asyncio
async def test_unknown_venues_and_incompatible_currency_dimensions_refuse(
    exchange: str, base: str, quote: str
) -> None:
    """Require a known cash source and compatible explicit base and quote dimensions.

    Given: An unsupported source venue or missing, padded or identical currency legs.
    When: Direct paper admission checks its exact mapped source UUID.
    Then: It refuses without repairing the metadata or inventing another cash profile.
    """
    repo = _repo(_spec(), exchange)
    repo.get_active_instrument_valuation_metadata.return_value = {
        "exchange": exchange,
        "base": base,
        "quote": quote,
        "spec": _spec(),
    }
    refusal = await manual_order_execution_refusal(repo, _terms(), _NOW)
    assert refusal is not None
    assert refusal.error_code == "paper_source_identity_unavailable"
