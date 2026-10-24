"""Validate tokenized cash order terms against exact authoritative catalog evidence."""

from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import InvalidOperation
from fractions import Fraction
from typing import Final

from snapper.application.trade.execution_modifiers import ModifierRefusal
from snapper.application.trade.execution_products import product_execution_refusal
from snapper.core.json_types import JsonObject
from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentOrderCapabilityRow
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import InstrumentValuationMetadataRow
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE
from snapper.infrastructure.exchanges.tokenized_spec_provenance import TOKENIZED_MAX_DECIMALS
from snapper.infrastructure.exchanges.tokenized_spec_provenance import TOKENIZED_SPEC_ETL_VERSION
from snapper.infrastructure.exchanges.tokenized_spec_provenance import TOKENIZED_SPEC_SOURCE
from snapper.infrastructure.symbols.functions import native_to_kraken_asset_class

_SPEC_MAX_AGE: Final[timedelta] = timedelta(hours=12)
_PRICE_FIELDS: Final[dict[str, tuple[str, ...]]] = {
    "market": (),
    "limit": ("price",),
    "stop": ("stop_price",),
    "stop_limit": ("price", "stop_price"),
}


@dataclass(frozen=True)
class ManualOrderTerms:
    """Exact identity, dimensions and modifiers of a validated manual order.

    Attributes:
        exchange: Resolved execution venue; paper replay supplies the paper venue.
        instrument: Exact catalog-native symbol selected by the caller.
        instrument_public_id: Exact instrument UUID bound to that symbol and venue.
        order_type: Manual order vocabulary: market, limit, stop or stop_limit.
        quantity: Requested base-asset quantity as a Decimal, without rounding.
        price: Optional limit price in quote currency per base unit.
        stop_price: Optional stop trigger in quote currency per base unit.
        leverage: Optional margin leverage requested by the caller.
        reduce_only: Whether the caller requests a position-reducing order.
        source_exchange: Original venue whose exact instrument UUID was resolved.
    """

    exchange: str
    instrument: str
    instrument_public_id: str
    order_type: str
    quantity: Decimal
    price: Decimal | None
    stop_price: Decimal | None
    leverage: int | None
    reduce_only: bool
    source_exchange: str | None = None


@dataclass(frozen=True)
class _TokenizedLimits:
    """Complete catalog limits for one exact tokenized cash instrument."""

    quantity_step: Decimal
    minimum_quantity: Decimal
    maximum_quantity: Decimal | None
    price_step: Decimal
    minimum_notional: Decimal
    supported_order_types: tuple[str, ...]


def _tokenized_cash_instrument(terms: ManualOrderTerms) -> bool:
    """Identify a tokenized Spot product solely from its active catalog mapping.

    Args:
        terms: Exact manual-order identity selected by the caller.

    Returns:
        Whether the mapping proves this is a tokenized Kraken cash product.
        Unknown mappings remain subject to the caller's existing identity and
        tradability guards; absence never grants permission to trade.
    """
    if terms.exchange != ExchangeEnum.KRAKEN:
        return False
    try:
        return native_to_kraken_asset_class(terms.instrument) == "tokenized_asset"
    except ValueError:
        return False


def _refusal(
    terms: ManualOrderTerms,
    code: str,
    message: str,
    details: JsonObject | None = None,
) -> ModifierRefusal:
    """Attach exact instrument identity to a transport-independent refusal.

    Args:
        terms: Identity of the order being refused.
        code: Stable machine-readable reason.
        message: Truthful explanation of the rejected terms.
        details: Additional sizing evidence useful to the caller.

    Returns:
        A refusal that REST and MCP can format consistently.
    """
    return ModifierRefusal(
        error_code=code,
        message=message,
        details={
            "exchange": terms.exchange,
            "symbol": terms.instrument,
            "instrument_public_id": terms.instrument_public_id,
            **(details or {}),
        },
    )


def _positive_catalog_decimal(value: object) -> Decimal | None:
    """Parse a positive catalog number through its decimal representation.

    Args:
        value: A persisted catalog numeric operand.

    Returns:
        A finite positive Decimal, or null when metadata is unusable.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _precision_valid(value: object) -> bool:
    """Accept only authoritative bounded integer decimal-place counts.

    Args:
        value: Catalog precision to validate without boolean coercion.

    Returns:
        Whether the precision is an integer within the producer's supported range.
    """
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= TOKENIZED_MAX_DECIMALS
    )


def _observation_fresh(spec: InstrumentSpecRow, as_of: datetime) -> bool:
    """Require an aware, nonfuture catalog observation less than twelve hours old.

    Args:
        spec: Exact specification whose observation is being evaluated.
        as_of: Aware evaluation time shared with the catalog reads.

    Returns:
        Whether the observation is usable at the evaluation instant.
    """
    observed = spec["spec_observed_at"]
    if observed is None or observed.utcoffset() is None or as_of.utcoffset() is None:
        return False
    return timedelta(0) <= as_of - observed < _SPEC_MAX_AGE


def _specification_valid(spec: InstrumentSpecRow, terms: ManualOrderTerms, as_of: datetime) -> bool:
    """Verify direct token provenance, dimensions, current status and freshness.

    Spot's unit_certified flag is intentionally irrelevant here because that
    flag certifies derivative reconciliation units, not tokenized cash trading.

    Args:
        spec: Active specification returned for the exact requested instrument.
        terms: Order identity that owns the specification.
        as_of: Evaluation time for observation freshness.

    Returns:
        Whether the specification supplies authoritative current token evidence.
    """
    identity_and_source = (
        spec["instrument_public_id"] == terms.instrument_public_id,
        spec["spec_source"] == TOKENIZED_SPEC_SOURCE,
        (spec["spec_version"] or "").startswith(f"{TOKENIZED_SPEC_ETL_VERSION}:"),
        spec["quantity_unit"] == "base_asset",
        spec["status"] == "online",
    )
    precisions = (spec["cost_decimals"], spec["qty_decimals"])
    return (
        all(identity_and_source)
        and all(_precision_valid(value) for value in precisions)
        and _observation_fresh(spec, as_of)
    )


def _maximum_quantity(
    spec: InstrumentSpecRow, capability: InstrumentOrderCapabilityRow
) -> tuple[bool, Decimal | None]:
    """Resolve optional catalog maxima while refusing malformed observed limits.

    Args:
        spec: Instrument specification with an optional quantity maximum.
        capability: Exact capability row with an optional client order maximum.

    Returns:
        Metadata validity and the stricter observed maximum, if one exists.
    """
    maxima: list[Decimal] = []
    for raw in (spec["max_order_size"], capability["max_order_size"]):
        if raw is None:
            continue
        parsed = _positive_catalog_decimal(raw)
        if parsed is None:
            return False, None
        maxima.append(parsed)
    return True, min(maxima, default=None)


def _catalog_limits(
    spec: InstrumentSpecRow, capability: InstrumentOrderCapabilityRow
) -> _TokenizedLimits | None:
    """Resolve complete positive sizing operands without guessing missing values.

    Args:
        spec: Exact authoritative instrument specification.
        capability: Unambiguous capability row for the same instrument and venue.

    Returns:
        Immutable validated limits, or null for incomplete or conflicting metadata.
    """
    step = _positive_catalog_decimal(spec["lot_size"])
    minimum = _positive_catalog_decimal(spec["min_order_size"])
    tick = _positive_catalog_decimal(spec["tick_size"])
    cost = _positive_catalog_decimal(capability["min_notional"])
    maximum_valid, maximum = _maximum_quantity(spec, capability)
    if step is None or minimum is None or tick is None or cost is None:
        return None
    if not maximum_valid or (maximum is not None and maximum < minimum):
        return None
    return _TokenizedLimits(
        quantity_step=step,
        minimum_quantity=minimum,
        maximum_quantity=maximum,
        price_step=tick,
        minimum_notional=cost,
        supported_order_types=tuple(capability["supported_order_types"]),
    )


async def _load_tokenized_limits(
    repo: Repository, terms: ManualOrderTerms, as_of: datetime
) -> _TokenizedLimits | None:
    """Bind all sizing evidence to the exact requested venue instrument identity.

    Args:
        repo: Repository used for temporal catalog reads.
        terms: Exact identity presented by the manual order.
        as_of: Shared catalog evaluation time.

    Returns:
        Verified limits, or null for missing, stale or ambiguous catalog evidence.
    """
    public_id = await repo.get_instrument_public_id_by_symbol(
        terms.instrument, terms.exchange, as_of
    )
    if public_id is None or public_id != terms.instrument_public_id:
        return None
    spec = await repo.get_instrument_spec(public_id, as_of)
    if spec is None or not _specification_valid(spec, terms, as_of):
        return None
    capabilities = await repo.get_instrument_capabilities(
        as_of, exchange=terms.exchange, instrument_public_id=public_id
    )
    if len(capabilities) != 1:
        return None
    capability = capabilities[0]
    if capability["instrument_public_id"] != public_id or capability["exchange"] != terms.exchange:
        return None
    return _catalog_limits(spec, capability)


def _positive_order_decimal(value: object) -> bool:
    """Validate a positive finite Decimal without coercing a manual order operand.

    Args:
        value: Quantity or price provided by the manual order.

    Returns:
        Whether the operand is a positive finite Decimal.
    """
    return isinstance(value, Decimal) and value.is_finite() and value > 0


def _is_multiple(value: Decimal, step: Decimal) -> bool:
    """Compare exact Decimal ratios independently of the caller's precision context.

    Args:
        value: Positive finite order quantity or price.
        step: Positive finite authoritative increment.

    Returns:
        Whether the value is an exact integer multiple of the increment.
    """
    return Fraction(value) % Fraction(step) == 0


def _quantity_refusal(terms: ManualOrderTerms, limits: _TokenizedLimits) -> ModifierRefusal | None:
    """Refuse malformed, undersized, oversized or nonintegral token quantities.

    Args:
        terms: Exact manual token order terms.
        limits: Authoritative sizing limits for that instrument.

    Returns:
        A quantity refusal, or null when the requested quantity is valid.
    """
    if not _positive_order_decimal(terms.quantity):
        return _refusal(
            terms, "order_quantity_invalid", "Quantity must be a positive finite Decimal."
        )
    if terms.quantity < limits.minimum_quantity:
        return _refusal(
            terms,
            "order_quantity_below_minimum",
            "Quantity is below the catalog minimum for this tokenized instrument.",
            {"minimum_quantity": str(limits.minimum_quantity)},
        )
    if limits.maximum_quantity is not None and terms.quantity > limits.maximum_quantity:
        return _refusal(
            terms,
            "order_quantity_above_maximum",
            "Quantity exceeds the catalog maximum for this tokenized instrument.",
            {"maximum_quantity": str(limits.maximum_quantity)},
        )
    if not _is_multiple(terms.quantity, limits.quantity_step):
        return _refusal(
            terms,
            "order_quantity_step_mismatch",
            "Quantity must be an exact multiple of the catalog quantity increment.",
            {"quantity_increment": str(limits.quantity_step)},
        )
    return None


def _price_refusal(terms: ManualOrderTerms, limits: _TokenizedLimits) -> ModifierRefusal | None:
    """Check applicable price ticks and exact limit-order minimum quote cost.

    Market and stop-market execution prices are unknown, so these orders do not
    receive a fabricated minimum-cost verdict from their absent limit leg.

    Args:
        terms: Exact token order terms with a client-supported order type.
        limits: Authoritative price increment and minimum quote-currency notional.

    Returns:
        A price or minimum-notional refusal, or null when applicable checks pass.
    """
    requested = {"price": terms.price, "stop_price": terms.stop_price}
    validated: dict[str, Decimal] = {}
    for name in _PRICE_FIELDS[terms.order_type]:
        price = requested[name]
        if not isinstance(price, Decimal) or not _positive_order_decimal(price):
            return _refusal(
                terms, "order_price_invalid", f"{name} must be a positive finite Decimal."
            )
        if not _is_multiple(price, limits.price_step):
            return _refusal(
                terms,
                "order_price_step_mismatch",
                f"{name} must be an exact multiple of the catalog price increment.",
                {"field": name, "price_increment": str(limits.price_step)},
            )
        validated[name] = price
    if "price" in validated:
        notional = Fraction(terms.quantity) * Fraction(validated["price"])
        if notional < Fraction(limits.minimum_notional):
            return _refusal(
                terms,
                "order_notional_below_minimum",
                "The limit notional is below this instrument's catalog minimum quote cost.",
                {
                    "minimum_notional": str(limits.minimum_notional),
                    "notional_unit": "quote_currency",
                },
            )
    return None


async def manual_order_terms_refusal(
    repo: Repository, terms: ManualOrderTerms, as_of: datetime
) -> ModifierRefusal | None:
    """Refuse unsafe tokenized cash terms without changing ordinary or paper orders.

    This helper never grants account eligibility or live activation. Callers must
    retain their existing authorization, UUID, can_trade and admission checks.

    Args:
        repo: Repository used for exact temporal catalog reads.
        terms: Validated manual order identity, quantities, prices and modifiers.
        as_of: Shared aware evaluation time.

    Returns:
        A transport-independent refusal, or null when token-specific checks pass
        or the instrument is outside this helper's tokenized cash scope.
    """
    if not _tokenized_cash_instrument(terms):
        return None
    if terms.leverage is not None or terms.reduce_only:
        return _refusal(
            terms,
            "tokenized_cash_order_only",
            "Snapper's tokenized-asset cash execution path does not support margin or reduce-only requests.",
            {
                "leverage_requested": terms.leverage is not None,
                "reduce_only_requested": terms.reduce_only,
            },
        )
    limits = await _load_tokenized_limits(repo, terms, as_of)
    if limits is None:
        return _refusal(
            terms,
            "tokenized_order_metadata_unavailable",
            "Complete current tokenized sizing and order capabilities could not be resolved for this exact instrument.",
        )
    if (
        terms.order_type not in _PRICE_FIELDS
        or terms.order_type not in limits.supported_order_types
    ):
        return _refusal(
            terms,
            "order_type_unsupported",
            "This order type is not enabled by Snapper's cash path for the tokenized instrument.",
            {"supported_order_types": list(limits.supported_order_types)},
        )
    return _quantity_refusal(terms, limits) or _price_refusal(terms, limits)


def _paper_source_unavailable(terms: ManualOrderTerms) -> ModifierRefusal:
    """Refuse paper creation when no positively supported cash profile is proven.

    Args:
        terms: Exact manual identity whose cash-only paper profile is unknown.

    Returns:
        The shared refusal without treating absent evidence as permission.
    """
    return _refusal(
        terms,
        "paper_source_identity_unavailable",
        "Cash-only paper execution requires an authoritative source identity and a supported cash instrument profile.",
    )


async def _paper_source_metadata(
    repo: Repository, terms: ManualOrderTerms
) -> InstrumentValuationMetadataRow | None:
    """Resolve a paper mirror to its exact source UUID without symbol guessing.

    Args:
        repo: Existing source resolver and atomic current identity/spec reader.
        terms: Transport-validated original source or directly selected paper UUID.

    Returns:
        Current authoritative source metadata, or None for absent or contradictory identity.
    """
    public_id = terms.instrument_public_id
    expected_exchange = terms.source_exchange
    if expected_exchange in {None, ExchangeEnum.PAPER}:
        resolution = await repo.resolve_source_instrument_public_id(public_id)
        if not resolution["is_paper"] or not resolution["mapped"]:
            return None
        source_id = resolution["valuation_public_id"]
        if source_id == public_id:
            return None
        public_id = source_id
        expected_exchange = None
    metadata = await repo.get_active_instrument_valuation_metadata(public_id)
    if metadata is None or metadata["exchange"] == ExchangeEnum.PAPER:
        return None
    if expected_exchange is not None and metadata["exchange"] != expected_exchange:
        return None
    spec = metadata["spec"]
    if spec is not None and spec["instrument_public_id"] != public_id:
        return None
    return metadata


def _paper_spec_has_contract_evidence(spec: InstrumentSpecRow) -> bool:
    """Detect any explicit contradiction to cash-only paper quantity accounting.

    Args:
        spec: Exact source UUID specification already matched to its identity.

    Returns:
        Whether contract units, payoff, kind, multiplier or provenance require refusal.
    """
    return (
        spec["quantity_unit"] not in {None, "base_asset"}
        or spec.get("native_order_quantity_unit") not in {None, "base_asset"}
        or spec["instrument_kind"] not in {None, "spot", "etf"}
        or spec["contract_size"] is not None
        or any(
            spec.get(field) is not None
            for field in ("contract_payoff_model", "contract_value_currency", "settlement_currency")
        )
        or spec["spec_source"] == FUTURES_SPEC_SOURCE
    )


def _paper_cash_profile_proven(metadata: InstrumentValuationMetadataRow) -> bool:
    """Admit only explicit known cash profiles after contradictory evidence is excluded.

    The preceding product barrier accepts Kraken only after its authoritative
    class lookup positively returns currency. An unknown class refuses there.

    Args:
        metadata: Atomic exact-source identity and optional current specification.

    Returns:
        Whether this source positively proves the supported cash simulation assumption.
    """
    base, quote = metadata["base"], metadata["quote"]
    if not base.strip() or not quote.strip() or base == quote:
        return False
    if base != base.strip() or quote != quote.strip():
        return False
    if metadata["exchange"] == ExchangeEnum.KRAKEN:
        return True
    if metadata["exchange"] not in {ExchangeEnum.POLYGON, ExchangeEnum.WALUTOMAT}:
        return False
    spec = metadata["spec"]
    return spec is not None and spec["instrument_kind"] in {"spot", "etf"}


async def _paper_cash_profile_refusal(
    repo: Repository, terms: ManualOrderTerms
) -> ModifierRefusal | None:
    """Require a positively supported source profile for every manual paper order.

    Args:
        repo: Existing authoritative source and current catalog readers.
        terms: Already identity-validated order remapped to the cash-only simulator.

    Returns:
        A source, contract or token refusal, or None only for a proven cash profile.
    """
    metadata = await _paper_source_metadata(repo, terms)
    if metadata is None:
        return _paper_source_unavailable(terms)
    refusal = product_execution_refusal(terms.instrument, metadata["exchange"])
    if refusal is not None:
        if refusal.error_code == "execution_product_identity_unavailable":
            return _paper_source_unavailable(terms)
        return refusal
    spec = metadata["spec"]
    if spec is not None and _paper_spec_has_contract_evidence(spec):
        return _refusal(
            terms,
            "derivative_accounting_unavailable",
            "The cash-only paper simulator cannot model derivative cash, margin and contract positions.",
            {"product_type": "derivative", "source_exchange": metadata["exchange"]},
        )
    if spec is not None and spec["spec_source"] == TOKENIZED_SPEC_SOURCE:
        return _refusal(
            terms,
            "tokenized_unit_accounting_unavailable",
            "The cash-only paper simulator cannot model token unit and corporate-action rebase accounting.",
            {"product_type": "tokenized_asset", "source_exchange": metadata["exchange"]},
        )
    if not _paper_cash_profile_proven(metadata):
        return _paper_source_unavailable(terms)
    return None


async def manual_order_execution_refusal(
    repo: Repository, terms: ManualOrderTerms, as_of: datetime
) -> ModifierRefusal | None:
    """Apply product accounting support and exact catalog terms before admission.

    Args:
        repo: Repository supplying sizing evidence for supported products.
        terms: Validated order with its actual execution venue.
        as_of: Shared aware catalog evaluation time.

    Returns:
        A product or sizing refusal, or null when these checks permit admission.
        Authorization, identity, capability and risk checks remain required.
    """
    refusal = product_execution_refusal(terms.instrument, terms.exchange)
    if refusal is not None:
        return refusal
    if terms.source_exchange is not None and terms.source_exchange != terms.exchange:
        refusal = product_execution_refusal(terms.instrument, terms.source_exchange)
        if refusal is not None:
            if (
                terms.exchange == ExchangeEnum.PAPER
                and refusal.error_code == "execution_product_identity_unavailable"
            ):
                return _paper_source_unavailable(terms)
            return refusal
    if terms.exchange == ExchangeEnum.PAPER:
        refusal = await _paper_cash_profile_refusal(repo, terms)
        if refusal is not None:
            return refusal
    return await manual_order_terms_refusal(repo, terms, as_of)
