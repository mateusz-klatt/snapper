"""Refuse products whose accounting Snapper cannot yet execute consistently."""

from snapper.application.trade.execution_modifiers import ModifierRefusal
from snapper.core.types import ExchangeEnum
from snapper.infrastructure.symbols.functions import native_to_kraken_asset_class


def product_execution_refusal(instrument: str, execution_exchange: str) -> ModifierRefusal | None:
    """Describe an implemented-accounting barrier independently of cached capabilities.

    These refusals describe Snapper's execution support, not venue availability,
    account eligibility, or the caller's permission to invest. They apply only
    to creation; cancellation and reconciliation remain available.

    Args:
        instrument: Native product identity already resolved by the caller.
        execution_exchange: Actual execution venue, including paper when selected.

    Returns:
        The accounting refusal, or no product refusal for other execution paths.
    """
    if execution_exchange == ExchangeEnum.KRAKEN_FUTURES:
        return ModifierRefusal(
            error_code="derivative_accounting_unavailable",
            message=(
                "Snapper cannot send derivative orders until contract payoff, cash, "
                "margin, and engine accounting are integrated."
            ),
            details={
                "instrument": instrument,
                "exchange": execution_exchange,
                "product_type": "derivative",
            },
        )
    if execution_exchange != ExchangeEnum.KRAKEN:
        return None
    asset_class: str | None
    try:
        asset_class = native_to_kraken_asset_class(instrument)
    except ValueError:
        asset_class = None
    if asset_class == "currency":
        return None
    if asset_class != "tokenized_asset":
        return ModifierRefusal(
            error_code="execution_product_identity_unavailable",
            message=(
                "Snapper cannot send Kraken orders until the active product identity "
                "is classified by the venue catalog."
            ),
            details={
                "instrument": instrument,
                "exchange": execution_exchange,
                "product_type": "unknown",
            },
        )
    return ModifierRefusal(
        error_code="tokenized_unit_accounting_unavailable",
        message=(
            "Snapper cannot send tokenized-asset orders until immutable quantity units "
            "and corporate-action rebase accounting are integrated."
        ),
        details={
            "instrument": instrument,
            "exchange": execution_exchange,
            "product_type": "tokenized_asset",
        },
    )
