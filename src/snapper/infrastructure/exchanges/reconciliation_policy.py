"""Concrete-adapter policy for portfolio reconciliation classification.

Policy is registered against exact exchange identifiers and concrete exchange
client classes.  Provisioning callers resolve the registered class first and
then read its policy, so an exchange-looking string or an account capability
cannot silently select a reconciliation method.
"""

from dataclasses import dataclass
from typing import Final

from snapper.application.portfolio.reconciliation_methods import PortfolioAccountMode
from snapper.application.portfolio.reconciliation_methods import RealPortfolioReconciliationMethod
from snapper.core.types import ExchangeEnum
from snapper.infrastructure.exchanges.base import ExchangeClientBase
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


@dataclass(frozen=True, slots=True)
class ReconciliationMethodPolicy:
    """Allowed reconciliation methods proven by one concrete adapter class."""

    structural_default: RealPortfolioReconciliationMethod | None
    allowed: frozenset[RealPortfolioReconciliationMethod]


@dataclass(frozen=True, slots=True)
class RegisteredReconciliationAdapter:
    """Exact exchange registration for one concrete account adapter."""

    adapter_class: type[ExchangeClientBase]
    policy: ReconciliationMethodPolicy


_FUTURES_POSITION: Final[RealPortfolioReconciliationMethod] = "futures_position"
_SPOT_EXECUTION_REPLAY: Final[RealPortfolioReconciliationMethod] = "spot_execution_replay"
_MARGIN_LEDGER_REPLAY: Final[RealPortfolioReconciliationMethod] = "margin_ledger_replay"

_NO_RECONCILIATION_POLICY: Final = ReconciliationMethodPolicy(
    structural_default=None,
    allowed=frozenset(),
)

_REGISTERED_RECONCILIATION_ADAPTERS: Final[dict[str, RegisteredReconciliationAdapter]] = {
    ExchangeEnum.KRAKEN.value: RegisteredReconciliationAdapter(
        adapter_class=KrakenExchangeClient,
        policy=ReconciliationMethodPolicy(
            structural_default=None,
            allowed=frozenset({_SPOT_EXECUTION_REPLAY, _MARGIN_LEDGER_REPLAY}),
        ),
    ),
    ExchangeEnum.KRAKEN_FUTURES.value: RegisteredReconciliationAdapter(
        adapter_class=KrakenFuturesExchangeClient,
        policy=ReconciliationMethodPolicy(
            structural_default=_FUTURES_POSITION,
            allowed=frozenset({_FUTURES_POSITION}),
        ),
    ),
    ExchangeEnum.WALUTOMAT.value: RegisteredReconciliationAdapter(
        adapter_class=WalutomatExchangeClient,
        policy=ReconciliationMethodPolicy(
            structural_default=_SPOT_EXECUTION_REPLAY,
            allowed=frozenset({_SPOT_EXECUTION_REPLAY}),
        ),
    ),
    ExchangeEnum.PAPER.value: RegisteredReconciliationAdapter(
        adapter_class=PaperExchangeClient,
        policy=_NO_RECONCILIATION_POLICY,
    ),
    ExchangeEnum.KRAKEN_EQUITIES.value: RegisteredReconciliationAdapter(
        adapter_class=KrakenEquitiesExchangeClient,
        policy=_NO_RECONCILIATION_POLICY,
    ),
    ExchangeEnum.POLYGON.value: RegisteredReconciliationAdapter(
        adapter_class=PolygonExchangeClient,
        policy=_NO_RECONCILIATION_POLICY,
    ),
}
_RECONCILIATION_POLICIES_BY_ADAPTER_CLASS: Final[
    dict[type[ExchangeClientBase], ReconciliationMethodPolicy]
] = {
    registration.adapter_class: registration.policy
    for registration in _REGISTERED_RECONCILIATION_ADAPTERS.values()
}


def account_mode_for_exchange(exchange: str) -> PortfolioAccountMode:
    """Mirror executor account-mode discrimination for one exact venue.

    Args:
        exchange: Canonical exchange identifier.

    Returns:
        ``paper`` only for the exact paper venue; otherwise ``live``.
    """
    if exchange == ExchangeEnum.PAPER.value:
        return "paper"
    return "live"


def get_registered_reconciliation_adapter(
    exchange: str,
) -> RegisteredReconciliationAdapter | None:
    """Return the exact registered concrete adapter for an exchange.

    Args:
        exchange: Canonical exchange identifier.

    Returns:
        Registered concrete adapter metadata, or ``None`` when unregistered.
    """
    return _REGISTERED_RECONCILIATION_ADAPTERS.get(exchange)


def get_reconciliation_method_policy(exchange: str) -> ReconciliationMethodPolicy:
    """Return concrete-adapter policy, defaulting unknown adapters to empty.

    Args:
        exchange: Canonical exchange identifier.

    Returns:
        The registered adapter policy or the fail-closed empty policy.
    """
    registration = get_registered_reconciliation_adapter(exchange)
    if registration is None:
        return _NO_RECONCILIATION_POLICY
    return _RECONCILIATION_POLICIES_BY_ADAPTER_CLASS[registration.adapter_class]


def is_reconciliation_method_allowed(
    exchange: str,
    method: RealPortfolioReconciliationMethod,
) -> bool:
    """Return whether the registered concrete adapter proves ``method``.

    Args:
        exchange: Canonical exchange identifier.
        method: Real reconciliation method proposed by an operator.

    Returns:
        Whether the exact registered adapter allows the proposed method.
    """
    return method in get_reconciliation_method_policy(exchange).allowed
