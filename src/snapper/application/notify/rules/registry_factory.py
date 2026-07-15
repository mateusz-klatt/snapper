"""Factory for the default ``RuleRegistry`` wired with all default rules.

Kept in a dedicated module so the top-level rule-class imports live
here instead of in ``rules.base``, where they would form a circular
dependency (every rule module imports ``AlertRule`` from ``base``).
"""

from snapper.application.notify.rules.base import RuleRegistry
from snapper.application.notify.rules.critical_system_error import CriticalSystemErrorRule
from snapper.application.notify.rules.margin_warning import MarginWarningRule
from snapper.application.notify.rules.order_fill_full import OrderFillFullRule
from snapper.application.notify.rules.order_rejected import OrderRejectedRule
from snapper.application.notify.rules.order_unknown import OrderUnknownRule
from snapper.application.notify.rules.portfolio_drift import PortfolioDriftRule
from snapper.application.notify.rules.position_stop_loss_fired import PositionStopLossFiredRule


def load_default_registry() -> RuleRegistry:
    """Build the default registry with all seven default alert rules.

    ``MarginWarningRule`` is registered before ``OrderRejectedRule``
    so the longest-prefix tiebreak (when both share the
    ``orders.events.`` prefix) is irrelevant — the two rules
    partition the rejected-events space via the shared
    ``is_margin_related_rejection`` predicate, so registration order
    is documentation, not behaviour. ``OrderUnknownRule`` likewise
    shares the prefix but fires only on the ``.unknown``
    suffix, disjoint from every other rule.

    Returns:
        A freshly-instantiated ``RuleRegistry`` with the rules
        registered in canonical order — ``order_fill_full``,
        ``margin_warning``, ``order_rejected``, ``order_unknown``,
        ``position_stop_loss_fired``, ``drift``,
        ``critical_system_error``.
    """
    registry = RuleRegistry()
    registry.register(OrderFillFullRule())
    registry.register(MarginWarningRule())
    registry.register(OrderRejectedRule())
    registry.register(OrderUnknownRule())
    registry.register(PositionStopLossFiredRule())
    registry.register(PortfolioDriftRule())
    registry.register(CriticalSystemErrorRule())
    return registry
