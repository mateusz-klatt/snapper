"""Factory for the default ``RuleRegistry`` wired with the 4 P0 rules.

Kept in a dedicated module so the top-level rule-class imports live
here instead of in ``rules.base``, where they would form a circular
dependency (every rule module imports ``AlertRule`` from ``base``).
"""

from snapper.application.notify.rules.base import RuleRegistry
from snapper.application.notify.rules.critical_system_error import CriticalSystemErrorRule
from snapper.application.notify.rules.order_fill_full import OrderFillFullRule
from snapper.application.notify.rules.order_rejected import OrderRejectedRule
from snapper.application.notify.rules.position_stop_loss_fired import PositionStopLossFiredRule


def load_default_registry() -> RuleRegistry:
    """Build the default v1.11 registry with all four P0 rules.

    Returns:
        A freshly-instantiated ``RuleRegistry`` with the four v1.11
        P0 rules registered in canonical order — ``order_fill_full``,
        ``order_rejected``, ``position_stop_loss_fired``,
        ``critical_system_error``.
    """
    registry = RuleRegistry()
    registry.register(OrderFillFullRule())
    registry.register(OrderRejectedRule())
    registry.register(PositionStopLossFiredRule())
    registry.register(CriticalSystemErrorRule())
    return registry
