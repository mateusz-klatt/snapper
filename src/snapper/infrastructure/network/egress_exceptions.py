"""Exceptions raised by the egress pool subsystem."""


class AllRoutesQuarantinedError(RuntimeError):
    """Raised by ``EgressPool.reserve`` when no usable route is available.

    Fires only when ``on_all_quarantined == "raise"`` AND every
    enabled route is currently inside its quarantine window. The
    reconnect-storm watchdog will treat the raise as a
    connect failure and eventually rebuild the WS connection.

    With the default ``on_all_quarantined == "wait"`` setting the
    pool returns the direct route (quarantined or not) so the SDK's
    ``_patched_get_reconnect_wait`` can translate the situation
    into a single long sleep via
    ``EgressPool.earliest_release_in_seconds()``.
    """
