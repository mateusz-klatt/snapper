"""Exceptions raised by the egress pool subsystem."""


class AllRoutesQuarantinedError(RuntimeError):
    """No VPN route is available for public traffic or private policy refused a route."""
