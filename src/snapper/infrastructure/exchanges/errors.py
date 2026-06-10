"""Cross-venue exchange error types shared by executor and venue clients.

The single class here exists for one reason: order submission is the
only non-idempotent venue mutation in the system, and its transport
failures split into two safety classes that demand opposite handling.
A failure that provably happened BEFORE the request left the process
(credentials missing, circuit breaker open, symbol mapping error) may
be treated as a definitive rejection. A failure AFTER the send may mean
the venue accepted and even filled the order although the response was
lost — treating that as rejected fabricates wrong state: the engine
clears its in-flight intent, can re-emit a replacement order, and the
live "ghost" doubles real exposure (#145 audit, gap P0-1).

Venue clients wrap only the genuinely ambiguous failures of their
``create_order`` call in :class:`AmbiguousOrderSubmitError`; everything
else keeps its native type. The executor maps the wrapper to the
non-terminal UNKNOWN order event and venue verification instead of the
REJECTED path.
"""


class AmbiguousOrderSubmitError(RuntimeError):
    """Order submit failed in a way where the venue MAY have the order.

    Raised by venue client ``create_order`` implementations when the
    request may have reached the exchange but no usable response came
    back (request timeout, connection reset after send, 5xx from a
    gateway, unparseable body on an HTTP 200). The original transport
    error is chained as ``__cause__`` via ``raise ... from``.

    Attributes:
        client_order_id: Idempotency identity of the submit; the
            executor verifies this id against the venue before deciding
            the order's fate.
        instrument: Native instrument symbol of the submit, carried for
            log and alert context.
    """

    def __init__(self, client_order_id: str, instrument: str, message: str) -> None:
        """Initialize the ambiguous-submit error.

        Args:
            client_order_id: Client order id the submit was sent with.
            instrument: Native instrument symbol of the order.
            message: Human-readable description of the failure.
        """
        super().__init__(message)
        self.client_order_id = client_order_id
        self.instrument = instrument
