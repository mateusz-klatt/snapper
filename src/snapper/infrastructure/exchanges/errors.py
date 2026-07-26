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
live "ghost" doubles real exposure.

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
        client_order_id: Correlation id the submit was sent with,
            carried for log and alert context only. Every adapter in
            this repo passes ``request.client_order_id`` unmodified, and
            that equality is an ADAPTER INVARIANT — a convention this
            class does not and cannot enforce. Nothing checks it: the
            constructor accepts any string, and ``ExchangeOrderRequest``
            constrains only what an adapter is GIVEN, never what it
            chooses to raise with. Keep the invariant when adding a
            venue. NOTHING in ``src`` reads this attribute, and in
            particular the executor does NOT verify against it:
            ``_verify_ambiguous_submit`` queries the venue with
            ``order.client_order_id``, taken from the core order it is
            resolving. So a divergent value here would not merely be
            cosmetic-but-harmless — it would be an invisible lie in the
            logs and alerts an operator reaches for first, pointing at
            an id the recovery path never used. Do not treat this field
            as the thing that decides the order's fate; treating it that
            way is what made a fabricated venue id look survivable.
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


class RestPoolDispatchError(RuntimeError):
    """Scheduling onto the bounded REST pool failed after possible enqueue.

    Raised by ``ExchangeClientBase._dispatch_blocking`` when the
    synchronous scheduling step fails with anything OTHER than the
    pre-submit refusals (closed-pool lifecycle guard, executor-shutdown
    race): ``ThreadPoolExecutor.submit`` enqueues the work item BEFORE
    spawning a worker thread, so a failure like ``can't start new
    thread`` leaves the callable queued and it may still execute once a
    busy worker frees. Submit paths must therefore treat this as an
    ambiguous outcome (wrap in :class:`AmbiguousOrderSubmitError`), never
    as a definitive reject; read-only paths may treat it as an ordinary
    failure because a delayed duplicate read is harmless.
    """


class CircuitBreakerOpenError(RuntimeError):
    """Order submit refused locally because the venue circuit breaker is open.

    Raised BEFORE any request leaves the process, so it is an
    authoritative not-submitted signal — but NOT a venue rejection: the
    executor gives it a distinct disposition (durable
    ``order_breaker_open`` event + command FAILED) instead of the
    ``order_rejected`` path, because a rejected command may legally be
    re-published by the outbox while a breaker-open one must wait for
    the engine to decide anew after its intent is released.
    """
