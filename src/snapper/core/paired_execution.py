"""Canonical key helpers for the paired-execution guard.

The paired-execution arming barrier validates a group's full leg set from
any single leg by comparing the sorted per-leg ``{exchange}:{instrument}:{mode}``
tokens against the ``group_key`` stamped by the originating strategy. The
formula MUST be byte-identical between the strategy that emits the key
(``BaseStrategy``) and the coordinator that validates it
(``try_arm_paired_execution_group_if_complete``), exactly like
``compute_shard_key``. Keeping it in one leaf module prevents drift that would
either fail every arming (legs never match the key) or, worse, let a wrong leg
set arm.

The token components are plain strings: callers pass exchange/mode enum
members (which are ``str`` subclasses) at emission time and the persisted
``str`` leg columns at validation time, so a ``str`` signature serves both
without casts while the values themselves remain the canonical enum strings.
"""


def paired_group_leg_token(exchange: str, instrument: str, mode: str) -> str:
    """Return the canonical single-leg token ``{exchange}:{instrument}:{mode}``.

    Args:
        exchange: Order-capable exchange identifier for the leg.
        instrument: The traded symbol (e.g. ``"BTC-USD"``).
        mode: Execution mode — ``"live"`` or ``"paper"``.

    Returns:
        The canonical leg token used to build a ``group_key``.
    """
    return f"{exchange}:{instrument}:{mode}"


def compute_paired_group_key(legs: list[tuple[str, str, str]]) -> str:
    """Return the canonical ``group_key`` for a paired-execution leg set.

    The key is the sorted per-leg tokens joined with ``"|"``. Sorting makes
    the key independent of leg emission/arrival order so any coordinator can
    reconstruct and compare it.

    Args:
        legs: One ``(exchange, instrument, mode)`` tuple per group leg.

    Returns:
        The canonical, order-independent group key.
    """
    tokens = sorted(
        paired_group_leg_token(exchange, instrument, mode) for exchange, instrument, mode in legs
    )
    return "|".join(tokens)


PAIRED_HALT_REASON_PREFIX = "paired-execution:"
"""Prefix shared by every :func:`paired_halt_reason` key.

The guard scanner's quiet-halt sweep enumerates a shard's in-memory halt
reasons by this prefix to find the PAIRED ones, then releases exactly those
whose scope no longer has an active durable halt — so the prefix and the key
builder must stay in lockstep (the builder interpolates this constant).
"""


def paired_halt_reason(wallet_public_id: str, strategy_id: str, group_key: str) -> str:
    """Return the canonical in-memory shard-halt reason for a paired halt scope.

    The reason doubles as the SELECTIVE un-halt key for the reason-scoped
    ``TradeService`` shard halts: the guard scanner's halt mirror,
    the startup recovery mirror, and the completion un-halt MUST all derive the
    byte-identical string or a completed pair would never release its shards.
    Keyed per HALT SCOPE — ``(wallet, strategy, group_key)`` — matching the
    durable ``paired_execution_halts`` active-unique constraint, NOT per group:
    two groups sharing a scope share one durable halt, so their mirrors must
    share one key, and the scope-quiet clear releases it exactly once. A shard
    hosting legs of two DIFFERENT scopes carries two distinct reasons, so
    completing one scope leaves the other halted.

    Args:
        wallet_public_id: Wallet scope component of the durable halt.
        strategy_id: Strategy scope component of the durable halt.
        group_key: Canonical pair key scope component of the durable halt.

    Returns:
        The canonical reason string / selective un-halt key for the scope.
    """
    return f"{PAIRED_HALT_REASON_PREFIX}{wallet_public_id}:{strategy_id}:{group_key}"
