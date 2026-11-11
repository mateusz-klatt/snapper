"""Reviewed Walutomat order-ownership guarantee for the spot anchor bootstrap.

The bootstrap classifies a seal-page row as owned when its order id lies in the
owned order set ``S`` and as external when a usable order id lies outside it.
Both readings rest on two venue premises: order ids are never reused across the
account's instruments, and every order placed through the API carries its order
id in the account history. Completeness of ``S`` comes from those premises
combined with local proofs — the frozen sentinel-tip Order population read after
the capture, and the strict unresolved-submit gate, which keeps an own order
whose best-effort persistence has not happened yet unresolved. The venue can
attest only to its own premises, never to the local database contents.

This module records the reviewed source of each venue premise as a frozen
artifact with a content-addressed version, in the manner of the documentary
precision artifact. :data:`WALUTOMAT_ORDER_OWNERSHIP_GUARANTEE` stays ``None``
until the venue's answer has been reviewed, and while it is ``None`` every
Walutomat seal refuses ``venue_ownership_guarantee_unrecorded``. Recording it is
a separate reviewed change; a non-null order id outside ``S`` certifies nothing
on its own.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Final

WALUTOMAT_ORDER_ID_NON_REUSE_PREMISE: Final = (
    "Walutomat never reuses an order id across the instruments of one account."
)
"""The first venue premise: an order id names one order for the whole account."""

WALUTOMAT_ORDER_ID_IN_HISTORY_PREMISE: Final = (
    "Every order placed through the Walutomat API carries its order id on its "
    "account-history rows."
)
"""The second venue premise: no API order can appear in history without its id."""


@dataclass(frozen=True)
class WalutomatOrderOwnershipGuarantee:
    """One immutable review of the venue premises the ownership contract needs.

    ``order_id_non_reuse_source`` and ``order_id_in_history_source`` name where
    each premise was established (documentation URL or support reference);
    ``reviewed_at`` and ``revision`` identify the review. The premise statements
    themselves are module constants bound into the version digest.
    """

    revision: str
    reviewed_at: datetime
    order_id_non_reuse_source: str
    order_id_in_history_source: str


WALUTOMAT_ORDER_OWNERSHIP_GUARANTEE: WalutomatOrderOwnershipGuarantee | None = None
"""The recorded guarantee, or ``None`` while the venue's answer is unreviewed."""


def canonical_walutomat_order_ownership_guarantee_content(
    guarantee: WalutomatOrderOwnershipGuarantee,
) -> str:
    """Serialize every reviewed field and both premises into canonical JSON.

    Args:
        guarantee: The reviewed venue premises and their sources.

    Returns:
        Stable JSON suitable for a content-addressed version digest.
    """
    payload: dict[str, str] = {
        "order_id_in_history_premise": WALUTOMAT_ORDER_ID_IN_HISTORY_PREMISE,
        "order_id_in_history_source": guarantee.order_id_in_history_source,
        "order_id_non_reuse_premise": WALUTOMAT_ORDER_ID_NON_REUSE_PREMISE,
        "order_id_non_reuse_source": guarantee.order_id_non_reuse_source,
        "reviewed_at": guarantee.reviewed_at.isoformat(),
        "revision": guarantee.revision,
    }
    return json.dumps(payload, allow_nan=False, separators=(",", ":"), sort_keys=True)


def walutomat_order_ownership_guarantee_version(
    guarantee: WalutomatOrderOwnershipGuarantee,
) -> str:
    """Return the revision-prefixed SHA-256 digest of the complete guarantee.

    Args:
        guarantee: The reviewed venue premises and their sources.

    Returns:
        Content-bound version suitable for bootstrap evidence.
    """
    content = canonical_walutomat_order_ownership_guarantee_content(guarantee)
    digest = hashlib.sha256(content.encode()).hexdigest()
    return f"{guarantee.revision}:{digest}"


def recorded_walutomat_order_ownership_guarantee_version() -> str | None:
    """Return the recorded guarantee's version, or ``None`` while it is unrecorded.

    Returns:
        The version of :data:`WALUTOMAT_ORDER_OWNERSHIP_GUARANTEE`, or ``None``.
    """
    guarantee = WALUTOMAT_ORDER_OWNERSHIP_GUARANTEE
    if guarantee is None:
        return None
    return walutomat_order_ownership_guarantee_version(guarantee)
