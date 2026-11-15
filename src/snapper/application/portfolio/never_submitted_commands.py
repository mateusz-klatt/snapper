"""Reviewed exclusions of trade commands that provably never produced a venue order.

The spot anchor bootstrap's unresolved-submit gate settles a ``create`` or
``submit`` command only through a venue order id in the owned order set. A
``failed`` or ``rejected`` command that never reached a venue, or that the
venue refused without creating an order, carries no such id, so on its own it
would refuse the gate forever. No status text, ``last_error`` or generic
``order_rejected`` event proves that a command never produced an order, and the
venue events of older commands carry no command id, so no rule can clear them.

An exclusion is therefore a human review of one command's exact snapshot facts:
its scope and identifiers, every version of its history, every venue event
linked to it by client order id, venue client id or command id, and the number
of Orders carrying its client order id. The never-submitted document renders
those facts canonically, and this module pins the SHA-256 digest of each
reviewed document. The gate rebuilds the document from the current snapshot on
every evaluation, so any later version, event or Order, or any change to a
bound value, yields a different digest and leaves the command unresolved again
until a new review. The review evidence behind each digest is kept outside this
repository, and a digest names no identifier.
"""

import hashlib
import json
from typing import Final

from snapper.core.json_types import JsonObject

NEVER_SUBMITTED_DOCUMENT_SCHEMA: Final = "snapper.never_submitted_command.v1"
"""Schema tag carried by every never-submitted document and bound into its digest."""

NEVER_SUBMITTED_COMMAND_DIGESTS: Final[frozenset[str]] = frozenset()
"""Digests of the reviewed never-submitted documents the gate may exclude.

Empty until a reviewed change pins a digest. An empty set excludes nothing,
so the gate behaves exactly as without exclusions.
"""


def never_submitted_digest(document: JsonObject) -> str:
    """Return the SHA-256 hex digest of a never-submitted document's canonical JSON.

    The canonical form sorts keys, uses compact separators, escapes every
    non-ASCII character and refuses non-finite numbers, so the same facts
    always hash to the same digest.

    Args:
        document: A never-submitted document.

    Returns:
        The lower-case hex SHA-256 digest of the canonical UTF-8 bytes.
    """
    content = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )
    return hashlib.sha256(content.encode()).hexdigest()
