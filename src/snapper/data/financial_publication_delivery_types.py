"""Share the minimal read-only shape used by actual local publication receipts.

This typing interface issues no capability. Its callers must first rebind the
actual publication through the historical reader for its declared version.
"""

from datetime import datetime
from typing import Protocol

from snapper.core.execution_route_authority import ExecutionAccountScope


class PublicationReceiptPayload(Protocol):
    """Common immutable source scope and recording clock across notification versions."""

    @property
    def scope(self) -> ExecutionAccountScope:
        """Return the actual notification account scope.

        Returns:
            The complete original account scope, independent of credential revisions.
        """

    @property
    def recorded_at(self) -> datetime:
        """Return the retained notification recording clock.

        Returns:
            The original aware local clock, unchanged by transport retries.
        """


class PublicationReceiptSource(Protocol):
    """Read-only projection of an independently rebound actual publication."""

    @property
    def public_id(self) -> str:
        """Return the immutable publication identity.

        Returns:
            The existing UUID used for every delivery of this notification.
        """

    @property
    def content_digest(self) -> str:
        """Return the full version-specific publication digest.

        Returns:
            The verified digest of the original canonical notification.
        """

    @property
    def payload(self) -> PublicationReceiptPayload:
        """Return the common fields of the actual source-bound message.

        Returns:
            The original payload with its complete scope and retained clock.
        """
