"""Validate explicit half-open historical repair windows before external access."""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime


@dataclass(frozen=True)
class Window:
    """Explicit half-open UTC minute range, fixed before any external access."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        """Reject reversed, unaligned, naive or still-open ranges."""
        for value in (self.start, self.end):
            offset = value.utcoffset()
            if value.tzinfo is None or offset is None:
                raise ValueError("start and end must have an explicit UTC timezone")
            if offset.total_seconds() or value.second or value.microsecond:
                raise ValueError("start and end must be UTC minute boundaries")
        if self.start >= self.end:
            raise ValueError("start must precede end")
        if self.end > datetime.now(UTC).replace(second=0, microsecond=0):
            raise ValueError("end must not include the still-open current minute")

    def contains(self, opening: datetime) -> bool:
        """Test membership in the closed historical request.

        Args:
            opening: Candle opening timestamp.

        Returns:
            Whether the opening belongs to this half-open window.
        """
        return self.start <= opening < self.end
