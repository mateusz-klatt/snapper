"""Explicit ticker translation boundary for a prevalidated subscription scope."""

from typing import Protocol

from snapper.infrastructure.exchanges.contracts import TickerUpdate


class TickerSymbolCodec(Protocol):
    """Translate selected ticker symbols without consulting a global catalog."""

    def to_wire_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and translate the complete selected native subscription.

        Args:
            symbols: Explicit native symbols requested by the collector.

        Returns:
            Corresponding exact venue aliases.
        """
        ...

    def parse_ticks(self, payload: object) -> list[TickerUpdate]:
        """Parse selected venue ticks without expanding the admitted symbol scope.

        Args:
            payload: Untrusted ticker payload supplied by the venue client.

        Returns:
            Valid typed ticks carrying selected native symbols.
        """
        ...
