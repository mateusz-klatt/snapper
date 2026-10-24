"""Current futures provenance for payoff evidence and explicit native order quantity units.

The internal reconciliation quantity convention remains independent of the
venue-native order unit carried in the same versioned metadata observation.
"""

from typing import Final

FUTURES_SPEC_SOURCE: Final[str] = "kraken_futures:rest.get_instruments"
FUTURES_SPEC_ETL_VERSION: Final[str] = "s2a-v4"
FUTURES_QUANTITY_ETL_VERSIONS: Final[frozenset[str]] = frozenset({"s2a-v3", "s2a-v4"})
