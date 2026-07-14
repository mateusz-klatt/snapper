"""Type contracts for durable portfolio reconciliation classification."""

from typing import Literal

type RealPortfolioReconciliationMethod = Literal[
    "futures_position",
    "spot_execution_replay",
    "margin_ledger_replay",
]
type PortfolioReconciliationMethod = Literal[
    "futures_position",
    "spot_execution_replay",
    "margin_ledger_replay",
    "unclassified",
]
type PortfolioAccountMode = Literal["live", "paper"]
