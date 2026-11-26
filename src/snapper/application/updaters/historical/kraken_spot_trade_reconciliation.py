"""Compare documented Kraken Spot recovery candidates with stored trades."""

from collections.abc import Iterable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    KrakenSpotTradeEvidence,
)


@dataclass(frozen=True, slots=True)
class StoredKrakenSpotTrade:
    """Stored venue trade identity and fields used for comparison."""

    instrument_public_id: str
    trade_id: str
    executed_at: datetime
    price: float
    size: float
    side: str


@dataclass(frozen=True, slots=True)
class KrakenSpotTradeRecoveryPlan:
    """Candidate-level result of comparing evidence with a stored baseline."""

    missing: tuple[KrakenSpotTradeEvidence, ...]
    already_present: int


def _matches_evidence(stored: StoredKrakenSpotTrade, evidence: KrakenSpotTradeEvidence) -> bool:
    """Compare exact time and the value representation used by live ingestion.

    Args:
        stored: Baseline row for the same instrument and venue ID.
        evidence: Candidate carrying the venue's original decimal values.

    Returns:
        Whether the stored row agrees with the candidate.
    """
    if stored.executed_at.utcoffset() is None or evidence.executed_at.utcoffset() is None:
        return False
    return (
        stored.executed_at.astimezone(UTC) == evidence.executed_at.astimezone(UTC)
        and stored.price == float(evidence.price)
        and stored.size == float(evidence.size)
        and stored.side == evidence.side
    )


def plan_kraken_spot_trade_recovery(
    evidence: Sequence[KrakenSpotTradeEvidence],
    stored: Iterable[StoredKrakenSpotTrade],
) -> KrakenSpotTradeRecoveryPlan:
    """Classify the documented candidates against the supplied stored rows.

    Matches use instrument and venue ID so a different execution timestamp
    for the same venue identity is detected. Results describe the supplied
    candidates against this baseline and make no claim about gap completeness.

    Args:
        evidence: Documented candidates in the desired report order.
        stored: Baseline rows, which can include unrelated trades.

    Returns:
        Missing candidates in input order and the number already present.

    Raises:
        ValueError: A candidate identity is repeated or has duplicate or
            conflicting matching rows.
    """
    candidates: dict[tuple[str, str], KrakenSpotTradeEvidence] = {}
    for candidate in evidence:
        identity = (candidate.instrument_public_id, candidate.trade_id)
        if identity in candidates:
            raise ValueError(
                f"Duplicate Kraken Spot evidence for {candidate.symbol} trade {candidate.trade_id}"
            )
        candidates[identity] = candidate

    matched: set[tuple[str, str]] = set()
    for row in stored:
        identity = (row.instrument_public_id, row.trade_id)
        documented = candidates.get(identity)
        if documented is None:
            continue
        if not _matches_evidence(row, documented):
            raise ValueError(
                f"Conflicting stored Kraken Spot trade for {documented.symbol} trade "
                f"{documented.trade_id}"
            )
        if identity in matched:
            raise ValueError(
                f"Duplicate stored Kraken Spot trade for {documented.symbol} trade {documented.trade_id}"
            )
        matched.add(identity)

    missing = tuple(
        candidate for identity, candidate in candidates.items() if identity not in matched
    )
    return KrakenSpotTradeRecoveryPlan(missing=missing, already_present=len(matched))
