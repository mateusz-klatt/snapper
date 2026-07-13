"""Read-surface mapping for venue account truth (PnL Phase 3).

Maps a stored ``venue_account_states`` row into its fail-closed API response:
it derives the EFFECTIVE status (staleness/clock over the raw stored value),
STRICTLY revalidates the stored JSON payloads at read time (rejecting non-finite
numbers, wrong types, empty/absent currency codes and unknown position sides —
not just malformed JSON), and independently re-checks the observed-row
coherence invariants the DB CHECKs enforce. Any violation marks the whole state
``corrupt`` and clears the balances/positions, so a corrupt, tampered, or stale
row can never be served as authoritative truth. Shared by the REST and MCP read
surfaces so both label truth identically.
"""

import json
import math
from datetime import datetime
from typing import cast

from snapper.application.portfolio.account_status import derive_effective_account_status
from snapper.core.types import OrderExchange
from snapper.data.repository_types import VenueAccountStateRow
from snapper.interface.websocket.schemas import ExecutionMode
from snapper.messaging.schemas.data import AccountBalanceEntry
from snapper.messaging.schemas.data import AccountPositionEntry
from snapper.messaging.schemas.data import PortfolioAccountState

EFFECTIVE_CORRUPT = "corrupt"
"""A stored payload failed read-time revalidation, or an observed row violated
its coherence invariants — never authoritative."""

_PAYLOAD_NOT_A_LIST_MSG = "account payload is not a JSON list"
_PAYLOAD_ENTRY_NOT_OBJECT_MSG = "account payload entry is not a JSON object"
_NOT_FINITE_NUMBER_MSG = "account payload number is missing or not finite"
_BAD_CURRENCY_MSG = "account balance currency is not a non-empty string"
_BAD_SYMBOL_MSG = "account position symbol is not a non-empty string"
_BAD_SIDE_MSG = "account position side is not buy/sell"
_BAD_TIMESTAMP_MSG = "account position timestamp is not an ISO string"
_VALID_SIDES = ("buy", "sell")


def _finite_number(value: object) -> float:
    """Coerce a JSON number to a finite float, rejecting anything else.

    Strict on purpose: a ``bool`` (which ``float()`` would silently turn into
    1.0/0.0), a numeric STRING, or a non-finite value (``inf``/``nan``, e.g.
    ``1e400``) is a corrupt payload, never an authoritative balance/price.

    Args:
        value: The raw JSON value.

    Returns:
        The finite float.

    Raises:
        ValueError: When the value is not a finite int/float.
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(_NOT_FINITE_NUMBER_MSG)
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(_NOT_FINITE_NUMBER_MSG)
    return number


def _parse_balances(raw: str) -> list[AccountBalanceEntry]:
    """Strictly parse the stored balances JSON into typed entries.

    Args:
        raw: The stored ``balances_json`` string.

    Returns:
        The parsed native balance entries.

    Raises:
        ValueError: When the payload is not a list of well-formed objects, a
            currency is not a non-empty string, or a number is non-finite.
        json.JSONDecodeError: When the payload is not valid JSON.
    """
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(_PAYLOAD_NOT_A_LIST_MSG)
    entries: list[AccountBalanceEntry] = []
    for item in data:
        if not isinstance(item, dict):
            raise ValueError(_PAYLOAD_ENTRY_NOT_OBJECT_MSG)
        currency = item.get("currency")
        if not isinstance(currency, str) or not currency:
            raise ValueError(_BAD_CURRENCY_MSG)
        free = item.get("free")
        used = item.get("used")
        entries.append(
            AccountBalanceEntry(
                currency=currency,
                total=_finite_number(item.get("total")),
                free=None if free is None else _finite_number(free),
                used=None if used is None else _finite_number(used),
            )
        )
    return entries


def _parse_positions(raw: str) -> list[AccountPositionEntry]:
    """Strictly parse the stored positions JSON into typed entries.

    Args:
        raw: The stored ``open_positions_json`` string.

    Returns:
        The parsed native position entries.

    Raises:
        ValueError: When the payload is not a list of well-formed objects, a
            symbol is not a non-empty string, a side is not buy/sell, a
            timestamp is not an ISO string, or a number is non-finite.
        json.JSONDecodeError: When the payload is not valid JSON.
    """
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(_PAYLOAD_NOT_A_LIST_MSG)
    entries: list[AccountPositionEntry] = []
    for item in data:
        if not isinstance(item, dict):
            raise ValueError(_PAYLOAD_ENTRY_NOT_OBJECT_MSG)
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError(_BAD_SYMBOL_MSG)
        side = item.get("side")
        if side not in _VALID_SIDES:
            raise ValueError(_BAD_SIDE_MSG)
        timestamp = item.get("timestamp")
        if not isinstance(timestamp, str):
            raise ValueError(_BAD_TIMESTAMP_MSG)
        entries.append(
            AccountPositionEntry(
                symbol=symbol,
                side=side,
                size=_finite_number(item.get("size")),
                entry_price=_finite_number(item.get("entry_price")),
                mark_price=_finite_number(item.get("mark_price")),
                unrealized_pnl=_finite_number(item.get("unrealized_pnl")),
                unrealized_funding=_finite_number(item.get("unrealized_funding")),
                timestamp=datetime.fromisoformat(timestamp),
            )
        )
    return entries


def _row_is_coherent(row: VenueAccountStateRow) -> bool:
    """Re-check the observed-row coherence invariants at read time.

    Independent of the DB CHECK constraints (defence in depth against a
    tampered/migrated/bypassed row): an ``observed`` roll-up REQUIRES an
    observed balance and observed/absent positions (so an incoherent roll-up
    can never read authoritative); a ``simulated`` status only rides a paper
    row; an ``observed`` balance/positions component MUST carry its JSON
    payload and observation timestamp; an ``observed`` roll-up MUST carry an
    authority window; a payload and its source observation id are inseparable
    (both-null or both-non-null); and a FRESH (observed/simulated) component's
    payload source MUST be THIS attempt (``current_attempt_observation_id``) —
    a mismatched non-null source is a forged/retained payload masquerading as
    fresh. ``valuation_status`` must be ``native_only`` (Phase 3), and a
    structurally-absent component (``not_applicable``/``unsupported``) must
    carry neither a payload NOR an observation timestamp (the DAL clears both
    on structural absence; a lingering value there is forged/retained). A
    violation is treated as ``corrupt``.

    Args:
        row: The stored account-state row.

    Returns:
        True when the row satisfies every invariant, False otherwise.
    """
    if row["valuation_status"] != "native_only":
        return False
    if row["balance_status"] == "unsupported" and (
        row["balances_json"] is not None or row["balance_observed_at"] is not None
    ):
        return False
    if row["position_status"] in ("not_applicable", "unsupported") and (
        row["open_positions_json"] is not None or row["position_observed_at"] is not None
    ):
        return False
    if row["sync_status"] == "observed" and row["balance_status"] != "observed":
        return False
    if row["sync_status"] == "observed" and row["position_status"] not in (
        "observed",
        "not_applicable",
    ):
        return False
    if (row["sync_status"] == "simulated" or row["balance_status"] == "simulated") and row[
        "mode"
    ] != "paper":
        return False
    if row["balance_status"] == "observed" and (
        row["balances_json"] is None or row["balance_observed_at"] is None
    ):
        return False
    if row["position_status"] == "observed" and (
        row["open_positions_json"] is None or row["position_observed_at"] is None
    ):
        return False
    if row["sync_status"] == "observed" and row["authoritative_until"] is None:
        return False
    if (row["balances_json"] is None) != (row["balance_payload_source_observation_id"] is None):
        return False
    if (row["open_positions_json"] is None) != (
        row["position_payload_source_observation_id"] is None
    ):
        return False
    if row["balance_status"] in ("observed", "simulated") and (
        row["balance_payload_source_observation_id"] != row["current_attempt_observation_id"]
    ):
        return False
    return not (
        row["position_status"] == "observed"
        and row["position_payload_source_observation_id"] != row["current_attempt_observation_id"]
    )


def build_portfolio_account_state(
    row: VenueAccountStateRow, now: datetime
) -> PortfolioAccountState:
    """Map a stored account-state row to its fail-closed read response.

    Derives the EFFECTIVE status (stale/clock_error over the stored value),
    strictly revalidates the stored JSON payloads, and re-checks the
    observed-row coherence invariants; a payload that fails to parse OR a row
    that violates coherence marks the whole state ``corrupt`` and clears the
    balances/positions. ``is_authoritative`` is True only when the effective
    status is exactly ``observed``.

    Args:
        row: The active ``venue_account_states`` row.
        now: The read instant (for staleness/clock derivation).

    Returns:
        The mapped account-state response item.
    """
    corrupt = not _row_is_coherent(row)
    balances: list[AccountBalanceEntry] | None = None
    positions: list[AccountPositionEntry] | None = None
    if row["balances_json"] is not None:
        try:
            balances = _parse_balances(row["balances_json"])
        except json.JSONDecodeError, ValueError:
            corrupt = True
    if row["open_positions_json"] is not None:
        try:
            positions = _parse_positions(row["open_positions_json"])
        except json.JSONDecodeError, ValueError:
            corrupt = True
    if corrupt:
        effective_status = EFFECTIVE_CORRUPT
        balances = None
        positions = None
    else:
        effective_status = derive_effective_account_status(
            sync_status=row["sync_status"],
            balance_observed_at=row["balance_observed_at"],
            position_observed_at=row["position_observed_at"],
            authoritative_until=row["authoritative_until"],
            now=now,
        )
    return PortfolioAccountState(
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        wallet_public_id=row["wallet_public_id"],
        exchange=cast(OrderExchange, row["exchange"]),
        mode=cast(ExecutionMode, row["mode"]),
        sync_status=row["sync_status"],
        effective_status=effective_status,
        is_authoritative=effective_status == "observed",
        balance_status=row["balance_status"],
        position_status=row["position_status"],
        valuation_status=row["valuation_status"],
        balances=balances,
        open_positions=positions,
        balance_observed_at=row["balance_observed_at"],
        position_observed_at=row["position_observed_at"],
        authoritative_until=row["authoritative_until"],
        current_attempt_observation_id=row["current_attempt_observation_id"],
        balance_payload_source_observation_id=row["balance_payload_source_observation_id"],
        position_payload_source_observation_id=row["position_payload_source_observation_id"],
        error=row["error"],
    )
