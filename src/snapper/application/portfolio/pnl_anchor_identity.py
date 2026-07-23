"""Canonical identity helpers for durable portfolio P&L activation anchors."""

from typing import Final
from uuid import NAMESPACE_URL
from uuid import UUID
from uuid import uuid5

_PORTFOLIO_PNL_ANCHOR_NAMESPACE: Final[UUID] = uuid5(
    NAMESPACE_URL,
    "snapper.portfolio.pnl.activation-anchor.v1",
)
_VALUATION_CCY_LENGTH: Final[int] = 3


def normalize_portfolio_pnl_wallet_public_id(wallet_public_id: str) -> str:
    """Return one canonical portfolio wallet UUID.

    Args:
        wallet_public_id: Wallet UUID spelling to normalize.

    Returns:
        The canonical lowercase hyphenated UUID spelling.

    Raises:
        ValueError: If the value is not a UUID.
    """
    try:
        return str(UUID(wallet_public_id))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("portfolio P&L anchor wallet identity is invalid") from exc


def normalize_portfolio_pnl_valuation_ccy(valuation_ccy: str) -> str:
    """Return one canonical three-letter portfolio valuation currency.

    Args:
        valuation_ccy: Currency code to strip and uppercase.

    Returns:
        The normalized three-letter currency code.

    Raises:
        ValueError: If the value is not a three-letter alphabetic code.
    """
    normalized = valuation_ccy.strip().upper()
    if len(normalized) != _VALUATION_CCY_LENGTH or not normalized.isalpha():
        raise ValueError("portfolio P&L valuation currency must be a three-letter code")
    return normalized


def portfolio_pnl_anchor_public_id(
    wallet_public_id: str,
    mode: str,
    valuation_ccy: str,
) -> str:
    """Return the deterministic UUID5 identity for one activation scope.

    Args:
        wallet_public_id: Canonical UUID identity of the portfolio wallet.
        mode: Portfolio execution mode, either ``live`` or ``paper``.
        valuation_ccy: Portfolio valuation currency to normalize.

    Returns:
        The stable UUID5 public identity for the normalized scope.

    Raises:
        ValueError: If any scope identity component is invalid.
    """
    canonical_wallet_public_id = normalize_portfolio_pnl_wallet_public_id(wallet_public_id)
    if mode not in ("live", "paper"):
        raise ValueError("portfolio P&L anchor mode must be 'live' or 'paper'")
    normalized_ccy = normalize_portfolio_pnl_valuation_ccy(valuation_ccy)
    scope = f"{canonical_wallet_public_id}|{mode}|{normalized_ccy}"
    return str(uuid5(_PORTFOLIO_PNL_ANCHOR_NAMESPACE, scope))
