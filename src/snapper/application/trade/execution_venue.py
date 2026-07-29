"""Shared execution-venue validation for manual order transports."""

from dataclasses import dataclass
from datetime import datetime

from snapper.core.json_types import JsonObject
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.data.repository import Repository


@dataclass(frozen=True)
class ExecutionVenueError(Exception):
    """Describe a fail-closed manual-order venue refusal."""

    error_code: str
    details: JsonObject


def derive_manual_execution_mode(exchange: str) -> ExecutionModeEnum:
    """Derive MCP manual-order mode from its requested execution venue.

    Args:
        exchange: Venue named by the MCP caller.

    Returns:
        Paper mode for the paper simulator, otherwise live mode.
    """
    if exchange == ExchangeEnum.PAPER.value:
        return ExecutionModeEnum.PAPER
    return ExecutionModeEnum.LIVE


async def resolve_execution_venue(
    repo: Repository,
    exchange: str,
    mode: ExecutionModeEnum,
    wallet_public_id: str,
    as_of: datetime,
) -> str:
    """Validate wallet, mode, and credential parity and return the venue.

    Args:
        repo: Repository providing active wallet and credential rows.
        exchange: Requested market or execution exchange.
        mode: Execution mode selected by the calling transport.
        wallet_public_id: Scope-checked wallet identifier.
        as_of: Temporal lookup time.

    Returns:
        Effective execution venue after paper remapping.

    Raises:
        ExecutionVenueError: When wallet, mode, or credentials are inconsistent.
    """
    wants_paper = mode == ExecutionModeEnum.PAPER
    wallets = await repo.list_active_wallets(as_of=as_of)
    wallet = next((row for row in wallets if row["public_id"] == wallet_public_id), None)
    if wallet is None:
        raise ExecutionVenueError(
            "unknown_wallet",
            {
                "wallet_public_id": wallet_public_id,
                "reason": "no active wallet row for the supplied wallet_public_id",
            },
        )
    if wallet["is_paper"] != wants_paper:
        raise ExecutionVenueError(
            "mode_wallet_mismatch",
            {
                "mode": mode.value,
                "wallet_public_id": wallet_public_id,
                "wallet_is_paper": wallet["is_paper"],
                "reason": "mode='paper' requires a paper wallet and mode='live' a live wallet",
            },
        )
    effective_exchange = ExchangeEnum.PAPER.value if wants_paper else exchange
    credentials = await repo.list_active_wallet_credentials(as_of=as_of)
    has_credential = any(
        credential["wallet_public_id"] == wallet_public_id
        and credential["exchange"] == effective_exchange
        for credential in credentials
    )
    if not has_credential:
        raise ExecutionVenueError(
            "wallet_credential_missing",
            {
                "wallet_public_id": wallet_public_id,
                "exchange": effective_exchange,
                "reason": (
                    "no active wallet credential for the execution venue; "
                    "no executor instance exists to consume the command"
                ),
            },
        )
    return effective_exchange
