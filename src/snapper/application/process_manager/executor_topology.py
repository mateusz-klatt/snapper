"""Shared executor-topology policy for wallet-scoped exchange credentials.

The wallet pinned by ``kraken_equities_realtime_wallet_public_id`` exists to
mint Kraken Equities realtime WebSocket tokens on an isolated exchange nonce
counter. Running order executors against that identity would recreate the
mint-versus-executor nonce contention the pin was introduced to eliminate, so
every topology consumer must agree that its eligible credentials have no order
executor.

Pin resolution deliberately fails open. An empty or non-string value, a blank
or ambiguous label, a repository failure, or trading evidence all produce an
unrestricted topology because silently dropping a trading executor is the
worse failure. The per-exchange sole-holder belt provides the final guard: even
a successfully resolved pin cannot leave an exchange without any executor.

The pin remains an explicit operator declaration. An admin manual order aimed
at an eligible declared mint wallet is intentionally left without an executor;
executors belonging to other wallets discard the foreign command, because
executing through the mint identity is exactly what this isolation prevents.
The ``start_per_wallet_instance_by_name`` manual-start path is deliberately not
gated: an explicit process start is an operator override.

This module owns declaration resolution, trading-evidence refusal, and the
sole-holder belt so process launch and downstream consumers classify every
credential scope identically.
"""

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Final

from loguru import logger

from snapper.core.json_types import JsonValue
from snapper.data.repository import Repository
from snapper.data.repository_types import WalletCredentialRow

MINT_WALLET_PIN_SETTING_KEY: Final[str] = "kraken_equities_realtime_wallet_public_id"
_WALLET_LABEL_PIN_PREFIX: Final[str] = "label:"


class ExecutorCredentialDisposition(StrEnum):
    """Executor outcome for one active wallet credential."""

    RUN = "run"
    MINT_WALLET_EXCLUDED = "mint_wallet_excluded"
    MINT_WALLET_ONLY_EXCHANGE_HOLDER = "mint_wallet_only_exchange_holder"


@dataclass(frozen=True, slots=True)
class ExecutorTopology:
    """Resolved mint identity and exchange coverage held by other wallets."""

    mint_wallet_public_id: str
    exchanges_with_other_wallets: frozenset[str]

    def disposition_for(self, credential: WalletCredentialRow) -> ExecutorCredentialDisposition:
        """Return the shared executor outcome for one credential.

        A resolved mint wallet is excluded only where at least one other wallet
        holds the same exchange. Every other credential runs, including a mint
        candidate that is the exchange's sole credential holder.

        Args:
            credential: Active wallet credential being classified.

        Returns:
            The executor disposition enforced by every topology consumer.
        """
        if (
            not self.mint_wallet_public_id
            or credential["wallet_public_id"] != self.mint_wallet_public_id
        ):
            return ExecutorCredentialDisposition.RUN
        if credential["exchange"] not in self.exchanges_with_other_wallets:
            return ExecutorCredentialDisposition.MINT_WALLET_ONLY_EXCHANGE_HOLDER
        return ExecutorCredentialDisposition.MINT_WALLET_EXCLUDED


async def resolve_executor_topology(
    repository: Repository,
    credentials: Sequence[WalletCredentialRow],
    raw_pin: JsonValue,
    clock: Callable[[], datetime],
) -> ExecutorTopology:
    """Resolve the executor topology for one active credential catalogue.

    Pin resolution fails open. Empty or non-string pins, invalid label pins,
    repository errors, order history, or active scope grants all resolve to an
    unrestricted topology. That direction is intentional: losing a trading
    executor is more dangerous than temporarily running an executor for a mint
    identity. The coverage belt is derived from the already-loaded complete
    credential catalogue and is deliberately exchange-class agnostic so it
    exactly matches process-launch behavior.

    Args:
        repository: Repository exposing wallets, orders, and scope grants.
        credentials: Complete active credential catalogue for the decision.
        raw_pin: Parsed mint-wallet setting payload.
        clock: Current-time source sampled separately for catalogue and
            trading-evidence reads.

    Returns:
        Resolved topology used to classify every credential.
    """
    mint_wallet_public_id = await _resolve_mint_wallet_public_id(repository, raw_pin, clock)
    exchanges_with_other_wallets = frozenset(
        credential["exchange"]
        for credential in credentials
        if credential["wallet_public_id"] != mint_wallet_public_id
    )
    return ExecutorTopology(
        mint_wallet_public_id=mint_wallet_public_id,
        exchanges_with_other_wallets=exchanges_with_other_wallets,
    )


async def _resolve_mint_wallet_public_id(
    repository: Repository, raw_pin: JsonValue, clock: Callable[[], datetime]
) -> str:
    """Resolve and vet the declared token-mint wallet, failing open.

    Args:
        repository: Repository exposing wallet and trading evidence reads.
        raw_pin: Parsed mint-wallet setting payload.
        clock: Current-time source for active-row reads.

    Returns:
        Vetted mint wallet public id, or an empty string for no exclusion.
    """
    pin = raw_pin.strip() if isinstance(raw_pin, str) else ""
    if not pin:
        return ""
    if not pin.startswith(_WALLET_LABEL_PIN_PREFIX):
        return await _reject_mint_wallet_with_trading_evidence(repository, pin, clock())
    label = pin.removeprefix(_WALLET_LABEL_PIN_PREFIX)
    if not label.strip():
        return ""
    try:
        wallets = await repository.list_active_wallets(clock())
    except Exception as exc:
        logger.warning(
            f"Per-wallet spawner: wallet catalogue lookup for mint-pin label failed "
            f"({exc}); spawning executors for every wallet"
        )
        return ""
    matches = [
        wallet["public_id"]
        for wallet in wallets
        if wallet["label"] == label and not wallet["is_paper"]
    ]
    if len(matches) == 1:
        return await _reject_mint_wallet_with_trading_evidence(repository, matches[0], clock())
    logger.warning(
        f"Per-wallet spawner: mint-pin label {label!r} matched {len(matches)} live "
        "wallets; spawning executors for every wallet"
    )
    return ""


async def _reject_mint_wallet_with_trading_evidence(
    repository: Repository, wallet_public_id: str, as_of: datetime
) -> str:
    """Refuse exclusion when the declared wallet has trading evidence.

    A genuine token-mint identity holds a read-only exchange key: it cannot
    produce an order, and strategy scope grants exist only to authorize trading.
    Either signal contradicts the declaration. The scope-grant probe closes the
    fresh-wallet bootstrap hole: a mis-pinned trading wallet can have zero orders
    while already carrying grants, so exclusion is refused before its missing
    executor could prevent the first order forever. Both probes fail open on any
    query error for the same reason as every other guard in this policy.

    Args:
        repository: Repository exposing order and scope-grant evidence.
        wallet_public_id: Candidate mint wallet to vet.
        as_of: Temporal horizon for evidence reads.

    Returns:
        Candidate id only when both evidence sets are empty, else an empty
        string so every credential remains executor-backed.
    """
    try:
        order_count = await repository.get_orders_total_count(
            as_of, wallet_public_ids=[wallet_public_id]
        )
        grants = await repository.list_active_scope_grants_for_wallet(wallet_public_id, as_of)
    except Exception as exc:
        logger.warning(
            f"Per-wallet spawner: trading-evidence probe for mint-pin wallet failed "
            f"({exc}); spawning executors for every wallet"
        )
        return ""
    if order_count or grants:
        logger.warning(
            f"Per-wallet spawner: mint-pin wallet={wallet_public_id} has "
            f"{order_count} order(s) and {len(grants)} scope grant(s) — that is a "
            "TRADING wallet, refusing the executor exclusion; check "
            "kraken_equities_realtime_wallet_public_id"
        )
        return ""
    return wallet_public_id
