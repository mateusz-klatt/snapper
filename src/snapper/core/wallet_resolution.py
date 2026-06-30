"""Strict wallet resolution for money-adjacent launch paths."""

from datetime import UTC
from datetime import datetime
from typing import Protocol

from snapper.core.types import ExecutionModeEnum
from snapper.data.repository_types import WalletRow


class WalletResolutionRepository(Protocol):
    """Repository contract required for wallet autolookup."""

    async def list_active_wallets(self, as_of: datetime) -> list[WalletRow]:
        """Return active wallet rows at ``as_of``.

        Args:
            as_of: Temporal anchor for the active-wallet catalogue.

        Returns:
            Active wallet rows in repository-defined deterministic order.
        """
        ...

    async def list_accessible_wallets_for_operators(
        self,
        operator_public_ids: list[str],
        as_of: datetime,
    ) -> list[WalletRow]:
        """Return wallet rows accessible to the supplied operators.

        Args:
            operator_public_ids: Operator IDs that define the caller scope.
            as_of: Temporal anchor for the access-scope lookup.

        Returns:
            Accessible wallet rows in repository-defined deterministic order.
        """
        ...


class WalletUnresolvedError(ValueError):
    """Raised when wallet autolookup finds no matching candidates."""

    def __init__(self, *, candidates: list[str]) -> None:
        """Store candidate IDs for caller diagnostics."""
        self.candidates = list(candidates)
        super().__init__("No wallet matched; specify wallet_public_id")


class WalletAmbiguousError(ValueError):
    """Raised when wallet autolookup finds more than one candidate."""

    def __init__(self, *, candidates: list[str]) -> None:
        """Store candidate IDs for caller diagnostics."""
        self.candidates = list(candidates)
        super().__init__("Multiple wallets matched; specify wallet_public_id")


def _filter_wallets_by_mode(wallets: list[WalletRow], mode: str | None) -> list[WalletRow]:
    """Apply optional live/paper filtering while preserving repository order."""
    if mode == ExecutionModeEnum.PAPER:
        return [wallet for wallet in wallets if wallet["is_paper"]]
    if mode == ExecutionModeEnum.LIVE:
        return [wallet for wallet in wallets if not wallet["is_paper"]]
    return wallets


async def resolve_wallet_or_default(
    repository: WalletResolutionRepository,
    *,
    explicit_wallet_public_id: str | None,
    operator_public_ids: list[str],
    is_admin: bool = False,
    mode: str | None = None,
    as_of: datetime | None = None,
) -> str:
    """Resolve an explicit wallet ID or strict single-wallet default.

    Explicit non-empty values pass through unchanged. Empty values are
    resolved from either the admin wallet catalogue or the operator-scoped
    accessible wallet set. Autolookup succeeds only when exactly one
    candidate remains after optional live/paper filtering.

    Args:
        repository: Repository exposing wallet catalogue lookups.
        explicit_wallet_public_id: Caller-supplied wallet ID, if any.
        operator_public_ids: Operator IDs used for non-admin scoping.
        is_admin: When True, use the active wallet catalogue.
        mode: Optional trading mode filter, ``live`` or ``paper``.
        as_of: Optional temporal anchor. Defaults to ``datetime.now(UTC)``.

    Returns:
        The explicit wallet ID or the single resolved candidate ID.

    Raises:
        WalletUnresolvedError: No wallet matched the lookup scope.
        WalletAmbiguousError: More than one wallet matched the lookup scope.
    """
    if explicit_wallet_public_id:
        return explicit_wallet_public_id
    lookup_as_of = as_of if as_of is not None else datetime.now(UTC)
    if is_admin:
        rows = await repository.list_active_wallets(lookup_as_of)
    else:
        rows = await repository.list_accessible_wallets_for_operators(
            operator_public_ids,
            lookup_as_of,
        )
    candidates = _filter_wallets_by_mode(rows, mode)
    candidate_ids = [wallet["public_id"] for wallet in candidates]
    if not candidate_ids:
        raise WalletUnresolvedError(candidates=candidate_ids)
    if len(candidate_ids) > 1:
        raise WalletAmbiguousError(candidates=candidate_ids)
    return candidate_ids[0]
