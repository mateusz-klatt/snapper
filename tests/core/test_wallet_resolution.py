"""Tests for strict wallet autolookup."""

from datetime import UTC
from datetime import datetime

import pytest

from snapper.core.wallet_resolution import WalletAmbiguousError
from snapper.core.wallet_resolution import WalletUnresolvedError
from snapper.core.wallet_resolution import resolve_wallet_or_default
from snapper.data.repository_types import WalletRow


def _wallet_row(public_id: str, *, is_paper: bool = False) -> WalletRow:
    """Build a wallet row for resolver tests."""
    return WalletRow(
        public_id=public_id,
        label=public_id,
        description=None,
        is_paper=is_paper,
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        session_id="test-sid",
        sequence_id=1,
    )


class FakeWalletRepository:
    """Typed repository double for wallet resolver tests."""

    def __init__(
        self,
        *,
        active_wallets: list[WalletRow] | None = None,
        accessible_wallets: list[WalletRow] | None = None,
    ) -> None:
        """Store configured result sets and call traces."""
        self.active_wallets = active_wallets if active_wallets is not None else []
        self.accessible_wallets = accessible_wallets if accessible_wallets is not None else []
        self.active_calls: list[datetime] = []
        self.accessible_calls: list[tuple[list[str], datetime]] = []

    async def list_active_wallets(self, as_of: datetime) -> list[WalletRow]:
        """Return configured active wallets."""
        self.active_calls.append(as_of)
        return list(self.active_wallets)

    async def list_accessible_wallets_for_operators(
        self,
        operator_public_ids: list[str],
        as_of: datetime,
    ) -> list[WalletRow]:
        """Return configured operator-scoped wallets."""
        self.accessible_calls.append((list(operator_public_ids), as_of))
        return list(self.accessible_wallets)


class TestResolveWalletOrDefault:
    """Coverage for the strict shared wallet resolver."""

    @pytest.mark.asyncio
    async def test_explicit_wallet_public_id_passes_through(self) -> None:
        """Explicit non-empty wallet IDs bypass repository lookup."""
        repo = FakeWalletRepository(
            active_wallets=[_wallet_row("admin-wallet")],
            accessible_wallets=[_wallet_row("operator-wallet")],
        )

        result = await resolve_wallet_or_default(
            repo,
            explicit_wallet_public_id="wallet-pinned",
            operator_public_ids=["op-1"],
        )

        assert result == "wallet-pinned"
        assert repo.active_calls == []
        assert repo.accessible_calls == []

    @pytest.mark.asyncio
    async def test_admin_lookup_uses_active_wallet_catalogue(self) -> None:
        """ADMIN lookup uses active wallets instead of operator grants."""
        as_of = datetime(2026, 2, 3, tzinfo=UTC)
        repo = FakeWalletRepository(
            active_wallets=[_wallet_row("admin-wallet")],
            accessible_wallets=[_wallet_row("operator-wallet")],
        )

        result = await resolve_wallet_or_default(
            repo,
            explicit_wallet_public_id=None,
            operator_public_ids=["op-1"],
            is_admin=True,
            as_of=as_of,
        )

        assert result == "admin-wallet"
        assert repo.active_calls == [as_of]
        assert repo.accessible_calls == []

    @pytest.mark.asyncio
    async def test_operator_lookup_uses_accessible_wallets(self) -> None:
        """Non-admin lookup is scoped to supplied operator IDs."""
        as_of = datetime(2026, 2, 4, tzinfo=UTC)
        repo = FakeWalletRepository(accessible_wallets=[_wallet_row("operator-wallet")])

        result = await resolve_wallet_or_default(
            repo,
            explicit_wallet_public_id=None,
            operator_public_ids=["op-1", "op-2"],
            as_of=as_of,
        )

        assert result == "operator-wallet"
        assert repo.active_calls == []
        assert repo.accessible_calls == [(["op-1", "op-2"], as_of)]

    @pytest.mark.asyncio
    async def test_paper_mode_filters_to_paper_wallets(self) -> None:
        """Paper mode keeps only paper wallet candidates."""
        repo = FakeWalletRepository(
            accessible_wallets=[
                _wallet_row("wallet-live", is_paper=False),
                _wallet_row("wallet-paper", is_paper=True),
            ]
        )

        result = await resolve_wallet_or_default(
            repo,
            explicit_wallet_public_id=None,
            operator_public_ids=["op-1"],
            mode="paper",
        )

        assert result == "wallet-paper"

    @pytest.mark.asyncio
    async def test_live_mode_filters_to_live_wallets(self) -> None:
        """Live mode keeps only live wallet candidates."""
        repo = FakeWalletRepository(
            accessible_wallets=[
                _wallet_row("wallet-paper", is_paper=True),
                _wallet_row("wallet-live", is_paper=False),
            ]
        )

        result = await resolve_wallet_or_default(
            repo,
            explicit_wallet_public_id=None,
            operator_public_ids=["op-1"],
            mode="live",
        )

        assert result == "wallet-live"

    @pytest.mark.asyncio
    async def test_zero_candidates_raise_unresolved(self) -> None:
        """Zero candidates fail closed."""
        repo = FakeWalletRepository(accessible_wallets=[])

        with pytest.raises(WalletUnresolvedError) as exc_info:
            await resolve_wallet_or_default(
                repo,
                explicit_wallet_public_id=None,
                operator_public_ids=["op-1"],
            )

        assert exc_info.value.candidates == []

    @pytest.mark.asyncio
    async def test_multiple_candidates_raise_ambiguous_with_ordered_ids(self) -> None:
        """Multiple candidates fail closed and preserve repository order."""
        repo = FakeWalletRepository(
            accessible_wallets=[
                _wallet_row("wallet-a"),
                _wallet_row("wallet-b"),
            ]
        )

        with pytest.raises(WalletAmbiguousError) as exc_info:
            await resolve_wallet_or_default(
                repo,
                explicit_wallet_public_id=None,
                operator_public_ids=["op-1"],
            )

        assert exc_info.value.candidates == ["wallet-a", "wallet-b"]
