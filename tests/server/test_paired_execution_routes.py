"""Tests for the paired-execution operator surface routes.

Exercises the per-scope incident composition (halt ∪ exposed-group union,
``halt_missing`` anomaly, wallet scoping, leg exposure math) and the
terminalize attestation flow (happy path, 404/403/409/503 mappings) by
calling the route functions directly with mocked repositories, following the
operator-routes test convention. The new ``MANAGE_PAIRED_EXECUTION``
permission's role mapping is asserted explicitly so a role-matrix regression
cannot silently open the attestation to viewers.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request

from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.types import PairedGroupTerminalizeOutcome
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.paired_execution_routes import list_paired_execution_incidents
from snapper.server.paired_execution_routes import terminalize_paired_execution_group

_T0 = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)


def _make_request() -> Request:
    """Return a ``Request`` mock with a real ``SequenceTracker`` attached."""
    mock_request = MagicMock(spec=Request)
    mock_request.app.state.rest_tracker = SequenceTracker()
    return mock_request


def _sql_repo() -> AsyncMock:
    """Return an AsyncMock that passes the route's SQL-repository narrowing."""
    return AsyncMock(spec=SQLAlchemyRepository)


def _group_row(
    public_id: str,
    *,
    wallet: str = "w-1",
    status: str = "manual_intervention",
    group_key: str = "kraken:BTC-USD:live|kraken:ETH-USD:live",
) -> Any:
    """Build a minimal group row dict for the incident projection."""
    return cast(
        Any,
        {
            "public_id": public_id,
            "wallet_public_id": wallet,
            "strategy_id": "pairs-alpha",
            "group_key": group_key,
            "status": status,
            "policy": "simultaneous",
            "failure_reason": "manual intervention",
            "halted_at": _T0,
            "created_at": _T0,
        },
    )


def _halt_row(
    *,
    wallet: str = "w-1",
    group_public_id: str = "grp-1",
    group_key: str = "kraken:BTC-USD:live|kraken:ETH-USD:live",
) -> Any:
    """Build a minimal durable halt row dict for the incident projection."""
    return cast(
        Any,
        {
            "public_id": "halt-1",
            "wallet_public_id": wallet,
            "strategy_id": "pairs-alpha",
            "group_key": group_key,
            "group_public_id": group_public_id,
            "reason": "paired-execution group broken",
            "created_at": _T0,
            "session_id": "sid-halt",
            "sequence_id": 5,
            "timestamp": _T0,
        },
    )


def _leg_row(public_id: str, *, filled: float, compensated: float) -> Any:
    """Build a minimal leg row dict for the exposure projection."""
    return cast(
        Any,
        {
            "public_id": public_id,
            "leg_index": 0,
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "shard_key": "kraken.BTC-USD.live",
            "side": "buy",
            "status": "manual_intervention",
            "filled_signed_qty": filled,
            "compensated_signed_qty": compensated,
            "compensation_seq": 1,
        },
    )


_ADMIN = AuthPrincipal(username="root", role=UserRole.ADMIN, user_public_id="user-admin")
_OPERATOR = AuthPrincipal(
    username="alice",
    role=UserRole.OPERATOR,
    user_public_id="user-alice",
    operator_public_ids=["op-1"],
)


class TestListIncidents:
    """Scope-union composition and visibility of the incidents read."""

    @pytest.mark.asyncio
    async def test_admin_sees_union_with_halt_missing_anomaly(self) -> None:
        """The incident list unions halts with exposed groups per scope.

        Given: one scope carrying both a halt and a manual group, and a second
            scope carrying an exposed broken group with NO halt row,
        When: an ADMIN lists incidents,
        Then: both scopes appear in deterministic order; the halt-less exposed
            scope is flagged halt_missing and the leg exposure carries
            open_qty = filled − compensated.
        """
        repo = _sql_repo()
        repo.list_active_paired_execution_halts = AsyncMock(return_value=[_halt_row()])
        repo.list_current_paired_execution_groups = AsyncMock(
            return_value=[
                _group_row("grp-1"),
                _group_row(
                    "grp-2",
                    status="broken",
                    group_key="kraken:ADA-USD:live|kraken:SOL-USD:live",
                ),
            ]
        )
        repo.get_current_paired_execution_legs = AsyncMock(
            return_value=[_leg_row("leg-1", filled=10.0, compensated=4.0)]
        )
        result = await list_paired_execution_incidents(
            request=_make_request(), principal=_ADMIN, repo=repo
        )
        assert result.count == 2
        ada_scope, btc_scope = result.payload
        assert ada_scope.group_key.startswith("kraken:ADA-USD")
        assert ada_scope.halt is None
        assert ada_scope.halt_missing is True
        assert btc_scope.halt is not None
        assert btc_scope.halt.halt_public_id == "halt-1"
        assert btc_scope.halt_missing is False
        leg = btc_scope.groups[0].legs[0]
        assert leg.open_qty == 6.0

    @pytest.mark.asyncio
    async def test_operator_is_wallet_scoped(self) -> None:
        """A non-admin caller only sees scopes for accessible wallets.

        Given: incidents on wallets w-1 and w-2 while the caller's operator
            set grants access to w-1 only,
        When: the OPERATOR lists incidents,
        Then: only the w-1 scope is returned.
        """
        repo = _sql_repo()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[cast(Any, {"public_id": "w-1"})]
        )
        repo.list_active_paired_execution_halts = AsyncMock(return_value=[_halt_row(wallet="w-2")])
        repo.list_current_paired_execution_groups = AsyncMock(
            return_value=[_group_row("grp-1", wallet="w-1")]
        )
        repo.get_current_paired_execution_legs = AsyncMock(return_value=[])
        result = await list_paired_execution_incidents(
            request=_make_request(), principal=_OPERATOR, repo=repo
        )
        assert result.count == 1
        assert result.payload[0].wallet_public_id == "w-1"
        assert result.payload[0].halt_missing is True

    @pytest.mark.asyncio
    async def test_zero_fill_broken_group_is_not_an_incident(self) -> None:
        """A zero-fill broken group never surfaces as a false halt_missing anomaly.

        Given: a broken group whose only leg has zero filled quantity (an
            assembly-timeout break the scanner deliberately does not halt) and
            no halt row,
        When: incidents are listed,
        Then: the scope produces NO incident — the read mirrors the scanner's
            exposure predicate instead of its broad status query.
        """
        repo = _sql_repo()
        repo.list_active_paired_execution_halts = AsyncMock(return_value=[])
        repo.list_current_paired_execution_groups = AsyncMock(
            return_value=[_group_row("grp-zero", status="broken")]
        )
        repo.get_current_paired_execution_legs = AsyncMock(
            return_value=[_leg_row("leg-zero", filled=0.0, compensated=0.0)]
        )
        result = await list_paired_execution_incidents(
            request=_make_request(), principal=_ADMIN, repo=repo
        )
        assert result.count == 0

    @pytest.mark.asyncio
    async def test_non_sql_repository_is_503(self) -> None:
        """A non-SQL repository deployment answers 503.

        Given: a repository that is not the SQL implementation,
        When: incidents are listed,
        Then: HTTP 503 with the paired_execution_unavailable error code.
        """
        with pytest.raises(HTTPException) as exc:
            await list_paired_execution_incidents(
                request=_make_request(), principal=_ADMIN, repo=AsyncMock()
            )
        assert exc.value.status_code == 503


class TestTerminalize:
    """Attestation flow outcome mapping and scope enforcement."""

    @pytest.mark.asyncio
    async def test_happy_path_returns_completed_group(self) -> None:
        """A successful attestation returns the completed group projection.

        Given: a current manual group whose DAL terminalize succeeds,
        When: the operator posts terminalize,
        Then: the response payload carries the refreshed COMPLETED group and
            the DAL received the attesting user's public id.
        """
        repo = _sql_repo()
        before = _group_row("grp-1")
        after = _group_row("grp-1", status="completed")
        repo.get_current_paired_execution_group = AsyncMock(side_effect=[before, after])
        repo.terminalize_paired_execution_group = AsyncMock(
            return_value=PairedGroupTerminalizeOutcome.TERMINALIZED
        )
        repo.get_current_paired_execution_legs = AsyncMock(
            return_value=[_leg_row("leg-1", filled=10.0, compensated=4.0)]
        )
        result = await terminalize_paired_execution_group(
            request=_make_request(),
            group_public_id="grp-1",
            principal=_ADMIN,
            _csrf=None,
            repo=repo,
        )
        assert result.payload.status == "completed"
        assert result.payload.legs[0].open_qty == 6.0
        attested_by = repo.terminalize_paired_execution_group.await_args.args[1]
        assert attested_by == "user-admin"

    @pytest.mark.asyncio
    async def test_attestation_subject_falls_back_to_username(self) -> None:
        """A principal without a user_public_id stamps its username instead.

        Given: an ADMIN principal whose user_public_id is empty (legacy token),
        When: terminalize is posted,
        Then: the DAL receives the username so the audit stamp is never blank.
        """
        repo = _sql_repo()
        repo.get_current_paired_execution_group = AsyncMock(
            side_effect=[_group_row("grp-1"), _group_row("grp-1", status="completed")]
        )
        repo.terminalize_paired_execution_group = AsyncMock(
            return_value=PairedGroupTerminalizeOutcome.TERMINALIZED
        )
        repo.get_current_paired_execution_legs = AsyncMock(return_value=[])
        legacy_admin = AuthPrincipal(username="root", role=UserRole.ADMIN)
        await terminalize_paired_execution_group(
            request=_make_request(),
            group_public_id="grp-1",
            principal=legacy_admin,
            _csrf=None,
            repo=repo,
        )
        attested_by = repo.terminalize_paired_execution_group.await_args.args[1]
        assert attested_by == "root"

    @pytest.mark.asyncio
    async def test_unknown_group_is_404(self) -> None:
        """An id with no current active group answers 404.

        Given: the current-active group read returns None,
        When: terminalize is posted,
        Then: HTTP 404.
        """
        repo = _sql_repo()
        repo.get_current_paired_execution_group = AsyncMock(return_value=None)
        with pytest.raises(HTTPException) as exc:
            await terminalize_paired_execution_group(
                request=_make_request(),
                group_public_id="grp-x",
                principal=_ADMIN,
                _csrf=None,
                repo=repo,
            )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_wallet_scope_denied_is_404_not_an_oracle(self) -> None:
        """A group outside the caller's wallets answers 404, exactly like absent.

        Given: an OPERATOR with access to w-1 and a group on wallet w-2,
        When: terminalize is posted,
        Then: HTTP 404 with the same not-found shape (NOT 403 — a scoped caller
            must not learn that another wallet's group id exists) and the DAL is
            never invoked.
        """
        repo = _sql_repo()
        repo.get_current_paired_execution_group = AsyncMock(
            return_value=_group_row("grp-1", wallet="w-2")
        )
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[cast(Any, {"public_id": "w-1"})]
        )
        with pytest.raises(HTTPException) as exc:
            await terminalize_paired_execution_group(
                request=_make_request(),
                group_public_id="grp-1",
                principal=_OPERATOR,
                _csrf=None,
                repo=repo,
            )
        assert exc.value.status_code == 404
        detail = cast(dict[str, str], exc.value.detail)
        assert detail["error_code"] == "paired_group_not_found"
        repo.terminalize_paired_execution_group.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("outcome", "expected_status"),
        [
            (PairedGroupTerminalizeOutcome.NOT_FOUND, 404),
            (PairedGroupTerminalizeOutcome.NOT_TERMINALIZABLE, 409),
        ],
    )
    async def test_dal_refusals_map_to_http_codes(
        self, outcome: PairedGroupTerminalizeOutcome, expected_status: int
    ) -> None:
        """DAL refusal outcomes map to 404 (raced away) and 409 (not attestable).

        Given: the DAL returning NOT_FOUND or NOT_TERMINALIZABLE,
        When: terminalize is posted,
        Then: the matching HTTP status is raised.
        """
        repo = _sql_repo()
        repo.get_current_paired_execution_group = AsyncMock(return_value=_group_row("grp-1"))
        repo.terminalize_paired_execution_group = AsyncMock(return_value=outcome)
        with pytest.raises(HTTPException) as exc:
            await terminalize_paired_execution_group(
                request=_make_request(),
                group_public_id="grp-1",
                principal=_ADMIN,
                _csrf=None,
                repo=repo,
            )
        assert exc.value.status_code == expected_status

    @pytest.mark.asyncio
    async def test_409_detail_reports_freshly_read_status(self) -> None:
        """The 409 body carries the group's re-read status, not the stale one.

        Given: a group read as manual_intervention that a concurrent transition
            moved to compensating before the DAL refused,
        When: terminalize is posted,
        Then: the 409 detail reports 'compensating' — the operator sees why the
            attestation refused rather than a pre-race snapshot.
        """
        repo = _sql_repo()
        repo.get_current_paired_execution_group = AsyncMock(
            side_effect=[_group_row("grp-1"), _group_row("grp-1", status="compensating")]
        )
        repo.terminalize_paired_execution_group = AsyncMock(
            return_value=PairedGroupTerminalizeOutcome.NOT_TERMINALIZABLE
        )
        with pytest.raises(HTTPException) as exc:
            await terminalize_paired_execution_group(
                request=_make_request(),
                group_public_id="grp-1",
                principal=_ADMIN,
                _csrf=None,
                repo=repo,
            )
        assert exc.value.status_code == 409
        detail = cast(dict[str, str], exc.value.detail)
        assert detail["status"] == "compensating"

    @pytest.mark.asyncio
    async def test_vanished_after_terminalize_is_404(self) -> None:
        """A group unreadable after a successful CAS still answers 404 defensively.

        Given: the DAL terminalizes but the refresh read returns None,
        When: terminalize is posted,
        Then: HTTP 404 rather than a 500.
        """
        repo = _sql_repo()
        repo.get_current_paired_execution_group = AsyncMock(side_effect=[_group_row("grp-1"), None])
        repo.terminalize_paired_execution_group = AsyncMock(
            return_value=PairedGroupTerminalizeOutcome.TERMINALIZED
        )
        with pytest.raises(HTTPException) as exc:
            await terminalize_paired_execution_group(
                request=_make_request(),
                group_public_id="grp-1",
                principal=_ADMIN,
                _csrf=None,
                repo=repo,
            )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_non_sql_repository_is_503(self) -> None:
        """A non-SQL repository deployment answers 503 on terminalize too.

        Given: a repository that is not the SQL implementation,
        When: terminalize is posted,
        Then: HTTP 503.
        """
        with pytest.raises(HTTPException) as exc:
            await terminalize_paired_execution_group(
                request=_make_request(),
                group_public_id="grp-1",
                principal=_ADMIN,
                _csrf=None,
                repo=AsyncMock(),
            )
        assert exc.value.status_code == 503


def test_manage_paired_execution_role_mapping() -> None:
    """The attestation permission is OPERATOR+ADMIN, never VIEWER or AI_DELEGATE.

    Given: the role-permission matrix,
    When: MANAGE_PAIRED_EXECUTION membership is checked per role,
    Then: OPERATOR and ADMIN hold it; VIEWER and AI_DELEGATE do not — an AI
        delegate or read-only user can never attest away a real-money halt.
    """
    assert Permission.MANAGE_PAIRED_EXECUTION in ROLE_PERMISSIONS[UserRole.OPERATOR]
    assert Permission.MANAGE_PAIRED_EXECUTION in ROLE_PERMISSIONS[UserRole.ADMIN]
    assert Permission.MANAGE_PAIRED_EXECUTION not in ROLE_PERMISSIONS[UserRole.VIEWER]
    assert Permission.MANAGE_PAIRED_EXECUTION not in ROLE_PERMISSIONS[UserRole.AI_DELEGATE]
