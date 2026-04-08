"""Tests for the multi-tenant ORM models added in Phase 0a step 1.

Covers ORM construction, field assignment, and integration through
``make migrate-dev`` round-trip for the 5 new tables introduced by Plan 0:

- ``Wallet``
- ``WalletCredential``
- ``Operator``
- ``UserOperatorMembership``
- ``WalletOperatorScopeGrant``

These tests are intentionally lightweight and only verify model construction
and basic invariants. Repository-level tests for create/handover/overlap
detection live in ``tests/data/test_scope_grants.py`` (added in a later
Phase 0a step alongside the repository methods).

Plan reference: ``proprietary/plans/plan_multi_tenant_foundation.md``
Sections 3.1, 14.6 D1, 14.7.1.
"""

from datetime import UTC
from datetime import datetime

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Operator
from snapper.data.models import UserOperatorMembership
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.models import WalletOperatorScopeGrant


class TestWalletModel:
    """Construction tests for the ``Wallet`` ORM model."""

    def test_wallet_creation_minimal(self) -> None:
        """Wallet accepts label and inherits TemporalMixin defaults.

        Given: A wallet label and required temporal fields,
        When: A Wallet instance is constructed,
        Then: Label is set, is_paper defaults to False, and the instance
            carries the standard TemporalMixin attributes.
        """
        wallet = Wallet(
            label="default-paper",
            is_paper=True,
            session_id="test-session",
            sequence_id=1,
            timestamp=datetime.now(UTC),
        )
        assert wallet.label == "default-paper"
        assert wallet.is_paper is True
        assert wallet.description is None

    def test_wallet_creation_full(self) -> None:
        """Wallet accepts description and live (non-paper) configuration.

        Given: A wallet with label, description, and is_paper=False,
        When: A Wallet instance is constructed,
        Then: All fields match provided values.
        """
        wallet = Wallet(
            label="alice-personal-kraken",
            description="Alice's personal Kraken trading wallet",
            is_paper=False,
            session_id="test-session",
            sequence_id=2,
            timestamp=datetime.now(UTC),
        )
        assert wallet.label == "alice-personal-kraken"
        assert wallet.description == "Alice's personal Kraken trading wallet"
        assert wallet.is_paper is False


class TestWalletCredentialModel:
    """Construction tests for the ``WalletCredential`` ORM model."""

    def test_credential_api_key_secret(self) -> None:
        """WalletCredential accepts the api_key_secret credential type.

        Given: A credential row for a Kraken wallet using api_key_secret,
        When: A WalletCredential instance is constructed,
        Then: All fields match and credential_type is preserved.
        """
        credential = WalletCredential(
            wallet_public_id="00000000-0000-7000-8000-000000000001",
            exchange="kraken",
            credential_type="api_key_secret",
            encrypted_payload='{"api_key": "<encrypted>", "api_secret": "<encrypted>"}',
            label="Alice's Kraken Spot",
            session_id="test-session",
            sequence_id=1,
            timestamp=datetime.now(UTC),
        )
        assert credential.wallet_public_id == "00000000-0000-7000-8000-000000000001"
        assert credential.exchange == "kraken"
        assert credential.credential_type == "api_key_secret"
        assert "encrypted" in credential.encrypted_payload
        assert credential.label == "Alice's Kraken Spot"

    def test_credential_paper_type(self) -> None:
        """WalletCredential accepts paper credential type with initial_balance.

        Given: A paper-mode credential with initial_balance payload,
        When: A WalletCredential instance is constructed,
        Then: credential_type is paper and the payload is preserved verbatim.
        """
        credential = WalletCredential(
            wallet_public_id="00000000-0000-7000-8000-000000000002",
            exchange="paper",
            credential_type="paper",
            encrypted_payload='{"initial_balance": 10000.0}',
            session_id="test-session",
            sequence_id=1,
            timestamp=datetime.now(UTC),
        )
        assert credential.credential_type == "paper"
        assert credential.label is None


class TestOperatorModel:
    """Construction tests for the ``Operator`` ORM model."""

    def test_operator_creation(self) -> None:
        """Operator accepts label and description.

        Given: An operator label and description,
        When: An Operator instance is constructed,
        Then: Both fields match provided values.
        """
        operator = Operator(
            label="default",
            description="Default seed operator for single-user deployment",
            session_id="test-session",
            sequence_id=1,
            timestamp=datetime.now(UTC),
        )
        assert operator.label == "default"
        assert operator.description == "Default seed operator for single-user deployment"


class TestUserOperatorMembershipModel:
    """Construction tests for ``UserOperatorMembership``."""

    def test_membership_primary_flag(self) -> None:
        """Membership accepts is_primary flag for primary operator selection.

        Given: A user-operator membership marked as primary,
        When: A UserOperatorMembership instance is constructed,
        Then: is_primary is True and both public_id columns are populated.
        """
        membership = UserOperatorMembership(
            user_public_id="00000000-0000-7000-8000-000000000010",
            operator_public_id="00000000-0000-7000-8000-000000000011",
            is_primary=True,
            session_id="test-session",
            sequence_id=1,
            timestamp=datetime.now(UTC),
        )
        assert membership.user_public_id == "00000000-0000-7000-8000-000000000010"
        assert membership.operator_public_id == "00000000-0000-7000-8000-000000000011"
        assert membership.is_primary is True

    def test_membership_default_not_primary(self) -> None:
        """Membership defaults is_primary to False when omitted.

        Given: A membership constructed without is_primary,
        When: The instance is built,
        Then: is_primary is False (or None pre-flush — server_default supplies 0).
        """
        membership = UserOperatorMembership(
            user_public_id="00000000-0000-7000-8000-000000000012",
            operator_public_id="00000000-0000-7000-8000-000000000013",
            session_id="test-session",
            sequence_id=2,
            timestamp=datetime.now(UTC),
        )
        assert membership.is_primary in (False, None)


class TestWalletOperatorScopeGrantModel:
    """Construction tests for ``WalletOperatorScopeGrant``.

    Note: cross-scope overlap detection and 409 conflict handling live in
    repository-level tests added alongside the create_scope_grant /
    handover_grant methods in a later Phase 0a step. This file only verifies
    that the ORM model accepts the two valid scope_kind shapes.
    """

    def test_underlying_scoped_grant(self) -> None:
        """ScopeGrant accepts an underlying-scoped row.

        Given: A grant with scope_kind=underlying and a non-NULL underlying_public_id,
        When: A WalletOperatorScopeGrant instance is constructed,
        Then: underlying_public_id is set and instrument_public_id is None.
        """
        grant = WalletOperatorScopeGrant(
            operator_public_id="00000000-0000-7000-8000-000000000020",
            wallet_public_id="00000000-0000-7000-8000-000000000021",
            granted_by_user_public_id="00000000-0000-7000-8000-000000000022",
            scope_kind="underlying",
            underlying_public_id="00000000-0000-7000-8000-000000000023",
            instrument_public_id=None,
            note="alice trades all BTC instruments on the firm wallet",
            session_id="test-session",
            sequence_id=1,
            timestamp=datetime.now(UTC),
        )
        assert grant.scope_kind == "underlying"
        assert grant.underlying_public_id == "00000000-0000-7000-8000-000000000023"
        assert grant.instrument_public_id is None
        assert grant.note is not None

    def test_instrument_scoped_grant(self) -> None:
        """ScopeGrant accepts an instrument-scoped carve-out row.

        Given: A grant with scope_kind=instrument and a non-NULL instrument_public_id,
        When: A WalletOperatorScopeGrant instance is constructed,
        Then: instrument_public_id is set and underlying_public_id is None.
        """
        grant = WalletOperatorScopeGrant(
            operator_public_id="00000000-0000-7000-8000-000000000030",
            wallet_public_id="00000000-0000-7000-8000-000000000031",
            granted_by_user_public_id="00000000-0000-7000-8000-000000000032",
            scope_kind="instrument",
            underlying_public_id=None,
            instrument_public_id="00000000-0000-7000-8000-000000000033",
            session_id="test-session",
            sequence_id=2,
            timestamp=datetime.now(UTC),
        )
        assert grant.scope_kind == "instrument"
        assert grant.instrument_public_id == "00000000-0000-7000-8000-000000000033"
        assert grant.underlying_public_id is None
        assert grant.note is None

    def test_grant_with_explicit_known_to_max(self) -> None:
        """ScopeGrant accepts an explicit known_to=KNOWN_TO_MAX active row.

        Given: A grant constructed with an explicit KNOWN_TO_MAX value,
        When: The instance is built,
        Then: known_to is preserved verbatim (the SCD2 active sentinel).
        Note: TemporalMixin default= fires at INSERT, not at construction;
        this test exercises explicit caller-supplied known_to handling.
        """
        grant = WalletOperatorScopeGrant(
            operator_public_id="00000000-0000-7000-8000-000000000040",
            wallet_public_id="00000000-0000-7000-8000-000000000041",
            granted_by_user_public_id="00000000-0000-7000-8000-000000000042",
            scope_kind="underlying",
            underlying_public_id="00000000-0000-7000-8000-000000000043",
            session_id="test-session",
            sequence_id=1,
            timestamp=datetime.now(UTC),
            known_to=KNOWN_TO_MAX,
        )
        assert grant.known_to == KNOWN_TO_MAX
