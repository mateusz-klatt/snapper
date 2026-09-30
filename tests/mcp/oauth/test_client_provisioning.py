"""Tests for operator-controlled MCP OAuth client provisioning."""

from pathlib import Path

import bcrypt
import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

import snapper.mcp.oauth.client_provisioning as provisioning
from snapper.data.models import OAuthClient
from snapper.data.repository import SQLAlchemyRepository
from snapper.mcp.oauth.client_provisioning import OAuthClientAlreadyExistsError
from snapper.mcp.oauth.client_provisioning import OAuthClientProvisioningError
from snapper.mcp.oauth.client_provisioning import provision_oauth_client
from snapper.mcp.oauth.scopes import OFFLINE_ACCESS
from snapper.mcp.oauth.scopes import SNAPPER_ACCOUNT_READ
from snapper.mcp.oauth.scopes import SNAPPER_READ
from snapper.mcp.oauth.scopes import SNAPPER_RESEARCH_WRITE
from snapper.mcp.oauth.scopes import SNAPPER_REVIEW_WRITE
from snapper.mcp.oauth.scopes import SNAPPER_TRADE
from snapper.mcp.oauth.store import MCPOAuthStore

_REDIRECT_URI = "https://chatgpt.com/connector/oauth/callback?tenant=private"


class _RecordingStore:
    """Record validated rows without opening a database."""

    def __init__(self) -> None:
        """Initialize an empty write record."""
        self.clients: list[OAuthClient] = []

    async def add_client(self, client: OAuthClient) -> None:
        """Record one client row."""
        self.clients.append(client)


class _IntegrityFailureStore:
    """Raise one non-duplicate database integrity failure."""

    async def add_client(self, client: OAuthClient) -> None:
        """Reject the row under an unrelated database constraint.

        Args:
            client: Validated row that would have been persisted.

        Raises:
            IntegrityError: Always, with no client-ID constraint marker.
        """
        raise IntegrityError("INSERT oauth_clients", {}, ValueError("unrelated check failed"))


class _ConstraintDiagnostic:
    """Expose one driver-style PostgreSQL constraint name."""

    def __init__(self, constraint_name: str) -> None:
        """Store the exact diagnostic constraint name."""
        self.constraint_name = constraint_name


class _DriverIntegrityError(Exception):
    """Model structured integrity payloads from supported database drivers."""

    def __init__(
        self,
        message: str,
        *,
        constraint_name: str | None = None,
        diagnostic_constraint_name: str | None = None,
        sqlite_errorcode: int | None = None,
    ) -> None:
        """Store optional asyncpg, psycopg, and SQLite diagnostics."""
        super().__init__(message)
        self.constraint_name = constraint_name
        self.diag = (
            None
            if diagnostic_constraint_name is None
            else _ConstraintDiagnostic(diagnostic_constraint_name)
        )
        self.sqlite_errorcode = sqlite_errorcode


def _wrapped_driver_error(driver_error: BaseException) -> BaseException:
    """Model SQLAlchemy asyncpg's adapter error and causal driver payload."""
    wrapper = _DriverIntegrityError("SQLAlchemy asyncpg adapter integrity error")
    wrapper.__cause__ = driver_error
    return wrapper


async def _store(tmp_path: Path) -> tuple[SQLAlchemyRepository, MCPOAuthStore, Path]:
    """Create one isolated OAuth store and expose its SQLite file."""
    database_path = tmp_path / "oauth-client.db"
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{database_path}")
    await repository.create_all()
    return repository, MCPOAuthStore(repository), database_path


@pytest.mark.asyncio
async def test_provisioning_generates_entropy_and_persists_only_bcrypt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify generated credentials are high entropy and hash-only at rest.

    Given: Valid client metadata and deterministic test entropy.
    When: The operator provisioning service commits the client.
    Then: It returns raw credentials while SQLite contains only a verifiable bcrypt hash.
    """
    repository, store, database_path = await _store(tmp_path)
    entropy_requests: list[int] = []

    def _token_urlsafe(byte_count: int) -> str:
        entropy_requests.append(byte_count)
        return {32: "client-id-entropy", 48: "raw-client-secret"}[byte_count]

    monkeypatch.setattr(provisioning, "token_urlsafe", _token_urlsafe)
    result = await provision_oauth_client(
        store,
        client_name="Private ChatGPT",
        redirect_uris=[_REDIRECT_URI],
    )

    assert entropy_requests == [32, 48]
    assert result.client_id == "snapper-mcp-client-id-entropy"
    assert result.client_secret == "raw-client-secret"
    assert result.client_secret not in repr(result)
    assert result.redirect_uris == (_REDIRECT_URI,)
    assert result.allowed_scopes == (SNAPPER_READ, OFFLINE_ACCESS)
    row = await store.get_client(result.client_id)
    assert row is not None
    assert row.client_secret_hash != result.client_secret
    assert bcrypt.checkpw(result.client_secret.encode(), row.client_secret_hash.encode())
    assert row.redirect_uris == [_REDIRECT_URI]
    assert row.allowed_scopes == [SNAPPER_READ, OFFLINE_ACCESS]
    assert result.client_secret.encode() not in database_path.read_bytes()
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_duplicate_generated_client_id_is_refused_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an inactive client's identifier remains durably reserved.

    Given: An inactive stored client and deterministic generation of the same identifier.
    When: A second client attempts to provision with that identifier.
    Then: The write is refused atomically and the original row remains unchanged.
    """
    repository, store, _database_path = await _store(tmp_path)

    def _token_urlsafe(byte_count: int) -> str:
        return {32: "same-client", 48: "same-secret"}[byte_count]

    monkeypatch.setattr(provisioning, "token_urlsafe", _token_urlsafe)
    first = await provision_oauth_client(
        store,
        client_name="First",
        redirect_uris=[_REDIRECT_URI],
    )
    async with repository.session() as session:
        stored = (
            await session.execute(
                select(OAuthClient).where(OAuthClient.client_id == first.client_id)
            )
        ).scalar_one()
        stored.is_active = False
        await session.commit()
    assert await store.get_client(first.client_id) is None
    with pytest.raises(OAuthClientAlreadyExistsError, match=first.client_id):
        await provision_oauth_client(
            store,
            client_name="Second",
            redirect_uris=[_REDIRECT_URI],
        )

    async with repository.session() as session:
        row = (
            await session.execute(
                select(OAuthClient).where(OAuthClient.client_id == first.client_id)
            )
        ).scalar_one()
    assert row.client_name == "First"
    assert row.is_active is False
    assert bcrypt.checkpw(first.client_secret.encode(), row.client_secret_hash.encode())
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_unrelated_integrity_failure_is_not_mislabeled_as_duplicate() -> None:
    """Verify only the client-ID constraint maps to the duplicate refusal.

    Given: A persistence failure from an unrelated integrity constraint.
    When: A validated client reaches the store.
    Then: The original database exception propagates without a misleading collision label.
    """
    store = _IntegrityFailureStore()
    with pytest.raises(IntegrityError, match="unrelated check failed"):
        await provision_oauth_client(
            store,
            client_name="Private ChatGPT",
            redirect_uris=[_REDIRECT_URI],
        )


@pytest.mark.parametrize(
    ("original", "expected"),
    [
        (
            _DriverIntegrityError(
                "duplicate",
                constraint_name="uq_oauth_clients_client_id",
            ),
            True,
        ),
        (
            _DriverIntegrityError(
                "duplicate",
                constraint_name="uq_oauth_clients_public_id",
            ),
            False,
        ),
        (
            _DriverIntegrityError(
                "duplicate",
                diagnostic_constraint_name="uq_oauth_clients_client_id",
            ),
            True,
        ),
        (
            _DriverIntegrityError(
                "duplicate",
                diagnostic_constraint_name="uq_oauth_clients_public_id",
            ),
            False,
        ),
        (
            _wrapped_driver_error(
                _DriverIntegrityError(
                    "duplicate",
                    constraint_name="uq_oauth_clients_client_id",
                )
            ),
            True,
        ),
        (
            _wrapped_driver_error(
                _DriverIntegrityError(
                    "duplicate",
                    constraint_name="uq_oauth_clients_public_id",
                )
            ),
            False,
        ),
        (
            _DriverIntegrityError(
                "UNIQUE constraint failed: oauth_clients.client_id",
                sqlite_errorcode=2067,
            ),
            True,
        ),
        (
            _DriverIntegrityError(
                "UNIQUE constraint failed: oauth_clients.public_id",
                sqlite_errorcode=2067,
            ),
            False,
        ),
        (
            _DriverIntegrityError(
                "NOT NULL constraint failed: oauth_clients.client_id",
                sqlite_errorcode=1299,
            ),
            False,
        ),
    ],
)
def test_client_id_conflict_detection_uses_structured_exact_evidence(
    original: BaseException,
    expected: bool,
) -> None:
    """Verify only supported client-ID unique shapes map to a collision.

    Given: One driver-shaped integrity error and its expected classification.
    When: The provisioning service inspects the structured database evidence.
    Then: Only the exact client-ID unique constraint is classified as a collision.
    """
    error = IntegrityError("INSERT oauth_clients", {}, original)

    assert provisioning._is_client_id_conflict(error) is expected


@pytest.mark.asyncio
async def test_custom_valid_scopes_and_post_auth_method_are_preserved() -> None:
    """Verify supported explicit metadata is preserved exactly.

    Given: Multiple exact redirect URIs, ordered scopes, and post authentication.
    When: The client is provisioned through a recording store.
    Then: The result and stored row preserve every ordered value.
    """
    store = _RecordingStore()

    result = await provision_oauth_client(
        store,
        client_name="Operator connector",
        redirect_uris=[_REDIRECT_URI, "http://127.0.0.1:8787/callback"],
        token_endpoint_auth_method="client_secret_post",
        allowed_scopes=[OFFLINE_ACCESS, SNAPPER_READ],
    )

    assert result.token_endpoint_auth_method == "client_secret_post"
    assert result.allowed_scopes == (OFFLINE_ACCESS, SNAPPER_READ)
    assert store.clients[0].redirect_uris == [
        _REDIRECT_URI,
        "http://127.0.0.1:8787/callback",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("redirect_uris", "error_match"),
    [
        (["http://example.com/callback"], "HTTPS or loopback HTTP"),
        (["https://example.com/callback#fragment"], "fragment"),
        ([" callback "], "whitespace"),
        (["https://example.com/call back"], "whitespace"),
        (["callback"], "invalid redirect URI"),
        (["HTTPS://example.com/callback"], "canonical form"),
        (["https://operator@example.com/callback"], "user information"),
        ([], "at least one redirect URI"),
        ([_REDIRECT_URI, _REDIRECT_URI], "duplicate redirect URI"),
    ],
)
async def test_invalid_redirect_uri_is_refused_before_persistence(
    redirect_uris: list[str],
    error_match: str,
) -> None:
    """Verify unsafe or ambiguous redirect metadata fails before persistence.

    Given: One invalid redirect URI collection.
    When: Client provisioning validates the request.
    Then: It raises a stable refusal and performs no store write.
    """
    store = _RecordingStore()

    with pytest.raises(OAuthClientProvisioningError, match=error_match):
        await provision_oauth_client(
            store,
            client_name="Private ChatGPT",
            redirect_uris=redirect_uris,
        )

    assert store.clients == []


@pytest.mark.asyncio
async def test_unknown_scope_and_auth_method_are_refused_before_persistence() -> None:
    """Verify values outside closed OAuth catalogs fail before persistence.

    Given: An unknown scope and an unsupported authentication method.
    When: Each client provisioning request is validated.
    Then: Both requests are refused and neither reaches the store.
    """
    store = _RecordingStore()

    with pytest.raises(OAuthClientProvisioningError, match="Unknown OAuth scope: snapper.admin"):
        await provision_oauth_client(
            store,
            client_name="Private ChatGPT",
            redirect_uris=[_REDIRECT_URI],
            allowed_scopes=[SNAPPER_READ, "snapper.admin"],
        )
    with pytest.raises(
        OAuthClientProvisioningError, match="Unsupported token endpoint auth method"
    ):
        await provision_oauth_client(
            store,
            client_name="Private ChatGPT",
            redirect_uris=[_REDIRECT_URI],
            token_endpoint_auth_method="none",
        )

    assert store.clients == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scope",
    (
        SNAPPER_ACCOUNT_READ,
        SNAPPER_TRADE,
        SNAPPER_REVIEW_WRITE,
        SNAPPER_RESEARCH_WRITE,
    ),
)
async def test_deferred_catalog_scopes_are_refused_before_persistence(scope: str) -> None:
    """Verify the dormant first connector cannot pre-register future authority.

    Given: One valid catalog scope deferred beyond the initial read-only release.
    When: An operator attempts to include it in a new client's scope ceiling.
    Then: Provisioning refuses before persisting any client row.
    """
    store = _RecordingStore()

    with pytest.raises(OAuthClientProvisioningError, match="initial read-only connector"):
        await provision_oauth_client(
            store,
            client_name="Private ChatGPT",
            redirect_uris=[_REDIRECT_URI],
            allowed_scopes=[SNAPPER_READ, scope],
        )

    assert store.clients == []


@pytest.mark.asyncio
async def test_empty_name_scope_set_and_duplicate_scope_are_refused() -> None:
    """Verify incomplete or ambiguous client metadata fails closed.

    Given: Invalid names plus empty and duplicate scope collections.
    When: Each client provisioning request is validated.
    Then: Every request is refused and no client row is recorded.
    """
    store = _RecordingStore()

    with pytest.raises(OAuthClientProvisioningError, match="client name"):
        await provision_oauth_client(store, client_name="  ", redirect_uris=[_REDIRECT_URI])
    with pytest.raises(OAuthClientProvisioningError, match="surrounding whitespace"):
        await provision_oauth_client(
            store,
            client_name=" Private ChatGPT",
            redirect_uris=[_REDIRECT_URI],
        )
    with pytest.raises(OAuthClientProvisioningError, match="200 characters"):
        await provision_oauth_client(
            store,
            client_name="x" * 201,
            redirect_uris=[_REDIRECT_URI],
        )
    with pytest.raises(OAuthClientProvisioningError, match="at least one OAuth scope"):
        await provision_oauth_client(
            store,
            client_name="Private ChatGPT",
            redirect_uris=[_REDIRECT_URI],
            allowed_scopes=[],
        )
    with pytest.raises(OAuthClientProvisioningError, match="duplicate OAuth scope"):
        await provision_oauth_client(
            store,
            client_name="Private ChatGPT",
            redirect_uris=[_REDIRECT_URI],
            allowed_scopes=[SNAPPER_READ, SNAPPER_READ],
        )

    assert store.clients == []
