"""Per-wallet credential resolver for executor processes.

Credentials live in the ``wallet_credentials`` table, are encrypted
at rest with the same
Fernet key as ``settings``, and are pulled exactly once at executor
process startup. Rotation requires a process restart of the affected
executor instance — there is no hot-reload contract.

The encrypted payload is a JSON envelope whose inner shape depends on
``credential_type`` (see ``WalletCredential`` docstring):

* ``api_key_secret`` -> ``{"api_key": "...", "api_secret": "..."}``
* ``rsa_pem``        -> ``{"api_key": "...", "private_key_pem": "..."}``
* ``oauth``          -> ``{"client_id": "...", "client_secret": "...",
                          "refresh_token": "..."}``
* ``paper``          -> ``{"initial_balance": 10000.0}`` (numeric values
                          are stringified after JSON decode)

Errors:

* ``CredentialNotFoundError`` is raised when no active credential row
  exists for the requested ``(exchange, wallet_public_id)`` pair. The
  executor process should fail fast at startup so the operator notices
  immediately.
* ``ValueError`` from the repository is propagated when
  ``wallet_public_id`` is not a parseable UUID. Valid UUID aliases are
  canonicalized before lookup.
* Decryption failures bubble up unwrapped from
  ``SettingsEncryptionService.decrypt`` so the cause (wrong master
  password, key rotation, payload corruption) surfaces in the log.
"""

import json
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from snapper.core.json_types import JsonObject
from snapper.data.repository import Repository
from snapper.infrastructure.security.encryption import SettingsEncryptionService
from snapper.infrastructure.security.encryption import get_encryption_service


class CredentialNotFoundError(Exception):
    """Raised when no active wallet credential exists for the request.

    Maps to executor startup failure with an actionable message: the
    operator must seed a credential for the requested ``(exchange,
    wallet_public_id)`` pair before the executor can boot.
    """

    def __init__(self, *, exchange: str, wallet_public_id: str) -> None:
        """Capture the missing credential identity for the executor log."""
        super().__init__(
            f"No active wallet credential for exchange={exchange!r} "
            f"wallet_public_id={wallet_public_id!r}. Seed a row in "
            "wallet_credentials before starting the executor process."
        )
        self.exchange = exchange
        self.wallet_public_id = wallet_public_id


@dataclass(frozen=True)
class CredentialVersion:
    """Public identity of the exact successfully decrypted credential revision.

    Secret envelope contents are deliberately absent. Consumers can bind the
    actual selected client to this revision without rereading a possibly rotated
    credential or putting a secret into an admission/accounting witness.
    """

    public_id: str
    wallet_public_id: str
    exchange: str
    credential_type: str
    timestamp: datetime


class CredentialResolver:
    """Resolve per-wallet credentials at executor process startup.

    The resolver pulls a fresh row from the repository on every call and
    never caches the decrypted payload. It retains only the public revision
    metadata of its last successful selection, clearing it before every lookup.
    Executor processes call ``get_credentials`` exactly once during
    ``start()`` and pass the resulting dict into the exchange client
    constructor. Rotation = restart.

    The resolver does NOT subscribe to any ZMQ topic and does NOT
    listen for ``settings`` updates: ``wallet_credentials`` is a
    pull-only surface by design (broadcast safety — no ZMQ propagation).
    """

    def __init__(
        self,
        repository: Repository,
        *,
        encryption_service: SettingsEncryptionService | None = None,
    ) -> None:
        """Bind the resolver to a repository and (optionally) an encryptor.

        Args:
            repository: The repository to query for credentials.
            encryption_service: Optional pre-built encryption service.
                When ``None``, the singleton from
                ``get_encryption_service()`` is used so production
                callers do not need to thread it through.
        """
        self._repository = repository
        self._encryption = encryption_service or get_encryption_service()
        self._selected_version: CredentialVersion | None = None

    @property
    def selected_version(self) -> CredentialVersion | None:
        """Return public revision evidence from the last successful resolution.

        Returns:
            Exact selected database revision, absent before resolution or after
            any failed lookup, decoding or decryption attempt.
        """
        return self._selected_version

    async def get_credentials(
        self,
        *,
        exchange: str,
        wallet_public_id: str,
        as_of: datetime | None = None,
    ) -> dict[str, str]:
        """Return the decrypted credential dict for ``(exchange, wallet)``.

        Args:
            exchange: Exchange identifier (case-insensitive — normalized
                to lowercase before the DB lookup).
            wallet_public_id: UUID of the wallet whose credentials to fetch.
                Uppercase and hyphenless aliases are canonicalized by the
                repository before lookup.
            as_of: Optional bus time for the temporal query. Defaults to
                ``datetime.now(UTC)`` which is the right answer at
                process startup; tests pass an explicit timestamp.

        Returns:
            Dict with all string values from the decrypted JSON envelope.
            Numeric values (e.g. ``initial_balance`` for paper mode) are
            stringified so the calling code does not need to know the
            inner schema for each ``credential_type``.

        Raises:
            CredentialNotFoundError: No active credential row matches.
            ValueError: The wallet identity is not a parseable UUID.
        """
        self._selected_version = None
        bus_time = as_of or datetime.now(UTC)
        normalized_exchange = exchange.lower()
        row = await self._repository.get_active_credential(
            exchange=normalized_exchange,
            wallet_public_id=wallet_public_id,
            as_of=bus_time,
        )
        if row is None:
            raise CredentialNotFoundError(
                exchange=normalized_exchange, wallet_public_id=wallet_public_id
            )
        plaintext = self._encryption.decrypt(row["encrypted_payload"])
        envelope: JsonObject = json.loads(plaintext)
        credentials = {key: str(value) for key, value in envelope.items()}
        self._selected_version = CredentialVersion(
            public_id=row["public_id"],
            wallet_public_id=row["wallet_public_id"],
            exchange=row["exchange"],
            credential_type=row["credential_type"],
            timestamp=row["timestamp"],
        )
        return credentials
