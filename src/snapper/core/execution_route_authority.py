"""Immutable public account-route evidence shared by admission and accounting.

Credentials are identified only by their public database identity and exact
selected revision. This module never accepts secret credential material. A
logical account scope remains stable across credential rotations so a new
authority cannot silently create a second spendable cash namespace.
"""

import hashlib
import json
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Literal
from typing import Self
from urllib.parse import urlsplit

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceExchange
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import SnapshotReceipt

ACCOUNT_SCOPE_HASH_DOMAIN = b"snapper:execution-account-scope:v1\x00"
CLIENT_SELECTION_HASH_DOMAIN = b"snapper:execution-client-selection:v1\x00"
ROUTE_SELECTION_HASH_DOMAIN = b"snapper:execution-route-selection:v1\x00"
ROUTE_AUTHORITY_HASH_DOMAIN = b"snapper:execution-route-authority:v1\x00"


def _public_origin(value: str) -> str:
    """Accept a public HTTPS origin without credentials, parameters or request paths."""
    parsed = urlsplit(value)
    try:
        _validated_port = parsed.port
    except ValueError as error:
        raise ValueError("Execution API origin has an invalid port") from error
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or value != f"https://{parsed.netloc}"
    ):
        raise ValueError("Execution API evidence must contain only a public HTTPS origin")
    return value


type PublicApiOrigin = Annotated[EvidenceText, AfterValidator(_public_origin)]


class _FrozenRouteEvidence(BaseModel):
    """Reject implicit coercion, unchecked model updates and unknown evidence fields."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class ExecutionEvidenceIdentity(_FrozenRouteEvidence):
    """Insertion identity with the durable knowledge clock supplied by its producer."""

    public_id: EvidenceUUID
    session_id: EvidenceUUID
    sequence_id: int = Field(ge=1)


class ExecutionAccountScope(_FrozenRouteEvidence):
    """One logical cash/margin account, independent of instruments and credential versions."""

    wallet_public_id: EvidenceUUID
    exchange: EvidenceExchange
    mode: Literal["live", "paper"]
    environment: Literal["production", "demo", "paper"]
    margin_account: EvidenceText

    @model_validator(mode="after")
    def validate_environment(self) -> Self:
        """Keep simulated and authenticated environments in disjoint account scopes.

        Returns:
            The same explicit, internally consistent account scope.
        """
        _validate_environment(self.exchange, self.mode, self.environment)
        return self


def _validate_environment(exchange: str, mode: str, environment: str) -> None:
    """Keep actual client and account environment namespaces consistently separated."""
    paper = exchange == "paper"
    if paper != (mode == "paper") or paper != (environment == "paper"):
        raise ValueError("Paper exchange, mode and environment must agree")


def _validate_selection(
    environment: str,
    credential_public_id: str | None,
    credential_timestamp: datetime | None,
    public_api_origin: str | None,
) -> None:
    """Refuse missing actual credentials/origins without manufacturing an account label."""
    if (credential_public_id is None) != (credential_timestamp is None):
        raise ValueError("Credential UUID and exact revision must be supplied together")
    if environment == "paper":
        if public_api_origin is not None:
            raise ValueError("Paper authority cannot claim an authenticated venue origin")
    elif credential_public_id is None or public_api_origin is None:
        raise ValueError("Authenticated authority requires an explicit credential and origin")


class ExecutionClientSelection(_FrozenRouteEvidence):
    """Actual selected client identity even when an incoming fill's margin account is unknown."""

    wallet_public_id: EvidenceUUID
    exchange: EvidenceExchange
    mode: Literal["live", "paper"]
    environment: Literal["production", "demo", "paper"]
    credential_public_id: EvidenceUUID | None
    credential_timestamp: EvidenceInstant | None
    public_api_origin: PublicApiOrigin | None

    @model_validator(mode="after")
    def validate_actual_client(self) -> Self:
        """Bind actual environment and public credential revision without guessing a cash scope.

        Returns:
            The same explicit client selection, independent of margin/instrument interpretation.
        """
        _validate_environment(self.exchange, self.mode, self.environment)
        _validate_selection(
            self.environment,
            self.credential_public_id,
            self.credential_timestamp,
            self.public_api_origin,
        )
        return self


def client_selection_digest(selection: ExecutionClientSelection) -> str:
    """Hash actual public client routing without requiring a known margin account.

    Args:
        selection: Actual selected wallet, venue, environment, credential revision and API origin.

    Returns:
        Versioned SHA-256 usable by raw capture before financial interpretation exists.
    """
    proven = ExecutionClientSelection.model_validate(selection)
    material = json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(CLIENT_SELECTION_HASH_DOMAIN + material.encode()).hexdigest()


def account_scope_digest(scope: ExecutionAccountScope) -> str:
    """Hash a logical account without forking its cash when credentials rotate.

    Args:
        scope: Actual wallet, venue, mode, environment and shared margin account.

    Returns:
        Stable versioned SHA-256 of the complete account scope.
    """
    proven = ExecutionAccountScope.model_validate(scope)
    material = json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(ACCOUNT_SCOPE_HASH_DOMAIN + material.encode()).hexdigest()


def route_selection_digest(
    scope: ExecutionAccountScope,
    credential_public_id: str | None,
    credential_timestamp: datetime | None,
    public_api_origin: str | None,
) -> str:
    """Identify the actual selected public route independently of response freshness.

    Args:
        scope: Actual shared account namespace.
        credential_public_id: Exact selected credential UUID, or explicit Paper absence.
        credential_timestamp: Selected SCD2 credential revision, paired with its UUID.
        public_api_origin: Actual selected client HTTPS origin, absent only for Paper.

    Returns:
        Versioned SHA-256 over public route identity, without secret credential contents.
    """
    selection = ExecutionRouteSelection(
        scope=scope,
        credential_public_id=credential_public_id,
        credential_timestamp=credential_timestamp,
        public_api_origin=public_api_origin,
    )
    material = json.dumps(selection.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(ROUTE_SELECTION_HASH_DOMAIN + material.encode()).hexdigest()


class ExecutionRouteSelection(_FrozenRouteEvidence):
    """The public credential revision and actual endpoint selected by the client."""

    scope: ExecutionAccountScope
    credential_public_id: EvidenceUUID | None
    credential_timestamp: EvidenceInstant | None
    public_api_origin: PublicApiOrigin | None

    @model_validator(mode="after")
    def validate_selection(self) -> Self:
        """Refuse implicit live/demo credentials and origins or a partial credential identity.

        Returns:
            The same complete route selection.
        """
        _validate_selection(
            self.scope.environment,
            self.credential_public_id,
            self.credential_timestamp,
            self.public_api_origin,
        )
        return self


def client_selection_from_authority(
    authority: ExecutionRouteSelection,
) -> ExecutionClientSelection:
    """Project an account-bound authority onto the same public client identity used by raw capture.

    Args:
        authority: Immutable authority or actual account-route selection.

    Returns:
        Actual client identity excluding the separately interpreted margin account.
    """
    scope = authority.scope
    return ExecutionClientSelection(
        wallet_public_id=scope.wallet_public_id,
        exchange=scope.exchange,
        mode=scope.mode,
        environment=scope.environment,
        credential_public_id=authority.credential_public_id,
        credential_timestamp=authority.credential_timestamp,
        public_api_origin=authority.public_api_origin,
    )


class ExecutionRouteAuthorityPayload(ExecutionRouteSelection):
    """One bounded account observation through a proven actual client route.

    A caller observing the actual authenticated client attests the route-to-
    response binding. The store additionally verifies durable wallet,
    credential and account-observation lineage. Existing account observations
    alone cannot establish which credential produced a response.
    """

    schema_version: Literal[1] = 1
    venue_account_id: EvidenceText | None = None
    account_observation_public_id: EvidenceUUID
    account_observation_timestamp: EvidenceInstant
    balance_observed_at: EvidenceInstant
    balance_receipt_json: EvidenceText
    observed_at: EvidenceInstant
    valid_until: EvidenceInstant
    route_revision: EvidenceDigest

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        """Reject booleans and floats masquerading as the integer schema version."""
        if type(value) is not int:
            raise ValueError("Route authority schema version must be a strict integer")
        return value

    @field_validator("balance_receipt_json")
    @classmethod
    def _decoded_balances(cls, value: str) -> str:
        """Validate decoded balance evidence without pretending it retains wire bytes."""
        SnapshotReceipt(
            representation="decoded_json_text",
            material=value,
            endpoint_or_channel="snapper:venue_account_observations.balances_json",
            observed_at=datetime(1970, 1, 1, tzinfo=UTC),
        )
        if not isinstance(json.loads(value), list):
            raise ValueError("Account balance evidence must be a decoded JSON array")
        return value

    @model_validator(mode="after")
    def validate_authority(self) -> Self:
        """Bind public selection identity and preserve response/knowledge clock order.

        Returns:
            The same immutable bounded authority observation.
        """
        if not (
            self.balance_observed_at
            <= self.account_observation_timestamp
            <= self.observed_at
            < self.valid_until
        ):
            raise ValueError("Account authority observation clocks must be ordered")
        if (
            self.credential_timestamp is not None
            and self.credential_timestamp > self.balance_observed_at
        ):
            raise ValueError("Account evidence cannot precede the selected credential revision")
        expected = route_selection_digest(
            self.scope,
            self.credential_public_id,
            self.credential_timestamp,
            self.public_api_origin,
        )
        if self.route_revision != expected:
            raise ValueError("Route revision does not bind the selected public client route")
        return self


def canonical_route_authority_payload(payload: ExecutionRouteAuthorityPayload) -> str:
    """Serialize immutable account-route evidence without insertion-local row identities.

    Args:
        payload: Validated route and bounded authenticated or simulated account evidence.

    Returns:
        Canonical JSON preserving the receipt's original decoded material verbatim.
    """
    proven = ExecutionRouteAuthorityPayload.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def route_authority_content_digest(payload: ExecutionRouteAuthorityPayload) -> str:
    """Hash the exact route, credential revision and bounded account observation.

    Args:
        payload: Strict immutable authority evidence.

    Returns:
        A versioned SHA-256 distinct from the stable logical account scope digest.
    """
    return hashlib.sha256(
        ROUTE_AUTHORITY_HASH_DOMAIN + canonical_route_authority_payload(payload).encode()
    ).hexdigest()
