"""Safe private-source payload without authentication or financial permission.

The closed authenticated receive producer must independently bind actual raw,
key-proof/root and optional direct-original rows. A valid public payload proves
only static consistency; it cannot mint an authenticated receive ticket or turn
later identity evidence into ownership of an older frame.
"""

import hashlib
import json
import re
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.native_account_read import native_account_read_family_digest
from snapper.core.private_frame_ticket import PrivateSourceFrameContext

PRIVATE_SOURCE_OWNERSHIP_HASH_DOMAIN = b"snapper:private-source-ownership:v1\x00"


class PrivateSourceOwnershipReference(BaseModel):
    """Exact retained sidecar identity and source copies requiring the actual bound reader."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    public_id: EvidenceUUID
    content_digest: EvidenceDigest
    raw_receipt_public_id: EvidenceUUID
    raw_content_digest: EvidenceDigest
    target_json_pointer: str | None
    recorded_at: EvidenceInstant

    @field_validator("target_json_pointer")
    @classmethod
    def validate_canonical_pointer(cls, value: str | None) -> str | None:
        """Keep source references canonical without guessing their actual feed or child membership.

        Args:
            value: Exact copied raw selector, with None denoting an entire original frame.

        Returns:
            The unchanged canonical selector for later equality against the actual bound payload.
        """
        if (
            value is not None
            and re.fullmatch(r"(/new_entry|/(fills|logs)/(0|[1-9][0-9]*))", value) is None
        ):
            raise ValueError("Private ownership reference selector is not canonical")
        return value


class PrivateSourceOwnershipPayload(BaseModel):
    """One safe retained raw/source relation requiring actual closed producer and bound readers."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    schema_version: Literal[1] = 1
    family: Literal["kraken_futures_private_ws"] = "kraken_futures_private_ws"
    source_policy: Literal["kraken_futures_closed_private_transport_v1"] = (
        "kraken_futures_closed_private_transport_v1"
    )
    binding_kind: Literal["actual_receive", "recovery_projection"]
    frame_context: PrivateSourceFrameContext
    raw_receipt_public_id: EvidenceUUID
    raw_content_digest: EvidenceDigest
    target_json_pointer: str | None
    frame_basis_digest: EvidenceDigest
    raw_observed_at: EvidenceInstant
    raw_recorded_at: EvidenceInstant
    recorded_at: EvidenceInstant
    origin_sidecar_public_id: EvidenceUUID | None = None
    origin_sidecar_content_digest: EvidenceDigest | None = None

    @field_validator("schema_version", mode="before")
    @classmethod
    def strict_version(cls, value: object) -> object:
        """Refuse bool and float aliases before literal-version validation.

        Args:
            value: Original version value supplied at the public shape boundary.

        Returns:
            The same actual integer for the supported-version check.
        """
        if type(value) is not int:
            raise ValueError("Private ownership version must be an integer")
        return value

    @model_validator(mode="after")
    def validate_retained_source(self) -> Self:
        """Require exact original clocks, selector shape and a direct-origin tuple.

        Returns:
            The unchanged coherent shape, without certifying actual referenced rows or authentication.
        """
        frame = self.frame_context
        if (
            not frame.frame_observed_at
            <= self.raw_observed_at
            <= self.raw_recorded_at
            <= self.recorded_at
        ):
            raise ValueError("Private ownership retained chronology is inconsistent")
        origin = (self.origin_sidecar_public_id, self.origin_sidecar_content_digest)
        if self.binding_kind == "actual_receive":
            if any(value is not None for value in origin):
                raise ValueError("Actual private receive cannot claim a recovery origin")
        elif any(value is None for value in origin):
            raise ValueError("Private recovery requires one explicit direct original")
        self.validate_selector()
        return self

    def validate_selector(self) -> None:
        """Require only canonical documented child syntax while leaving actual membership to the parser."""
        pointer = self.target_json_pointer
        if pointer is None:
            if self.binding_kind != "actual_receive":
                raise ValueError("Private recovery requires an explicit selected child")
            return
        feed = self.frame_context.raw_feed
        if feed == "account_log":
            valid = pointer == "/new_entry"
        else:
            prefix = "fills" if feed in {"fills", "fills_snapshot"} else "logs"
            valid = re.fullmatch(r"/" + prefix + r"/(0|[1-9][0-9]*)", pointer) is not None
        if not valid:
            raise ValueError("Private ownership selector is not canonical for its original feed")


def canonical_private_source_ownership(payload: PrivateSourceOwnershipPayload) -> str:
    """Serialize only safe public evidence under the new independent source family.

    Args:
        payload: Strict safe context and actual retained-source references requiring independent binding.

    Returns:
        Canonical safe JSON containing neither financial bytes nor authentication material/hashes.
    """
    proven = PrivateSourceOwnershipPayload.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def private_source_ownership_digest(payload: PrivateSourceOwnershipPayload) -> str:
    """Hash the safe sidecar without changing any existing raw/frame/unit hash domain.

    Args:
        payload: The actual relation's safe public projection, not an authentication capability.

    Returns:
        Lowercase SHA-256 over the new versioned canonical safe payload.
    """
    canonical = canonical_private_source_ownership(payload)
    return hashlib.sha256(PRIVATE_SOURCE_OWNERSHIP_HASH_DOMAIN + canonical.encode()).hexdigest()


def private_source_ownership_projection(
    payload: PrivateSourceOwnershipPayload,
) -> dict[str, object]:
    """Project 39 payload columns into the unchanged 44-column prospective physical schema.

    The actual writer adds public_id/session_id/sequence_id/known_to and the
    database generates id. It must require its own insertion timestamp equals
    payload.recorded_at, rebind every source row and reject a self-origin UUID.

    Args:
        payload: Coherent safe material whose referenced actual rows still require independent proof.

    Returns:
        Exact flat constructor/projection values, including safe canonical bytes and their digest.
    """
    proven = PrivateSourceOwnershipPayload.model_validate(payload)
    frame = proven.frame_context
    selected = frame.client_selection
    return {
        "timestamp": proven.recorded_at,
        "wallet_public_id": selected.wallet_public_id,
        "exchange": selected.exchange,
        "mode": selected.mode,
        "environment": selected.environment,
        "margin_account": None,
        "account_family_digest": native_account_read_family_digest(selected),
        "client_selection_digest": client_selection_digest(selected),
        "credential_public_id": selected.credential_public_id,
        "credential_timestamp": selected.credential_timestamp,
        "public_api_origin": selected.public_api_origin,
        "raw_receipt_public_id": proven.raw_receipt_public_id,
        "raw_content_digest": proven.raw_content_digest,
        "target_json_pointer": proven.target_json_pointer,
        "frame_basis_digest": proven.frame_basis_digest,
        "key_proof_public_id": frame.key_proof_public_id,
        "key_proof_content_digest": frame.key_proof_content_digest,
        "key_proof_recorded_at": frame.key_proof_recorded_at,
        "key_owner_root_public_id": frame.key_owner_root_public_id,
        "key_owner_root_content_digest": frame.key_owner_root_content_digest,
        "client_capture_public_id": frame.client_capture_public_id,
        "connection_epoch_public_id": frame.connection_epoch_public_id,
        "subscription_dispatch_public_id": frame.subscription_dispatch_public_id,
        "frame_receive_public_id": frame.frame_receive_public_id,
        "subscription_feed": frame.subscription_feed,
        "raw_feed": frame.raw_feed,
        "auth_encoded_at": frame.auth_encoded_at,
        "ack_observed_at": frame.ack_observed_at,
        "frame_observed_at": frame.frame_observed_at,
        "raw_observed_at": proven.raw_observed_at,
        "raw_recorded_at": proven.raw_recorded_at,
        "family": proven.family,
        "schema_version": proven.schema_version,
        "source_policy": proven.source_policy,
        "binding_kind": proven.binding_kind,
        "origin_sidecar_public_id": proven.origin_sidecar_public_id,
        "origin_sidecar_content_digest": proven.origin_sidecar_content_digest,
        "content_digest": private_source_ownership_digest(proven),
        "canonical_payload": canonical_private_source_ownership(proven),
    }
