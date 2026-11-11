"""Tests for the reviewed Walutomat order-ownership guarantee artifact."""

import hashlib
import json
from dataclasses import fields
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import patch

import pytest

import snapper.application.portfolio.walutomat_order_ownership_artifact as artifact
from snapper.application.portfolio.walutomat_order_ownership_artifact import (
    WalutomatOrderOwnershipGuarantee,
)

_GUARANTEE = WalutomatOrderOwnershipGuarantee(
    revision="ownership-review-1",
    reviewed_at=datetime(2026, 11, 20, tzinfo=UTC),
    order_id_non_reuse_source="support-ticket-1",
    order_id_in_history_source="https://api.walutomat.pl/v2.0.0/#account-history",
)


def test_the_guarantee_ships_unrecorded() -> None:
    """No guarantee is recorded until the venue's answer has been reviewed.

    Given: The artifact module as shipped.
    When: The recorded guarantee and its version are read.
    Then: Both are absent, so every Walutomat seal refuses.
    """
    assert artifact.WALUTOMAT_ORDER_OWNERSHIP_GUARANTEE is None
    assert artifact.recorded_walutomat_order_ownership_guarantee_version() is None


def test_the_version_is_the_revision_prefixed_digest_of_the_canonical_content() -> None:
    """The version content-addresses every reviewed field and both premises.

    Given: One reviewed guarantee.
    When: Its canonical content and version are derived.
    Then: The content names both premises and every field, and the version is
        the revision followed by the content's SHA-256.
    """
    content = artifact.canonical_walutomat_order_ownership_guarantee_content(_GUARANTEE)
    assert json.loads(content) == {
        "order_id_in_history_premise": artifact.WALUTOMAT_ORDER_ID_IN_HISTORY_PREMISE,
        "order_id_in_history_source": _GUARANTEE.order_id_in_history_source,
        "order_id_non_reuse_premise": artifact.WALUTOMAT_ORDER_ID_NON_REUSE_PREMISE,
        "order_id_non_reuse_source": _GUARANTEE.order_id_non_reuse_source,
        "reviewed_at": _GUARANTEE.reviewed_at.isoformat(),
        "revision": _GUARANTEE.revision,
    }
    expected = hashlib.sha256(content.encode()).hexdigest()
    assert artifact.walutomat_order_ownership_guarantee_version(_GUARANTEE) == (
        f"ownership-review-1:{expected}"
    )


@pytest.mark.parametrize("name", [field.name for field in fields(WalutomatOrderOwnershipGuarantee)])
def test_every_field_changes_the_version(name: str) -> None:
    """A change to any reviewed field yields a different version.

    Given: The reviewed guarantee and a copy differing in exactly one field.
    When: Both versions are derived.
    Then: They differ.

    Args:
        name: The field changed in the copy.
    """
    value = getattr(_GUARANTEE, name)
    changed = value + timedelta(days=1) if isinstance(value, datetime) else f"{value}-changed"
    other = replace(_GUARANTEE, **{name: changed})
    assert artifact.walutomat_order_ownership_guarantee_version(
        other
    ) != artifact.walutomat_order_ownership_guarantee_version(_GUARANTEE)


def test_a_recorded_guarantee_reports_its_version() -> None:
    """Once recorded, the guarantee's version is what the bootstrap receives.

    Given: The module guarantee replaced by a reviewed one.
    When: The recorded version is read.
    Then: It equals that guarantee's version.
    """
    with patch.object(artifact, "WALUTOMAT_ORDER_OWNERSHIP_GUARANTEE", _GUARANTEE):
        recorded = artifact.recorded_walutomat_order_ownership_guarantee_version()
    assert recorded == artifact.walutomat_order_ownership_guarantee_version(_GUARANTEE)
