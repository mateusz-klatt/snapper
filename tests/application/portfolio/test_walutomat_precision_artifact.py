"""Tests for the reviewed Walutomat documentary precision artifact."""

import json
from dataclasses import replace

from snapper.application.portfolio.walutomat_precision_artifact import WALUTOMAT_PRECISION_ARTIFACT
from snapper.application.portfolio.walutomat_precision_artifact import (
    canonical_walutomat_precision_artifact_content,
)
from snapper.application.portfolio.walutomat_precision_artifact import (
    walutomat_precision_artifact_version,
)


def test_artifact_canonical_content_includes_constants_provenance_and_dates() -> None:
    """Every reviewed input participates in the content-addressed artifact.

    Given: The checked-in Walutomat precision review artifact.
    When: Its canonical content is serialized.
    Then: Values, official URLs, revision, and review dates are all present.
    """
    content = canonical_walutomat_precision_artifact_content(WALUTOMAT_PRECISION_ARTIFACT)
    payload = json.loads(content)

    assert payload == {
        "api_reference_url": "https://api.walutomat.pl/v2.0.0/",
        "cost_decimals": 2,
        "currency_amount_minimum_decimals": 2,
        "effective_at": "2026-07-16",
        "fee_decimals": 2,
        "fee_policy_url": "https://www.walutomat.pl/prowizje-i-rabaty/",
        "fee_rounding_quantum": "0.01",
        "limit_price_max_decimals": 4,
        "market_order_volume_decimals": 2,
        "reviewed_at": "2026-07-16T00:00:00+00:00",
        "revision": "s4c-1-v1",
        "rules_url": (
            "https://www.walutomat.pl/wp-content/uploads/2024/06/Regulamin-Walutomat.pdf"
        ),
    }


def test_artifact_version_is_stable_and_changes_with_content() -> None:
    """The persisted version binds to content rather than digest shape.

    Given: The current artifact and a same-revision copy with one changed constant.
    When: Both versions are derived.
    Then: The current version is stable and the changed content has another digest.
    """
    current = walutomat_precision_artifact_version(WALUTOMAT_PRECISION_ARTIFACT)
    changed = walutomat_precision_artifact_version(
        replace(WALUTOMAT_PRECISION_ARTIFACT, fee_rounding_quantum="0.001")
    )

    assert current == walutomat_precision_artifact_version(WALUTOMAT_PRECISION_ARTIFACT)
    assert current.startswith("s4c-1-v1:")
    assert len(current.removeprefix("s4c-1-v1:")) == 64
    assert changed != current
