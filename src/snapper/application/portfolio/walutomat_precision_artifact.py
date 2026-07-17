"""Reviewed Walutomat documentary precision artifact.

The artifact records the exact documentary inputs reviewed for S4c-1. Its
canonical content, including provenance URLs and dates, is hashed into every
persisted documentary specification version. A changed artifact therefore
invalidates specifications produced from an earlier revision without
pretending that a periodic symbol fetch re-observed the documentary rules.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime


@dataclass(frozen=True)
class WalutomatPrecisionArtifact:
    """One immutable review of Walutomat order and fee precision rules."""

    revision: str
    reviewed_at: datetime
    effective_at: date
    market_order_volume_decimals: int
    cost_decimals: int
    limit_price_max_decimals: int
    currency_amount_minimum_decimals: int | None
    fee_rounding_quantum: str
    fee_decimals: int
    api_reference_url: str
    rules_url: str
    fee_policy_url: str


WALUTOMAT_PRECISION_ARTIFACT = WalutomatPrecisionArtifact(
    revision="s4c-1-v1",
    reviewed_at=datetime(2026, 7, 16, tzinfo=UTC),
    effective_at=date(2026, 7, 16),
    market_order_volume_decimals=2,
    cost_decimals=2,
    limit_price_max_decimals=4,
    currency_amount_minimum_decimals=2,
    fee_rounding_quantum="0.01",
    fee_decimals=2,
    api_reference_url="https://api.walutomat.pl/v2.0.0/",
    rules_url="https://www.walutomat.pl/wp-content/uploads/2024/06/Regulamin-Walutomat.pdf",
    fee_policy_url="https://www.walutomat.pl/prowizje-i-rabaty/",
)
WALUTOMAT_DOCUMENTARY_SPEC_SOURCE = "walutomat:reviewed-documentary-precision-artifact"
WALUTOMAT_DOCUMENTARY_FEE_SOURCE = "walutomat:reviewed-documentary-fee-artifact"


def canonical_walutomat_precision_artifact_content(
    artifact: WalutomatPrecisionArtifact,
) -> str:
    """Serialize every reviewed artifact field into canonical JSON.

    Args:
        artifact: Reviewed documentary constants and their provenance.

    Returns:
        Stable JSON suitable for a content-addressed version digest.
    """
    payload: dict[str, object] = {
        "api_reference_url": artifact.api_reference_url,
        "cost_decimals": artifact.cost_decimals,
        "currency_amount_minimum_decimals": artifact.currency_amount_minimum_decimals,
        "effective_at": artifact.effective_at.isoformat(),
        "fee_decimals": artifact.fee_decimals,
        "fee_policy_url": artifact.fee_policy_url,
        "fee_rounding_quantum": artifact.fee_rounding_quantum,
        "limit_price_max_decimals": artifact.limit_price_max_decimals,
        "market_order_volume_decimals": artifact.market_order_volume_decimals,
        "reviewed_at": artifact.reviewed_at.isoformat(),
        "revision": artifact.revision,
        "rules_url": artifact.rules_url,
    }
    return json.dumps(payload, allow_nan=False, separators=(",", ":"), sort_keys=True)


def walutomat_precision_artifact_version(
    artifact: WalutomatPrecisionArtifact,
) -> str:
    """Return the revision-prefixed digest of the complete artifact content.

    Args:
        artifact: Reviewed documentary constants and their provenance.

    Returns:
        Content-bound version suitable for persisted evidence.
    """
    content = canonical_walutomat_precision_artifact_content(artifact)
    digest = hashlib.sha256(content.encode()).hexdigest()
    return f"{artifact.revision}:{digest}"
