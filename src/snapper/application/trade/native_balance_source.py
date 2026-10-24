"""Rebuild native balance candidates from immutable retained source receipts.

This reader proves a deterministic retained chain, not a margin-account alias,
spendable balance, complete venue history or local execution-prefix alignment.
It joins a caller-owned transaction and grants no current order admission.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import client_selection_digest
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.infrastructure.exchanges.contracts import ExecutionObservationContext
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.infrastructure.exchanges.kraken_futures_balance_stream import (
    CapturedNativeBalanceFrame,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_stream import (
    KrakenFuturesBalanceStream,
)
from snapper.infrastructure.exchanges.native_balance_source import NativeBalanceProfileObservation
from snapper.infrastructure.exchanges.native_balance_source import NativeBalanceStreamProof


class NativeBalanceBindingError(ValueError):
    """The immutable retained chain does not support the supplied native source candidate."""


def _verify_selection(raw: RawAccountingObservation, proof: NativeBalanceStreamProof) -> None:
    """Bind source routing without treating any raw margin label as a profile mapping."""
    selection = proof.client_selection
    actual = (
        raw.scope.wallet_public_id,
        raw.scope.exchange,
        raw.scope.mode,
        raw.scope.environment,
        raw.credential_public_id,
        raw.credential_timestamp,
        raw.route_revision,
        raw.kind,
    )
    expected = (
        selection.wallet_public_id,
        selection.exchange,
        selection.mode,
        selection.environment,
        selection.credential_public_id,
        selection.credential_timestamp,
        client_selection_digest(selection),
        "account_cash",
    )
    if actual != expected:
        raise NativeBalanceBindingError("native_balance_raw_client_selection_mismatch")


async def get_bound_native_balance_profiles(
    session: AsyncSession, proof: NativeBalanceStreamProof
) -> tuple[NativeBalanceProfileObservation, ...]:
    """Validate every retained raw envelope and reconstruct exact independent profile quantities.

    Args:
        session: Caller-owned transaction used for immutable source lookup.
        proof: Candidate physical source epoch, retained chain and original tip metadata.

    Returns:
        Exact replayed native profiles, with no margin mapping or current financial permission.

    Raises:
        ValueError: If source evidence is absent, changed, conflicted or cannot prove this chain.
    """
    evidence = NativeBalanceStreamProof.model_validate(proof)
    stream = KrakenFuturesBalanceStream()
    for public_id in evidence.receipt_public_ids:
        raw = await get_bound_accounting_observation(session, public_id)
        _verify_selection(raw, evidence)
        source = PrivateExecutionObservation(
            context=ExecutionObservationContext(
                kind="account_cash",
                channel=raw.channel,
                observed_at=raw.observed_at,
                target_json_pointer=raw.target_json_pointer,
                source_identity=raw.source_identity,
            ),
            representation=raw.representation,
            material=raw.material,
        )
        stream.observe(
            CapturedNativeBalanceFrame(
                observation=source,
                receipt_public_id=public_id,
                client_selection=evidence.client_selection,
                connection_epoch_public_id=evidence.connection_epoch_public_id,
            )
        )
    profiles = stream.profiles()
    if profiles[0].source_proof != evidence:
        raise NativeBalanceBindingError("native_balance_raw_tip_projection_mismatch")
    return profiles
