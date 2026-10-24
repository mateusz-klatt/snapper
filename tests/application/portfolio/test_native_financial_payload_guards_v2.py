"""Reject copied native financial models that contradict actual retained source evidence."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import BaseModel
from pydantic import ValidationError

from snapper.application.portfolio.accounting_native_source_v2 import (
    AccountingNativeSourceClaimPayloadV2,
)
from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashAnchorV2
from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import NativeFinancialSourceEvidence
from snapper.data.accounting_native_source_store_v2 import get_bound_native_source_claim_v2
from snapper.data.native_cash_history_v2 import get_bound_native_cash_event_v2
from snapper.data.native_cash_store_v2 import NativeCashAccountingRequestV2
from snapper.data.native_cash_store_v2 import append_native_cash_from_receipt_v2
from snapper.data.native_position_fill_binding_v2 import OriginalPositionFillRequestV2
from snapper.data.native_position_history_v2 import get_bound_native_position_event_v2
from snapper.data.native_position_source_v2 import NativePositionSourceRequestV2
from snapper.data.native_position_store_v2 import append_native_position_from_receipts_v2
from tests.helpers.native_account_authority import native_authority_uuid
from tests.helpers.native_cash_accounting_v2 import native_cash_identity
from tests.helpers.native_cash_accounting_v2 import native_cash_scenario_v2
from tests.helpers.native_position_accounting_v2 import native_position_scenario_v2


def _refuse_copy(
    model: type[BaseModel], payload: BaseModel, changes: dict[str, object], reason: str
) -> None:
    """Revalidate a copied public model through its actual strict boundary and fixed refusal text."""
    with pytest.raises(ValidationError, match=reason):
        model.model_validate(payload.model_copy(update=changes))


async def test_native_claim_rejects_version_aliases_and_crossed_actual_receipts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep claim classification and exact source references bound to one actual native entry.

    Given: A real owned account-log entry has committed its cash event and shared source claim.
    When: A caller copies that claim with another version type, receipt or financial classification.
    Then: Strict revalidation refuses each copy while the original retained claim still rebinds.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        result = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(8000),
            scenario.source.policy,
        )
        async with scenario.source.repository.session() as session:
            claim = await get_bound_native_source_claim_v2(
                session, result.public_id, datetime.now(UTC)
            )
            for version in (True, 2.0, "2"):
                _refuse_copy(
                    AccountingNativeSourceClaimPayloadV2,
                    claim,
                    {"schema_version": version},
                    "Native source claim schema version must be an integer",
                )
            _refuse_copy(
                AccountingNativeSourceClaimPayloadV2,
                claim,
                {"raw_receipt_public_id": native_authority_uuid(8999)},
                "Native claim contradicts its source references or account scope",
            )
            _refuse_copy(
                AccountingNativeSourceClaimPayloadV2,
                claim,
                {"kind": "contract_position_fill"},
                "Native currency balance is not a position claim",
            )
            assert (
                await get_bound_native_source_claim_v2(session, result.public_id, datetime.now(UTC))
                == claim
            )


async def test_native_cash_models_reject_clock_and_authority_copy_forgery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Require posting knowledge, owner decision and cash references to agree independently.

    Given: Real current authority, policy, ownership, cash anchor and cash event are retained.
    When: Copied public models cross a source family, owner clock, receipt selector or authority UUID.
    Then: The precise source or wrapper validator refuses instead of accepting copied evidence.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        result = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(8000),
            scenario.source.policy,
        )
        async with scenario.source.repository.session() as session:
            bound = await get_bound_native_cash_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
        event, anchor, source = bound.payload, bound.anchor, bound.payload.source
        _refuse_copy(
            NativeFinancialSourceEvidence,
            source,
            {
                "authority_reference": source.authority_reference.model_copy(
                    update={"family": "legacy_route_v1"}
                )
            },
            "Native financial authority reference contradicts its payload",
        )
        _refuse_copy(
            NativeFinancialSourceEvidence,
            source,
            {
                "owner_evidence": source.owner_evidence.model_copy(
                    update={"decided_at": source.recorded_at + timedelta(microseconds=1)}
                )
            },
            "Native financial owner decision does not belong to this posting",
        )
        _refuse_copy(
            NativeFinancialSourceEvidence,
            source,
            {
                "ownership_reference": source.ownership_reference.model_copy(
                    update={"recorded_at": source.recorded_at + timedelta(microseconds=1)}
                )
            },
            "Native financial posting precedes retained source knowledge",
        )
        for version in (True, 2.0, "2"):
            _refuse_copy(
                FuturesNativeCashEventV2,
                event,
                {"schema_version": version},
                "Native financial version must be an integer",
            )
        _refuse_copy(
            FuturesNativeCashEventV2,
            event,
            {
                "source": source.model_copy(
                    update={
                        "ownership_reference": source.ownership_reference.model_copy(
                            update={"target_json_pointer": "/logs/99"}
                        )
                    }
                )
            },
            "Native financial operands contradict the selected account-log source",
        )
        _refuse_copy(
            FuturesNativeCashAnchorV2,
            anchor,
            {
                "operands": anchor.operands.model_copy(
                    update={"route_authority_public_id": native_authority_uuid(8999)}
                )
            },
            "Native cash anchor authority copies disagree",
        )
        _refuse_copy(
            FuturesNativeCashEventV2,
            event,
            {
                "operands": event.operands.model_copy(
                    update={"route_authority_public_id": native_authority_uuid(8999)}
                )
            },
            "Native cash event authority copies disagree",
        )
        _refuse_copy(
            NativeCashAccountingRequestV2,
            scenario.request,
            {
                "authority_reference": scenario.authority_reference.model_copy(
                    update={"family": "legacy_route_v1"}
                )
            },
            "Native financial cash requires an explicit native authority reference",
        )


async def test_native_position_models_keep_original_fill_separate_from_later_native_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject crossed original-fill and later-log coordinates without reconstructing source operands.

    Given: A genuine original fill, actual execution unit and later native position log are booked.
    When: Copies replace the fill authority, raw receipt or explicit native log family.
    Then: Each original reference boundary refuses the copied model before any financial write.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        result = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(8000),
            scenario.source.policy,
        )
        async with scenario.source.repository.session() as session:
            bound = await get_bound_native_position_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
        event = bound.payload
        _refuse_copy(
            ContractPositionEventV2,
            event,
            {
                "fill_authority_reference": event.fill_authority_reference.model_copy(
                    update={"public_id": native_authority_uuid(8999)}
                )
            },
            "Native position original fill and later log references disagree",
        )
        _refuse_copy(
            ContractPositionEventV2,
            event,
            {
                "fill_ownership_reference": event.fill_ownership_reference.model_copy(
                    update={"raw_receipt_public_id": native_authority_uuid(8999)}
                )
            },
            "Native position execution clock, raw source or decimal policy disagrees",
        )
        _refuse_copy(
            OriginalPositionFillRequestV2,
            scenario.request.fill,
            {"raw_receipt_public_id": native_authority_uuid(8999)},
            "Original position fill ownership names another raw receipt",
        )
        _refuse_copy(
            NativePositionSourceRequestV2,
            scenario.request,
            {
                "position_authority_reference": scenario.request.position_authority_reference.model_copy(
                    update={"family": "legacy_route_v1"}
                )
            },
            "Native position posting requires an explicit native account-log authority",
        )
