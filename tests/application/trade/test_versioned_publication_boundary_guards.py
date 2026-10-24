"""Refuse contradictory real notification inputs before transport or account-cache publication."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from uuid import uuid7

import pytest

from snapper.application.portfolio.versioned_contract_account_projection import (
    VersionedContractAccountProjection,
)
from snapper.application.trade.financial_publication_transport import FinancialPublicationTransport
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.data.contract_account_projection_store import ContractAccountProjectionError
from snapper.messaging.schemas.financial_publication import FinancialPublicationData
from tests.application.trade.test_financial_publication_lifecycle import _publication_bus
from tests.helpers.contract_account_projection import (
    projection_scenario as shared_projection_scenario,
)
from tests.helpers.contract_account_projection import record_projection_position
from tests.helpers.contract_position_accounting import ContractPositionScenario

pytestmark = pytest.mark.asyncio
projection_scenario = shared_projection_scenario


async def test_transport_rejects_another_actual_destination_without_sending_original_intent(
    projection_scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep an actual committed notification bound to its original destination before allocation/send.

    Given: A genuine original position publication and the real borrowed local publisher.
    When: Transport is asked to send that payload under another valid instrument topic.
    Then: It refuses before any real publisher send and retains the original source identity.

    Args:
        projection_scenario: Actual source-bound isolated financial journal and publication.
        monkeypatch: Lifetime owner of the real local XPUB/SUB fixture.
    """
    intent = await record_projection_position(projection_scenario)
    wrong_topic = intent.topic.rsplit(".", 1)[0] + "." + str(uuid7())
    async with _publication_bus(monkeypatch) as bus:
        with pytest.raises(ValueError, match="transport topic contradicts"):
            await FinancialPublicationTransport(bus.publisher)(
                wrong_topic, intent.public_id, intent.payload
            )
        assert bus.sends == []


async def test_projection_refuses_a_separately_received_topic_that_differs_from_the_real_stamp(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Require the independent transport frame instead of trusting a correctly stamped JSON copy.

    Given: A real source-bound intent and a genuine held account snapshot.
    When: The same valid stamped envelope arrives under another actual frame topic.
    Then: Receive refuses and leaves the last actual SQL snapshot unchanged.

    Args:
        projection_scenario: Actual isolated source-backed account and committed position.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    projection = VersionedContractAccountProjection(repository, intent.payload.scope)
    before = await projection.refresh()
    message = FinancialPublicationData(
        public_id=intent.public_id,
        wallet_public_id=intent.payload.scope.wallet_public_id,
        session_id=str(uuid7()),
        sequence_id=1,
        timestamp=datetime.now(UTC),
        topic=intent.topic,
        payload=intent.payload,
    )
    wrong_topic = intent.topic.rsplit(".", 1)[0] + "." + str(uuid7())
    with pytest.raises(ContractAccountProjectionError, match="transport_topic_mismatch"):
        await projection.receive_message(wrong_topic, message)
    assert projection.snapshot == before


@pytest.mark.parametrize("mismatch", ["topic", "payload", "scope"])
async def test_real_publication_rebind_refuses_another_input_without_replacing_the_cache(
    projection_scenario: ContractPositionScenario,
    mismatch: Literal["topic", "payload", "scope"],
) -> None:
    """Rebind the actual durable graph under its wallet fence before certifying a received reference.

    Given: A genuine committed publication and an actual SQL-hydrated account view.
    When: A caller changes the received topic, valid payload clock or requested original scope.
    Then: The complete real source reader refuses the contradiction and preserves the old view.

    Args:
        projection_scenario: Actual source-backed financial journal and original intent.
        mismatch: Independent notification or account-context assertion that contradicts stored source.
    """
    intent = await record_projection_position(projection_scenario)
    scope = intent.payload.scope
    if mismatch == "scope":
        scope = ExecutionAccountScope.model_validate(
            {**scope.model_dump(mode="python"), "margin_account": "another-original-book"}
        )
    projection = VersionedContractAccountProjection(
        projection_scenario.fill.admission.repository, scope
    )
    before = await projection.refresh()
    topic, payload = intent.topic, intent.payload
    if mismatch == "topic":
        topic = topic.rsplit(".", 1)[0] + "." + str(uuid7())
    if mismatch == "payload":
        payload = payload.model_copy(
            update={"recorded_at": payload.recorded_at + timedelta(microseconds=1)}
        )
    with pytest.raises(ContractAccountProjectionError, match="actual_publication_mismatch"):
        await projection(topic, intent.public_id, payload)
    assert projection.snapshot == before
