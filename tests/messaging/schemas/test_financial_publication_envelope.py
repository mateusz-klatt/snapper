"""Enforce wallet, clock and destination copies at the real financial message boundaries."""

from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.core.versioned_financial_publication_payload import (
    versioned_financial_publication_topic,
)
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.financial_publication import FinancialPublicationData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message
from tests.helpers.unit_admission import admission_id
from tests.messaging.infrastructure.test_financial_publication_transport import _structural_envelope


@pytest.mark.parametrize("contradiction", ("wallet", "clock", "topic"))
def test_real_financial_envelope_rejects_contradictory_copies(contradiction: str) -> None:
    """Revalidate an unsafe typed copy before it can enter the actual registered parser.

    Given: An explicitly structural valid financial message with its original reference payload.
    When: Its copied wallet, transport timestamp or stamped destination contradicts that payload.
    Then: Both direct message validation and the real wire parser refuse the contradiction.
    """
    original = _structural_envelope(SequenceTracker())
    topic = versioned_financial_publication_topic(original.payload)
    other_topic = topic.replace(original.wallet_public_id, admission_id(99001))
    if contradiction == "wallet":
        candidate = original.model_copy(update={"wallet_public_id": admission_id(99001)})
        reason = "wallet contradicts its original payload"
    elif contradiction == "clock":
        candidate = original.model_copy(
            update={"timestamp": original.payload.recorded_at - timedelta(microseconds=1)}
        )
        reason = "transport predates its original recording"
    else:
        candidate = original.model_copy(update={"topic": other_topic})
        reason = "topic contradicts its original payload"
    with pytest.raises(ValidationError, match=reason):
        FinancialPublicationData.model_validate(candidate)
    with pytest.raises(MessageParseError, match=reason):
        parse_message(candidate.model_dump_json(), topic=topic)


def test_financial_publish_boundary_refuses_another_valid_destination() -> None:
    """Reject a well-formed foreign scope before the actual transport stamping helper runs.

    Given: A valid reference message and a different syntactically valid account destination.
    When: Its public publish method receives that foreign destination.
    Then: It refuses while its original destination still roundtrips through the registered parser.
    """
    original = _structural_envelope(SequenceTracker())
    topic = versioned_financial_publication_topic(original.payload)
    other_topic = topic.replace(original.wallet_public_id, admission_id(99002))
    with pytest.raises(ValueError, match="cannot publish to another topic"):
        original.publish_to(other_topic)
    wire = original.publish_to(topic)
    parsed = parse_message(wire.decode(), topic=topic)
    assert isinstance(parsed, FinancialPublicationData)
    assert parsed.public_id == original.public_id and parsed.payload == original.payload
    assert parsed.topic == topic and original.topic is None
