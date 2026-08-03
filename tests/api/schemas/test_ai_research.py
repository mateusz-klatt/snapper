"""Tests for strict submitted market-view validation."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.api.schemas.ai_research import SubmittedMarketView


def _event(when_utc: datetime) -> dict[str, object]:
    """Build one valid next-event payload."""
    return {
        "when_utc": when_utc,
        "name": "CPI release",
        "severity": "high",
    }


def _source(retrieved_at: datetime) -> dict[str, object]:
    """Build one valid source payload."""
    return {
        "url": "https://example.com/research",
        "title": "Market briefing",
        "retrieved_at": retrieved_at,
    }


def _payload(**overrides: object) -> dict[str, object]:
    """Build a complete valid submitted market view."""
    as_of = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
    payload: dict[str, object] = {
        "as_of": as_of,
        "valid_until": as_of + timedelta(hours=2),
        "regime": "neutral",
        "bias": "longs_ok",
        "confidence": 0.75,
        "horizon_hours": 2,
        "key_risks": ["Unexpected inflation surprise"],
        "next_events": [_event(as_of + timedelta(hours=1))],
        "sources": [_source(as_of - timedelta(minutes=5))],
        "rationale": "Balanced conditions with event risk ahead.",
    }
    payload.update(overrides)
    return payload


def test_submitted_market_view_accepts_exact_caps_and_confidence_endpoints() -> None:
    """Every inclusive artifact cap accepts its exact boundary.

    Given a complete market view at every maximum allowed size,
    When strict validation runs at both confidence endpoints,
    Then the artifact is accepted without truncation.
    """
    as_of = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
    for confidence in (0.0, 1.0):
        view = SubmittedMarketView.model_validate(
            _payload(
                confidence=confidence,
                key_risks=["r" * 200] * 3,
                next_events=[_event(as_of + timedelta(hours=index + 1)) for index in range(5)],
                rationale="ą" * 1024,
            )
        )
        assert view.confidence == confidence
        assert len(view.key_risks) == 3
        assert len(view.next_events) == 5
        assert len(view.rationale.encode("utf-8")) == 2048


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"key_risks": ["risk"] * 4}, id="too_many_key_risks"),
        pytest.param({"key_risks": ["r" * 201]}, id="key_risk_too_long"),
        pytest.param(
            {
                "next_events": [
                    _event(datetime(2026, 7, 21, index, tzinfo=UTC)) for index in range(6)
                ]
            },
            id="too_many_next_events",
        ),
        pytest.param({"confidence": -0.01}, id="confidence_below_zero"),
        pytest.param({"confidence": 1.01}, id="confidence_above_one"),
        pytest.param({"sources": []}, id="empty_sources"),
        pytest.param({"rationale": "ą" * 1024 + "x"}, id="rationale_over_two_kib"),
    ],
)
def test_submitted_market_view_rejects_artifact_cap_violations(
    overrides: dict[str, object],
) -> None:
    """Every bounded artifact field rejects values beyond its cap.

    Given an otherwise valid artifact with one over-limit field,
    When strict validation runs,
    Then validation rejects the complete submission.
    """
    over_cap_payload = _payload(**overrides)
    with pytest.raises(ValidationError):
        SubmittedMarketView.model_validate(over_cap_payload)


def test_submitted_market_view_requires_sources() -> None:
    """Omitting the mandatory source list is rejected.

    Given a market view with no sources field,
    When strict validation runs,
    Then validation reports the required field as missing.
    """
    payload = _payload()
    del payload["sources"]
    with pytest.raises(ValidationError):
        SubmittedMarketView.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("regime", "bullish", id="regime"),
        pytest.param("bias", "buy_everything", id="bias"),
    ],
)
def test_submitted_market_view_rejects_unknown_enums(field: str, value: str) -> None:
    """Closed regime and bias vocabularies reject unknown strings.

    Given an artifact with an unknown decision-bearing enum value,
    When strict validation runs,
    Then validation rejects the value outside the closed vocabulary.
    """
    unknown_enum_payload = _payload(**{field: value})
    with pytest.raises(ValidationError):
        SubmittedMarketView.model_validate(unknown_enum_payload)


@pytest.mark.parametrize("field", ["as_of", "valid_until"])
def test_submitted_market_view_rejects_naive_top_level_datetimes(field: str) -> None:
    """Replay clocks must identify real timezone-aware instants.

    Given an artifact with a naive top-level replay clock,
    When strict validation runs,
    Then validation rejects the timestamp without a timezone.
    """
    naive_clock_payload = _payload(**{field: datetime(2026, 7, 21, 8, 0)})
    with pytest.raises(ValidationError):
        SubmittedMarketView.model_validate(naive_clock_payload)


def test_submitted_market_view_rejects_naive_nested_datetimes() -> None:
    """Event and source timestamps must also identify aware instants.

    Given nested event and source items with naive timestamps,
    When each artifact is strictly validated,
    Then both nested timestamp variants are rejected.
    """
    naive_event_payload = _payload(next_events=[_event(datetime(2026, 7, 21, 9, 0))])
    with pytest.raises(ValidationError):
        SubmittedMarketView.model_validate(naive_event_payload)
    naive_source_payload = _payload(sources=[_source(datetime(2026, 7, 21, 7, 55))])
    with pytest.raises(ValidationError):
        SubmittedMarketView.model_validate(naive_source_payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("public_id", "forged-view", id="public_id"),
        pytest.param("trigger", "forged-trigger", id="trigger"),
        pytest.param("status", "completed", id="status"),
        pytest.param(
            "submitted_at",
            datetime(2026, 7, 21, 8, 5, tzinfo=UTC),
            id="submitted_at",
        ),
    ],
)
def test_submitted_market_view_rejects_server_owned_fields(
    field: str,
    value: object,
) -> None:
    """Strict parsing prevents authors from forging server-owned metadata.

    Given a submission containing one server-owned artifact field,
    When strict extra-field validation runs,
    Then the forged metadata is rejected.
    """
    server_owned_payload = _payload(**{field: value})
    with pytest.raises(ValidationError):
        SubmittedMarketView.model_validate(server_owned_payload)
