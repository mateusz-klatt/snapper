"""Validation contracts for submitted AI-research market views."""

from typing import Annotated
from typing import Literal

from pydantic import AwareDatetime
from pydantic import Field
from pydantic import field_validator

from snapper.api.schemas.base import StrictBody

MarketViewRegime = Literal["risk_on", "neutral", "risk_off", "event_window"]
MarketViewBias = Literal["longs_ok", "neutral", "avoid_new_longs", "avoid_all"]
MarketViewKeyRisk = Annotated[str, Field(max_length=200)]


class MarketViewNextEvent(StrictBody):
    """One authored event that may invalidate or alter the market view."""

    when_utc: AwareDatetime
    name: str
    severity: str


class MarketViewSource(StrictBody):
    """One external source supporting an authored market view."""

    url: str
    title: str
    retrieved_at: AwareDatetime


class SubmittedMarketView(StrictBody):
    """Strict author-owned payload for one market-view submission.

    Server-owned identity, trigger, lifecycle status, and ``submitted_at``
    are deliberately absent. ``StrictBody`` rejects attempts to forge those
    fields, while the persistence layer binds them from the pending research
    round and the server clock.
    """

    as_of: AwareDatetime
    valid_until: AwareDatetime
    regime: MarketViewRegime
    bias: MarketViewBias
    confidence: float = Field(ge=0.0, le=1.0)
    horizon_hours: int
    key_risks: list[MarketViewKeyRisk] = Field(max_length=3)
    next_events: list[MarketViewNextEvent] = Field(max_length=5)
    sources: list[MarketViewSource] = Field(min_length=1)
    rationale: str

    @field_validator("rationale")
    @classmethod
    def validate_rationale_size(cls, value: str) -> str:
        """Limit rationale to 2 KiB of UTF-8 text.

        Args:
            value: Author-supplied rationale text.

        Returns:
            The unchanged rationale when it fits the byte limit.

        Raises:
            ValueError: When the UTF-8 representation exceeds 2 KiB.
        """
        if len(value.encode("utf-8")) > 2048:
            raise ValueError("rationale must not exceed 2048 UTF-8 bytes")
        return value
